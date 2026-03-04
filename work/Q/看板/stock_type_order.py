import pandas as pd
import ast
from pathlib import Path
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import threading
import queue
import pymongo

# ==========================================================
#                   工具函数
# ==========================================================
def safe_eval(x, default):
    if pd.isna(x) or x in ("", "nan"):
        return default
    try:
        return ast.literal_eval(x)
    except:
        return default

def stringify_keys(d):
    if isinstance(d, dict):
        return {str(k): stringify_keys(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [stringify_keys(i) for i in d]
    else:
        return d


# ==========================================================
#                 CSV 多进程 worker（分块处理）
# ==========================================================
def process_csv_worker(args):
    file_path, times_list, time_map, processed_files = args
    file_path = Path(file_path)
    cat_id = file_path.stem

    if cat_id in processed_files:
        return None

    result = {"cat_id": cat_id, "updates": {}}
    top10_brands_202510 = []

    try:
        chunksize = 100_000

        for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):

            # ===== monthly_sale_trend =====
            chunk["monthly_sale_trend"] = chunk.get(
                "monthly_sale_trend", [{}] * len(chunk)
            ).apply(lambda x: safe_eval(x, {}))

            # ===== offersInf =====
            chunk["offersInf"] = chunk.get("offersInf", "[]").fillna("[]").apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) else []
            )

            # ===== brand =====
            chunk["brand"] = chunk.get("brand", "未知品牌").fillna("未知品牌")

            # ===== sellerType =====
            chunk["sellerType"] = chunk.get("sellerType", "others").fillna("others")

            # ===== stock_type =====
            if "stock_type" not in chunk.columns:
                chunk["stock_type"] = "未知"

            # ===== active_price =====
            if "active_price" not in chunk.columns:
                chunk["active_price"] = 0

            # ===== 按月份统计 =====
            for t in times_list:
                try:
                    time, _, _ = time_map[t]

                    chunk[f"sale_{time}"] = chunk["monthly_sale_trend"].apply(
                        lambda x: x.get(str(time), 0)
                    )
                    chunk[f"gmv_{time}"] = chunk[f"sale_{time}"] * chunk["active_price"]

                    # ===== stock_type：销量 + 销售额 =====
                    stock_group = chunk.groupby("stock_type").agg(
                        sale_total=(f"sale_{time}", "sum"),
                        gmv_total=(f"gmv_{time}", "sum")
                    )

                    stock_sales = stock_group["sale_total"].to_dict()
                    stock_gmv = stock_group["gmv_total"].to_dict()

                    # ===== 累加 =====
                    if t not in result["updates"]:
                        result["updates"][t] = {
                            "stock_type_sale": stock_sales,
                            "stock_type_gmv": stock_gmv,
                        }
                    else:
                        for k, v in stock_sales.items():
                            result["updates"][t]["stock_type_sale"][k] = \
                                result["updates"][t]["stock_type_sale"].get(k, 0) + v

                        for k, v in stock_gmv.items():
                            result["updates"][t]["stock_type_gmv"][k] = \
                                result["updates"][t]["stock_type_gmv"].get(k, 0) + v

                except Exception:
                    continue

        # ===== 统一计算均价（GMV / 销量）=====
        for t, v in result["updates"].items():
            avg_price = {}
            for k in v["stock_type_sale"]:
                sale = v["stock_type_sale"].get(k, 0)
                gmv = v["stock_type_gmv"].get(k, 0)
                avg_price[k] = round(gmv / sale, 2) if sale else 0

            v["stock_type_avg_price"] = avg_price

    except Exception:
        return None

    return result


# ==========================================================
#                 Mongo 写入线程
# ==========================================================
def mongo_writer_thread(q, log_file):
    os.environ["NET"] = "TUNNEL"
    os.environ["NET3"] = "NXQ"
    from dm.connector.mongo.manager3 import get_collection
    visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

    buffer = []
    BATCH = 50

    while True:
        item = q.get()
        if item == "STOP":
            break

        buffer.append(item)

        if len(buffer) >= BATCH:
            flush_bulk(buffer, visual_plus, log_file)
            buffer = []

    if buffer:
        flush_bulk(buffer, visual_plus, log_file)


def flush_bulk(buffer, visual_plus, log_file):
    ops = []

    for task in buffer:
        cat_id = task["cat_id"]
        update_doc = {}

        for t, v in task["updates"].items():
            update_doc[f"stock_type.{t}"] = stringify_keys(v["stock_type_sale"])
            update_doc[f"stock_type_gmv.{t}"] = stringify_keys(v["stock_type_gmv"])
            update_doc[f"stock_type_avg_price.{t}"] = stringify_keys(v["stock_type_avg_price"])

        ops.append(
            pymongo.UpdateOne(
                {"cat_id": cat_id},
                {"$set": update_doc},
                upsert=True
            )
        )

        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"{cat_id}\n")

    if ops:
        try:
            visual_plus.bulk_write(ops, ordered=False)
        except Exception as e:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"❌ Bulk write error: {e}\n")


# ==========================================================
#                     主程序
# ==========================================================
def main():
    folder_path = Path(r"D:\data\看板\20251230_sku")
    log_file = folder_path / "processed_top10_log_260105.txt"
    folder_path.mkdir(parents=True, exist_ok=True)

    times_list = [
        202510, 202509, 202508, 202507, 202506, 202505,
        202504, 202503, 202502, 202501,
        202412, 202411, 202410, 202511
    ]

    time_map = {
        202510:(2510,2509,2410),202509:(2509,2508,2409),202508:(2508,2507,2408),
        202507:(2507,2506,2407),202506:(2506,2505,2406),202505:(2505,2504,2405),
        202504:(2504,2503,2404),202503:(2503,2502,2403),202502:(2502,2501,2402),
        202501:(2501,2412,2401),202412:(2412,2411,2312),202411:(2411,2410,2311),
        202410:(2410,2409,2310),202511:(2511,2510,2411)
    }

    processed_files = set()
    if log_file.exists():
        processed_files = {x.strip() for x in log_file.read_text().splitlines()}

    # csv_list = ["D:/data/看板/202512_sku_p_info_2/MLM4620.csv"]

    csv_dir = Path(r"D:\data\看板\_改")
    csv_list = list(csv_dir.glob("*.csv"))

    q = queue.Queue()
    writer = threading.Thread(target=mongo_writer_thread, args=(q, log_file))
    writer.start()

    args_list = [(str(f), times_list, time_map, processed_files) for f in csv_list]

    with ProcessPoolExecutor(max_workers=8) as exe:
        for res in tqdm(exe.map(process_csv_worker, args_list), total=len(args_list)):
            if res:
                q.put(res)

    q.put("STOP")
    writer.join()
    print("全部完成！")


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
