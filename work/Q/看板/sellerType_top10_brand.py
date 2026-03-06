import pandas as pd
import ast
from pathlib import Path
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
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
#                 CSV 多进程 worker
# ==========================================================
def process_csv_worker(args):
    file_path, times_list, time_map, processed_files = args
    file_path = Path(file_path)
    cat_id = file_path.stem
    if cat_id in processed_files:
        return None

    # 尝试获取文件大小用于进度条
    file_size = file_path.stat().st_size

    result = {"cat_id": cat_id, "updates": {}}
    top10_brand_map = {}  # 用于 202510 的品牌排序记录

    # 用于累计所有块的 sellerType / brand / stock_type结果
    sellerType_all_time = {t:{} for t in times_list}
    stock_sales_all_time = {t:{} for t in times_list}

    # ===================================================
    #            开始分块读取 CSV
    # ===================================================
    chunksize = 500000
    read_bytes = 0

    with open(file_path, "rb") as f:  # 用来统计进度
        with tqdm(total=file_size, desc=f"{cat_id}", unit="B", unit_scale=True) as pbar:

            for chunk in pd.read_csv(file_path, low_memory=False, chunksize=chunksize):
                # ≈≈≈ 进度条更新 ≈≈≈
                read_bytes += chunk.memory_usage(index=True).sum()
                pbar.update(chunk.memory_usage(index=True).sum())

                # ---------- 基础字段 ----------
                chunk["monthly_sale_trend"] = chunk.get(
                    "monthly_sale_trend",
                    pd.Series([{}] * len(chunk))
                ).apply(lambda x: safe_eval(x, {}))

                chunk["brand"] = chunk.get(
                    "brand",
                    pd.Series(["未知品牌"] * len(chunk))
                ).fillna("未知品牌")

                chunk["sellerType"] = chunk.get(
                    "sellerType",
                    pd.Series(["others"] * len(chunk))
                ).fillna("others")

                chunk["active_price"] = chunk.get(
                    "active_price",
                    pd.Series([0] * len(chunk))
                )

                if "stock_type" not in chunk.columns:
                    chunk["stock_type"] = "未知"

                # ===================================================
                #           针对所有月份累计统计
                # ===================================================
                for t in times_list:
                    try:
                        time, _, _ = time_map[t]
                        chunk[f"sale_{time}"] = chunk["monthly_sale_trend"].apply(
                            lambda x: x.get(f"{time}", 0)
                        )
                        chunk[f"gmv_{time}"] = chunk[f"sale_{time}"] * chunk["active_price"]

                        # sellerType → brand 聚合
                        for st, group in chunk.groupby("sellerType"):
                            brand_sum = group.groupby("brand")[f"sale_{time}"].sum()

                            st = str(st)
                            if st not in sellerType_all_time[t]:
                                sellerType_all_time[t][st] = brand_sum
                            else:
                                sellerType_all_time[t][st] = sellerType_all_time[t][st].add(brand_sum, fill_value=0)

                        # stock_type 聚合
                        stock_sum = chunk.groupby("stock_type")[f"sale_{time}"].sum()
                        seller_sum_dict = sellerType_all_time[t]

                        if t not in stock_sales_all_time:
                            stock_sales_all_time[t] = stock_sum
                        else:
                            stock_sales_all_time[t] = stock_sales_all_time[t].add(stock_sum, fill_value=0)

                    except Exception:
                        continue

    # ===================================================
    #   所有 chunk 处理完后，再计算 top10、others
    # ===================================================
    for t in times_list:
        sellerType_stats = {}

        for st, brand_sum in sellerType_all_time[t].items():
            brand_sum = brand_sum.sort_values(ascending=False)

            # ----- 当期是 202510：需要记录 top10 品牌序列 ------
            if t == 202510:
                top10 = brand_sum.head(10).astype(int).to_dict()
                others_sum = int(brand_sum[~brand_sum.index.isin(top10)].sum())
                if others_sum > 0:
                    top10["others"] = others_sum

                # 保存顺序
                top10_sorted = dict(sorted(top10.items(), key=lambda x: x[1], reverse=True))
                sellerType_stats[st] = top10_sorted
                top10_brand_map[st] = list(top10_sorted.keys())

            else:
                # 其余月份按 202510 的顺序输出
                tmp = {}
                order_list = top10_brand_map.get(st, [])

                for b in order_list:
                    tmp[b] = int(brand_sum.get(b, 0))

                others_sum = int(brand_sum[~brand_sum.index.isin(order_list)].sum())
                if others_sum > 0:
                    tmp["others"] = others_sum

                sellerType_stats[st] = tmp

        result["updates"][t] = {
            "sellerType_top10_brand_order": sellerType_stats,
            "stock_type": {k: int(v) for k, v in stock_sales_all_time[t].items()}
        }

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
    record_lock = threading.Lock()

    while True:
        item = q.get()
        if item == "STOP":
            break
        buffer.append(item)
        if len(buffer) >= BATCH:
            flush_bulk(buffer, visual_plus, log_file, record_lock)
            buffer = []

    if buffer:
        flush_bulk(buffer, visual_plus, log_file, record_lock)

def flush_bulk(buffer, visual_plus, log_file, record_lock):
    ops = []
    for task in buffer:
        cat_id = task["cat_id"]
        update_doc = {}
        for t, v in task["updates"].items():
            update_doc[f"sellerType_top10_brand_order.{t}"] = stringify_keys(v["sellerType_top10_brand_order"])
            update_doc[f"stock_type.{t}"] = stringify_keys(v["stock_type"])
        ops.append(pymongo.UpdateOne({"cat_id": cat_id}, {"$set": update_doc}, upsert=True))
        with record_lock:
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
    folder_path = Path(r"D:\data\看板\processed_j_cbt_price")
    log_file = folder_path / "processed_cbt_top10_brand.txt"
    folder_path.mkdir(parents=True, exist_ok=True)

    times_list = [
        202510,
        202509,
        # 202508,
        # 202507,
        # 202506,
        # 202505,
        # 202504,
        # 202503,
        # 202502,
        # 202501,
        # 202412,
        # 202411,
        # 202410
    ]

    time_map = {
        202510: (2510, 2509, 2410),
        202509: (2509, 2508, 2409),
        # 202508: (2508, 2507, 2408),
        # 202507: (2507, 2506, 2407),
        # 202506: (2506, 2505, 2406),
        # 202505: (2505, 2504, 2405),
        # 202504: (2504, 2503, 2404),
        # 202503: (2503, 2502, 2403),
        # 202502: (2502, 2501, 2402),
        # 202501: (2501, 2412, 2401),
        # 202412: (2412, 2411, 2312),
        # 202411: (2411, 2410, 2311),
        # 202410: (2410, 2409, 2310)
    }

    processed_files = set()
    if log_file.exists():
        processed_files = {x.strip() for x in log_file.read_text().splitlines()}

    csv_list = list(folder_path.glob("*.csv"))
    csv_list = ["D:/data/看板/processed_j_cbt_price/all.csv"]

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

# ==========================================================
#                Windows 入口保护
# ==========================================================
if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
