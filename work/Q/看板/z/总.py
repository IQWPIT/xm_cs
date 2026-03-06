import pandas as pd
import numpy as np
import os
import ast
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import threading
import queue
import pymongo

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# ==== 文件夹 & 日志 ====
source_folder = Path(r"D:\data\看板\202512_sku_p_info_2")
log_file = source_folder / "processing_log.txt"
source_folder.mkdir(parents=True, exist_ok=True)
csv_files = list(source_folder.glob("*.csv"))
# ===== 全局时间参数 =====

# 统一使用 YYMM（与你现有 monthly_sale_trend 的 key 保持一致）
TIME_LIST = [2510, 2509]
# 单月统计（sellerType_price / range_price / conversion）
MAIN_TIME = 2510



file_names = [f.stem for f in csv_files]  # 可按需指定文件
log_lock = threading.Lock()

# ==== 工具函数 ====
def safe_eval(x, default={}):
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

# ==== Mongo 写入线程 ====
def mongo_writer_thread(q, log_file):
    buffer = []
    BATCH = 50
    record_lock = threading.Lock()
    while True:
        item = q.get()
        if item == "STOP":
            break
        buffer.append(item)
        if len(buffer) >= BATCH:
            flush_bulk(buffer, log_file, record_lock)
            buffer = []
    if buffer:
        flush_bulk(buffer, log_file, record_lock)

def flush_bulk(buffer, log_file, record_lock):
    ops = []
    for task in buffer:
        cat_id = task["cat_id"]
        update_doc = {}

        # sellerType_top10_brand_order
        for t, v in task.get("top10_brand", {}).items():
            update_doc[f"sellerType_top10_brand_order.20{t}"] = stringify_keys(v["sellerType"])


        # sellerType_price
        for t, v in task.get("sellerType_price", {}).items():
            update_doc[f"sellerType_price.20{t}"] = stringify_keys(v)

        # range_price & conversion
        for t, v in task.get("range_price", {}).items():
            update_doc[f"range_price.20{t}"] = stringify_keys(v)
        for t, v in task.get("conversion", {}).items():
            update_doc[f"conversion.20{t}"] = stringify_keys(v)

        ops.append(pymongo.UpdateOne({"cat_id": cat_id}, {"$set": update_doc}, upsert=True))

        with record_lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{cat_id} ✅\n")

    if ops:
        try:
            visual_plus.bulk_write(ops, ordered=False)
        except Exception as e:
            print(e)
            # with open(log_file, "a", encoding="utf-8") as f:
            #     f.write(f"❌ Bulk write error: {e}\n")

# ==== CSV 处理 worker ====
def process_csv_worker(args):
    file_path, done_set = args
    file_path = Path(file_path)
    cat_id = file_path.stem
    if cat_id in done_set:
        return None

    result = {"cat_id": cat_id, "top10_brand": {}, "sellerType_price": {}, "range_price": {}, "conversion": {}}

    # 读取 CSV
    chunksize = 10_000
    try:
        all_chunks = []
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            # ---------------- 基础清洗 ----------------
            chunk["active_price"] = pd.to_numeric(chunk.get("active_price", 0), errors="coerce")
            chunk = chunk[chunk["active_price"] > 0]

            chunk["brand"] = chunk.get("brand", "未知品牌").fillna("未知品牌")
            chunk["sellerType"] = chunk.get("sellerType", "others").fillna("others")
            chunk["stock_type"] = chunk.get("stock_type", "未知")
            chunk["monthly_sale_trend"] = chunk.get("monthly_sale_trend", "{}").apply(lambda x: safe_eval(x, {}))

            all_chunks.append(chunk)

        df = pd.concat(all_chunks, ignore_index=True)

        # ---------------- top10 品牌统计 ----------------
        times_list = TIME_LIST
        top10_brand_map = {}
        sellerType_all_time = {t:{} for t in times_list}
        stock_sales_all_time = {t:{} for t in times_list}

        for t in times_list:
            for st, group in df.groupby("sellerType"):
                brand_sum = group.groupby("brand")["monthly_sale_trend"].apply(
                    lambda x: sum(d.get(str(t),0) for d in x)
                )
                sellerType_all_time[t][st] = brand_sum.sort_values(ascending=False)
                stock_sum = group.groupby("stock_type")["monthly_sale_trend"].apply(
                    lambda x: sum(d.get(str(t),0) for d in x)
                )
                stock_sales_all_time[t][st] = stock_sum

        for t in times_list:
            sellerType_stats = {}
            for st, brand_sum in sellerType_all_time[t].items():
                top10 = brand_sum.head(10).astype(int).to_dict()
                others_sum = int(brand_sum[~brand_sum.index.isin(top10)].sum())
                if others_sum > 0:
                    top10["others"] = others_sum
                sellerType_stats[st] = top10
                top10_brand_map[st] = list(top10.keys())

            result["top10_brand"][t] = {
                "sellerType": sellerType_stats
            }

        # ---------------- sellerType_price ----------------
        bins = np.floor(df["active_price"].quantile(np.linspace(0,1,11))/100)*100
        bins = np.unique(bins)
        bins[0] = 0.001
        df["price_bin"] = pd.cut(df["active_price"], bins=bins, include_lowest=True)
        time_single = MAIN_TIME
        df[f"sale_{time_single}"] = df["monthly_sale_trend"].apply(lambda x: x.get(str(time_single), 0))
        for st, group in df.groupby("sellerType"):
            bin_sum = group.groupby("price_bin")[f"sale_{time_single}"].sum()
            bin_dict = {str(int(iv.right)): int(v) for iv,v in bin_sum.items()}
            result["sellerType_price"][st] = bin_dict

        # ---------------- range_price & conversion ----------------
        conversion_time = MAIN_TIME
        col_order = "conversion_all_order"
        col_view0 = "conversion_all_view"

        quantiles = np.linspace(0,1,11)
        bins = df["active_price"].quantile(quantiles).values
        bins_floor = np.floor(bins/100)*100
        bins_ceil = np.ceil(bins/100)*100
        bins_rounded = bins_floor.copy()
        bins_rounded[-1] = bins_ceil[-1]
        bins_rounded[0] = 0
        bins_rounded = np.unique(bins_rounded)
        bins_rounded[0] += 0.001
        df["price_bin"] = pd.cut(df["active_price"], bins=bins_rounded, include_lowest=True)

        # range_price
        bin_sum = df.groupby("price_bin")[f"sale_{conversion_time}"].sum().sort_index()
        result["range_price"][conversion_time] = {str(int(iv.right)): int(sale) for iv,sale in bin_sum.items()}

        # conversion
        if col_order in df.columns and col_view0 in df.columns:
            conversion_df = df.groupby("price_bin")[[col_order, col_view0]].sum()
            conversion_dict = {}
            for iv, row in conversion_df.iterrows():
                order_sum = int(row[col_order])
                view_sum = int(row[col_view0])
                ratio = round(order_sum/view_sum,4) if view_sum!=0 else None
                conversion_dict[str(int(iv.right))] = {
                    "conversion_all_order": order_sum,
                    "conversion_all_view": view_sum,
                    "conversion_ratio": ratio
                }
            result["conversion"][conversion_time] = conversion_dict

        return result

    except Exception as e:
        return {"cat_id": cat_id, "error": str(e)}

# ==== 主程序 ====
def main():
    done_set = set()
    if log_file.exists():
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.endswith("✅"):
                    done_set.add(line.replace("✅","").strip())

    q = queue.Queue()
    writer = threading.Thread(target=mongo_writer_thread, args=(q, log_file))
    writer.start()

    args_list = [(str(source_folder/f"{f}.csv"), done_set) for f in file_names]

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
