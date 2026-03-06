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
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
time = 2512
# ==== 文件夹 & 日志 ====
source_folder = Path(r"D:\data\看板\20260104")
r = Path(r"D:\data\看板\日志")
log_file = r / f"总{time}.txt"
source_folder.mkdir(parents=True, exist_ok=True)
csv_files = list(source_folder.glob("*.csv"))

file_names = [f.stem for f in csv_files]
log_lock = threading.Lock()

# ==== 工具函数 ====
def safe_eval(x, default=None):
    if default is None:
        default = {}
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

# ==== CSV 处理 worker ====
def process_csv_worker(args):
    file_path, done_set, target_month = args
    file_path = Path(file_path)
    cat_id = file_path.stem
    if cat_id in done_set:
        return None

    result = {"cat_id": cat_id, "top10_brand": {}, "sellerType_price": {}, "range_price": {}, "conversion": {}}

    try:
        # ---------- 读取 CSV ----------
        chunksize = 10_0000
        all_chunks = []
        for chunk in pd.read_csv(file_path, chunksize=chunksize,low_memory=False):
            chunk["active_price"] = pd.to_numeric(chunk.get("active_price", 0), errors="coerce")
            chunk = chunk[chunk["active_price"] > 0]
            chunk["brand"] = chunk.get("brand", "未知品牌").fillna("未知品牌")
            chunk["sellerType"] = chunk.get("sellerType", "others").fillna("others")
            chunk["stock_type"] = chunk.get("stock_type", "未知")
            chunk["monthly_sale_trend"] = chunk.get("monthly_sale_trend", "{}").apply(lambda x: safe_eval(x, {}))
            all_chunks.append(chunk)
        df = pd.concat(all_chunks, ignore_index=True)

        # ---------- 计算 2510 基准 ----------
        # top10 品牌
        top10_brand_map_2510 = {}
        for st, group in df.groupby("sellerType"):
            brand_sum_2510 = group.groupby("brand")["monthly_sale_trend"].apply(lambda x: sum(d.get("2510",0) for d in x))
            top10_keys = brand_sum_2510.sort_values(ascending=False).head(10).index.tolist()
            top10_brand_map_2510[st] = top10_keys

        # 价格段
        bins_2510 = np.floor(df["active_price"].quantile(np.linspace(0,1,11))/100)*100
        bins_2510 = np.unique(bins_2510)
        bins_2510[0] = 0.001

        # ---------- top10 品牌统计 ----------
        sellerType_stats = {}
        for st, group in df.groupby("sellerType"):
            brand_sum_month = group.groupby("brand")["monthly_sale_trend"].apply(lambda x: sum(d.get(str(target_month),0) for d in x))
            top10 = {k:int(brand_sum_month.get(k,0)) for k in top10_brand_map_2510[st]}
            others_sum = int(brand_sum_month[~brand_sum_month.index.isin(top10_brand_map_2510[st])].sum())
            if others_sum > 0:
                top10["others"] = others_sum
            sellerType_stats[st] = top10
        result["top10_brand"][target_month] = {"sellerType": sellerType_stats}

        # ---------- sellerType_price ----------
        df["price_bin"] = pd.cut(df["active_price"], bins=bins_2510, include_lowest=True)
        df[f"sale_{target_month}"] = df["monthly_sale_trend"].apply(lambda x: x.get(str(target_month), 0))
        for st, group in df.groupby("sellerType"):
            bin_sum = group.groupby("price_bin")[f"sale_{target_month}"].sum()
            bin_dict = {str(int(iv.right)): int(v) for iv,v in bin_sum.items()}
            if target_month not in result["sellerType_price"]:
                result["sellerType_price"][target_month] = {}
            result["sellerType_price"][target_month][st] = bin_dict

        # ---------- range_price & conversion ----------
        conversion_time = target_month
        col_order = "conversion_all_order"
        col_view0 = "conversion_all_view"

        df["price_bin"] = pd.cut(df["active_price"], bins=bins_2510, include_lowest=True)

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

from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# ==== 主程序 ====
def main(target_month=2511):
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

    args_list = [(str(source_folder/f"{f}.csv"), done_set, target_month) for f in file_names]

    with ProcessPoolExecutor(max_workers=8) as exe:
        futures = [exe.submit(process_csv_worker, args) for args in args_list]
        for future in tqdm(as_completed(futures), total=len(futures), desc="处理 CSV"):
            res = future.result()
            if res:
                q.put(res)

    q.put("STOP")
    writer.join()
    print("全部完成！")

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main(target_month=time)
