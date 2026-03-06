import pandas as pd
import numpy as np
import os
from pathlib import Path
import ast
from tqdm import tqdm
from pymongo import UpdateOne
import threading

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# ==== 文件夹 ====
source_folder = Path(r"D:\data\看板\processed_j_cbt_price")
log_file = Path(r"D:\data\看板\processing_log.txt")  # 日志文件

csv_files = list(source_folder.glob("*.csv"))
file_names = [f.stem for f in csv_files]
# file_names = ["all"]  # 可以按需选择具体文件

time = 2510
times = 202510

success_list = []
fail_list = []

log_lock = threading.Lock()


def parse_sale_dict(s):
    """安全解析销量字典"""
    if pd.isna(s):
        return {}
    try:
        return ast.literal_eval(s) if isinstance(s, str) else s
    except:
        return {}


for cat_id in tqdm(file_names, desc="Processing cat_id files"):
    try:
        cat_file = source_folder / f"{cat_id}.csv"

        # 分块读取 CSV
        chunksize = 100_000
        reader = pd.read_csv(cat_file, usecols=["active_price", "sellerType", "monthly_sale_trend"],
                             chunksize=chunksize)

        all_chunks = []
        for chunk in tqdm(reader, desc=f"{cat_id} chunks", leave=False):
            # 数据清洗
            chunk["active_price"] = pd.to_numeric(chunk["active_price"], errors='coerce')
            chunk = chunk[chunk["active_price"] > 0]

            # sellerType 填充
            chunk["sellerType"] = chunk.get("sellerType", pd.Series(["others"] * len(chunk))).fillna("others")

            # 提取销量
            chunk[f"sale_{time}"] = chunk["monthly_sale_trend"].map(parse_sale_dict).map(lambda d: d.get(str(time), 0))

            all_chunks.append(chunk)

        # 合并所有块
        df = pd.concat(all_chunks, ignore_index=True)

        # 价格段
        bins = np.floor(df["active_price"].quantile(np.linspace(0, 1, 11)) / 100) * 100
        bins = np.unique(bins)
        bins[0] = 0.001
        df["price_bin"] = pd.cut(df["active_price"], bins=bins, include_lowest=True)

        # 构造结果字典
        cbt_stats = {}
        for cbt_value, group in df.groupby("sellerType"):
            if str(cbt_value).lower() == "false":
                continue

            bin_sum = group.groupby("price_bin")[f"sale_{time}"].sum()
            intervals = list(bin_sum.index)
            bin_dict = {}
            for i, interval in enumerate(intervals):
                if i == len(intervals) - 1:
                    key = f"{int(intervals[i].right)}"
                else:
                    key = str(int(interval.right))
                bin_dict[key] = int(bin_sum[interval])

            cbt_stats[str(cbt_value)] = bin_dict

        # MongoDB 更新
        visual_plus.update_one(
            {"cat_id": cat_id},
            {"$set": {f"sellerType_price.{times}": cbt_stats}},
            upsert=True
        )

        with log_lock:
            with open(log_file, "a", encoding="utf-8") as log_f:
                log_f.write(f"{cat_id} ✅\n")

        success_list.append(cat_id)

    except Exception as e:
        print(cat_id, e)
        fail_list.append(f"{cat_id} ❌ {e}")
