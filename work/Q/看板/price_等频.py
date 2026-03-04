import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from pathlib import Path
import ast
from concurrent.futures import ThreadPoolExecutor
import threading

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# ==== 文件夹 ====
source_folder = Path(r"D:\data\看板\processed_j_cbt_price")
log_file = Path(r"D:\data\看板\processing_price_等频.txt")  # 日志文件

csv_files = list(source_folder.glob("*.csv"))
file_names = [f.stem for f in csv_files]
time = 2509
times = 202509

# ==== 线程安全的日志写入 ====
log_lock = threading.Lock()

def process_file(cat_id):
    try:
        cat_file = source_folder / f"{cat_id}.csv"
        df = pd.read_csv(cat_file)
        col_price = "active_price"
        col_cbt = "sellerType"
        col_sale = "monthly_sale_trend"
        col_order = "conversion_all_order"
        col_view0 = "conversion_all_view0"

        # 过滤价格无效
        df[col_price] = pd.to_numeric(df[col_price], errors='coerce')
        df = df[df[col_price] > 0]

        # sellerType
        if col_cbt in df.columns:
            df[col_cbt] = df[col_cbt].fillna("others")
        else:
            df[col_cbt] = ["others"] * len(df)

        # 提取销量
        def extract_sale(s):
            if pd.isna(s):
                return 0
            try:
                if isinstance(s, str):
                    s = ast.literal_eval(s)
                return s.get(f"{time}", 0)
            except:
                return 0

        df[f"sale_{time}"] = df[col_sale].apply(extract_sale)

        # 价格段
        quantiles = np.linspace(0, 1, 11)
        bins = df[col_price].quantile(quantiles).values
        bins_floor = np.floor(bins / 100) * 100
        bins_ceil = np.ceil(bins / 100) * 100
        bins_rounded = bins_floor.copy()
        bins_rounded[-1] = bins_ceil[-1]
        bins_rounded[0] = 0
        bins_rounded = np.unique(bins_rounded)
        bins_rounded[0] += 0.001

        df["price_bin"] = pd.cut(df[col_price], bins=bins_rounded, include_lowest=True)

        # 构造结果字典
        cbt_stats = {}
        conversion_stats = {}
        for cbt_value, group in df.groupby(col_cbt):
            if str(cbt_value).lower() == "false":
                continue

            # 销量统计
            bin_sum = group.groupby("price_bin")[f"sale_{time}"].sum().sort_index()
            bin_dict = {str(int(interval.right)): int(total_sale)
                        for interval, total_sale in bin_sum.items()}
            cbt_stats[str(cbt_value)] = bin_dict

            # conversion 统计
            conv_dict = {}
            for interval, sub_group in group.groupby("price_bin"):
                total_order = sub_group[col_order].sum()
                total_view0 = sub_group[col_view0].sum()
                ratio = float(total_order) / float(total_view0) if total_view0 else None
                conv_dict[str(int(interval.right))] = {
                    "conversion_all_order": float(total_order),
                    "conversion_all_view0": float(total_view0),
                    "conversion_ratio": ratio
                }
            conversion_stats[str(cbt_value)] = conv_dict

        # 更新 MongoDB
        update_data = {
            f"sellerType_price.{times}": cbt_stats,
            f"conversion.{times}": conversion_stats
        }
        visual_plus.update_one({"cat_id": cat_id}, {"$set": update_data}, upsert=True)

        # 日志记录
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as log_f:
                log_f.write(f"{cat_id} ✅\n")
            print(cat_id, "✅")

    except Exception as e:
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as log_f:
                log_f.write(f"{cat_id} ❌ {e}\n")
            print(f"❌ 出错 ({cat_id})：", e)

# ==== 多线程执行 ====
max_workers = 8  # 根据电脑 CPU 核心调整
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    list(tqdm(executor.map(process_file, file_names), total=len(file_names), desc="Processing files"))
