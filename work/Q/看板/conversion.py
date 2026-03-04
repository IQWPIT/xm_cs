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
source_folder = Path(r"D:\data\看板\202512_sku")
log_file = Path(r"D:\data\看板\processing_range_price_等频.txt")

csv_files = list(source_folder.glob("*.csv"))
file_names = [f.stem for f in csv_files]
# file_names = [
#     "MLM1747","MLM189530","MLM1403","MLM1367","MLM1368","MLM1384",
#     "MLM1246","MLM1051","MLM1648","MLM1144","MLM1500","MLM1039",
#     "MLM1276","MLM1575","MLM1000","MLM186863","MLM1574","MLM1499",
#     "MLM1182","MLM3937","MLM1132","MLM3025","MLM1071","MLM1953",
#     "MLM44011","MLM1430","MLM187772"
# ]

# ==== 读取已处理列表 ====
done_set = set()
if log_file.exists():
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.endswith("✅"):
                done_set.add(line.replace("✅", "").strip())

print(f"已处理 {len(done_set)} 个 cat_id，将跳过它们…")

# ==== 线程安全日志 ====
log_lock = threading.Lock()


def process_file(cat_id):

    conversion_time = 202510  # 用变量代替固定时间

    # ==== 如果已处理过 -> 直接跳过 ====
    if cat_id in done_set:
        return f"{cat_id} ⏭️ 已跳过"

    times = [
        2510, 2509, 2508, 2507, 2506,
        2505, 2504, 2503, 2502, 2501,
        2412, 2411, 2410
    ]

    try:
        cat_file = source_folder / f"{cat_id}.csv"
        df = pd.read_csv(cat_file)
        col_price = "active_price"
        col_sale = "monthly_sale_trend"
        col_order = "conversion_all_order"
        col_view0 = "conversion_all_view"

        # --- 过滤价格 ---
        df[col_price] = pd.to_numeric(df[col_price], errors='coerce')
        df = df[df[col_price] > 0]

        # --- 解析销量字典 ---
        def extract_sale(s, time):
            if pd.isna(s):
                return 0
            try:
                if isinstance(s, str):
                    s = ast.literal_eval(s)
                return s.get(f"{time}", 0)
            except:
                return 0

        # --- 计算价格段（基于 conversion_time） ---
        quantiles = np.linspace(0, 1, 11)
        bins = df[col_price].quantile(quantiles).values
        bins_floor = np.floor(bins / 100) * 100
        bins_ceil = np.ceil(bins / 100) * 100
        bins_rounded = bins_floor.copy()
        bins_rounded[-1] = bins_ceil[-1]
        bins_rounded[0] = 0
        bins_rounded = np.unique(bins_rounded)
        bins_rounded[0] += 0.001

        # --- 按时间循环统计销量 ---
        for time in times:
            df[f"sale_{time}"] = df[col_sale].apply(lambda x: extract_sale(x, time))
            df["price_bin"] = pd.cut(df[col_price], bins=bins_rounded, include_lowest=True)

            bin_sum = df.groupby("price_bin")[f"sale_{time}"].sum().sort_index()
            result_dict = {str(int(iv.right)): int(sale) for iv, sale in bin_sum.items()}

            # 更新 Mongo
            visual_plus.update_one(
                {"cat_id": cat_id},
                {"$set": {f"range_price.20{time}": result_dict}},
                upsert=True
            )

        # --- 仅对 conversion_time 计算 conversion ---
        df["price_bin"] = pd.cut(df[col_price], bins=bins_rounded, include_lowest=True)
        conversion_df = df.groupby("price_bin")[[col_order, col_view0]].sum()
        conversion_dict = {}
        for iv, row in conversion_df.iterrows():
            order_sum = int(row[col_order])
            view_sum = int(row[col_view0])
            ratio = round(order_sum/view_sum, 4) if view_sum != 0 else None
            conversion_dict[str(int(iv.right))] = {
                "conversion_all_order": order_sum,
                "conversion_all_view": view_sum,
                "conversion_ratio": ratio
            }

        # --- 更新 MongoDB conversion ---
        visual_plus.update_one(
            {"cat_id": cat_id},
            {"$unset": {"conversion": ""}},

        )
        visual_plus.update_one(
            {"cat_id": cat_id},
            {"$set": {f"conversion.{conversion_time}": conversion_dict}},
            upsert=True
        )

        # --- 记录日志 ---
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as log_f:
                log_f.write(f"{cat_id} ✅\n")

        return f"{cat_id} ✅"

    except Exception as e:
        return f"{cat_id} ❌ {e}"

# ==== 多线程执行 ====
max_workers = 20
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for msg in tqdm(executor.map(process_file, file_names), total=len(file_names), desc="Processing files"):
        print(msg)
