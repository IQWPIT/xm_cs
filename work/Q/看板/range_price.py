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

visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")

# ==== 文件夹 ====
source_folder = Path(r"D:\data\看板\processed_j_cbt_price")
log_file = Path(r"D:\data\看板\processing_range_price_等频.txt")

# ==== 要处理的 cat_id ====
# file_names = [
#     "MLM1747","MLM189530","MLM1403","MLM1367","MLM1368","MLM1384",
#     "MLM1246","MLM1051","MLM1648","MLM1144","MLM1500","MLM1039",
#     "MLM1276","MLM1575","MLM1000","MLM186863","MLM1574","MLM1499",
#     "MLM1182","MLM3937","MLM1132","MLM3025","MLM1071","MLM1953",
#     "MLM44011","MLM1430","MLM187772"
# ]
file_names = ["all"]
# ==== 读取已处理列表 ====
done_set = set()
if log_file.exists():
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.endswith("✅"):
                done_set.add(line.replace("✅", "").strip())

print(f"已处理 {len(done_set)} 个 cat_id，将跳过它们…")

log_lock = threading.Lock()

# ============================================================================
#                           分块读取 + 进度条
# ============================================================================

def process_file(cat_id):

    if cat_id in done_set:
        return f"{cat_id} ⏭️ 已跳过"

    try:
        file_path = source_folder / f"{cat_id}.csv"

        # ------------- 第 1 步：预扫描文件，获取 202510 的价格段 bins -------------
        chunksize = 500_000
        prices = []

        file_size = file_path.stat().st_size
        read_bytes = 0

        print(f"\n[{cat_id}] 第一步：扫描价格生成 202510 的等频价格段…")
        with open(file_path, "rb") as f:
            pass  # 用于初始化文件 I/O，不做其他操作

        for chunk in tqdm(pd.read_csv(file_path, chunksize=chunksize, usecols=["active_price"]),
                          desc=f"{cat_id} 扫描价格",
                          total=file_size // (chunksize * 50),
                          miniters=1):

            chunk["active_price"] = pd.to_numeric(chunk["active_price"], errors='coerce')
            chunk = chunk[chunk["active_price"] > 0]
            prices.extend(chunk["active_price"].values)

        if len(prices) == 0:
            return f"{cat_id} ❌ 无有效价格"

        # --- 生成价格段 bins（等频 + 百元向下取整） ---
        quantiles = np.linspace(0, 1, 11)
        bins = np.quantile(prices, quantiles)
        bins_floor = np.floor(bins / 100) * 100
        bins_ceil = np.ceil(bins / 100) * 100
        bins_rounded = bins_floor.copy()
        bins_rounded[-1] = bins_ceil[-1]
        bins_rounded[0] = 0
        bins_rounded = np.unique(bins_rounded)
        bins_rounded[0] += 0.001

        # ------------- 第 2 步：按时间循环，用同样 bins 再次分块读取 CSV 聚合销量 -------------
        times = [
            2510, 2509, 2508, 2507, 2506,
            2505, 2504, 2503, 2502, 2501,
            2412, 2411, 2410
        ]

        print(f"[{cat_id}] 第二步：逐时间聚合销量…")
        for time in times:

            # 每个时间都需要累计
            price_bin_sum = {}

            for chunk in tqdm(pd.read_csv(file_path, chunksize=chunksize),
                              desc=f"{cat_id} 处理 {time}",
                              miniters=1):

                # --- 价格 ---
                chunk["active_price"] = pd.to_numeric(chunk["active_price"], errors='coerce')
                chunk = chunk[chunk["active_price"] > 0]

                # --- 解析销售字典 ---
                def extract_sale(s):
                    if pd.isna(s):
                        return 0
                    try:
                        if isinstance(s, str):
                            s = ast.literal_eval(s)
                        return s.get(f"{time}", 0)
                    except:
                        return 0

                chunk[f"sale_{time}"] = chunk["monthly_sale_trend"].apply(extract_sale)

                # --- 分箱 ---
                chunk["price_bin"] = pd.cut(chunk["active_price"], bins=bins_rounded, include_lowest=True)

                # --- 聚合 ---
                grp = chunk.groupby("price_bin")[f"sale_{time}"].sum().to_dict()
                for k, v in grp.items():
                    price_bin_sum[k] = price_bin_sum.get(k, 0) + v

            # --- 处理好格式，写入 Mongo ---
            result_dict = {str(int(iv.right)): int(sale) for iv, sale in price_bin_sum.items()}

            visual_plus.update_one(
                {"cat_id": cat_id},
                {"$set": {f"range_price.20{time}": result_dict}},
                upsert=True
            )

        # ==== 写入日志 ====
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as log_f:
                log_f.write(f"{cat_id} ✅\n")

        return f"{cat_id} ✅"

    except Exception as e:
        return f"{cat_id} ❌ {e}"


# ============================================================================
#                           启动多线程
# ============================================================================

max_workers = 6  # 建议不要超过 6，避免 IO 争抢
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for msg in tqdm(executor.map(process_file, file_names), total=len(file_names), desc="总进度"):
        print(msg)
