import duckdb
import pandas as pd
import numpy as np
import ast
import os
import pickle
from tqdm import tqdm
from collections import defaultdict
# ===== 环境变量 =====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
# ===== MongoDB collection =====
visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")

# ===== 参数 =====
TARGET_MONTH = "2511"
DUCKDB_PATH = r"D:\gg_xm\Q\DuckDB\cx_all.duckdb"
COLLECTION_NAME = "top10_brand_range_price"
BINS_DIR = r"D:/gg_xm/Q/看板/z改/价格段"
TOP10_DIR = r"D:/gg_xm/Q/看板/z改/top10品牌"
CHUNK_SIZE = 50000

# ===== DuckDB 连接 =====
con = duckdb.connect(DUCKDB_PATH, read_only=True)

# ===== 工具函数 =====
def safe_dict(x):
    """确保返回字典，不会返回 None"""
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        try:
            val = ast.literal_eval(x)
            if isinstance(val, dict):
                return val
        except Exception:
            pass
    return {}

def compute_bins(table_name: str, bins_file: str):
    """生成或读取价格段"""
    if os.path.exists(bins_file):
        with open(bins_file, "rb") as f:
            return pickle.load(f)

    prices = []
    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    for offset in range(0, total_rows, CHUNK_SIZE):
        chunk = con.execute(
            f"SELECT active_price FROM {table_name} LIMIT {CHUNK_SIZE} OFFSET {offset}"
        ).df()
        prices.extend(chunk['active_price'].dropna().astype(float).tolist())

    if prices:
        bins = np.quantile(np.array(prices), np.linspace(0, 1, 11))
        bins_floor = np.floor(bins / 100) * 100
        bins_ceil = np.ceil(bins / 100) * 100
        bins_floor[0] = 0.001
        bins_floor[-1] = bins_ceil[-1]
        bins_right = np.unique(bins_floor)[1:]
    else:
        bins_right = np.array([999999.0])

    with open(bins_file, "wb") as f:
        pickle.dump(bins_right, f)
    return bins_right

def compute_top10(table_name: str, top10_file: str):
    """计算 top10 品牌"""
    if os.path.exists(top10_file):
        with open(top10_file, "rb") as f:
            return pickle.load(f)

    brand_sales = defaultdict(int)
    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    for offset in range(0, total_rows, CHUNK_SIZE):
        chunk = con.execute(
            f"SELECT brand, monthly_sale_trend FROM {table_name} LIMIT {CHUNK_SIZE} OFFSET {offset}"
        ).df()
        chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].apply(safe_dict)
        chunk['order'] = chunk['monthly_sale_trend'].apply(lambda x: x.get(TARGET_MONTH, 0) if isinstance(x, dict) else 0)
        for b, o in zip(chunk['brand'], chunk['order']):
            if pd.notna(b):
                brand_sales[b] += o

    top10 = [b for b, _ in sorted(brand_sales.items(), key=lambda x: x[1], reverse=True)[:10]]
    with open(top10_file, "wb") as f:
        pickle.dump(top10, f)
    return top10

def process_table(table_name: str):
    """处理单张表，生成 top10_brand_range_price 并写入 MongoDB"""
    print(f"Processing {table_name}...")

    bins_file = os.path.join(BINS_DIR, f"{table_name}.pkl")
    top10_file = os.path.join(TOP10_DIR, f"top10_brands_{table_name}.pkl")

    bins_right = compute_bins(table_name, bins_file)
    top10_brands = compute_top10(table_name, top10_file)

    data_dict = {TARGET_MONTH: {}}
    for brand in top10_brands + ["others"]:
        data_dict[TARGET_MONTH][brand] = {"price": 0, "range_price": {str(int(r)): {"order": 0, "gmv": 0} for r in bins_right}}

    total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    for offset in tqdm(range(0, total_rows, CHUNK_SIZE), desc=f"{table_name} 处理中", unit="chunk"):
        chunk = con.execute(
            f"SELECT brand, active_price, monthly_sale_trend FROM {table_name} LIMIT {CHUNK_SIZE} OFFSET {offset}"
        ).df()
        chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].apply(safe_dict)
        chunk['order'] = chunk['monthly_sale_trend'].apply(lambda x: x.get(TARGET_MONTH, 0) if isinstance(x, dict) else 0)
        chunk['active_price'] = chunk['active_price'].fillna(0).astype(float)
        chunk['gmv'] = chunk['order'] * chunk['active_price']

        for _, row in chunk.iterrows():
            brand = row['brand'] if row['brand'] in top10_brands else "others"
            price = float(row['active_price'])
            order = int(row['order'])
            gmv = float(row['gmv'])
            for r in bins_right:
                key = str(int(r))
                if price <= r:
                    data_dict[TARGET_MONTH][brand]["range_price"][key]["order"] += order
                    data_dict[TARGET_MONTH][brand]["range_price"][key]["gmv"] += gmv
                    break

    # 计算平均价格
    for brand in top10_brands + ["others"]:
        total_orders = sum(v["order"] for v in data_dict[TARGET_MONTH][brand]["range_price"].values())
        total_gmv = sum(v["gmv"] for v in data_dict[TARGET_MONTH][brand]["range_price"].values())
        data_dict[TARGET_MONTH][brand]["price"] = round(total_gmv / total_orders, 2) if total_orders > 0 else 0

    # 写入 MongoDB
    visual_plus.update_one(
        {"cat_id": table_name},
        {"$set": {f"{COLLECTION_NAME}.20{TARGET_MONTH}": data_dict[TARGET_MONTH]}},
        upsert=True
    )
    print(f"{TARGET_MONTH} 月 top10_brand_range_price 已写入 MongoDB，_id={table_name}。")

# ===== 主流程 =====
if __name__ == "__main__":
    table_names = [t[0] for t in con.execute("SHOW TABLES").fetchall() if t[0].startswith("MLM")]
    for table in table_names:
        process_table(table)
