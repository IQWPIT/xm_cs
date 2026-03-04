import duckdb
import pandas as pd
import os
import ast
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# ===== 环境变量 =====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

# ===== 参数 =====
TARGET_MONTH = "2511"
DUCKDB_PATH = r"D:\gg_xm\Q\DuckDB\cx_all.duckdb"
COLLECTION_NAME = "stock_type_top10_seller"
MAX_WORKERS = 4  # 并行表数量
LOG_FILE = r"D:/gg_xm/Q/看板/z改/日志/stock_type_top10_log.txt"
CHUNK_SIZE = 50000

# ===== MongoDB collection =====
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")

# ===== DuckDB 连接 =====
con = duckdb.connect(DUCKDB_PATH, read_only=True)

# ===== 获取 DuckDB 表列表 =====
tables = [t[0] for t in con.execute("SHOW TABLES").fetchall() if t[0].startswith("MLM")]

# ===== safe_dict 函数 =====
def safe_dict(x):
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

# ===== 处理单张表 =====
def process_table(table_name):
    try:
        stock_seller_orders = defaultdict(lambda: defaultdict(int))
        total_rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        for offset in tqdm(range(0, total_rows, CHUNK_SIZE), desc=f"{table_name} 处理中", unit="chunk"):
            chunk = con.execute(f"""
                SELECT stock_type, sellerName, monthly_sale_trend 
                FROM {table_name} 
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """).df()

            # 转换 monthly_sale_trend
            chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].apply(safe_dict)
            chunk['order'] = chunk['monthly_sale_trend'].apply(lambda x: x.get(TARGET_MONTH, 0) if isinstance(x, dict) else 0)

            for _, row in chunk.iterrows():
                stock_seller_orders[row['stock_type']][row['sellerName']] += row['order']

        # 构建 top10 写入结构
        stock_type_top10 = {}
        for stock_type, sellers in stock_seller_orders.items():
            sorted_sellers = sorted(sellers.items(), key=lambda x: x[1], reverse=True)[:10]
            stock_type_top10[stock_type] = {s: o for s, o in sorted_sellers}

        # 写入 MongoDB
        visual_plus.update_one(
            {"cat_id": table_name},
            {"$set": {f"{COLLECTION_NAME}.{TARGET_MONTH}": stock_type_top10}},
            upsert=True
        )

        # 写入日志
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{table_name}\n")

    except Exception as e:
        print(f"{table_name} 处理失败: {e}")

# ===== 并行处理 =====
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    list(executor.map(process_table, tables))

print("所有表处理完成，成功处理的表名称已记录在日志文件中。")
