import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import ast

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# ==== 文件夹 & 日志 & 输出 ====
source_folder = Path(r"D:\data\看板\processed_j_cbt_price")
log_file = Path(r"D:\gg_xm\Q\李新大客户\1.txt")
output_file = Path(r"D:\gg_xm\Q\李新大客户\top_sales_summary.xlsx")

# ==== 从 MLM.txt 读取文件名 ====
with open(r"D:\gg_xm\Q\李新大客户\MLM.txt", "r", encoding="utf-8") as f:
    file_names = [line.strip() for line in f if line.strip()]

# ==== 线程安全日志 ====
log_lock = threading.Lock()

times = [
    2510, 2509, 2508, 2507, 2506, 2505, 2504, 2503, 2502, 2501, 2412, 2411, 2410
]

# ==== 保存结果的字典 ====
results_dict = {}

def process_file(cat_id):
    try:
        cat_file = source_folder / f"{cat_id}.csv"
        df = pd.read_csv(cat_file)

        col_price = "active_price"
        col_sale = "monthly_sale_trend"
        col_brand = "brand"
        col_seller = "sellerID"

        # 过滤价格无效
        df[col_price] = pd.to_numeric(df[col_price], errors='coerce')
        df = df[df[col_price] > 0]

        cat_result = {}

        for time in times:
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

            # 按销量排序
            df_sorted = df.sort_values(by=f"sale_{time}", ascending=False)

            year = 2000 + int(str(time)[:2])
            month = int(str(time)[2:])
            month_str = f"{year}年{month}月"

            # Top50
            top50 = df_sorted.head(50)
            cat_result[f"Top50销量_{month_str}"] = top50[f"sale_{time}"].sum()
            cat_result[f"Top50销售额_{month_str}"] = (top50[f"sale_{time}"] * top50[col_price]).sum()
            cat_result[f"Top50店铺数_{month_str}"] = top50[col_seller].nunique()
            cat_result[f"Top50品牌数_{month_str}"] = top50[col_brand].nunique()

            # Top10
            top10 = df_sorted.head(10)
            cat_result[f"Top10销量_{month_str}"] = top10[f"sale_{time}"].sum()
            cat_result[f"Top10销售额_{month_str}"] = (top10[f"sale_{time}"] * top10[col_price]).sum()

        results_dict[cat_id] = cat_result

    except Exception as e:
        with log_lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{cat_id} 出错: {e}\n")
        print(f"❌ 出错 ({cat_id})：", e)


# ==== 多线程执行 ====
max_workers = 8
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    list(tqdm(executor.map(process_file, file_names), total=len(file_names), desc="Processing files"))

# ==== 保存为宽表 Excel（中文列名） ====
df_result = pd.DataFrame.from_dict(results_dict, orient='index')
df_result.index.name = '类目ID'
df_result.reset_index(inplace=True)
df_result.to_excel(output_file, index=False)
print(f"✅ 已导出结果到 {output_file}")
