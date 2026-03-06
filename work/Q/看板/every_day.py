import pandas as pd
import ast
from pathlib import Path
import os
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# ===================== 全局变量 =====================
TARGET_MONTH =2511
start_date = 20240901
end_date = 20251205
VISUAL_PLUS_FOLDER = Path(r"D:\data\看板\202512_sku_p_info_2")
OUTPUT_FOLDER = Path(r"D:\data\看板\every_day")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 100000
BATCH_QUERY_SIZE = 500

every_day_sku = get_collection("main_ml_mx", "ml_mx", "every_day_sku")

# ===================== 工具函数 =====================
def safe_eval_dict(x):
    """安全将字符串转字典"""
    if isinstance(x, dict):
        return x
    if pd.isna(x) or x in ("", "nan", None):
        return {}
    try:
        return ast.literal_eval(x)
    except Exception:
        return {}

def to_list_safe(x):
    """安全将字符串转列表"""
    if isinstance(x, list):
        return x
    if pd.isna(x) or x in ("", None):
        return []
    try:
        return ast.literal_eval(x)
    except:
        return []

def chunked_mongo_query(collection, sl_list, batch_size=BATCH_QUERY_SIZE):
    data = []
    for i in range(0, len(sl_list), batch_size):
        batch = sl_list[i:i+batch_size]
        try:
            query = {
                "sl": {"$in": batch},
                "dT": {"$gte": start_date, "$lte": end_date}
            }
            projection = {"_id": 0, "dT": 1, "oD": 1, "pR": 1, "sl": 1}
            batch_data = list(collection.find(query, projection))
            projection = {"_id": 0, "dT": 1, "oD": 1, "pR": 1, "sl": 1}
            batch_data = list(collection.find(query, projection))
            print(f"Mongo 查询: batch {i//batch_size + 1}, 返回 {len(batch_data)} 条记录")
            data.extend(batch_data)
        except Exception as e:
            print(f"Mongo 查询异常: {e} for batch {i//batch_size + 1}")
    return data

def merge_od_pr(row):
    try:
        dt_list = to_list_safe(row.get("dT", []))
        oD_list = to_list_safe(row.get("oD", []))
        pR_list = to_list_safe(row.get("pR", []))

        if dt_list and oD_list and len(dt_list) != len(oD_list):
            print(f"长度不匹配 oD: sl={row.get('sl')} len(dT)={len(dt_list)} len(oD)={len(oD_list)}")
            od_dict = {}
        else:
            od_dict = dict(zip(dt_list, oD_list)) if dt_list and oD_list else {}

        if dt_list and pR_list and len(dt_list) != len(pR_list):
            print(f"长度不匹配 pR: sl={row.get('sl')} len(dT)={len(dt_list)} len(pR)={len(pR_list)}")
            pr_dict = {}
        else:
            pr_dict = dict(zip(dt_list, pR_list)) if dt_list and pR_list else {}

        return pd.Series([od_dict, pr_dict])
    except Exception as e:
        print(f"merge_od_pr error: {e} for sl={row.get('sl')}")
        return pd.Series([{}, {}])

# ===================== 处理单文件 =====================
def process_file(csv_file: Path):
    print(f"\n处理文件: {csv_file.name}")

    top_sku_set = set()

    reader = pd.read_csv(csv_file, chunksize=CHUNK_SIZE)
    for chunk in tqdm(reader, desc="读取 CSV"):
        if "monthly_sale_trend" not in chunk.columns:
            print(f"跳过 {csv_file.name}，缺少 monthly_sale_trend 字段")
            return

        chunk["monthly_sale_trend"] = chunk["monthly_sale_trend"].apply(safe_eval_dict)
        chunk[f"sale_{TARGET_MONTH}"] = chunk["monthly_sale_trend"].apply(lambda x: x.get(TARGET_MONTH, 0))
        chunk_sorted = chunk.sort_values(f"sale_{TARGET_MONTH}", ascending=False)

        # Top SKU
        top_sku_set.update(chunk_sorted.head(100)["sku_id"].tolist())
        top_sku_set.update(chunk_sorted.groupby("stock_type").head(100)["sku_id"].tolist())

    # 读取整表获取品牌/卖家 Top10
    df_full = pd.read_csv(csv_file)
    df_full["monthly_sale_trend"] = df_full["monthly_sale_trend"].apply(safe_eval_dict)
    df_full[f"sale_{TARGET_MONTH}"] = df_full["monthly_sale_trend"].apply(lambda x: x.get(TARGET_MONTH, 0))

    brand_top10 = df_full.groupby("brand")[f"sale_{TARGET_MONTH}"].sum().nlargest(10).index
    seller_top10 = df_full.groupby("sellerName")[f"sale_{TARGET_MONTH}"].sum().nlargest(10).index

    top_sku_set.update(df_full[df_full["brand"].isin(brand_top10)]["sku_id"].tolist())
    top_sku_set.update(df_full[df_full["sellerName"].isin(seller_top10)]["sku_id"].tolist())

    all_sku = list(top_sku_set)
    print(f"{csv_file.name} - 需要导出的 SKU 数: {len(all_sku)}")

    # 查询 Mongo
    data = chunked_mongo_query(every_day_sku, all_sku)
    if not data:
        print(f"{csv_file.name} - Mongo 没有返回数据")
        return

    export_df = pd.DataFrame(data)
    # 1. 按 SKU 聚合 dT/oD/pR 列表
    agg_df = export_df.groupby("sl").agg({
        "dT": lambda x: list(x),
        "oD": lambda x: list(x),
        "pR": lambda x: list(x)
    }).reset_index()

    # 2. 生成字典
    agg_df[["oD", "pR"]] = agg_df.apply(merge_od_pr, axis=1)

    # 3. 保留最终列
    export_df_final = agg_df[["sl", "oD", "pR"]]
    print(export_df_final)

    # 导出 CSV
    output_file = OUTPUT_FOLDER / f"{csv_file.stem}_every_day.csv"
    export_df_final.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"导出完成: {output_file}")

# ===================== 批量处理 =====================
csv_files = list(VISUAL_PLUS_FOLDER.glob("*.csv"))
print(f"找到 {len(csv_files)} 个 CSV 文件")

# 单文件测试
# csv_files = [Path(r"D:\data\看板\202512_sku_p_info_2\MLM1073.csv")]

for csv_file in csv_files:
    process_file(csv_file)

print("\n所有文件处理完成！")
