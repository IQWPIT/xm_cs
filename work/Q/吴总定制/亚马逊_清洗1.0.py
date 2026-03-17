import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

import pandas as pd
from pymongo import UpdateOne
from dm.connector.mongo.manager3 import get_collection

# ================== MongoDB ==================
dst_col = get_collection("ml","customize_common","vivo_amazon_parsed_2")

# ================== 文件路径 ==================
file_path = r"D:\gg_xm\Q\吴总定制\亚马逊数据\亚马逊_20260302.xlsx"

# ================== 价格区间 ==================
price_ranges = [
    (0,1000), (1000,1500), (1500,2000), (2000,2500), (2500,3000),
    (3000,4000), (4000,5000), (5000,6000), (6000,8000), (8000,10000)
]

def get_price_range_index(price):
    for idx, (low, high) in enumerate(price_ranges):
        if low <= price < high:
            return idx
    return 10

def default_price_trend():
    return [{"od":0,"gmv":0} for _ in range(11)]

# ================== 读取主表 ==================
df = pd.read_excel(file_path, header=0)
df["ASIN"] = df["ASIN"].astype(str).str.strip()
df["ParentASIN"] = df["ParentASIN"].astype(str).str.strip()
df["价格"] = pd.to_numeric(df["实际价格(R$)"], errors="coerce").fillna(0)

df["主图"] = df["主图"].astype(str).str.strip()
df["URL"] = df["URL"].astype(str).str.strip()
df["评价数量"] = df["评价数量"].astype(str).str.strip()
df["评分星级"] = df["评分星级"].astype(str).str.strip()

# ================== 读取销量趋势 ==================
df2 = pd.read_excel(file_path, sheet_name="销量趋势", header=0)
df2["ASIN"] = df2["ASIN"].astype(str).str.strip()
df2["ParentASIN"] = df2["ParentASIN"].astype(str).str.strip()

# ================== merge ==================
merged = pd.merge(df, df2, on=["ASIN","ParentASIN"], how="left")

month_cols = [col for col in df2.columns if col not in ["ASIN","ParentASIN"]]

updates = []

for _, row in merged.iterrows():

    price = row["价格"]
    price_idx = get_price_range_index(price)

    trend_dict = {}
    binfo_dict = {}
    price_trend_dict = {}

    for month in month_cols:

        if pd.isna(row[month]):
            continue

        month_key = month.replace("-", "")
        order30d = int(row[month])
        gmv = int(price * order30d)

        # === trend ===
        trend_dict[month_key] = order30d

        # === bInfo ===
        binfo_dict[month_key] = {"order": order30d, "gmv": gmv}

        # === priceTrend ===
        pt = default_price_trend()
        pt[price_idx]["od"] = order30d
        pt[price_idx]["gmv"] = gmv
        price_trend_dict[month_key] = {"priceTrend":pt,"gmv":gmv,"order":order30d}

    update_doc = {

        "picture": row["主图"],
        "URL": row["URL"],
        "score_count": row["评价数量"],
        "score": row["评分星级"],

        "sku_id": row["ASIN"],
        "sellerID": row["ParentASIN"],
        "spu_title": row.get("产品名称"),
        "brand": row.get("品牌"),
        "price": price,
        # "trend": trend_dict,
        "bInfo": price_trend_dict,
        # "bInfo.gmv": gmv,
        # "bInfo.order": order30d,
        "order_gmv": binfo_dict,
    }

    updates.append(
        UpdateOne(
            {
                # "sku_id": row["ASIN"],
                "sellerID": row["ParentASIN"]
            },
            {"$set": update_doc},
            upsert=True
        )
    )

# ================== 批量写入 MongoDB ==================
if updates:
    result = dst_col.bulk_write(updates, ordered=False)
    print("写入完成")
    print("插入:", result.upserted_count)
    print("修改:", result.modified_count)
else:
    print("没有数据需要写入")