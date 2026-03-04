import os
import pandas as pd
from pymongo import MongoClient

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# ==============================
# 集合
# ==============================
sku_col = get_collection("ml", "customize_common", "wu_custom_made_shopee")

all_data = []

for doc in sku_col.find({"monthly_sale": {"$exists": True}}, {"item_id": 1, "monthly_sale": 1}):
    item_id = doc["item_id"]
    for month, sale in doc["monthly_sale"].items():
        try:
            sale = int(sale)
        except:
            continue
        all_data.append({
            "item_id": item_id,
            "month": month,
            "monthly_sale": sale
        })

df = pd.DataFrame(all_data)

if df.empty:
    print("⚠️ 没有月度销量数据，无法生成趋势")
else:
    monthly_summary = df.groupby("month")["monthly_sale"].sum().reset_index()
    monthly_summary = monthly_summary.sort_values("month")
    monthly_summary.to_csv("monthly_sale_summary.csv", index=False)
    print("✅ 月度销量趋势已输出到 monthly_sale_summary.csv")