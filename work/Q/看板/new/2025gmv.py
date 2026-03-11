# -*- coding: utf-8 -*-
import os
from tqdm import tqdm
from pymongo import UpdateOne

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# ===================== Mongo 集合 =====================
big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
visualize_table = get_collection("main_ml_mx", "ml_mx", "visualize_table")

datas = visualize_table.find(
    {},
    {
        "monthly_gmv_trend": 1,
        "cat_id": 1
    }
)

months = [f"{i:02d}" for i in range(1, 13)]

ops = []
batch_size = 1000

for data in tqdm(datas):

    cat_id = data.get("cat_id")
    gmv = data.get("monthly_gmv_trend", {})

    if not cat_id or not gmv:
        continue

    gmv_2024 = 0
    gmv_2025 = 0

    for m in months:

        month_2024 = f"2024{m}"
        month_2025 = f"2025{m}"

        gmv_2024 += gmv.get(month_2024, 0)
        gmv_2025 += gmv.get(month_2025, 0)

    ratio = gmv_2025 / gmv_2024 if gmv_2024 else 0

    ops.append(
        UpdateOne(
            {"cat_id": cat_id},
            {"$set": {"2025_gmv_growth_order_ratio": ratio}},
            upsert=False
        )
    )

    if len(ops) >= batch_size:
        big_Excel.bulk_write(ops, ordered=False)
        ops = []

# 最后一批
if ops:
    big_Excel.bulk_write(ops, ordered=False)

print("Finished")