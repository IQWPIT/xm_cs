# -*- coding: utf-8 -*-
import os
import re
from calendar import monthrange
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
from datetime import datetime, timezone, timedelta
# ================= Mongo 连接 =================
visual_plus_col = get_collection("main_ml_mx", "ml_mx", "visual_plus")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
every_day_sku_col = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
_big_Excel_col = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
scrapy_buffer_col = get_collection("slave_ml_mx", "ml_scrapy_buffer", "ml_mx")

def prev_month(yyyymm: int) -> int:
    year = yyyymm // 100
    month = yyyymm % 100
    if month == 1:
        return (year - 1) * 100 + 12
    else:
        return year * 100 + (month - 1)

cat_ids = [doc["cat_id"] for doc in visual_plus_col.find({}, {"cat_id":1})]
# cat_ids = ["MLM116384"]
print(f"总类目数: {len(cat_ids)}")

time_list = [
    202411, 202412, 202501, 202502, 202503, 202504, 202505, 202506,
    202507, 202508, 202509, 202510,
    202511,
    202512
]
for cat_id in cat_ids:
    bulk = []
    for t in time_list:
        prev_t = prev_month(t)

        cur_doc = _big_Excel_col.find_one(
            {"cat_id": cat_id, "time": t},
            {"top50_month_sales": 1,
             "total_sales":1}
        )
        prev_doc = _big_Excel_col.find_one(
            {"cat_id": cat_id, "time": prev_t},
            {"top50_month_sales": 1,
             "total_sales":1}
        )

        # ===== 安全校验 =====
        if not cur_doc or not prev_doc:
            print(f"[SKIP] {cat_id} {t} 缺数据")
            continue

        cur_sales = cur_doc.get("total_sales", 0)
        prev_sales = prev_doc.get("total_sales", 0)

        if prev_sales == 0:
            print(f"[SKIP] {cat_id} {t} 上月为0")
            continue

        ratio = cur_sales / prev_sales

        print(f"{cat_id} {t} 环比: {ratio}")

        bulk.append(
            UpdateOne(
                {"cat_id": cat_id, "time": t},
                {"$set": {"total_sales_growth_index": ratio}}
            )
        )
    if bulk:
        _big_Excel_col.bulk_write(bulk, ordered=False)




