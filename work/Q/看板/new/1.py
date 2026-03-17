# -*- coding: utf-8 -*-
import os
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection


# ===================== Mongo 集合 =====================
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")  # 如果没用到可删除
mongo_uri = "mongodb://erp:Damai20230214*.*@42.193.215.253:27017/?authMechanism=DEFAULT&authSource=erp&directConnection=true"

client = MongoClient(mongo_uri)
db = client["erp"]

bi_site_data = db["bi_category_data"]
bi_site_data_ml_mx = get_collection("main_ml_mx", "ml_mx", "bi_category_data")


MONTHS = ["202602", "202601", "202512"]
BATCH_SIZE = 1000


def sync_month_data(month: str):
    """
    同步单个月份数据：
    从 erp.bi_category_data 拉取 month 对应数据，
    upsert 到 main_ml_mx.ml_mx.bi_category_data
    """
    cursor = bi_site_data.find(
        {"month": month},
        no_cursor_timeout=True
    ).batch_size(BATCH_SIZE)

    ops = []
    total = 0

    try:
        for data in tqdm(cursor, desc=f"处理月份 {month}"):
            # 避免把源表 _id 写入目标表
            data.pop("_id", None)

            cat_id = data.get("cat_id")
            if not cat_id:
                continue

            ops.append(
                UpdateOne(
                    {"cat_id": cat_id, "month": month},
                    {"$set": data},
                    upsert=True
                )
            )

            if len(ops) >= BATCH_SIZE:
                bi_site_data_ml_mx.bulk_write(ops, ordered=False)
                total += len(ops)
                ops = []

        if ops:
            bi_site_data_ml_mx.bulk_write(ops, ordered=False)
            total += len(ops)

    finally:
        cursor.close()

    return total


def main():
    grand_total = 0
    for month in tqdm(MONTHS, desc="月份进度"):
        count = sync_month_data(month)
        grand_total += count
        print(f"{month} 同步完成，写入/更新 {count} 条")

    print(f"全部完成，总写入/更新 {grand_total} 条")


if __name__ == "__main__":
    main()