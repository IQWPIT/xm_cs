# -*- coding: utf-8 -*-
import os
from tqdm import tqdm
from pymongo import UpdateOne
from pymongo import MongoClient
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# ===================== Mongo 集合 =====================
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
mongo_uri = "mongodb://erp:Damai20230214*.*@42.193.215.253:27017/?authMechanism=DEFAULT&authSource=erp&directConnection=true"
client = MongoClient(mongo_uri)
db = client["erp"]
bi_site_data = db["bi_category_data"]
# ===================== 配置 =====================
MONTHS = ["202512"]
BATCH_SIZE = 1000


def safe_float(v, default=0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def main():
    total_source = 0
    total_ops = 0
    total_modified = 0
    total_matched = 0
    total_skipped = 0

    for t in MONTHS:
        print(f"\n开始处理月份: {t}")

        cursor = bi_site_data.find(
            {"month": t},
            {
                "_id": 0,
                "cat_id": 1,
                "clicks": 1,
                "cost": 1,
                "cpc": 1,
                "refund_rate": 1,
            },
            no_cursor_timeout=True
        ).batch_size(BATCH_SIZE)

        ops = []
        month_source = 0
        month_skipped = 0
        month_matched = 0
        month_modified = 0

        try:
            for d in tqdm(cursor, desc=f"处理 {t}"):
                month_source += 1
                total_source += 1

                cat_id = d.get("cat_id")
                if not cat_id:
                    month_skipped += 1
                    total_skipped += 1
                    continue

                clicks = safe_float(d.get("clicks"), 0)
                cost = safe_float(d.get("cost"), 0)
                cpc = safe_float(d.get("cpc"), 0)
                refund_rate = safe_float(d.get("refund_rate"), 0)

                ops.append(
                    UpdateOne(
                        {"cat_id": cat_id},
                        {
                            "$set": {
                                f"ad_click_rate2.{t}": clicks,
                                f"avg_ad_cost_ratio2.{t}": cost,
                                f"avg_cpc_usd2.{t}": cpc,
                                f"avg_refund_rate2.{t}": refund_rate,
                            }
                        },
                        upsert=False
                    )
                )

                if len(ops) >= BATCH_SIZE:
                    result = visual_plus.bulk_write(ops, ordered=False)
                    month_matched += result.matched_count
                    month_modified += result.modified_count
                    total_matched += result.matched_count
                    total_modified += result.modified_count
                    total_ops += len(ops)
                    ops = []

            if ops:
                result = visual_plus.bulk_write(ops, ordered=False)
                month_matched += result.matched_count
                month_modified += result.modified_count
                total_matched += result.matched_count
                total_modified += result.modified_count
                total_ops += len(ops)

        finally:
            cursor.close()

        print(
            f"月份 {t} 处理完成 | "
            f"源数据: {month_source} | "
            f"跳过: {month_skipped} | "
            f"提交更新: {month_source - month_skipped} | "
            f"匹配到: {month_matched} | "
            f"实际修改: {month_modified}"
        )

    print("\n全部完成：")
    print(f"总源数据: {total_source}")
    print(f"总跳过: {total_skipped}")
    print(f"总提交更新: {total_ops}")
    print(f"总匹配到: {total_matched}")
    print(f"总实际修改: {total_modified}")


if __name__ == "__main__":
    main()