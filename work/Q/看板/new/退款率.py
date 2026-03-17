# -*- coding: utf-8 -*-
import os
from collections import defaultdict
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from pymongo.errors import BulkWriteError

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
MONTHS = [
    "202501", "202502", "202503", "202504", "202505", "202506",
    "202507", "202508", "202509", "202510", "202511", "202512",
    "202601",
]

SOURCE_BATCH_SIZE = 3000       # 读源表 batch_size
FLUSH_CAT_COUNT = 2000         # 聚合多少个 cat_id 后刷一次写入
WRITE_ORDERED = False          # 无序写入更快


def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def flush_updates(update_map, stats):
    """
    将 update_map 中按 cat_id 聚合好的更新一次性 bulk_write 到 visual_plus
    """
    if not update_map:
        return

    ops = []
    for cat_id, set_fields in update_map.items():
        ops.append(
            UpdateOne(
                {"cat_id": cat_id},
                {"$set": set_fields},
                upsert=False
            )
        )

    try:
        result = visual_plus.bulk_write(ops, ordered=WRITE_ORDERED)
        stats["write_batches"] += 1
        stats["submitted_ops"] += len(ops)
        stats["matched"] += result.matched_count
        stats["modified"] += result.modified_count
    except BulkWriteError as e:
        stats["write_batches"] += 1
        stats["submitted_ops"] += len(ops)
        stats["bulk_errors"] += 1

        details = e.details or {}
        stats["matched"] += details.get("nMatched", 0)
        stats["modified"] += details.get("nModified", 0)

        write_errors = details.get("writeErrors", [])
        print(f"\n[BulkWriteError] 本批次失败 {len(write_errors)} 条")
        if write_errors:
            print("示例错误：", write_errors[0])

    update_map.clear()


def main():
    stats = {
        "source_total": 0,      # 源数据总行数
        "skipped": 0,           # 跳过行数
        "submitted_ops": 0,     # 实际提交到 bulk_write 的 UpdateOne 数
        "matched": 0,           # 匹配到的 visual_plus 文档数
        "modified": 0,          # 实际修改数
        "write_batches": 0,     # 写入批次数
        "bulk_errors": 0,       # bulk error 次数
    }

    # 按 cat_id 聚合，减少重复写
    # 结构：
    # {
    #   "cat1": {
    #       "ad_click_rate2.202501": 123,
    #       "avg_ad_cost_ratio2.202501": 45,
    #       ...
    #   }
    # }
    update_map = defaultdict(dict)

    query = {"month": {"$in": MONTHS}}
    projection = {
        "_id": 0,
        "month": 1,
        "cat_id": 1,
        "clicks": 1,
        "cost": 1,
        "cpc": 1,
        "refund_rate": 1,
    }

    print("开始读取源数据并聚合更新字段...")

    cursor = bi_site_data.find(
        query,
        projection,
        no_cursor_timeout=True
    ).batch_size(SOURCE_BATCH_SIZE)

    try:
        for d in tqdm(cursor, desc="处理 bi_site_data"):
            stats["source_total"] += 1

            cat_id = d.get("cat_id")
            month = d.get("month")

            if not cat_id or not month:
                stats["skipped"] += 1
                continue

            month = str(month)
            if month not in MONTHS:
                stats["skipped"] += 1
                continue

            clicks = safe_float(d.get("clicks"), 0)
            cost = safe_float(d.get("cost"), 0)
            cpc = safe_float(d.get("cpc"), 0)
            refund_rate = safe_float(d.get("refund_rate"), 0)

            # 同一个 cat_id 的不同月份合并到一次更新里
            update_map[cat_id][f"ad_click_rate2.{month}"] = clicks
            update_map[cat_id][f"avg_ad_cost_ratio2.{month}"] = cost
            update_map[cat_id][f"avg_cpc_usd2.{month}"] = cpc
            update_map[cat_id][f"avg_refund_rate2.{month}"] = refund_rate

            # 聚合到一定 cat_id 数量后刷库
            if len(update_map) >= FLUSH_CAT_COUNT:
                flush_updates(update_map, stats)

        # 最后一批
        flush_updates(update_map, stats)

    finally:
        cursor.close()

    print("\n全部完成：")
    print(f"总源数据: {stats['source_total']}")
    print(f"总跳过: {stats['skipped']}")
    print(f"总提交更新: {stats['submitted_ops']}")
    print(f"总匹配到: {stats['matched']}")
    print(f"总实际修改: {stats['modified']}")
    print(f"总写入批次: {stats['write_batches']}")
    print(f"bulk_write 异常批次: {stats['bulk_errors']}")


if __name__ == "__main__":
    main()