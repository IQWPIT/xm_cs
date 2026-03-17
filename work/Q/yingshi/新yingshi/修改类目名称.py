# -*- coding: utf-8 -*-
import os
import json
from loguru import logger
from pymongo import UpdateOne
from collections import Counter
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
# =========================
# 配置
# =========================
SRC_MONTH = 202510
DST_MONTH = 202512
cat_ids = {
    'MLB7073': "ml_br",
    'MLB455251': "ml_br",
    'MLM437575': "ml_mx",
    'MLM455251': "ml_mx",
    'MLM420128': "ml_mx",
    'MLC5713': "ml_cl",
    'MCO5844': "ml_co",
    "MLA5959": "ml_ar",
    "MLA417835":"ml_ar",
}
# =========================
# 工具函数
# =========================
def normalize_categories(x):
    """
    保证 categories 最终是 dict / list
    """
    if isinstance(x, (dict)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return None
    return None
# =========================
# 主流程
# =========================
for cat_id, SITE in cat_ids.items():
    logger.info("====== 开始修复 | cat_id={} | site={} ======", cat_id, SITE)
    monthly_sku = get_collection(
        "yingshi",
        "yingshi",
        f"{SITE}_monthly_sku"
    )
    # =========================
    # 1️⃣ 取 SRC_MONTH 的干净 categories 作为模板
    # =========================
    src_doc = monthly_sku.find_one(
        {
            "month": SRC_MONTH,
            "category_id": cat_id,
            "categories": {"$exists": True}
        },
        {"categories": 1}
    )
    if not src_doc:
        raise RuntimeError(
            f"找不到 month={SRC_MONTH} cat_id={cat_id} 的 categories 样本"
        )
    sample_categories = normalize_categories(src_doc.get("categories"))
    if sample_categories is None:
        raise RuntimeError(
            f"SRC month={SRC_MONTH} cat_id={cat_id} 的 categories 是脏数据，拒绝写库"
        )
    logger.info("使用 month={} 的 categories 作为模板", SRC_MONTH)
    # =========================
    # 2️⃣ 统计 DST_MONTH 原始 categories 类型（只读）
    # =========================
    cursor = monthly_sku.find(
        {"month": DST_MONTH, "category_id": cat_id},
        {"categories": 1}
    )
    type_counter = Counter()
    for doc in cursor:
        type_counter[type(doc.get("categories"))] += 1
    logger.info(
        "DST categories 类型分布 | cat_id={} | {}",
        cat_id,
        dict(type_counter)
    )
    # =========================
    # 3️⃣ 仅修复脏数据 + 删除 cpid
    # =========================
    bulk = []
    total = 0
    dirty_cursor = monthly_sku.find(
        {
            "month": DST_MONTH,
            "category_id": cat_id,
            # 可根据需求开启下面条件判断脏数据
            # "$or": [
            #     {"categories": {"$exists": False}},
            #     {"categories": {"$type": "string"}},
            #     {"categories": {"$type": "null"}},
                # {"cpid": {"$exists": True}}
            # ]
        },
        {"_id": 1}
    )
    for doc in dirty_cursor:
        bulk.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {"categories": sample_categories},
                    # "$unset": {"cpid": ""}  # 删除 cpid 字段
                }
            )
        )
        total += 1
        if len(bulk) >= 1000:
            monthly_sku.bulk_write(bulk, ordered=False)
            bulk.clear()
    if bulk:
        monthly_sku.bulk_write(bulk, ordered=False)

    if total == 0:
        logger.warning(
            "month={} | cat_id={} | 未发现需要修复的数据",
            DST_MONTH,
            cat_id
        )
    else:
        logger.success(
            "categories 修复 + 删除 cpid 完成 | month={} | cat_id={} | 更新 {} 条",
            DST_MONTH,
            cat_id,
            total
        )
