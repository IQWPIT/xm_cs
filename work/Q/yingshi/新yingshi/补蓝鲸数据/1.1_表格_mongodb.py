# -*- coding: utf-8 -*-
import os
import json
import pathlib
import re
from loguru import logger
from pymongo import UpdateOne

# =========================
# 环境
# =========================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =========================
# 参数配置
# =========================
SITE = "ml_ar"
TIME = 202512
CAT_ID = "MLA5959"

DATA_DIR = pathlib.Path(f"{SITE}_datas/{CAT_ID}")
BATCH_SIZE = 1000

# =========================
# Mongo collection
# =========================
c_monthly_sku = get_collection(
    "yingshi",
    "yingshi",
    f"{TIME}_{SITE}_{CAT_ID}",
)

# =========================
# 正则（关键修复点）
# =========================
ITEM_ID_RE = re.compile(r"商品ID\s*[:：]\s*(MLA\d+)")
FOLLOWED_ID_RE = re.compile(r"被跟卖商品ID\s*[:：]\s*(\d+)")

# =========================
# 解析 JSON → mapping
# =========================
def extract_followed_map() -> dict[str, str]:
    mapping: dict[str, str] = {}

    files = list(DATA_DIR.glob("*.json"))
    if not files:
        logger.warning("未找到任何 json 文件")
        return mapping

    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                texts = json.load(f)
        except Exception as e:
            logger.warning(f"{path} 解析失败：{e!r}")
            continue

        for text in texts:
            # 类型保护
            if not isinstance(text, str):
                continue

            # 快速剪枝
            if "被跟卖商品ID" not in text:
                continue

            item_m = ITEM_ID_RE.search(text)
            followed_m = FOLLOWED_ID_RE.search(text)

            if not item_m or not followed_m:
                continue

            item_id = item_m.group(1)
            followed_id = followed_m.group(1)

            # 去重：同一个商品只保留第一次
            mapping.setdefault(item_id, followed_id)

    logger.info(f"解析得到 {len(mapping)} 条 被跟卖关系")
    return mapping

# =========================
# 批量更新 Mongo
# =========================
def bulk_update_followed(mapping: dict[str, str]):
    if not mapping:
        logger.warning("mapping 为空，跳过更新")
        return

    ops = []
    total_matched = 0
    total_modified = 0

    for 商品ID, 被跟卖商品ID in mapping.items():
        ops.append(
            UpdateOne(
                {
                    "商品ID": 商品ID,
                    "month": TIME,   # ⚠️ 非常重要，避免误更新历史数据
                },
                {
                    "$set": {
                        "被跟卖商品ID": 被跟卖商品ID
                    }
                },
            )
        )

        if len(ops) >= BATCH_SIZE:
            r = c_monthly_sku.bulk_write(ops, ordered=False)
            total_matched += r.matched_count
            total_modified += r.modified_count
            ops.clear()

    if ops:
        r = c_monthly_sku.bulk_write(ops, ordered=False)
        total_matched += r.matched_count
        total_modified += r.modified_count

    logger.info(
        f"Mongo 更新完成 | matched={total_matched} | modified={total_modified}"
    )

# =========================
# main
# =========================
def main():
    mapping = extract_followed_map()
    bulk_update_followed(mapping)

if __name__ == "__main__":
    main()
