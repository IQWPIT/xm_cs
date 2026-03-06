# -*- coding: utf-8 -*-
import os
import datetime
from loguru import logger
from pymongo import UpdateOne

# =========================
# 环境变量
# =========================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =========================
# 参数
# =========================
SITE = "ml_ar"
CAT_ID = "MLA417835"
TIME = 202512
BATCH_SIZE = 500

# =========================
# Mongo
# =========================
c_rawdata = get_collection("yingshi", "yingshi", f"{TIME}_{SITE}_{CAT_ID}")
c_sku = get_collection(f"main_{SITE}", SITE, f"{SITE}_sku")
c_monthly = get_collection("yingshi", "yingshi", f"{SITE}_monthly_sku")

# =========================
# 预取 categories（只查一次）
# =========================
sku_doc = c_sku.find_one(
    {"category_id": CAT_ID},
    {"categories": 1}
)
categories = sku_doc.get("categories") if sku_doc else None
logger.info(f"categories loaded: {categories is not None}")

# =========================
# 主逻辑
# =========================
req = []
now_str = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")

# Mongo client（用于 session）
client = c_rawdata.database.client

with client.start_session() as session:
    cursor = c_rawdata.find(
        {},
        no_cursor_timeout=True,
        session=session
    ).batch_size(500)

    try:
        for ind, raw in enumerate(cursor):
            if ind % 1000 == 0:
                logger.info(f"processing {ind} docs")

            # ---------- 上架日期 ----------
            try:
                onsaledate = int(
                    datetime.datetime.strptime(
                        raw.get("上架日期", ""),
                        "%Y-%m-%dT%H:%M:%S"
                    ).strftime("%Y%m%d")
                )
            except Exception:
                onsaledate = None

            # ---------- 月销量 ----------
            monthlyorder = raw.get(f"202512月销量", 0)
            if not isinstance(monthlyorder, (int, float)):
                monthlyorder = 0

            # ---------- activeprice ----------
            try:
                activeprice = raw["销售额"] / raw["30天销量"]
            except Exception:
                activeprice = raw.get("价格", 0) or 0

            # ---------- update 内容 ----------
            update = {
                "j": 0,
                "categories": categories,
                "sellerName": raw.get("店铺名称"),
                "sellerID": None,
                "title": raw.get("商品名称"),
                "url": raw.get("商品链接"),
                "category_id": CAT_ID,
                "onsaledate": onsaledate,
                "brand": raw.get("品牌"),
                "activeprice": activeprice,
                "monthlyorder": monthlyorder,
                "monthlygmv": round(monthlyorder * activeprice, 2),
                "totalorder": 0,    # TODO
                "totalgmv": 0,      # TODO
                "monthlyreview": 0,
                "review_num": 0,
                "lanjing_source": True,
                "lanjing_source_update_time": now_str,
            }

            # ---------- filter ----------
            if "被跟卖商品ID" in raw:
                _filter = {
                    "sku_id": None,
                    "item_id": raw.get("商品ID"),
                    "month": TIME,
                }
            else:
                _filter = {
                    "sku_id": raw.get("商品ID"),
                    "item_id": None,
                    "month": TIME,
                }

            # 商品ID 兜底校验
            if not (_filter.get("sku_id") or _filter.get("item_id")):
                continue

            req.append(
                UpdateOne(
                    _filter,
                    {"$setOnInsert": update},
                    upsert=True
                )
            )

            # ---------- bulk flush ----------
            if len(req) >= BATCH_SIZE:
                c_monthly.bulk_write(req, ordered=False)
                req.clear()

        # ---------- final flush ----------
        if req:
            c_monthly.bulk_write(req, ordered=False)

    finally:
        cursor.close()

logger.success("monthly sku sync finished")
