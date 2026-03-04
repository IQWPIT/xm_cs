import os
import datetime
from loguru import logger
from pymongo import UpdateOne
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
from dm.ml.items import GetItemsAPIDataV2
import argparse

# =========================
# 命令行参数
# =========================
parser = argparse.ArgumentParser(description="补充蓝鲸数据到 MongoDB")
parser.add_argument("--site", type=str,default="ml_cl",  help="站点，例如 ml_cl")
parser.add_argument("--cat_id", type=str, default="MLC5713",  help="分类ID，例如 MLC5713")
parser.add_argument("--time", type=int, default=202512,  help="年月，例如 202512")
parser.add_argument("--bulk_size", type=int, default=100, help="批量写入大小")

args = parser.parse_args()

SITE = args.site
CAT_ID = args.cat_id
TIME = args.time
BULK_SIZE = args.bulk_size

# =========================
# MongoDB collection
# =========================
c_yingshi_lanjing_rawdata = get_collection("yingshi", "yingshi", f"{TIME}_{SITE}_{CAT_ID}")
c_sku = get_collection(f"main_{SITE}", SITE, f"sku")
c_yingshi_monthly_sku = get_collection("yingshi", "yingshi", f"{SITE}_monthly_sku")

# 获取分类信息
categories = c_sku.find_one({"category_id": CAT_ID}, {"categories": 1})["categories"]

# =========================
# 已存在 SKU 集合
# =========================
existing_skus = {doc["sku_id"] for doc in c_yingshi_monthly_sku.find({}, {"sku_id": 1, "_id": 0})}
existing_item = {doc["item_id"] for doc in c_yingshi_monthly_sku.find({}, {"item_id": 1, "_id": 0})}
# =========================
# 函数封装
# =========================
def parse_onsaledate(date_str):
    try:
        return int(datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%d"))
    except Exception:
        return None

def get_active_price_and_item(sku_id):
    try:
        doc = c_sku.find_one({"$or": [{"sku_id": sku_id}, {"offersInf.item_id": sku_id}]},
                             {"active_price": 1, "sku_id": 1, "item_id": 1})
        if doc:
            return doc["sku_id"], doc.get("item_id", 0), doc.get("active_price")

        api_fields = [
            "id",
            "initial_quantity",
            "seller_id",
            "date_created",
            "thumbnail_id",
            "catalog_listing",
            "catalog_product_id",
        ]
        api = GetItemsAPIDataV2()
        data = api(item_ids=sku_id, fields=api_fields, return_raw_res=True)
        if not data or not data[0].get("catalog_product_id"):
            logger.warning(f"API未返回有效 item_id: {sku_id}")
            return sku_id,None,0,
        return data[0].get("catalog_product_id", None),sku_id, None

    except Exception as e:
        logger.warning(f"获取 SKU 数据失败, 绕过: {sku_id}, error={e!r}")
        return sku_id, None, None

# =========================
# 批量插入 MongoDB
# =========================
req = []

for ind, raw in enumerate(c_yingshi_lanjing_rawdata.find().batch_size(100)):
    sku_id = raw["商品ID"]
    if sku_id in existing_skus or sku_id in existing_item:
        continue


    logger.info(f"{ind=}, {sku_id=}, title={raw.get('商品名称')}")

    onsaledate = parse_onsaledate(raw.get("上架日期"))

    monthlyorder = raw.get(f"{TIME}月销量") or 0
    monthlyorder = int(monthlyorder) if isinstance(monthlyorder, int) else 0

    sku_id_s, item_id_s, active_price_api = get_active_price_and_item(sku_id)

    activeprice = raw.get("价格") or active_price_api or 0
    monthlygmv = round(monthlyorder * activeprice, 2)
    qqq = c_yingshi_monthly_sku.find_one({"sku_id": sku_id_s}, {"totalorder": 1, "totalgmv": 1})
    if qqq is None:
        totalorder = monthlyorder
        totalgmv = monthlygmv
    else:
        totalorder = qqq.get("totalorder", 0) + monthlyorder
        totalgmv = qqq.get("totalgmv", 0) + monthlygmv

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
        "monthlyorder": totalorder,
        "monthlygmv": totalgmv,
        "totalorder": 0,
        "totalgmv": 0,
        "monthlyreview": 0,
        "review_num": 0,
        "lanjing_source": True,
        "lanjing_source_update_time": datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"),
        "item_id": item_id_s,
        "sku_id": sku_id_s,
    }

    _filter = {"sku_id": sku_id_s, "month": TIME}
    req.append(UpdateOne(_filter, {"$setOnInsert": update}, upsert=True))

    if len(req) >= BULK_SIZE:
        c_yingshi_monthly_sku.bulk_write(req)
        req.clear()

if req:
    c_yingshi_monthly_sku.bulk_write(req)

logger.info("MongoDB 数据更新完成")
