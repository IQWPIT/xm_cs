# -*- coding: utf-8 -*-

import re
from tqdm import tqdm
from pymongo import UpdateOne
import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection
sku = get_collection("main_ml_mx","ml_mx","sku")
ml_co_item_stock = get_collection("task_buffer","task_buffer","ml_co_item_stock")
# =============================
# 解析函数
# =============================

def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def parse_price_br(value):
    """
    巴西价格:
    'R$ 4.149,00' -> 4149.0
    """
    if value is None:
        return 0.0

    s = str(value).strip()
    s = s.replace("R$", "").replace("\xa0", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def parse_score(value):
    """
    '48' -> 4.8
    '4,8' -> 4.8
    """
    if value is None:
        return 0.0

    s = str(value).strip().replace(",", ".")

    if s.isdigit() and len(s) == 2:
        return float(f"{s[0]}.{s[1]}")

    try:
        return float(s)
    except Exception:
        return 0.0


def parse_rating_count(value):
    """
    '(8.740)' -> 8740
    """
    if value is None:
        return 0

    s = str(value).strip("() ").replace(".", "").replace(",", "")

    try:
        return int(s)
    except Exception:
        return 0


def parse_sales_30d(value):
    """
    'Mais de 1 mil compras' -> 1000
    'Mais de 2,5 mil compras' -> 2500
    """
    if value is None:
        return 0

    s = str(value).lower().replace("\xa0", " ")

    m = re.search(r"(\d+(?:[.,]\d+)?)\s*mil", s)
    if m:
        num = m.group(1).replace(",", ".")
        try:
            return int(float(num) * 1000)
        except Exception:
            return 0

    m = re.search(r"\d+", s)
    if m:
        try:
            return int(m.group())
        except Exception:
            return 0

    return 0


def parse_stock(value):
    """
    'Somente 1 em estoque.' -> 1
    其他情况无数字时返回 None
    """
    if not value:
        return None

    s = str(value)
    m = re.search(r"(\d+)", s)

    if m:
        try:
            return int(m.group())
        except Exception:
            return None

    return None


def normalize_crawl_time(crawl_time):
    """
    保存成 YYYYMMDD 格式
    例如:
    '2026-03-03 10:33:39' -> '20260303'
    None -> '20260301'
    """
    if not crawl_time:
        return "20260301"

    s = str(crawl_time).strip()
    if not s:
        return "20260301"

    s = s.split(" ")[0].strip()
    s = s.replace("-", "").replace("/", "").replace(".", "")

    m = re.search(r"(\d{8})", s)
    if m:
        return m.group(1)

    return "20260301"


def extract_main_result(doc):
    json_list = safe_get(doc, "response", "data", "json", default=[])

    if not json_list:
        return {}

    results = safe_get(json_list[0], "data", "results", default=[])

    if not results:
        return {}

    return results[0]


def extract_brand(item):
    brand = (item.get("brand") or "").strip()

    if brand:
        return brand

    for x in item.get("productOverview", []) or []:
        if x.get("key") == "Marca":
            return (x.get("value") or "").strip()

    return ""


# =============================
# 标准化结构
# =============================

def standardize_amazon_doc(doc):
    item = extract_main_result(doc)

    if not item:
        return None

    asin = item.get("asin") or doc.get("asin")
    if not asin:
        return None

    crawl_time = normalize_crawl_time(doc.get("crawl_time"))

    seller = item.get("seller") or {}
    seller_id = seller.get("id") or item.get("merchant_id")
    seller_name = seller.get("name")

    brand = extract_brand(item)
    title = item.get("title")
    url = safe_get(doc, "response", "data", "url")

    price = parse_price_br(item.get("price"))
    original_price = parse_price_br(
        safe_get(item, "strikethroughPrice", "value")
    )

    sales_text = item.get("sales")
    sales_30d = parse_sales_30d(sales_text)

    rating_score = parse_score(item.get("star"))
    rating_count = parse_rating_count(item.get("rating"))

    stock_text = item.get("inStock")
    stock_qty = parse_stock(stock_text)

    image = item.get("image")

    return {
        "asin": asin,
        "sku_id": asin,
        "crawl_time": crawl_time,

        "seller_id": seller_id,
        "seller_name": seller_name,

        "brand": brand,
        "title": title,

        "url": url,
        "picture": image,

        "price": price,
        "original_price": original_price,

        "sales_30d_text": sales_text,
        "sales_30d_est": sales_30d,

        "rating_score": rating_score,
        "rating_count": rating_count,

        "stock_text": stock_text,
        "stock_qty": stock_qty,
    }


def build_update_fields(doc):
    """
    构造 MongoDB 的 $set 更新字段
    一条 asin 一条文档，每个 crawl_time 保存一份快照
    """
    crawl_time = doc["crawl_time"]

    update_fields = {
        "asin": doc["asin"],
        "sku_id": doc["sku_id"],

        "seller_id": doc["seller_id"],
        "seller_name": doc["seller_name"],

        "brand": doc["brand"],
        "title": doc["title"],
        "url": doc["url"],
        "picture": doc["picture"],

        "original_price": doc["original_price"],
        f"rating_score.{crawl_time}": doc["rating_score"],
        f"rating_count.{crawl_time}": doc["rating_count"],

        "last_crawl_time": crawl_time,

        f"sales_30d_text.{crawl_time}": doc["sales_30d_text"],
        f"order.{crawl_time}": {
            "sales_30d_est": doc["sales_30d_est"],
            "price": doc["price"]
        },
        f"stock_text.{crawl_time}": doc["stock_text"],
        f"stock_qty.{crawl_time}": doc["stock_qty"],
    }

    return update_fields


# =============================
# MongoDB
# =============================

amazon_item_detail = get_collection(
    "ml",
    "customize_common",
    "amazon_item_detail"
)

amazon_datas = get_collection(
    "ml",
    "customize_common",
    "amazon_datas"
)


# 一条 asin 一条文档，所以唯一索引改成 asin
try:
    amazon_datas.create_index(
        [("asin", 1)],
        unique=True,
        background=True
    )
except Exception as e:
    print("create_index error:", e)


# 如果你后面经常按品牌/卖家查，也可以补索引
try:
    amazon_datas.create_index([("brand", 1)], background=True)
    amazon_datas.create_index([("seller_id", 1)], background=True)
except Exception as e:
    print("extra index error:", e)


# =============================
# 读取并解析后写入
# =============================

BATCH_SIZE = 500
ops = []
total = 0
success = 0
skipped = 0
errors = 0

cursor = amazon_item_detail.find({}, no_cursor_timeout=True).batch_size(1000)

try:
    for raw_doc in tqdm(cursor, desc="Processing amazon_item_detail"):
        total += 1

        try:
            doc = standardize_amazon_doc(raw_doc)
            if not doc:
                skipped += 1
                continue

            update_fields = build_update_fields(doc)

            ops.append(
                UpdateOne(
                    {"asin": doc["asin"]},
                    {"$set": update_fields},
                    upsert=True
                )
            )

            if len(ops) >= BATCH_SIZE:
                result = amazon_datas.bulk_write(ops, ordered=False)
                success += result.upserted_count + result.modified_count
                ops = []

        except Exception as e:
            errors += 1
            print(f"[ERROR] asin={raw_doc.get('asin')} err={e}")

    if ops:
        result = amazon_datas.bulk_write(ops, ordered=False)
        success += result.upserted_count + result.modified_count

finally:
    cursor.close()

print(f"total={total}, success={success}, skipped={skipped}, errors={errors}")