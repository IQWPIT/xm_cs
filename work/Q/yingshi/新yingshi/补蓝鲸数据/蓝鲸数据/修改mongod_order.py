# -*- coding: utf-8 -*-
import os
import datetime
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from loguru import logger
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection


def to_int(v):
    try:
        return int(float(v))
    except Exception:
        return 0


def to_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def parse_onsaledate(v):
    """
    支持:
    - 2025-07-01T00:00:00
    - 2025-07-01
    返回: 20250701 / None
    """
    if not v:
        return None

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.datetime.strptime(str(v), fmt).strftime("%Y%m%d"))
        except Exception:
            pass
    return None


def flush_bulk(collection, ops, total_ops, flush_times):
    if not ops:
        return total_ops, flush_times

    try:
        res = collection.bulk_write(ops, ordered=False)
        total_ops += len(ops)
        flush_times += 1
        logger.info(
            f"bulk_write #{flush_times}: ops={len(ops)}, "
            f"matched={res.matched_count}, modified={res.modified_count}, "
            f"upserted={len(res.upserted_ids)}"
        )
    except BulkWriteError as e:
        logger.error(f"BulkWriteError details: {e.details}")
        raise
    finally:
        ops.clear()

    return total_ops, flush_times


def main():
    ml_br_monthly_sku = get_collection("yingshi", "yingshi", "ml_br_monthly_sku")
    _lj_data = get_collection("yingshi", "yingshi", "_lj_data")

    MONTHS = [202507, 202508, 202509, 202510, 202511]
    MONTH_FIELD_MAP = {
        202507: "202507月销量",
        202508: "202508月销量",
        202509: "202509月销量",
        202510: "202510月销量",
        202511: "202511月销量",
    }

    BATCH_SIZE = 5000

    logger.info("start create indexes...")
    ml_br_monthly_sku.create_index(
        [("month", 1), ("sku_id", 1)],
        name="idx_month_sku_id"
    )
    ml_br_monthly_sku.create_index(
        [("month", 1), ("item_id", 1)],
        name="idx_month_item_id"
    )
    logger.info("indexes created.")

    projection = {
        "_id": 0,
        "商品ID": 1,
        "被跟卖商品ID": 1,
        "价格": 1,
        "销售额": 1,
        "30天销量": 1,
        "商品名称": 1,
        "商品链接": 1,
        "品牌": 1,
        "店铺名称": 1,
        "上架日期": 1,
        "类目": 1,
        "category_id": 1,
        "202507月销量": 1,
        "202508月销量": 1,
        "202509月销量": 1,
        "202510月销量": 1,
        "202511月销量": 1,
    }

    ops = []
    total_ops = 0
    total_docs = 0
    skipped = 0
    flush_times = 0

    cursor = _lj_data.find(
        {},
        projection=projection,
        no_cursor_timeout=True
    ).batch_size(2000)

    try:
        for raw in tqdm(cursor, desc="Building bulk ops"):
            total_docs += 1

            goods_id = raw.get("商品ID")
            if not goods_id:
                skipped += 1
                continue

            followed_item_id = raw.get("被跟卖商品ID")

            title = raw.get("商品名称")
            url = raw.get("商品链接")
            brand = raw.get("品牌")
            seller_name = raw.get("店铺名称")
            onsaledate = parse_onsaledate(raw.get("上架日期"))
            categories = {
                            "cat_1": "Casa, Móveis e Decoração",
                            "cat_2": "Segurança para Casa",
                            "cat_3": "Sistemas de Monitoramento",
                            "cat_4": "Câmeras de Segurança"
                          }
            category_id = "MLB7073"

            try:
                activeprice = round(
                    to_float(raw.get("销售额", 0)) / to_float(raw.get("30天销量", 0)),
                    2
                )
                if activeprice <= 0:
                    raise ValueError("activeprice <= 0")
            except Exception:
                activeprice = to_float(raw.get("价格", 0))

            # 按你参考代码的逻辑：
            # 有“被跟卖商品ID”字段 => 存 item_id=商品ID, sku_id=None
            # 没有“被跟卖商品ID”字段 => 存 sku_id=商品ID, item_id=None
            has_followed_item = followed_item_id not in (None, "", 0, "0")

            if has_followed_item:
                base_filter = {"item_id": goods_id}
                insert_identity = {
                    "sku_id": None,
                    "item_id": goods_id,
                }
            else:
                base_filter = {"sku_id": goods_id}
                insert_identity = {
                    "sku_id": goods_id,
                    "item_id": None,
                }

            update_time = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")

            for m in MONTHS:
                monthlyorder_raw = raw.get(MONTH_FIELD_MAP[m], 0)
                monthlyorder = 0 if isinstance(monthlyorder_raw, str) else to_int(monthlyorder_raw)
                monthlygmv = round(monthlyorder * activeprice, 2)

                filter_doc = {
                    **base_filter,
                    "month": m,
                }

                # 每次都更新的字段
                set_doc = {
                    "categories": categories,
                    "category_id": category_id,
                    "monthlyorder": monthlyorder,
                    "monthlygmv": monthlygmv,
                    "activeprice": activeprice,
                    "brand": brand,
                    "sellerName": seller_name,
                    "title": title,
                    "url": url,
                    "onsaledate": onsaledate,
                    "lanjing_source": True,
                    "lanjing_source_update_time": update_time,
                }

                # 仅在插入时初始化，不能和 $set 重复
                set_on_insert_doc = {
                    **insert_identity,
                    "month": m,
                    # "categories": categories,
                    # "category_id": category_id,
                    "sellerID": None,
                    "totalorder": 0,
                    "totalgmv": 0,
                    "monthlyreview": 0,
                    "review_num": 0,
                }

                ops.append(
                    UpdateOne(
                        filter=filter_doc,
                        update={
                            "$set": set_doc,
                            "$setOnInsert": set_on_insert_doc,
                        },
                        upsert=True,
                    )
                )

            if len(ops) >= BATCH_SIZE:
                total_ops, flush_times = flush_bulk(
                    ml_br_monthly_sku, ops, total_ops, flush_times
                )

        total_ops, flush_times = flush_bulk(
            ml_br_monthly_sku, ops, total_ops, flush_times
        )

    finally:
        cursor.close()

    logger.info(
        f"Finished. total_docs={total_docs}, total_ops={total_ops}, "
        f"skipped={skipped}, flush_times={flush_times}"
    )


if __name__ == "__main__":
    main()