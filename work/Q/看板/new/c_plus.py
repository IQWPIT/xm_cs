# -*- coding: utf-8 -*-
import os
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import UpdateOne
from tqdm import tqdm
from dm.connector.mongo.manager3 import get_collection
# ================= Mongo 连接 =================
visual_plus_col = get_collection("main_ml_mx", "ml_mx", "visual_plus")
_big_Excel_col = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
# ================= 日志配置 =================
LOG_PATH = "写入plus2.log"

logger = logging.getLogger("visual_plus_sync")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    LOG_PATH,
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=5,
    encoding="utf-8"
)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ================= 任务函数 =================
def process_one_cat(cat_id: str, time_list: list[int]) -> bool:
    """
    单个类目处理（线程任务）
    """
    try:
        bulk = []

        for t in time_list:
            cur_doc = _big_Excel_col.find_one(
                {"cat_id": cat_id, "time": t},
                {
                    "_id": 0,
                    "ad_click_rate":1,
                    "avg_cpc_usd":1,
                    "avg_ad_cost_ratio":1,
                    "avg_refund_rate":1,
                    "total_gmv_usd":1,
                    "top50_new_30d_sales_ratio": 1,
                    "top50_new_90d_sales_ratio": 1,
                    "top50_new_180d_sales_ratio": 1,
                    "top50_avg_order_price": 1,
                    "top50_seller_count": 1,
                    "top50_brand_count": 1,
                    "full_stock_avg_price": 1,
                    "local_seller_avg_price": 1,
                    "crossborder_seller_avg_price": 1,
                    "seller_concentration": 1,
                    "brand_concentration": 1,
                    "new_30d_sales_ratio": 1,
                    "new_90d_sales_ratio": 1,
                    "new_180d_sales_ratio": 1,
                }
            )

            if not cur_doc:
                continue

            bulk.append(
                UpdateOne(
                    {"cat_id": cat_id},
                    {"$set": {
                        f"ad_click_rate2.{t}": cur_doc.get("ad_click_rate", 0),
                        f"avg_cpc_usd2.{t}": cur_doc.get("avg_cpc_usd", 0),
                        f"avg_ad_cost_ratio2.{t}": cur_doc.get("avg_ad_cost_ratio", 0),
                        f"avg_refund_rate2.{t}": cur_doc.get("avg_refund_rate", 0),
                        f"total_gmv_usd.{t}": cur_doc.get("total_gmv_usd", 0),
                        f"top50_new_30d_sales_ratio.{t}": cur_doc.get("top50_new_30d_sales_ratio", 0) * 100,
                        f"top50_new_90d_sales_ratio.{t}": cur_doc.get("top50_new_90d_sales_ratio", 0) * 100,
                        f"top50_new_180d_sales_ratio.{t}": cur_doc.get("top50_new_180d_sales_ratio", 0) * 100,
                        f"top50_avg_order_price.{t}": cur_doc.get("top50_avg_order_price", 0),
                        f"top50_seller_count.{t}": cur_doc.get("top50_seller_count", 0),
                        f"top50_brand_count.{t}": cur_doc.get("top50_brand_count", 0),
                        f"full_stock_avg_price.{t}": cur_doc.get("full_stock_avg_price", 0),
                        f"local_seller_avg_price.{t}": cur_doc.get("local_seller_avg_price", 0),
                        f"crossborder_seller_avg_price.{t}": cur_doc.get("crossborder_seller_avg_price", 0),
                        f"seller_concentration.{t}": cur_doc.get("seller_concentration", 0),
                        f"brand_concentration.{t}": cur_doc.get("brand_concentration", 0),
                    }}
                )
            )

        if bulk:
            result = visual_plus_col.bulk_write(bulk, ordered=False)
            logger.info(
                f"cat_id={cat_id} bulk_write success | "
                f"matched={result.matched_count} modified={result.modified_count}"
            )
        else:
            logger.info(f"cat_id={cat_id} no data to update")

        return True

    except Exception:
        logger.exception(f"cat_id={cat_id} failed")
        return False

# ================= 主流程 =================
def main():
    cat_ids = []
    for doc in visual_plus_col.find({}, {"cat_id": 1}):
        cat_id = doc.get("cat_id")
        if cat_id:
            cat_ids.append(cat_id)
    # cat_ids = ["MLM189045"]

    time_list = [
        # 202411, 202412, 202501, 202502, 202503, 202504, 202505, 202506,
        # 202507, 202508, 202509, 202510, 202511,202512,202601,
        202602,
    ]

    MAX_WORKERS = min(8, (os.cpu_count() or 4) * 2)

    logger.info(
        f"job start | cats={len(cat_ids)} "
        f"| months={len(time_list)} | workers={MAX_WORKERS}"
    )

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_one_cat, cat_id, time_list): cat_id
            for cat_id in cat_ids
        }

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="处理类目"
        ):
            if future.result():
                success += 1
            else:
                failed += 1

    logger.info(f"job finished | success={success} failed={failed}")
    print(f"完成：success={success}, failed={failed}")

# ================= 入口 =================
if __name__ == "__main__":
    main()
