import os
import argparse
from pymongo import UpdateOne
from tqdm import tqdm
from loguru import logger
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# =====================================================
# 日志模块
# =====================================================
def init_logger(log_file: str):
    logger.remove()
    logger.add(
        log_file,
        rotation="100 MB",
        retention="14 days",
        level="INFO",
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=True
    )

# =====================================================
# Mongo 集合模块
# =====================================================
def get_collections(site: str):
    c_sku = get_collection(f"main_{site}", site, "sku")
    c_item_stock = get_collection("yingshi", site, "item_stock")
    c_target = get_collection("yingshi", "yingshi", f"_cx_{site}_sku_3")
    c_new_every_day_item = get_collection(f"main_{site}", site, "new_every_day_item")
    c_every_day_sku = get_collection(f"main_{site}", site, "every_day_sku")
    return c_sku, c_item_stock, c_target, c_new_every_day_item, c_every_day_sku

# =====================================================
# 构建 UpdateOne（核心业务逻辑）
# =====================================================
def build_update_op(data: dict, c_sku, c_new_every_day_item, c_every_day_sku, time) -> UpdateOne | None:
    q_id = data.get("sl")
    cat_id = data.get("cid")
    if not q_id:
        raise ValueError(f"q_id is empty for data={data}")
    if "M" in data.get("pid",""):
        main_sku = c_sku.find_one({"sku_id": data.get("pid")}, {"_id": 0,"sku_id":0,"item_id":0})
    else:
        main_sku = c_sku.find_one({"sku_id": q_id}, {"_id": 0,"sku_id":0,"item_id":0})
    trends = c_new_every_day_item.find_one({"sl": q_id}, {"_id": 0})

    # -----------------------------------评论数统计----------------------------------------
    start_day = int(f"{time}01")
    end_day = int(f"{time}99")  # 用 99 确保覆盖当月所有天
    comment_data = c_every_day_sku.find(
        {"sl": q_id, "dT": {"$gte": start_day, "$lte": end_day}},
        {"_id": 0, "dT": 1, "dR": 1, "rN": 1}
    )
    total_dR = 0
    last_rN = None
    last_day = start_day
    for row in comment_data:
        dT = row.get("dT")
        dR = row.get("dR", 0)
        rN = row.get("rN", None)

        total_dR += dR if dR else 0

        if dT >= last_day:
            last_day = dT
            if rN:
                last_rN = rN

    # --------------------------同步 main_sku 数据-----------------------------------------
    if not main_sku:
        raise ValueError(f"main_sku not found for q_id={q_id}")
    if "M" in data.get("pid",""):
        one_key = data.get("pid")
        update_doc = {
            "q_id": q_id,
            "dT": time,
            "one_key": one_key,

            # "sku_id": None,
            "sku_id": None,
            # "item_id": data.get("pid"),
            "item_id":data.get("sl"),

            "cat_id":cat_id,
            f"dR_sum.{time}": total_dR,
            f"rN.{time}": last_rN,
            **main_sku,
            **(trends or {})
        }
        return UpdateOne(
            {
                # "one_key": one_key,
                "q_id": q_id,
                "dT": time,
                # "item_id": data.get("sl"),
                # "sku_id": None,
            },
            {"$set": update_doc},
            upsert=True
        )
    else:
        one_key = data.get("sl")

        update_doc = {
            "q_id": q_id,
            "one_key": one_key,
            "dT":time,

            "sku_id": data.get("sl"),
            "item_id": None,
            "cat_id": cat_id,
            f"dR_sum.{time}": total_dR,
            f"rN.{time}": last_rN,
            **main_sku,
            **(trends or {})
        }
        return UpdateOne(
            {
                # "one_key": one_key,
                "q_id": q_id,
                "dT":time,
                # "item_id": None,
                # "sku_id": data.get("sl"),
            },
            {"$set": update_doc},
            upsert=True
        )

# =====================================================
# 主处理流程（bulk + 进度条）
# =====================================================
def process_cursor(
    cursor,
    total: int,
    c_sku,
    c_new_every_day_item,
    c_target,
    c_every_day_sku,
    time,
    batch_size: int = 500
):
    bulk_ops = []
    success = 0
    failed = 0

    for data in tqdm(cursor, total=total, desc="Sync SKU"):
        try:
            op = build_update_op(data, c_sku, c_new_every_day_item, c_every_day_sku, time)
            bulk_ops.append(op)

            if len(bulk_ops) >= batch_size:
                c_target.bulk_write(bulk_ops, ordered=False)
                success += len(bulk_ops)
                tqdm.write(f"bulk_write 提交 {len(bulk_ops)} 条")
                logger.info("bulk_write 提交 {} 条", len(bulk_ops))
                bulk_ops.clear()

        except Exception as e:
            failed += 1
            logger.error("处理失败 | data={} | error={}", data, str(e))

    # 写入剩余
    if bulk_ops:
        try:
            c_target.bulk_write(bulk_ops, ordered=False)
            success += len(bulk_ops)
            tqdm.write(f"bulk_write 提交 {len(bulk_ops)} 条（最后一批）")
            logger.info("bulk_write 提交 {} 条（最后一批）", len(bulk_ops))
        except Exception as e:
            logger.error("最后批次 bulk_write 失败 | error={}", str(e))

    return success, failed

# =====================================================
# main 入口（CLI 支持）
# =====================================================
def main():
    parser = argparse.ArgumentParser(description="同步 SKU 数据到sku_3")
    parser.add_argument("--site", type=str, default="ml_cl", help="站点")
    parser.add_argument("--time", type=int, default=202512, help="月份")
    args = parser.parse_args()

    site = args.site
    if args.site == "ml_cl":
        cat_id = ["MLC5713"]
    elif args.site == "ml_mx":
        cat_id = ["MLM437575", "MLM455251", "MLM420128"]
    elif args.site == "ml_br":
        cat_id = ["MLB7073", "MLB455251"]
    elif args.site == "ml_co":
        cat_id = ["MCO5844"]
    elif args.site == "ml_ar":
        cat_id = ["MLA5959"]
    batch_size = 500
    time = args.time

    log_file = f"{site}_sku_sync.log"
    init_logger(log_file)
    logger.info("任务开始 | site={} | cat_id={}", site, cat_id)

    c_sku, c_item_stock, c_target, c_new_every_day_item, c_every_day_sku = get_collections(site)

    query = {"cid": {"$in": cat_id}}
    total = c_item_stock.count_documents(query)
    cursor = c_item_stock.find(query, {"sl": 1, "pid": 1,"cid":1})

    success, failed = process_cursor(
        cursor=cursor,
        total=total,
        c_sku=c_sku,
        c_new_every_day_item=c_new_every_day_item,
        c_target=c_target,
        c_every_day_sku=c_every_day_sku,
        time=time,
        batch_size=batch_size
    )

    logger.info("任务完成 | 成功={} | 失败={} | 总数={}", success, failed, total)
    print(f"✅ 完成，同步详情见 {log_file}")

if __name__ == "__main__":
    main()
