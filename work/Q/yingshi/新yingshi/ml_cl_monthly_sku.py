import os
import argparse
from pymongo import UpdateOne
from tqdm import tqdm
from loguru import logger

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection


# ===========================
# 日志初始化
# ===========================
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
        diagnose=True,
    )


# ===========================
# 上一个月计算
# ===========================
def prev_month(time: int) -> int:
    year = time // 100
    month = time % 100
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    return year * 100 + month


# ===========================
# 构建 UpdateOne
# ===========================
def build_update_op(
    c_item_id_map,
    c_shop_info,
    sku_item: dict,
    time: int
) -> UpdateOne:
    """
    构建单条 SKU 的月度 UpdateOne 操作
    """
    # ===========================
    # 基础字段
    # ===========================
    q_id = sku_item.get("q_id")
    sku_id = sku_item.get("sku_id")
    item_id = sku_item.get("item_id")
    category_id = sku_item.get("cat_id")
    # ===========================
    # 当月评价 / 价格
    # ===========================
    dR_sum = sku_item.get("dR_sum", {}).get(time, 0)
    rN = sku_item.get("rN", {}).get(time, 0)
    active_price = sku_item.get("active_price", 0)
    # ===========================
    # 统计当月销量
    # ===========================
    trends = sku_item.get("trends", {})
    month_str = str(time)
    total_oD = 0
    total_gmv = 0
    for date_key, value in trends.items():
        if str(date_key).startswith(month_str):
            total_oD += value.get("oD", 0)
            total_gmv += value.get("pR", 0)*value.get("oD", 0)
    monthly_gmv = total_gmv
    price = monthly_gmv/total_oD if total_oD else 0
    # ===========================
    # 历史累加
    # ===========================
    totalgmv = sku_item.get("totalgmv", 0)
    totalorder = sku_item.get("totalorder", 0)
    l_sum_gmv = totalgmv + monthly_gmv
    l_sum_oD = totalorder + total_oD
    # ===========================
    # seller 信息（安全查询）
    # ===========================
    seller_id = None
    seller_name = None
    if item_id:
        item_map = c_item_id_map.find_one(
            {"id": item_id},
            {"seller_id": 1}
        )
        if item_map:
            seller_id = item_map.get("seller_id")

    if seller_id:
        shop_info = c_shop_info.find_one(
            {"sellerID": seller_id},
            {"sellerName": 1}
        )
        if shop_info:
            seller_name = shop_info.get("sellerName")

    # ===========================
    # 更新文档
    # ===========================
    update_doc = {
        "q_id": q_id,
        "month": time,

        "sku_id": sku_id,
        "item_id": item_id,
        "category_id": category_id,

        "Calidad_de_resolución": sku_item.get("Calidad_de_resolución"),
        "Conectividad": sku_item.get("Conectividad"),
        "Conectividade": sku_item.get("Conectividade"),
        "Modelo": sku_item.get("Modelo"),
        "Qualidade_de_resoluçao": sku_item.get("Qualidade_de_resoluçao"),
        "Tipo_de_cámara_de_vigilancia": sku_item.get("Tipo_de_cámara_de_vigilancia"),
        "Tipo_de_câmera_de_vigilância": sku_item.get("Tipo_de_câmera_de_vigilância"),

        "brand": sku_item.get("brand"),
        "categories": sku_item.get("categories"),
        "cpid": sku_item.get("cpid"),

        "activeprice": price,

        "monthlygmv": monthly_gmv,
        "monthlyorder": total_oD,
        "monthlyreview": dR_sum,
        "review_num": rN,

        "totalgmv": l_sum_gmv,
        "totalorder": l_sum_oD,

        "onsaledate": sku_item.get("puton_date"),
        "sellerID": seller_id,
        "sellerName": seller_name,

        "title": sku_item.get("spu_title"),
        "url": sku_item.get("spu_url"),
    }
    # ===========================
    # 唯一键：防止覆盖
    # ===========================
    filter_doc = {
        "q_id": q_id,
        "month": time,
        # "sku_id": sku_id
    }
    return UpdateOne(
        filter_doc,
        {"$set": update_doc},
        upsert=True
    )


# ===========================
# 批量处理函数
# ===========================
def process_bulk_write(c_item_id_map,c_shop_info,cursor, time: int, batch_size: int = 500):
    c_monthly_sku = get_collection("yingshi", "yingshi", f"_cx_{site}_monthly_sku")

    bulk_ops = []
    success = 0
    failed = 0

    for sku_item in tqdm(cursor, desc=f"Sync SKU {time}"):
        try:
            op = build_update_op(c_item_id_map,c_shop_info,sku_item, time)
            bulk_ops.append(op)
            if len(bulk_ops) >= batch_size:
                c_monthly_sku.bulk_write(bulk_ops, ordered=False)
                success += len(bulk_ops)
                logger.info("Bulk write 提交 {} 条", len(bulk_ops))
                bulk_ops.clear()
        except Exception as e:
            failed += 1
            logger.error("处理失败 | sku_item={} | error={}", sku_item, str(e))

    if bulk_ops:
        try:
            c_monthly_sku.bulk_write(bulk_ops, ordered=False)
            success += len(bulk_ops)
            logger.info("最后批次 Bulk write 提交 {} 条", len(bulk_ops))
        except Exception as e:
            logger.error("最后批次 Bulk write 失败 | error={}", str(e))

    return success, failed


# ===========================
# main 入口
# ===========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量同步 SKU 月度数据")
    parser.add_argument("--site", type=str, default="ml_cl", help="站点")
    parser.add_argument("--time", type=int, default=202512, help="月份，例如 202512")
    args = parser.parse_args()

    site = args.site
    time = args.time
    batch_size = 500

    log_file = f"{site}_monthly_sku_sync_{time}.log"
    init_logger(log_file)
    logger.info("任务开始 | site={} | month={}", site, time)

    # 获取数据
    c_target = get_collection("yingshi", "yingshi", f"{site}_sku_3")
    cursor = c_target.find({"dT":time}, {"_id": 0})  # 你可以根据需要筛选字段

    c_item_id_map = get_collection("task_buffer", f"{site}", "item_id_map")
    c_shop_info = get_collection(f"main_{site}", site, "shop_info")

    success, failed = process_bulk_write(c_item_id_map,c_shop_info,cursor, time, batch_size)

    logger.info("任务完成 | 成功={} | 失败={}", success, failed)
    print(f"✅ 完成，同步详情见 {log_file}")
