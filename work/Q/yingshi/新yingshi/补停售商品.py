import os
from tqdm import tqdm
from loguru import logger
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# =========================
# 配置
# =========================
MONTHS = [202508, 202509, 202510, 202511, 202512]
site = "ml_cl"

if site == "ml_cl":
    CAT_IDs = ["MLC5713"]
elif site == "ml_mx":
    CAT_IDs = ["MLM437575", "MLM455251", "MLM420128"]
elif site == "ml_br":
    CAT_IDs = ["MLB7073", "MLB455251"]
elif site == "ml_co":
    CAT_IDs = ["MCO5844"]
elif site == "ml_ar":
    CAT_IDs = ["MLA5959"]

# =========================
# 集合
# =========================
main_sku = get_collection(f"main_{site}", site, "sku")
sku_3 = get_collection("yingshi", "yingshi", f"{site}_sku_3")
ml_visits = get_collection("ml_visits", "ml_visits", f"{site}_visits")
cat = get_collection(f"main_{site}", site, "cat")

# =========================
# 遍历 CAT_ID
# =========================
for CAT_ID in CAT_IDs:
    logger.info(f"Processing CAT_ID={CAT_ID} ...")

    # 获取该类目所有 SKU
    sku_list = [
        d["sku_id"]
        for d in main_sku.find(
            {"category_id": CAT_ID},
            {"sku_id": 1, "monthly_sale_trend": 1}
        )
    ]
    sku_set = set(sku_list)

    # # 获取已经存在 sku_3 的 SKU
    exist_sku = set()
    # for d in sku_3.find(
    #     {"$or": [
    #         {"sku_id": {"$in": sku_list}},
    #         {"item_id": {"$in": sku_list}}
    #     ]},
    #     {"sku_id": 1, "item_id": 1}
    # ):
    #     if d.get("sku_id"):
    #         exist_sku.add(d["sku_id"])
    #     if d.get("item_id"):
    #         exist_sku.add(d["item_id"])

    # 构造 SKU → monthly_sale_trend 映射，同时排除已存在 sku_3 的 SKU
    datas = {
        d["sku_id"]: d.get("monthly_sale_trend") or {}
        for d in main_sku.find(
            {"sku_id": {"$in": list(sku_set - exist_sku)}},
            {"sku_id": 1, "monthly_sale_trend": 1}
        )
    }

    # =========================
    # 统计每个月的总订单
    # =========================
    for month in MONTHS:
        month_key_str = str(month)[2:6]  # '2508'
        month_key_int = int(month_key_str)  # 2508
        total_order = 0

        for trend in datas.values():
            # 兼容 key 为 str 或 int
            v = trend.get(month_key_str) or trend.get(month_key_int, 0)
            total_order += v

        logger.info(f"CAT={CAT_ID}, MONTH={month}, TOTAL_ORDER={total_order}")
