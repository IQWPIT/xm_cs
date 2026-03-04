import os
from tqdm import tqdm
from loguru import logger

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =========================
# 配置
# =========================
MONTHS = [202508,202509,202510, 202511, 202512]
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

for CAT_ID in CAT_IDs:
    # =========================
    # 转化率
    # =========================
    cat_doc = cat.find_one({"id": CAT_ID}, {"conversion": 1}) or {}
    z = cat_doc.get("conversion", {}).get("data", 0.05)
    logger.info(f"[CAT={CAT_ID}] conversion={z}")

    # =========================
    # 当前类目 SKU
    # =========================
    sku_list = [
        d["sku_id"]
        for d in main_sku.find(
            {"category_id": CAT_ID},
            {"sku_id": 1,"monthly_sale_trend":1}
        )
    ]
    sku_set = set(sku_list)
    logger.info(f"category sku size={len(sku_set)}")

    # =========================
    # 已存在 sku_3（一次性）
    # =========================
    exist_sku = set()
    for d in sku_3.find(
        {
            "$or": [
                {"sku_id": {"$in": sku_list}},
                {"item_id": {"$in": sku_list}},
            ]
        },
        {"sku_id": 1, "item_id": 1}
    ):
        if d.get("sku_id"):
            exist_sku.add(d["sku_id"])
        if d.get("item_id"):
            exist_sku.add(d["item_id"])

    logger.info(f"exist sku size={len(exist_sku)}")

    # =========================
    # visits（一次性）
    # =========================
    visit_map = {
        d["sl"]: (d.get("visits") or {})
        for d in ml_visits.find(
            {"sl": {"$in": sku_list}},
            {"sl": 1, "visits": 1}
        )
    }
    logger.info(f"visit_map size={len(visit_map)}")

    # =========================
    # 多月汇总
    # =========================
    for month in MONTHS:
        month_prefix = str(month)

        total_order = 0
        total_visits = 0
        valid_sku_cnt = 0

        for sku_id in sku_list:

            if sku_id in exist_sku:
                continue

            visits = visit_map.get(sku_id)
            if not visits:
                continue

            v = sum(
                cnt for day, cnt in visits.items()
                if day.startswith(month_prefix)
            )
            if v <= 0:
                continue

            order = int(v * z + 0.5)

            total_visits += v
            total_order += order
            valid_sku_cnt += 1

        logger.info(
            f"[CAT={CAT_ID}][MONTH={month}] "
            f"valid_sku={valid_sku_cnt} "
            f"total_visits={total_visits} "
            f"total_order={total_order}"
        )
