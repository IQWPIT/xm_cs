from datetime import datetime, timedelta
from collections import defaultdict
import heapq
from pprint import pprint
import os
import calendar
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
# =============================
# 环境变量 & 集合
# =============================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
price_segment_col = get_collection("main_ml_mx", "ml_mx", "price_segment")
every_day_sku_col = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
visual_plus_cx = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
TIMES_4 = "2601"
LOG_FILE = "processed_cat.log"  # 用于记录已处理的 cat_id
time = {
    "2601": "202601",
    "2501": "202501",
    "2502": "202502",
    "2503": "202503",
    "2504": "202504",
    "2505": "202505",
    "2506": "202506",
    "2507": "202507",
    "2508": "202508",
    "2509": "202509",
    "2510": "202510",
    "2511": "202511",
    "2512": "202512",
}
# =======================================================================================================================
# 读取处理过的cat_id
# =======================================================================================================================
def load_processed_cats():
    """读取已处理过的 cat_id"""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f.readlines())
    return set()
# =======================================================================================================================
# 将处理过数据写入日志
# =======================================================================================================================
def save_processed_cat(cat_id):
    """将处理完成的 cat_id 写入日志"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{cat_id}\n")
# =======================================================================================================================
# 工具函数
# =======================================================================================================================
def get_last_n_months(times_4, n=13):
    year = int("20" + times_4[:2])
    month = int(times_4[2:])
    months = []
    for _ in range(n):
        months.append(f"{str(year)[2:]}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(months))
# =======================================================================================================================
#
# =======================================================================================================================
def get_price_ranges(cat_id):
    doc = price_segment_col.find_one({"cat_id": cat_id}, {"price_ranges": 1})
    if not doc:
        return []
    ranges = []
    for r in doc.get("price_ranges", []):
        min_p, max_p = r.split("-")
        ranges.append((float(min_p), float(max_p), r))
    return ranges
# =======================================================================================================================
#
# =======================================================================================================================
def build_price_brand_structure(price_ranges, sku_list, top_brands):
    result = {label: {b: {"order": 0} for b in top_brands}
              for _, _, label in price_ranges}

    for _, brand, price, order in sku_list:
        if brand not in top_brands:
            continue
        for min_p, max_p, label in price_ranges:
            if min_p <= price < max_p:
                result[label][brand]["order"] += order
                break
    return result
# =======================================================================================================================
#
# =======================================================================================================================
def get_sku_data_multi_month(cats, months):
    cursor = sku_col.find(
        {"category_id": {"$in": cats}},
        {"_id": 0,
         "sku_id": 1,
         "brand": 1,
         "sellerName": 1,
         "monthly_sale_trend": 1,
         "active_price": 1,
         "puton_date": 1,
         "stock_type": 1}
    ).batch_size(2000)

    month_data = {
        m: {
            "sku_list": [],
            "sku_infos": {},
            "stock_map": {},
            "brand_total": defaultdict(int),
            "seller_total": defaultdict(int)
        }
        for m in months
    }

    for d in cursor:
        trend = d.get("monthly_sale_trend")
        if not trend:
            continue

        sku_id = d.get("sku_id")
        brand = d.get("brand") or "Unknown"
        seller = d.get("sellerName") or "UnknownSeller"
        stock_type = d.get("stock_type") or "unknown"
        price = d.get("active_price")

        if not sku_id or price is None:
            continue

        for m in months:
            order = trend.get(m, 0)
            if order:
                month_data[m]["sku_list"].append((sku_id, brand, price, order))
                month_data[m]["sku_infos"][sku_id] = d
                month_data[m]["stock_map"][sku_id] = stock_type
                month_data[m]["brand_total"][brand] += order
                month_data[m]["seller_total"][seller] += order

    return month_data
# =======================================================================================================================
#
# =======================================================================================================================
def batch_new_sales(sku_infos, sku_ids, month_start, delta_days, month_end):
    start_int = int(month_start.strftime("%Y%m%d"))
    month_end_int = int(month_end.strftime("%Y%m%d"))
    sku_limit = {}

    for sku in sku_ids:
        puton = str(sku_infos[sku].get("puton_date", ""))
        if not puton.isdigit():
            continue

        if len(puton) == 6:
            puton += "01"
        elif len(puton) != 8:
            continue

        try:
            y, m, d = int(puton[:4]), int(puton[4:6]), int(puton[6:])
            d = min(d, calendar.monthrange(y, m)[1])
            puton_dt = datetime(y, m, d)
        except:
            continue

        end_dt = min(puton_dt + timedelta(days=delta_days), month_end)
        if end_dt < month_start:
            continue

        sku_limit[sku] = int(end_dt.strftime("%Y%m%d"))

    if not sku_limit:
        return {}, {}

    cursor = every_day_sku_col.find(
        {"sl": {"$in": list(sku_limit.keys())},
         "dT": {"$gte": start_int, "$lt": month_end_int}},
        {"sl": 1, "oD": 1, "dT": 1, "pR": 1}
    )

    totals = defaultdict(int)
    gmvs = defaultdict(float)

    for doc in cursor:
        sku = doc["sl"]
        if doc["dT"] <= sku_limit.get(sku, month_end_int):
            od = doc.get("oD", 0)
            pr = doc.get("pR", 0)
            totals[sku] += od
            gmvs[sku] += od * pr

    return totals, gmvs
# =======================================================================================================================
#
# =======================================================================================================================
def get_top_brands(brand_total, top_n=10):
    return dict(heapq.nlargest(top_n, brand_total.items(), key=lambda x: x[1]))
# =======================================================================================================================
#
# =======================================================================================================================
def get_top_sellers(seller_total, top_n=10):
    return dict(heapq.nlargest(top_n, seller_total.items(), key=lambda x: x[1]))
# =======================================================================================================================
#
# =======================================================================================================================
def get_top_skus(sku_list, top_n=10):
    total_order = sum([o for _, _, _, o in sku_list])
    sku_orders = defaultdict(int)
    for sku_id, _, _, order in sku_list:
        sku_orders[sku_id] += order

    top_skus = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    return {sku: {"order": order, "order_ratio": order / total_order if total_order else 0}
            for sku, order in top_skus}
# =======================================================================================================================
#                   top50商品新品存储类型
# =======================================================================================================================
def top50_sku_new_stock_stats(z_order,sku_infos, sku_ids, stock_map, month_start, month_end, top_n=50):
    # 先统计当月销量 TopN SKU
    sku_orders = defaultdict(int)
    for sku in sku_ids:
        sku_data = sku_infos[sku]
        trend = sku_data.get("monthly_sale_trend", {})
        order = trend.get(TIMES_4, 0)
        if order:
            sku_orders[sku] += order

    top_skus = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    top_sku_ids = [sku for sku, _ in top_skus]

    # 计算新品销量
    new30, new30_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 30, month_end)
    new90, new90_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 90, month_end)
    new180, new180_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 180, month_end)

    stats = defaultdict(lambda: {
        "new30_count": 0, "new30_order": 0, "new30_price": 0,"new30_order_ratio": 0,
        "new90_count": 0, "new90_order": 0, "new90_price": 0,"new90_order_ratio": 0,
        "new180_count": 0, "new180_order": 0, "new180_price": 0,"new180_order_ratio": 0,
    })

    for sku in top_sku_ids:
        st = stock_map.get(sku,"unknown")
        if new30.get(sku,0) > 0:
            stats[st]["new30_count"] += 1
            stats[st]["new30_order"] += new30[sku]
        if new90.get(sku,0) > 0:
            stats[st]["new90_count"] += 1
            stats[st]["new90_order"] += new90[sku]
        if new180.get(sku,0) > 0:
            stats[st]["new180_count"] += 1
            stats[st]["new180_order"] += new180[sku]
    stock_stats_list = ["ful", "ci", "nor"]
    for s in stock_stats_list:
        stats[s]["new30_order_ratio"] = stats[s]["new30_order"] / z_order
        stats[s]["new90_order_ratio"] = stats[s]["new90_order"] / z_order
        stats[s]["new180_order_ratio"] = stats[s]["new180_order"] / z_order

    # 计算均价
    for st,v in stats.items():
        v["new30_price"] = round(sum([new30_gmv.get(s,0) for s in top_sku_ids if stock_map.get(s,"unknown")==st])/v["new30_order"],2) if v["new30_order"]>0 else 0
        v["new90_price"] = round(sum([new90_gmv.get(s,0) for s in top_sku_ids if stock_map.get(s,"unknown")==st])/v["new90_order"],2) if v["new90_order"]>0 else 0
        v["new180_price"] = round(sum([new180_gmv.get(s,0) for s in top_sku_ids if stock_map.get(s,"unknown")==st])/v["new180_order"],2) if v["new180_order"]>0 else 0

    return dict(stats)
# =======================================================================================================================
#                   计算gmv
# =======================================================================================================================
def calculate_year_gmv(month_data, months, year_str):
    """
    统计指定年份的总 GMV
    year_str: '24' -> 2024, '25' -> 2025
    """
    total_gmv = 0
    for m in months:
        if m.startswith(year_str):
            # 该月的 SKU 列表
            sku_list = month_data[m]["sku_list"]
            # GMV = sum(price * order)
            for sku_id, _, price, order in sku_list:
                total_gmv += price * order
    return total_gmv
# =======================================================================================================================
#                   插入大表格
# =======================================================================================================================
def import_big_Excel(cat_id,report,month):
    #25年GMV增长率
    gmv_growth_order_ratio = report["gmv_growth"]["2025"]["order_ratio"]
    #top10商品销量 top10商品销量占比
    top10_commodity_sku_id_order = report["top10_commodity"][month]
    top10_commodity_sku_id_order_sum = 0
    top10_commodity_sku_id_order_sum_ratio = 0
    for sku_id,orders in top10_commodity_sku_id_order.items():
        a = orders["order"]
        b = orders["order_ratio"]
        top10_commodity_sku_id_order_sum += a
        top10_commodity_sku_id_order_sum_ratio += b
    #top1店铺销量占比 top3店铺销量占比
    top3_seller_sales_order_ratio_data = report["top10_seller_sales"][month][month]
    top3_seller_sales_order_ratio_sum_ratio = 0
    top1_seller_sales_order_ratio_sum_ratio = 0
    i = 0
    for seller_sales,orders in top3_seller_sales_order_ratio_data.items():
        i = i + 1
        if i == 1:
            top1_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top3_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
        else:
            top3_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
            if i>3:
                break
    # top品牌销量占比
    top10_brand_sales_order_ratio_data = report["top10_brand_sales"][month][month]
    top1_brand_sales_order_ratio_sum_ratio = 0
    top1_brand_sales_order_ratio_name = ""
    top3_brand_sales_order_ratio_sum_ratio = 0
    i = 0
    for brand_sales,orders in top10_brand_sales_order_ratio_data.items():
        i = i + 1
        if i == 1:
            top1_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top3_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top1_brand_sales_order_ratio_name = brand_sales
        else:
            top3_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            if i>3:
                break
    # top50新品数据
    top50_sku_new_stock_stats_ci_new30_count = report["top50_sku_new_stock_stats"][month]["ci"]["new30_count"]
    top50_sku_new_stock_stats_ci_new90_count = report["top50_sku_new_stock_stats"][month]["ci"]["new90_count"]
    top50_sku_new_stock_stats_ci_new180_count = report["top50_sku_new_stock_stats"][month]["ci"]["new180_count"]
    top50_sku_new_stock_stats_ci_new30_order = report["top50_sku_new_stock_stats"][month]["ci"]["new30_order"]
    top50_sku_new_stock_stats_ci_new90_order = report["top50_sku_new_stock_stats"][month]["ci"]["new90_order"]
    top50_sku_new_stock_stats_ci_new180_order = report["top50_sku_new_stock_stats"][month]["ci"]["new180_order"]
    top50_sku_new_stock_stats_ci_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new30_order_ratio"]
    top50_sku_new_stock_stats_ci_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new90_order_ratio"]
    top50_sku_new_stock_stats_ci_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new180_order_ratio"]

    top50_sku_new_stock_stats_ful_new30_count = report["top50_sku_new_stock_stats"][month]["ful"]["new30_count"]
    top50_sku_new_stock_stats_ful_new90_count = report["top50_sku_new_stock_stats"][month]["ful"]["new90_count"]
    top50_sku_new_stock_stats_ful_new180_count = report["top50_sku_new_stock_stats"][month]["ful"]["new180_count"]
    top50_sku_new_stock_stats_ful_new30_order = report["top50_sku_new_stock_stats"][month]["ful"]["new30_order"]
    top50_sku_new_stock_stats_ful_new90_order = report["top50_sku_new_stock_stats"][month]["ful"]["new90_order"]
    top50_sku_new_stock_stats_ful_new180_order = report["top50_sku_new_stock_stats"][month]["ful"]["new180_order"]
    top50_sku_new_stock_stats_ful_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new30_order_ratio"]
    top50_sku_new_stock_stats_ful_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new90_order_ratio"]
    top50_sku_new_stock_stats_ful_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new180_order_ratio"]


    top50_sku_new_stock_stats_nor_new30_count = report["top50_sku_new_stock_stats"][month]["nor"]["new30_count"]
    top50_sku_new_stock_stats_nor_new90_count = report["top50_sku_new_stock_stats"][month]["nor"]["new90_count"]
    top50_sku_new_stock_stats_nor_new180_count = report["top50_sku_new_stock_stats"][month]["nor"]["new180_count"]
    top50_sku_new_stock_stats_nor_new30_order = report["top50_sku_new_stock_stats"][month]["nor"]["new30_order"]
    top50_sku_new_stock_stats_nor_new90_order = report["top50_sku_new_stock_stats"][month]["nor"]["new90_order"]
    top50_sku_new_stock_stats_nor_new180_order = report["top50_sku_new_stock_stats"][month]["nor"]["new180_order"]
    top50_sku_new_stock_stats_nor_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new30_order_ratio"]
    top50_sku_new_stock_stats_nor_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new90_order_ratio"]
    top50_sku_new_stock_stats_nor_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new180_order_ratio"]

    # all新品数据
    all_new_stock_stats_ci_new30_count = report["all_new_stock_stats"][month]["ci"]["new30_count"]
    all_new_stock_stats_ci_new90_count = report["all_new_stock_stats"][month]["ci"]["new90_count"]
    all_new_stock_stats_ci_new180_count = report["all_new_stock_stats"][month]["ci"]["new180_count"]
    all_new_stock_stats_ci_new30_order = report["all_new_stock_stats"][month]["ci"]["new30_order"]
    all_new_stock_stats_ci_new90_order = report["all_new_stock_stats"][month]["ci"]["new90_order"]
    all_new_stock_stats_ci_new180_order = report["all_new_stock_stats"][month]["ci"]["new180_order"]
    all_new_stock_stats_ci_new30_order_ratio = report["all_new_stock_stats"][month]["ci"]["new30_order_ratio"]
    all_new_stock_stats_ci_new90_order_ratio = report["all_new_stock_stats"][month]["ci"]["new90_order_ratio"]
    all_new_stock_stats_ci_new180_order_ratio = report["all_new_stock_stats"][month]["ci"]["new180_order_ratio"]

    all_new_stock_stats_ful_new30_count = report["all_new_stock_stats"][month]["ful"]["new30_count"]
    all_new_stock_stats_ful_new90_count = report["all_new_stock_stats"][month]["ful"]["new90_count"]
    all_new_stock_stats_ful_new180_count = report["all_new_stock_stats"][month]["ful"]["new180_count"]
    all_new_stock_stats_ful_new30_order = report["all_new_stock_stats"][month]["ful"]["new30_order"]
    all_new_stock_stats_ful_new90_order = report["all_new_stock_stats"][month]["ful"]["new90_order"]
    all_new_stock_stats_ful_new180_order = report["all_new_stock_stats"][month]["ful"]["new180_order"]
    all_new_stock_stats_ful_new30_order_ratio = report["all_new_stock_stats"][month]["ful"]["new30_order_ratio"]
    all_new_stock_stats_ful_new90_order_ratio = report["all_new_stock_stats"][month]["ful"]["new90_order_ratio"]
    all_new_stock_stats_ful_new180_order_ratio = report["all_new_stock_stats"][month]["ful"]["new180_order_ratio"]
    all_new_stock_stats_ful_new30_price = report["all_new_stock_stats"][month]["ful"]["new30_price"]
    all_new_stock_stats_ful_new90_price = report["all_new_stock_stats"][month]["ful"]["new90_price"]
    all_new_stock_stats_ful_new180_price = report["all_new_stock_stats"][month]["ful"]["new180_price"]

    all_new_stock_stats_nor_new30_count = report["all_new_stock_stats"][month]["nor"]["new30_count"]
    all_new_stock_stats_nor_new90_count = report["all_new_stock_stats"][month]["nor"]["new90_count"]
    all_new_stock_stats_nor_new180_count = report["all_new_stock_stats"][month]["nor"]["new180_count"]
    all_new_stock_stats_nor_new30_order = report["all_new_stock_stats"][month]["nor"]["new30_order"]
    all_new_stock_stats_nor_new90_order = report["all_new_stock_stats"][month]["nor"]["new90_order"]
    all_new_stock_stats_nor_new180_order = report["all_new_stock_stats"][month]["nor"]["new180_order"]
    all_new_stock_stats_nor_new30_order_ratio = report["all_new_stock_stats"][month]["nor"]["new30_order_ratio"]
    all_new_stock_stats_nor_new90_order_ratio = report["all_new_stock_stats"][month]["nor"]["new90_order_ratio"]
    all_new_stock_stats_nor_new180_order_ratio = report["all_new_stock_stats"][month]["nor"]["new180_order_ratio"]


    result = {
    "cat_id": cat_id,  # 类目ID
    "time": int(month),  # 当前月份

    "2025_gmv_growth_order_ratio": gmv_growth_order_ratio,  # 25年GMV增长率
    "top10_commodity_sku_id_order": top10_commodity_sku_id_order_sum,  # top10商品销量
    "top10_commodity_sku_id_order_ratio": top10_commodity_sku_id_order_sum_ratio,  # top10商品销量占比
    "top1_seller_sales_order_ratio_sum_ratio": top1_seller_sales_order_ratio_sum_ratio,  # top1店铺销量占比
    "top3_seller_sales_order_ratio_sum_ratio": top3_seller_sales_order_ratio_sum_ratio,  # top3店铺销量占比
    "top1_brand_sales_order_ratio_sum_ratio": top1_brand_sales_order_ratio_sum_ratio,  # top1品牌销量占比
    "top1_brand_sales_order_ratio_name": top1_brand_sales_order_ratio_name,  # top1品牌名称
    "top3_brand_sales_order_ratio_sum_ratio": top3_brand_sales_order_ratio_sum_ratio,  # top3品牌销量占比

    # top50新品商品数量 ci
    "top50_sku_new_stock_stats_ci_new30_count": top50_sku_new_stock_stats_ci_new30_count,
    "top50_sku_new_stock_stats_ci_new90_count": top50_sku_new_stock_stats_ci_new90_count,
    "top50_sku_new_stock_stats_ci_new180_count": top50_sku_new_stock_stats_ci_new180_count,
    # top50新品商品销量 ci
    "top50_sku_new_stock_stats_ci_new30_order": top50_sku_new_stock_stats_ci_new30_order,
    "top50_sku_new_stock_stats_ci_new90_order": top50_sku_new_stock_stats_ci_new90_order,
    "top50_sku_new_stock_stats_ci_new180_order": top50_sku_new_stock_stats_ci_new180_order,
    # top50新品商品销量占比 ci
    "top50_sku_new_stock_stats_ci_new30_order_ratio": top50_sku_new_stock_stats_ci_new30_order_ratio,
    "top50_sku_new_stock_stats_ci_new90_order_ratio": top50_sku_new_stock_stats_ci_new90_order_ratio,
    "top50_sku_new_stock_stats_ci_new180_order_ratio": top50_sku_new_stock_stats_ci_new180_order_ratio,

    # top50新品商品数量 ful
    "top50_sku_new_stock_stats_ful_new30_count": top50_sku_new_stock_stats_ful_new30_count,
    "top50_sku_new_stock_stats_ful_new90_count": top50_sku_new_stock_stats_ful_new90_count,
    "top50_sku_new_stock_stats_ful_new180_count": top50_sku_new_stock_stats_ful_new180_count,
    # top50新品商品销量 ful
    "top50_sku_new_stock_stats_ful_new30_order": top50_sku_new_stock_stats_ful_new30_order,
    "top50_sku_new_stock_stats_ful_new90_order": top50_sku_new_stock_stats_ful_new90_order,
    "top50_sku_new_stock_stats_ful_new180_order": top50_sku_new_stock_stats_ful_new180_order,
    # top50新品商品销量占比 ful
    "top50_sku_new_stock_stats_ful_new30_order_ratio": top50_sku_new_stock_stats_ful_new30_order_ratio,
    "top50_sku_new_stock_stats_ful_new90_order_ratio": top50_sku_new_stock_stats_ful_new90_order_ratio,
    "top50_sku_new_stock_stats_ful_new180_order_ratio": top50_sku_new_stock_stats_ful_new180_order_ratio,

    # top50新品商品数量 nor
    "top50_sku_new_stock_stats_nor_new30_count": top50_sku_new_stock_stats_nor_new30_count,
    "top50_sku_new_stock_stats_nor_new90_count": top50_sku_new_stock_stats_nor_new90_count,
    "top50_sku_new_stock_stats_nor_new180_count": top50_sku_new_stock_stats_nor_new180_count,
    # top50新品商品销量 nor
    "top50_sku_new_stock_stats_nor_new30_order": top50_sku_new_stock_stats_nor_new30_order,
    "top50_sku_new_stock_stats_nor_new90_order": top50_sku_new_stock_stats_nor_new90_order,
    "top50_sku_new_stock_stats_nor_new180_order": top50_sku_new_stock_stats_nor_new180_order,
    # top50新品商品销量占比 nor
    "top50_sku_new_stock_stats_nor_new30_order_ratio": top50_sku_new_stock_stats_nor_new30_order_ratio,
    "top50_sku_new_stock_stats_nor_new90_order_ratio": top50_sku_new_stock_stats_nor_new90_order_ratio,
    "top50_sku_new_stock_stats_nor_new180_order_ratio": top50_sku_new_stock_stats_nor_new180_order_ratio,

    # all新品数据 ci
    "all_new_stock_stats_ci_new30_count": all_new_stock_stats_ci_new30_count,  # ci 新品30天数量
    "all_new_stock_stats_ci_new90_count": all_new_stock_stats_ci_new90_count,  # ci 新品90天数量
    "all_new_stock_stats_ci_new180_count": all_new_stock_stats_ci_new180_count,  # ci 新品180天数量
    "all_new_stock_stats_ci_new30_order": all_new_stock_stats_ci_new30_order,  # ci 新品30天销量
    "all_new_stock_stats_ci_new90_order": all_new_stock_stats_ci_new90_order,  # ci 新品90天销量
    "all_new_stock_stats_ci_new180_order": all_new_stock_stats_ci_new180_order,  # ci 新品180天销量
    "all_new_stock_stats_ci_new30_order_ratio": all_new_stock_stats_ci_new30_order_ratio,  # ci 新品30天销量占比
    "all_new_stock_stats_ci_new90_order_ratio": all_new_stock_stats_ci_new90_order_ratio,  # ci 新品90天销量占比
    "all_new_stock_stats_ci_new180_order_ratio": all_new_stock_stats_ci_new180_order_ratio,  # ci 新品180天销量占比

    # all新品数据 ful
    "all_new_stock_stats_ful_new30_count": all_new_stock_stats_ful_new30_count,  # ful 新品30天数量
    "all_new_stock_stats_ful_new90_count": all_new_stock_stats_ful_new90_count,  # ful 新品90天数量
    "all_new_stock_stats_ful_new180_count": all_new_stock_stats_ful_new180_count,  # ful 新品180天数量
    "all_new_stock_stats_ful_new30_order": all_new_stock_stats_ful_new30_order,  # ful 新品30天销量
    "all_new_stock_stats_ful_new90_order": all_new_stock_stats_ful_new90_order,  # ful 新品90天销量
    "all_new_stock_stats_ful_new180_order": all_new_stock_stats_ful_new180_order,  # ful 新品180天销量
    "all_new_stock_stats_ful_new30_order_ratio": all_new_stock_stats_ful_new30_order_ratio,  # ful 新品30天销量占比
    "all_new_stock_stats_ful_new90_order_ratio": all_new_stock_stats_ful_new90_order_ratio,  # ful 新品90天销量占比
    "all_new_stock_stats_ful_new180_order_ratio": all_new_stock_stats_ful_new180_order_ratio,  # ful 新品180天销量占比
    "all_new_stock_stats_ful_new30_price": all_new_stock_stats_ful_new30_price,  # ful 新品30天均价
    "all_new_stock_stats_ful_new90_price": all_new_stock_stats_ful_new90_price,  # ful 新品90天均价
    "all_new_stock_stats_ful_new180_price": all_new_stock_stats_ful_new180_price,  # ful 新品180天均价

    # all新品数据 nor
    "all_new_stock_stats_nor_new30_count": all_new_stock_stats_nor_new30_count,  # nor 新品30天数量
    "all_new_stock_stats_nor_new90_count": all_new_stock_stats_nor_new90_count,  # nor 新品90天数量
    "all_new_stock_stats_nor_new180_count": all_new_stock_stats_nor_new180_count,  # nor 新品180天数量
    "all_new_stock_stats_nor_new30_order": all_new_stock_stats_nor_new30_order,  # nor 新品30天销量
    "all_new_stock_stats_nor_new90_order": all_new_stock_stats_nor_new90_order,  # nor 新品90天销量
    "all_new_stock_stats_nor_new180_order": all_new_stock_stats_nor_new180_order,  # nor 新品180天销量
    "all_new_stock_stats_nor_new30_order_ratio": all_new_stock_stats_nor_new30_order_ratio,  # nor 新品30天销量占比
    "all_new_stock_stats_nor_new90_order_ratio": all_new_stock_stats_nor_new90_order_ratio,  # nor 新品90天销量占比
    "all_new_stock_stats_nor_new180_order_ratio": all_new_stock_stats_nor_new180_order_ratio,  # nor 新品180天销量占比
}
    big_Excel.update_one({"cat_id": cat_id,"time":int(month)}, {"$set": result}, upsert=True)
# =============================
# 主逻辑
# =============================
def build_category_report(cat_id):

    cat_doc = visual_plus.find_one({"cat_id": cat_id}, {"cat": 1, "order": 1})
    if not cat_doc:
        return {}

    z_order = cat_doc["order"][time[TIMES_4]]["monthly"]
    cats = cat_doc.get("cat", [])
    if not cats:
        return {}

    price_ranges = get_price_ranges(cat_id)
    months = get_last_n_months(TIMES_4, 13)
    month_data = get_sku_data_multi_month(cats, months)

    top10_brand_current = get_top_brands(month_data[TIMES_4]["brand_total"])
    top10_seller_current = get_top_sellers(month_data[TIMES_4]["seller_total"])
    total_seller_count = len(month_data[TIMES_4]["seller_total"])

    report = {
        "total_seller_count": {time[TIMES_4]: total_seller_count},
        "top10_brand_sales": {time[TIMES_4]: {}},
        "top10_brand_price_stats": {time[TIMES_4]: {}},
        "top10_seller_sales": {time[TIMES_4]: {}},
        "top10_commodity": {time[TIMES_4]: {}},
        "top50_sku_new_stock_stats": {time[TIMES_4]: {}},  # 新增Top50 SKU统计
        "all_new_stock_stats": {time[TIMES_4]: {}}
    }

    for m in months:
        report["top10_brand_sales"][time[TIMES_4]][time[m]] = {
            b: {
                "order": month_data[m]["brand_total"].get(b, 0),
                "order_ratio": month_data[m]["brand_total"].get(b, 0) / z_order if z_order else 0
            } for b in top10_brand_current
        }

        report["top10_brand_price_stats"][time[TIMES_4]][time[m]] = build_price_brand_structure(
            price_ranges,
            month_data[m]["sku_list"],
            top10_brand_current
        )

        report["top10_seller_sales"][time[TIMES_4]][time[m]] = {
            s: {
                "order": month_data[m]["seller_total"].get(s, 0),
                "order_ratio": month_data[m]["seller_total"].get(s, 0) / z_order if z_order else 0
            } for s in top10_seller_current
        }

        if m == TIMES_4:
            report["top10_commodity"][time[TIMES_4]] = get_top_skus(month_data[m]["sku_list"], top_n=10)

    # =============================
    # 新品统计
    # =============================
    year = int("20" + TIMES_4[:2])
    month = int(TIMES_4[2:])
    month_start = datetime(year, month, 1)
    month_end = datetime(year, month, calendar.monthrange(year, month)[1])

    sku_infos = month_data[TIMES_4]["sku_infos"]
    stock_map = month_data[TIMES_4]["stock_map"]
    sku_ids = list(sku_infos.keys())

    new30, new30_gmv = batch_new_sales(sku_infos, sku_ids, month_start, 30, month_end)
    new90, new90_gmv = batch_new_sales(sku_infos, sku_ids, month_start, 90, month_end)
    new180, new180_gmv = batch_new_sales(sku_infos, sku_ids, month_start, 180, month_end)

    stats = defaultdict(lambda: {
        "new30_count": 0, "new30_order": 0, "new30_price": 0,"new30_order_ratio": 0,
        "new90_count": 0, "new90_order": 0, "new90_price": 0,"new90_order_ratio": 0,
        "new180_count": 0, "new180_order": 0, "new180_price": 0,"new180_order_ratio": 0,
    })

    for sku in sku_ids:
        st = stock_map.get(sku, "unknown")
        if new30.get(sku, 0) > 0:
            stats[st]["new30_count"] += 1
            stats[st]["new30_order"] += new30[sku]
        if new90.get(sku, 0) > 0:
            stats[st]["new90_count"] += 1
            stats[st]["new90_order"] += new90[sku]
        if new180.get(sku, 0) > 0:
            stats[st]["new180_count"] += 1
            stats[st]["new180_order"] += new180[sku]

    stock_stats_list = ["ful","ci","nor"]
    for s in stock_stats_list:
        stats[s]["new30_order_ratio"] = stats[s]["new30_order"]/z_order
        stats[s]["new90_order_ratio"] = stats[s]["new90_order"]/z_order
        stats[s]["new180_order_ratio"] = stats[s]["new180_order"]/z_order

    for st, v in stats.items():
        if v["new30_order"] > 0:
            v["new30_price"] = round(
                sum(new30_gmv.get(s, 0)
                    for s in sku_ids if stock_map.get(s) == st) / v["new30_order"], 2)
        if v["new90_order"] > 0:
            v["new90_price"] = round(
                sum(new90_gmv.get(s, 0)
                    for s in sku_ids if stock_map.get(s) == st) / v["new90_order"], 2)
        if v["new180_order"] > 0:
            v["new180_price"] = round(
                sum(new180_gmv.get(s, 0)
                    for s in sku_ids if stock_map.get(s) == st) / v["new180_order"], 2)

    report["all_new_stock_stats"][time[TIMES_4]] = dict(stats)

    # =============================
    # Top50 SKU 新品按发货仓统计
    # =============================
    report["top50_sku_new_stock_stats"][time[TIMES_4]] = top50_sku_new_stock_stats(
        z_order, sku_infos, sku_ids, stock_map, month_start, month_end, top_n=50
    )

    # =============================
    # 年度 GMV 增长率统计
    # =============================
    gmv_2023 = calculate_year_gmv(month_data, months, "23")
    gmv_2024 = calculate_year_gmv(month_data, months, "24")
    gmv_2025 = calculate_year_gmv(month_data, months, "25")

    report["gmv_growth"] = {"2024":{}, "2025":{}}
    report["gmv_growth"]["2024"]["order"] = gmv_2024
    report["gmv_growth"]["2025"]["order"] = gmv_2025
    report["gmv_growth"]["2024"]["order_ratio"] = round((gmv_2024 - gmv_2023) / gmv_2023, 4) if gmv_2023 else 0
    report["gmv_growth"]["2025"]["order_ratio"] = round((gmv_2025 - gmv_2024) / gmv_2024, 4) if gmv_2024 else 0

    # =============================
    # 写入 visual_plus 表
    # =============================
    visual_plus_cx.update_one(
        {"cat_id": cat_id},
        {"$set": report},
        upsert=True
    )
    import_big_Excel(cat_id, report, time[TIMES_4])

    return report

# =============================
# 执行
# =============================
if __name__ == "__main__":
    datas_l = get_collection("main_ml_mx", "ml_mx", "visual_plus")

    # 加载已处理的 cat_id
    processed_cats = load_processed_cats()

    # 正式运行
    # datas = datas_l.find({}, {"cat_id": 1})
    # 测试运行
    cat_id = "MLM73299"
    datas = datas_l.find({"cat_id":cat_id}, {"cat_id": 1})

    # tqdm 进度条 + 断点续跑
    total_count = datas_l.count_documents({})
    with tqdm(total=total_count, desc="Processing categories", unit="cat") as pbar:
        for data in datas:
            cat_id = data["cat_id"]

            # 跳过已处理过的
            if cat_id in processed_cats:
                pbar.update(1)
                continue

            try:
                result = build_category_report(cat_id)
                save_processed_cat(cat_id)
            except Exception as e:
                tqdm.write(f"Error processing {cat_id}: {e}")
            finally:
                pbar.update(1)
