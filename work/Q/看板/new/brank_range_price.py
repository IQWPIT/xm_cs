# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from collections import defaultdict
import heapq
import os
import calendar
from tqdm import tqdm
import signal
import multiprocessing as mp
import argparse
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# =============================
# Mongo collections
# =============================
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
price_segment_col = get_collection("main_ml_mx", "ml_mx", "price_segment")
every_day_sku_col = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
visual_plus_cx = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")

# =============================
# Globals (overridden in __main__)
# =============================
TIMES_4 = "2512"          # YYMM
time = {}                # {"YYMM": "20YYMM"}
LOG_FILE = "processed_cat.log"  # ✅ 固定一个日志文件


# =======================================================================================================================
# 参数
# =======================================================================================================================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--month",
        required=True,
        help="统计月份，例如 2601 表示 2026年1月（格式：YYMM，4位数字）"
    )
    return parser.parse_args()


# =======================================================================================================================
# 工具函数
# =======================================================================================================================
def get_last_n_months(times_4: str, n: int = 13):
    """
    times_4: "YYMM" 例如 "2512"
    return: ["YYMM", ...] 按时间从旧到新排列，长度 n
    """
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


def make_time_map(months_yyMM):
    """把 YYMM -> 20YYMM"""
    return {m: "20" + m for m in months_yyMM}


# =======================================================================================================================
# ✅ 日志：固定一个文件，每行带 month
#   格式：YYYYMM \t cat_id \t run_ts
# =======================================================================================================================
def load_processed_cats(log_file: str, month_yyyymm: str):
    """
    只读取“指定月份 month_yyyymm”已经处理过的 cat_id
    """
    processed = set()
    if not os.path.exists(log_file):
        return processed

    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            m, cat_id = parts[0], parts[1]
            if m == month_yyyymm and cat_id:
                processed.add(cat_id)
    return processed


def save_processed_cat(log_file: str, month_yyyymm: str, cat_id: str):
    """
    追加一行处理记录：YYYYMM\tcat_id\tYYYY-MM-DD HH:MM:SS
    """
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{month_yyyymm}\t{cat_id}\t{run_ts}\n")


# =======================================================================================================================
# 价格带
# =======================================================================================================================
def get_price_ranges(cat_id):
    doc = price_segment_col.find_one({"cat_id": cat_id}, {"price_ranges": 1})
    if not doc:
        return []
    ranges = []
    for r in doc.get("price_ranges", []):
        try:
            min_p, max_p = r.split("-")
            ranges.append((float(min_p), float(max_p), r))
        except Exception:
            continue
    return ranges


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
# 新品销量（every_day_sku）
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
        except Exception:
            continue

        end_dt = min(puton_dt + timedelta(days=delta_days), month_end)
        if end_dt < month_start:
            continue

        sku_limit[sku] = int(end_dt.strftime("%Y%m%d"))

    if not sku_limit:
        return {}, {}

    # ✅ 包含 month_end 当天
    cursor = every_day_sku_col.find(
        {"sl": {"$in": list(sku_limit.keys())},
         "dT": {"$gte": start_int, "$lte": month_end_int}},
        {"sl": 1, "oD": 1, "dT": 1, "pR": 1}
    )

    totals = defaultdict(int)
    gmvs = defaultdict(float)

    for doc in cursor:
        sku = doc["sl"]
        if doc["dT"] <= sku_limit.get(sku, month_end_int):
            od = doc.get("oD", 0) or 0
            pr = doc.get("pR", 0) or 0
            totals[sku] += od
            gmvs[sku] += od * pr

    return totals, gmvs


# =======================================================================================================================
# TopN
# =======================================================================================================================
def get_top_brands(brand_total, top_n=10):
    return dict(heapq.nlargest(top_n, brand_total.items(), key=lambda x: x[1]))


def get_top_sellers(seller_total, top_n=10):
    return dict(heapq.nlargest(top_n, seller_total.items(), key=lambda x: x[1]))


def get_top_skus(sku_list, top_n=10):
    total_order = sum([o for _, _, _, o in sku_list]) or 0
    sku_orders = defaultdict(int)
    for sku_id, _, _, order in sku_list:
        sku_orders[sku_id] += order

    top_skus = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    return {
        sku: {"order": order, "order_ratio": (order / total_order) if total_order else 0}
        for sku, order in top_skus
    }


# =======================================================================================================================
# Top50 新品按仓统计
# =======================================================================================================================
def top50_sku_new_stock_stats(z_order, sku_infos, sku_ids, stock_map, month_start, month_end, top_n=50):
    sku_orders = defaultdict(int)
    for sku in sku_ids:
        sku_data = sku_infos.get(sku)
        if not sku_data:
            continue
        trend = sku_data.get("monthly_sale_trend", {}) or {}
        order = trend.get(TIMES_4, 0) or 0
        if order:
            sku_orders[sku] += order

    top_skus = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    top_sku_ids = [sku for sku, _ in top_skus]

    new30, new30_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 30, month_end)
    new90, new90_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 90, month_end)
    new180, new180_gmv = batch_new_sales(sku_infos, top_sku_ids, month_start, 180, month_end)

    stats = defaultdict(lambda: {
        "new30_count": 0, "new30_order": 0, "new30_price": 0, "new30_order_ratio": 0,
        "new90_count": 0, "new90_order": 0, "new90_price": 0, "new90_order_ratio": 0,
        "new180_count": 0, "new180_order": 0, "new180_price": 0, "new180_order_ratio": 0,
    })

    for sku in top_sku_ids:
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

    stock_stats_list = ["ful", "ci", "nor"]
    for s in stock_stats_list:
        stats[s]["new30_order_ratio"] = (stats[s]["new30_order"] / z_order) if z_order else 0
        stats[s]["new90_order_ratio"] = (stats[s]["new90_order"] / z_order) if z_order else 0
        stats[s]["new180_order_ratio"] = (stats[s]["new180_order"] / z_order) if z_order else 0

    for st, v in stats.items():
        v["new30_price"] = round(
            (sum(new30_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st) / v["new30_order"]),
            2
        ) if v["new30_order"] > 0 else 0

        v["new90_price"] = round(
            (sum(new90_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st) / v["new90_order"]),
            2
        ) if v["new90_order"] > 0 else 0

        v["new180_price"] = round(
            (sum(new180_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st) / v["new180_order"]),
            2
        ) if v["new180_order"] > 0 else 0

    return dict(stats)


# =======================================================================================================================
# 年度 GMV
# =======================================================================================================================
def calculate_year_gmv(month_data, months, year_str):
    total_gmv = 0.0
    for m in months:
        if m.startswith(year_str):
            for _, _, price, order in month_data[m]["sku_list"]:
                try:
                    total_gmv += float(price) * float(order)
                except Exception:
                    continue
    return total_gmv


# =======================================================================================================================
# 写入 big_Excel
# =======================================================================================================================
def import_big_Excel(cat_id, report, month_yyyymm: str):
    month_key = month_yyyymm

    gmv_growth_order_ratio = report.get("gmv_growth", {}).get("2025", {}).get("order_ratio", 0)

    top10_commodity_sku_id_order = report.get("top10_commodity", {}).get(month_key, {})
    top10_commodity_sku_id_order_sum = 0
    top10_commodity_sku_id_order_sum_ratio = 0.0
    for _, orders in top10_commodity_sku_id_order.items():
        top10_commodity_sku_id_order_sum += orders.get("order", 0) or 0
        top10_commodity_sku_id_order_sum_ratio += orders.get("order_ratio", 0) or 0

    top3_seller_sales_order_ratio_data = report.get("top10_seller_sales", {}).get(month_key, {}).get(month_key, {})
    top3_seller_sales_order_ratio_sum_ratio = 0.0
    top1_seller_sales_order_ratio_sum_ratio = 0.0
    i = 0
    for _, orders in top3_seller_sales_order_ratio_data.items():
        i += 1
        ratio = orders.get("order_ratio", 0) or 0
        if i == 1:
            top1_seller_sales_order_ratio_sum_ratio += ratio
            top3_seller_sales_order_ratio_sum_ratio += ratio
        else:
            top3_seller_sales_order_ratio_sum_ratio += ratio
            if i > 3:
                break

    top10_brand_sales_order_ratio_data = report.get("top10_brand_sales", {}).get(month_key, {}).get(month_key, {})
    top1_brand_sales_order_ratio_sum_ratio = 0.0
    top1_brand_sales_order_ratio_name = ""
    top3_brand_sales_order_ratio_sum_ratio = 0.0
    i = 0
    for brand_sales, orders in top10_brand_sales_order_ratio_data.items():
        i += 1
        ratio = orders.get("order_ratio", 0) or 0
        if i == 1:
            top1_brand_sales_order_ratio_sum_ratio += ratio
            top3_brand_sales_order_ratio_sum_ratio += ratio
            top1_brand_sales_order_ratio_name = brand_sales
        else:
            top3_brand_sales_order_ratio_sum_ratio += ratio
            if i > 3:
                break

    top50 = report.get("top50_sku_new_stock_stats", {}).get(month_key, {})
    all_new = report.get("all_new_stock_stats", {}).get(month_key, {})

    def g(d, *path, default=0):
        cur = d
        for p in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(p)
        return cur if cur is not None else default

    result = {
        "cat_id": cat_id,
        "time": int(month_key),

        "2025_gmv_growth_order_ratio": gmv_growth_order_ratio,

        "top10_commodity_sku_id_order": top10_commodity_sku_id_order_sum,
        "top10_commodity_sku_id_order_ratio": top10_commodity_sku_id_order_sum_ratio,

        "top1_seller_sales_order_ratio_sum_ratio": top1_seller_sales_order_ratio_sum_ratio,
        "top3_seller_sales_order_ratio_sum_ratio": top3_seller_sales_order_ratio_sum_ratio,

        "top1_brand_sales_order_ratio_sum_ratio": top1_brand_sales_order_ratio_sum_ratio,
        "top1_brand_sales_order_ratio_name": top1_brand_sales_order_ratio_name,
        "top3_brand_sales_order_ratio_sum_ratio": top3_brand_sales_order_ratio_sum_ratio,

        "top50_sku_new_stock_stats_ci_new30_count": g(top50, "ci", "new30_count"),
        "top50_sku_new_stock_stats_ci_new90_count": g(top50, "ci", "new90_count"),
        "top50_sku_new_stock_stats_ci_new180_count": g(top50, "ci", "new180_count"),
        "top50_sku_new_stock_stats_ci_new30_order": g(top50, "ci", "new30_order"),
        "top50_sku_new_stock_stats_ci_new90_order": g(top50, "ci", "new90_order"),
        "top50_sku_new_stock_stats_ci_new180_order": g(top50, "ci", "new180_order"),
        "top50_sku_new_stock_stats_ci_new30_order_ratio": g(top50, "ci", "new30_order_ratio"),
        "top50_sku_new_stock_stats_ci_new90_order_ratio": g(top50, "ci", "new90_order_ratio"),
        "top50_sku_new_stock_stats_ci_new180_order_ratio": g(top50, "ci", "new180_order_ratio"),

        "top50_sku_new_stock_stats_ful_new30_count": g(top50, "ful", "new30_count"),
        "top50_sku_new_stock_stats_ful_new90_count": g(top50, "ful", "new90_count"),
        "top50_sku_new_stock_stats_ful_new180_count": g(top50, "ful", "new180_count"),
        "top50_sku_new_stock_stats_ful_new30_order": g(top50, "ful", "new30_order"),
        "top50_sku_new_stock_stats_ful_new90_order": g(top50, "ful", "new90_order"),
        "top50_sku_new_stock_stats_ful_new180_order": g(top50, "ful", "new180_order"),
        "top50_sku_new_stock_stats_ful_new30_order_ratio": g(top50, "ful", "new30_order_ratio"),
        "top50_sku_new_stock_stats_ful_new90_order_ratio": g(top50, "ful", "new90_order_ratio"),
        "top50_sku_new_stock_stats_ful_new180_order_ratio": g(top50, "ful", "new180_order_ratio"),

        "top50_sku_new_stock_stats_nor_new30_count": g(top50, "nor", "new30_count"),
        "top50_sku_new_stock_stats_nor_new90_count": g(top50, "nor", "new90_count"),
        "top50_sku_new_stock_stats_nor_new180_count": g(top50, "nor", "new180_count"),
        "top50_sku_new_stock_stats_nor_new30_order": g(top50, "nor", "new30_order"),
        "top50_sku_new_stock_stats_nor_new90_order": g(top50, "nor", "new90_order"),
        "top50_sku_new_stock_stats_nor_new180_order": g(top50, "nor", "new180_order"),
        "top50_sku_new_stock_stats_nor_new30_order_ratio": g(top50, "nor", "new30_order_ratio"),
        "top50_sku_new_stock_stats_nor_new90_order_ratio": g(top50, "nor", "new90_order_ratio"),
        "top50_sku_new_stock_stats_nor_new180_order_ratio": g(top50, "nor", "new180_order_ratio"),

        "all_new_stock_stats_ci_new30_count": g(all_new, "ci", "new30_count"),
        "all_new_stock_stats_ci_new90_count": g(all_new, "ci", "new90_count"),
        "all_new_stock_stats_ci_new180_count": g(all_new, "ci", "new180_count"),
        "all_new_stock_stats_ci_new30_order": g(all_new, "ci", "new30_order"),
        "all_new_stock_stats_ci_new90_order": g(all_new, "ci", "new90_order"),
        "all_new_stock_stats_ci_new180_order": g(all_new, "ci", "new180_order"),
        "all_new_stock_stats_ci_new30_order_ratio": g(all_new, "ci", "new30_order_ratio"),
        "all_new_stock_stats_ci_new90_order_ratio": g(all_new, "ci", "new90_order_ratio"),
        "all_new_stock_stats_ci_new180_order_ratio": g(all_new, "ci", "new180_order_ratio"),

        "all_new_stock_stats_ful_new30_count": g(all_new, "ful", "new30_count"),
        "all_new_stock_stats_ful_new90_count": g(all_new, "ful", "new90_count"),
        "all_new_stock_stats_ful_new180_count": g(all_new, "ful", "new180_count"),
        "all_new_stock_stats_ful_new30_order": g(all_new, "ful", "new30_order"),
        "all_new_stock_stats_ful_new90_order": g(all_new, "ful", "new90_order"),
        "all_new_stock_stats_ful_new180_order": g(all_new, "ful", "new180_order"),
        "all_new_stock_stats_ful_new30_order_ratio": g(all_new, "ful", "new30_order_ratio"),
        "all_new_stock_stats_ful_new90_order_ratio": g(all_new, "ful", "new90_order_ratio"),
        "all_new_stock_stats_ful_new180_order_ratio": g(all_new, "ful", "new180_order_ratio"),
        "all_new_stock_stats_ful_new30_price": g(all_new, "ful", "new30_price"),
        "all_new_stock_stats_ful_new90_price": g(all_new, "ful", "new90_price"),
        "all_new_stock_stats_ful_new180_price": g(all_new, "ful", "new180_price"),

        "all_new_stock_stats_nor_new30_count": g(all_new, "nor", "new30_count"),
        "all_new_stock_stats_nor_new90_count": g(all_new, "nor", "new90_count"),
        "all_new_stock_stats_nor_new180_count": g(all_new, "nor", "new180_count"),
        "all_new_stock_stats_nor_new30_order": g(all_new, "nor", "new30_order"),
        "all_new_stock_stats_nor_new90_order": g(all_new, "nor", "new90_order"),
        "all_new_stock_stats_nor_new180_order": g(all_new, "nor", "new180_order"),
        "all_new_stock_stats_nor_new30_order_ratio": g(all_new, "nor", "new30_order_ratio"),
        "all_new_stock_stats_nor_new90_order_ratio": g(all_new, "nor", "new90_order_ratio"),
        "all_new_stock_stats_nor_new180_order_ratio": g(all_new, "nor", "new180_order_ratio"),
    }

    big_Excel.update_one({"cat_id": cat_id, "time": int(month_key)}, {"$set": result}, upsert=True)


# =======================================================================================================================
# 读取mongodb数据
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
            "brand_total_gmv": defaultdict(float),
            "seller_total": defaultdict(int)
        }
        for m in months
    }

    for d in cursor:
        trend = d.get("monthly_sale_trend") or {}
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
            order = trend.get(m, 0) or 0
            if order:
                month_data[m]["sku_list"].append((sku_id, brand, price, order))
                month_data[m]["sku_infos"][sku_id] = d
                month_data[m]["stock_map"][sku_id] = stock_type
                month_data[m]["brand_total"][brand] += order
                month_data[m]["brand_total_gmv"][brand] += order * price
                month_data[m]["seller_total"][seller] += order

    return month_data


# =======================================================================================================================
# 主逻辑：构建类目报表
# =======================================================================================================================
def build_category_report(cat_id):
    cat_doc = visual_plus.find_one({"cat_id": cat_id}, {"cat": 1, "order": 1})
    if not cat_doc:
        return {}

    # 当前月总销量
    z_order = (cat_doc.get("order", {}).get(time[TIMES_4], {}) or {}).get("monthly", 0) or 0

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
        "top50_sku_new_stock_stats": {time[TIMES_4]: {}},
        "all_new_stock_stats": {time[TIMES_4]: {}}
    }

    for m in months:
        z_order_m = (cat_doc.get("order", {}).get(time[m], {}) or {}).get("monthly", 0) or 0

        report["top10_brand_sales"][time[TIMES_4]][time[m]] = {
            b: {
                "order": month_data[m]["brand_total"].get(b, 0),
                "order_ratio": (month_data[m]["brand_total"].get(b, 0) / z_order_m) if z_order_m else 0,
                "gmv": month_data[m]["brand_total_gmv"].get(b, 0),
                "price": (month_data[m]["brand_total_gmv"].get(b, 0) / month_data[m]["brand_total"].get(b, 0))
                if month_data[m]["brand_total"].get(b, 0) else 0,
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
                "order_ratio": (month_data[m]["seller_total"].get(s, 0) / z_order_m) if z_order_m else 0
            } for s in top10_seller_current
        }

        if m == TIMES_4:
            report["top10_commodity"][time[TIMES_4]] = get_top_skus(month_data[m]["sku_list"], top_n=10)

    # =============================
    # 新品统计（全量）
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
        "new30_count": 0, "new30_order": 0, "new30_price": 0, "new30_order_ratio": 0,
        "new90_count": 0, "new90_order": 0, "new90_price": 0, "new90_order_ratio": 0,
        "new180_count": 0, "new180_order": 0, "new180_price": 0, "new180_order_ratio": 0,
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

    stock_stats_list = ["ful", "ci", "nor"]
    for s in stock_stats_list:
        stats[s]["new30_order_ratio"] = (stats[s]["new30_order"] / z_order) if z_order else 0
        stats[s]["new90_order_ratio"] = (stats[s]["new90_order"] / z_order) if z_order else 0
        stats[s]["new180_order_ratio"] = (stats[s]["new180_order"] / z_order) if z_order else 0

    for st, v in stats.items():
        if v["new30_order"] > 0:
            v["new30_price"] = round(
                sum(new30_gmv.get(s, 0) for s in sku_ids if stock_map.get(s) == st) / v["new30_order"], 2
            )
        if v["new90_order"] > 0:
            v["new90_price"] = round(
                sum(new90_gmv.get(s, 0) for s in sku_ids if stock_map.get(s) == st) / v["new90_order"], 2
            )
        if v["new180_order"] > 0:
            v["new180_price"] = round(
                sum(new180_gmv.get(s, 0) for s in sku_ids if stock_map.get(s) == st) / v["new180_order"], 2
            )

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

    report["gmv_growth"] = {"2024": {}, "2025": {}}
    report["gmv_growth"]["2024"]["order"] = gmv_2024
    report["gmv_growth"]["2025"]["order"] = gmv_2025
    report["gmv_growth"]["2024"]["order_ratio"] = round((gmv_2024 - gmv_2023) / gmv_2023, 4) if gmv_2023 else 0
    report["gmv_growth"]["2025"]["order_ratio"] = round((gmv_2025 - gmv_2024) / gmv_2024, 4) if gmv_2024 else 0

    # =============================
    # 写入 visual_plus 表
    # =============================
    visual_plus.update_one({"cat_id": cat_id}, {"$set": report}, upsert=True)

    # 写入 big_Excel
    import_big_Excel(cat_id, report, time[TIMES_4])

    return report


# =======================================================================================================================
# 子进程任务函数
# =======================================================================================================================
def worker(cat_id, log_file, month_yyyymm):
    try:
        build_category_report(cat_id)
        save_processed_cat(log_file, month_yyyymm, cat_id)
        return True, cat_id, None
    except Exception as e:
        return False, cat_id, str(e)


# =======================================================================================================================
# 子进程初始化函数
# =======================================================================================================================
def init_worker_globals(times_4, months_yyMM):
    """
    Windows spawn: 子进程不会执行 __main__，所以必须在这里初始化全局变量
    """
    global TIMES_4, time
    TIMES_4 = times_4
    time = make_time_map(months_yyMM)

# =============================
# 执行入口
# =============================
if __name__ == "__main__":
    args = parse_args()
    TIMES_4 = args.month  # YYMM

    if not (TIMES_4.isdigit() and len(TIMES_4) == 4):
        raise ValueError("--month 必须是 4 位数字，例如 2601 / 2512")

    months = get_last_n_months(TIMES_4, 13)
    time = make_time_map(months)

    month_yyyymm = time[TIMES_4]  # 例如 "202512"

    mp.set_start_method("spawn", force=True)  # Windows必须

    datas_l = get_collection("main_ml_mx", "ml_mx", "visual_plus")

    # ✅ 只过滤“当月已经处理过”的 cat_id
    processed_cats = load_processed_cats(LOG_FILE, month_yyyymm)

    TEST_CAT_ID = "MLM1055"  # 例如 "MLM73299"
    if TEST_CAT_ID:
        query = {"cat_id": TEST_CAT_ID}
    else:
        query = {"cat_id": {"$nin": list(processed_cats)}}

    datas = datas_l.find(query, {"cat_id": 1, "_id": 0}, no_cursor_timeout=True)
    cat_list = [d.get("cat_id") for d in datas if d.get("cat_id")]
    datas.close()

    total_count = len(cat_list)
    print(f"Month={month_yyyymm}, Total to process: {total_count}")

    # 优雅停止
    stop_event = mp.Event()

    def handle_sigint(sig, frame):
        print("\nGracefully stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    process_count = max(mp.cpu_count() - 1, 1)

    with mp.Pool(
            process_count,
            initializer=init_worker_globals,
            initargs=(TIMES_4, months),
    ) as pool:
        results = []
        for cat_id in cat_list:
            if stop_event.is_set():
                break
            results.append(pool.apply_async(worker, args=(cat_id, LOG_FILE, month_yyyymm)))

        pool.close()

        with tqdm(total=total_count, desc="Processing", unit="cat") as pbar:
            for r in results:
                if stop_event.is_set():
                    break
                success, cat_id, error = r.get()
                if not success:
                    tqdm.write(f"[ERROR] {cat_id}: {error}")
                pbar.update(1)

        pool.terminate()

    print("Finished.")