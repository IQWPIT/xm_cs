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
from functools import partial
from bisect import bisect_right

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
big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")

# =============================
# Globals (overridden in __main__/initializer)
# =============================
TIMES_4 = "2512"                 # YYMM
time = {}                        # {"YYMM": "20YYMM"}
LOG_FILE = "processed_cat.log"   # 固定一个日志文件


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
    return {m: "20" + m for m in months_yyMM}


# =======================================================================================================================
# 日志：固定一个文件，每行带 month
# 格式：YYYYMM \t cat_id \t run_ts
# =======================================================================================================================
def load_processed_cats(log_file: str, month_yyyymm: str):
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

    # ✅ 排序，保证二分查找正确
    ranges.sort(key=lambda x: x[0])
    return ranges


def build_price_index(price_ranges):
    """
    把 [(min,max,label)...] 预处理成二分可用结构
    返回：mins, maxs, labels
    """
    mins = [mn for mn, _, _ in price_ranges]
    maxs = [mx for _, mx, _ in price_ranges]
    labels = [lb for _, _, lb in price_ranges]
    return mins, maxs, labels


def price_to_label_idx(price_index, price):
    """
    二分查找价格带：找最后一个 min <= price 的区间，然后判断 price < max
    """
    mins, maxs, labels = price_index
    i = bisect_right(mins, price) - 1
    if i < 0:
        return None
    if price < maxs[i]:
        return labels[i]
    return None


def init_price_stats(months_keys, price_ranges, top_brands):
    template_label = {label: {b: {"order": 0} for b in top_brands} for _, _, label in price_ranges}
    return {
        mk: {label: {b: {"order": 0} for b in top_brands} for label in template_label.keys()}
        for mk in months_keys
    }


# =======================================================================================================================
# TopN
# =======================================================================================================================
def get_top_brands(brand_total, top_n=10):
    return dict(heapq.nlargest(top_n, brand_total.items(), key=lambda x: x[1]))


def get_top_sellers(seller_total, top_n=10):
    return dict(heapq.nlargest(top_n, seller_total.items(), key=lambda x: x[1]))


def build_top_skus_from_orders(sku_orders: dict, top_n=10):
    total_order = sum(sku_orders.values()) or 0
    top = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    return {
        sku: {"order": order, "order_ratio": (order / total_order) if total_order else 0}
        for sku, order in top
    }


# =======================================================================================================================
# 新品销量：一次 Mongo 查询同时算 30/90/180
# =======================================================================================================================
def batch_new_sales_multi(sku_infos, sku_ids, month_start, month_end, deltas=(30, 90, 180)):
    deltas = tuple(sorted(deltas))
    start_int = int(month_start.strftime("%Y%m%d"))
    end_int = int(month_end.strftime("%Y%m%d"))

    sku_limits = {}  # sku -> {delta: limit_dt_int}
    for sku in sku_ids:
        puton = str(sku_infos.get(sku, {}).get("puton_date", ""))
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

        limits = {}
        for dd in deltas:
            end_dt = min(puton_dt + timedelta(days=dd), month_end)
            if end_dt < month_start:
                continue
            limits[dd] = int(end_dt.strftime("%Y%m%d"))

        if limits:
            sku_limits[sku] = limits

    if not sku_limits:
        return {d: {} for d in deltas}, {d: {} for d in deltas}

    cursor = every_day_sku_col.find(
        {"sl": {"$in": list(sku_limits.keys())}, "dT": {"$gte": start_int, "$lte": end_int}},
        {"sl": 1, "oD": 1, "dT": 1, "pR": 1}
    )

    totals_map = {d: defaultdict(int) for d in deltas}
    gmvs_map = {d: defaultdict(float) for d in deltas}

    for doc in cursor:
        sku = doc["sl"]
        dT = doc.get("dT", 0) or 0
        od = doc.get("oD", 0) or 0
        pr = doc.get("pR", 0) or 0
        limits = sku_limits.get(sku)
        if not limits:
            continue

        for dd in deltas:
            limit_dt = limits.get(dd)
            if limit_dt and dT <= limit_dt:
                totals_map[dd][sku] += od
                gmvs_map[dd][sku] += od * pr

    totals_map = {d: dict(v) for d, v in totals_map.items()}
    gmvs_map = {d: dict(v) for d, v in gmvs_map.items()}
    return totals_map, gmvs_map


# =======================================================================================================================
# Pass1：聚合 13 个月品牌/店铺 + 当月 sku_infos/stock_map + 当月 sku_orders
# =======================================================================================================================
def scan_sku_aggregates_and_current(cats, months, cur_month):
    months_set = set(months)

    agg = {
        m: {"brand_total": defaultdict(int), "brand_total_gmv": defaultdict(float), "seller_total": defaultdict(int)}
        for m in months
    }

    cur_sku_infos = {}
    cur_stock_map = {}
    cur_sku_orders = defaultdict(int)

    cursor = sku_col.find(
        {"category_id": {"$in": cats}},
        {"_id": 0, "sku_id": 1, "brand": 1, "sellerName": 1, "monthly_sale_trend": 1,
         "active_price": 1, "puton_date": 1, "stock_type": 1}
    ).batch_size(2000)

    for d in cursor:
        trend = d.get("monthly_sale_trend") or {}
        if not trend:
            continue

        sku_id = d.get("sku_id")
        price = d.get("active_price")
        if not sku_id or price is None:
            continue

        brand = d.get("brand") or "Unknown"
        seller = d.get("sellerName") or "UnknownSeller"
        stock_type = d.get("stock_type") or "unknown"

        for m, order in trend.items():
            if m not in months_set or not order:
                continue

            a = agg[m]
            a["brand_total"][brand] += order
            a["brand_total_gmv"][brand] += order * price
            a["seller_total"][seller] += order

            if m == cur_month:
                cur_sku_orders[sku_id] += order
                cur_sku_infos[sku_id] = d
                cur_stock_map[sku_id] = stock_type

    for m in months:
        agg[m]["brand_total"] = dict(agg[m]["brand_total"])
        agg[m]["brand_total_gmv"] = dict(agg[m]["brand_total_gmv"])
        agg[m]["seller_total"] = dict(agg[m]["seller_total"])

    return agg, dict(cur_sku_orders), cur_sku_infos, cur_stock_map


# =======================================================================================================================
# Pass2：只对当月 top10 品牌做 13 个月价格带统计（每月准确）
# =======================================================================================================================
def scan_price_stats_for_top_brands(cats, months, top_brands, price_ranges, price_index):
    months_set = set(months)
    months_keys = [time[m] for m in months]
    price_stats = init_price_stats(months_keys, price_ranges, top_brands)

    cursor = sku_col.find(
        {"category_id": {"$in": cats}},
        {"_id": 0, "brand": 1, "monthly_sale_trend": 1, "active_price": 1}
    ).batch_size(2000)

    for d in cursor:
        brand = d.get("brand") or "Unknown"
        if brand not in top_brands:
            continue

        price = d.get("active_price")
        if price is None:
            continue
        try:
            price = float(price)
        except Exception:
            continue

        label = price_to_label_idx(price_index, price)
        if label is None:
            continue

        trend = d.get("monthly_sale_trend") or {}
        if not trend:
            continue

        for m, order in trend.items():
            if m not in months_set or not order:
                continue
            mk = time[m]
            price_stats[mk][label][brand]["order"] += order

    return price_stats


# =======================================================================================================================
# Top50 新品按仓统计
# =======================================================================================================================
def top50_sku_new_stock_stats(z_order, sku_infos, top_sku_ids, stock_map, month_start, month_end):
    totals_map, gmvs_map = batch_new_sales_multi(
        sku_infos, top_sku_ids, month_start, month_end, deltas=(30, 90, 180)
    )
    new30, new90, new180 = totals_map[30], totals_map[90], totals_map[180]
    new30_gmv, new90_gmv, new180_gmv = gmvs_map[30], gmvs_map[90], gmvs_map[180]

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

    for st in ["ful", "ci", "nor"]:
        stats[st]["new30_order_ratio"] = (stats[st]["new30_order"] / z_order) if z_order else 0
        stats[st]["new90_order_ratio"] = (stats[st]["new90_order"] / z_order) if z_order else 0
        stats[st]["new180_order_ratio"] = (stats[st]["new180_order"] / z_order) if z_order else 0

    for st, v in stats.items():
        if v["new30_order"] > 0:
            gmv = sum(new30_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st)
            v["new30_price"] = round(gmv / v["new30_order"], 2)
        if v["new90_order"] > 0:
            gmv = sum(new90_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st)
            v["new90_price"] = round(gmv / v["new90_order"], 2)
        if v["new180_order"] > 0:
            gmv = sum(new180_gmv.get(s, 0) for s in top_sku_ids if stock_map.get(s, "unknown") == st)
            v["new180_price"] = round(gmv / v["new180_order"], 2)

    return dict(stats)


# =======================================================================================================================
# 年度 GMV 增长率（用 brand_total_gmv 汇总）
# =======================================================================================================================
def calculate_year_gmv_from_agg(agg, months, year_str):
    total_gmv = 0.0
    for m in months:
        if m.startswith(year_str):
            total_gmv += sum(agg[m]["brand_total_gmv"].values()) if agg.get(m) else 0.0
    return total_gmv


# =======================================================================================================================
# 写入 big_Excel（保持你原字段不变）
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
# 主逻辑：构建类目报表
# =======================================================================================================================
def build_category_report(cat_id):
    cat_doc = visual_plus.find_one({"cat_id": cat_id}, {"cat": 1, "order": 1})
    if not cat_doc:
        return {}

    order_map = cat_doc.get("order", {}) or {}
    cats = cat_doc.get("cat", [])
    if not cats:
        return {}

    months = get_last_n_months(TIMES_4, 13)
    month_key = time[TIMES_4]  # "202512"

    z_order = (order_map.get(month_key, {}) or {}).get("monthly", 0) or 0

    price_ranges = get_price_ranges(cat_id)
    price_index = build_price_index(price_ranges)

    # Pass1
    agg, cur_sku_orders, cur_sku_infos, cur_stock_map = scan_sku_aggregates_and_current(cats, months, TIMES_4)

    top10_brand_current = get_top_brands(agg[TIMES_4]["brand_total"])
    top10_seller_current = get_top_sellers(agg[TIMES_4]["seller_total"])
    total_seller_count = len(agg[TIMES_4]["seller_total"])

    # Pass2：13个月每月价格带准确
    price_stats = scan_price_stats_for_top_brands(cats, months, top10_brand_current, price_ranges, price_index)

    # ✅ 这里必须是“嵌套结构”，不能用带点号的 key
    report = {
        "total_seller_count": {month_key: total_seller_count},
        "top10_brand_sales": {month_key: {}},
        "top10_brand_price_stats": {month_key: {}},
        "top10_seller_sales": {month_key: {}},
        "top10_commodity": {month_key: {}},
        "top50_sku_new_stock_stats": {month_key: {}},
        "all_new_stock_stats": {month_key: {}},
    }

    for m in months:
        m_key = time[m]
        z_order_m = (order_map.get(m_key, {}) or {}).get("monthly", 0) or 0

        report["top10_brand_sales"][month_key][m_key] = {
            b: {
                "order": agg[m]["brand_total"].get(b, 0),
                "order_ratio": (agg[m]["brand_total"].get(b, 0) / z_order_m) if z_order_m else 0,
                "gmv": agg[m]["brand_total_gmv"].get(b, 0),
                "price": (agg[m]["brand_total_gmv"].get(b, 0) / agg[m]["brand_total"].get(b, 0))
                if agg[m]["brand_total"].get(b, 0) else 0,
            } for b in top10_brand_current
        }

        report["top10_brand_price_stats"][month_key][m_key] = price_stats.get(m_key, {})
        report["top10_seller_sales"][month_key][m_key] = {
            s: {
                "order": agg[m]["seller_total"].get(s, 0),
                "order_ratio": (agg[m]["seller_total"].get(s, 0) / z_order_m) if z_order_m else 0
            } for s in top10_seller_current
        }

    report["top10_commodity"][month_key] = build_top_skus_from_orders(cur_sku_orders, top_n=10)

    # 新品统计（当月）
    year = int("20" + TIMES_4[:2])
    month = int(TIMES_4[2:])
    month_start = datetime(year, month, 1)
    month_end = datetime(year, month, calendar.monthrange(year, month)[1])

    sku_ids = list(cur_sku_infos.keys())

    totals_map, gmvs_map = batch_new_sales_multi(cur_sku_infos, sku_ids, month_start, month_end, deltas=(30, 90, 180))
    new30, new90, new180 = totals_map[30], totals_map[90], totals_map[180]
    new30_gmv, new90_gmv, new180_gmv = gmvs_map[30], gmvs_map[90], gmvs_map[180]

    stats = defaultdict(lambda: {
        "new30_count": 0, "new30_order": 0, "new30_price": 0, "new30_order_ratio": 0,
        "new90_count": 0, "new90_order": 0, "new90_price": 0, "new90_order_ratio": 0,
        "new180_count": 0, "new180_order": 0, "new180_price": 0, "new180_order_ratio": 0,
    })

    for sku in sku_ids:
        st = cur_stock_map.get(sku, "unknown")
        if new30.get(sku, 0) > 0:
            stats[st]["new30_count"] += 1
            stats[st]["new30_order"] += new30[sku]
        if new90.get(sku, 0) > 0:
            stats[st]["new90_count"] += 1
            stats[st]["new90_order"] += new90[sku]
        if new180.get(sku, 0) > 0:
            stats[st]["new180_count"] += 1
            stats[st]["new180_order"] += new180[sku]

    for st in ["ful", "ci", "nor"]:
        stats[st]["new30_order_ratio"] = (stats[st]["new30_order"] / z_order) if z_order else 0
        stats[st]["new90_order_ratio"] = (stats[st]["new90_order"] / z_order) if z_order else 0
        stats[st]["new180_order_ratio"] = (stats[st]["new180_order"] / z_order) if z_order else 0

    for st, v in stats.items():
        if v["new30_order"] > 0:
            gmv = sum(new30_gmv.get(s, 0) for s in sku_ids if cur_stock_map.get(s) == st)
            v["new30_price"] = round(gmv / v["new30_order"], 2)
        if v["new90_order"] > 0:
            gmv = sum(new90_gmv.get(s, 0) for s in sku_ids if cur_stock_map.get(s) == st)
            v["new90_price"] = round(gmv / v["new90_order"], 2)
        if v["new180_order"] > 0:
            gmv = sum(new180_gmv.get(s, 0) for s in sku_ids if cur_stock_map.get(s) == st)
            v["new180_price"] = round(gmv / v["new180_order"], 2)

    report["all_new_stock_stats"][month_key] = dict(stats)

    top50 = heapq.nlargest(50, cur_sku_orders.items(), key=lambda x: x[1])
    top50_ids = [sku for sku, _ in top50]
    report["top50_sku_new_stock_stats"][month_key] = top50_sku_new_stock_stats(
        z_order, cur_sku_infos, top50_ids, cur_stock_map, month_start, month_end
    )

    # 年度 GMV 增长率
    gmv_2023 = calculate_year_gmv_from_agg(agg, months, "23")
    gmv_2024 = calculate_year_gmv_from_agg(agg, months, "24")
    gmv_2025 = calculate_year_gmv_from_agg(agg, months, "25")
    report["gmv_growth"] = {"2024": {}, "2025": {}}
    report["gmv_growth"]["2024"]["order"] = gmv_2024
    report["gmv_growth"]["2025"]["order"] = gmv_2025
    report["gmv_growth"]["2024"]["order_ratio"] = round((gmv_2024 - gmv_2023) / gmv_2023, 4) if gmv_2023 else 0
    report["gmv_growth"]["2025"]["order_ratio"] = round((gmv_2025 - gmv_2024) / gmv_2024, 4) if gmv_2024 else 0
    print(report)
    report_s = {
        f"total_seller_count.{month_key}": report["total_seller_count"][month_key],
        f"top10_brand_sales.{month_key}": report["top10_brand_sales"][month_key],
        f"top10_brand_price_stats.{month_key}": report["top10_brand_price_stats"][month_key],
        f"top10_seller_sales.{month_key}": report["top10_seller_sales"][month_key],
        f"top10_commodity.{month_key}": report["top10_commodity"][month_key],
        f"top50_sku_new_stock_stats.{month_key}": report["top50_sku_new_stock_stats"][month_key],
        f"all_new_stock_stats.{month_key}": report["all_new_stock_stats"][month_key],
    }
    visual_plus.update_one({"cat_id": cat_id}, {"$set": report_s}, upsert=True)
    import_big_Excel(cat_id, report, month_key)
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
# 子进程初始化函数（Windows spawn 必需）
# =======================================================================================================================
def init_worker_globals(times_4, months_yyMM):
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
    month_yyyymm = time[TIMES_4]

    mp.set_start_method("spawn", force=True)

    datas_l = get_collection("main_ml_mx", "ml_mx", "visual_plus")
    processed_cats = load_processed_cats(LOG_FILE, month_yyyymm)

    TEST_CAT_ID = "MLM446796"  # None 表示跑全量
    if TEST_CAT_ID:
        query = {"cat_id": TEST_CAT_ID}
    else:
        query = {"cat_id": {"$nin": list(processed_cats)}}

    datas = datas_l.find(query, {"cat_id": 1, "_id": 0}, no_cursor_timeout=True)
    cat_list = [d.get("cat_id") for d in datas if d.get("cat_id")]
    datas.close()

    total_count = len(cat_list)
    print(f"Month={month_yyyymm}, Total to process: {total_count}")

    stop_event = mp.Event()

    def handle_sigint(sig, frame):
        print("\nGracefully stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    process_count = max(mp.cpu_count() - 1, 1)
    chunksize = 10

    worker_fn = partial(worker, log_file=LOG_FILE, month_yyyymm=month_yyyymm)

    with mp.Pool(
        process_count,
        initializer=init_worker_globals,
        initargs=(TIMES_4, months),
    ) as pool:

        it = pool.imap_unordered(worker_fn, cat_list, chunksize=chunksize)

        with tqdm(total=total_count, desc="Processing", unit="cat") as pbar:
            for success, cat_id, error in it:
                if stop_event.is_set():
                    break
                if not success:
                    tqdm.write(f"[ERROR] {cat_id}: {error}")
                pbar.update(1)

        pool.terminate()

    print("Finished.")