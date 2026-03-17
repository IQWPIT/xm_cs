# -*- coding: utf-8 -*-
"""
看板数据整合脚本 - ml_mx
整合功能：
  1. 生成字段说明表格 (生成表格.py)
  2. 全量类目报表构建 + 写入 visual_plus / _big_Excel (brank_range_price.py / 测试.py)
  3. 2025 GMV 增长率回填 _big_Excel (2025gmv.py)
  4. ERP 数据同步到 bi_category_data (1.py)
  5. 退款率/广告数据回填 visual_plus (退款率.py)
  6. Top50/Top100 SKU + offersInf 统计 (_big_Excel / visual_plus) (top50_sku.py)
  7. Top50 SKU 详细指标写入 _big_Excel (top50.py)
  8. _big_Excel → visual_plus 字段同步 (plus.py)
  9. 全量 _big_Excel 主指标计算 (all.py)

用法：
  python main.py --task all          # 按流程顺序全跑
  python main.py --task report       # 仅构建类目报表 (功能2)
  python main.py --task gmv2025      # 仅回填 2025 GMV (功能3)
  python main.py --task sync_erp     # 仅同步 ERP 数据 (功能4)
  python main.py --task refund       # 仅回填退款率/广告 (功能5)
  python main.py --task top50sku     # 仅 Top50 SKU 统计 (功能6)
  python main.py --task top50detail  # 仅 Top50 详细指标 (功能7)
  python main.py --task sync_plus    # 仅同步 plus 字段 (功能8)
  python main.py --task bigexcel     # 仅计算全量指标 (功能9)
  python main.py --task schema       # 仅生成字段说明 xlsx (功能1)

测试模式（加 --dry-run）：只读取 MongoDB，不写入任何数据，打印将要写入的内容。
  python main.py --task report --dry-run
  python main.py --task all    --dry-run --test_cat MLM446796
"""

import ast
import calendar
import heapq
import logging
import math
import multiprocessing as mp
import os
import re
import signal
import traceback
from bisect import bisect_right
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import partial
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock

import pandas as pd
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =============================================================================
# 测试模式（DRY_RUN）
# =============================================================================
# 由 parse_args() 在 __main__ 中设置，子进程通过 init_worker_globals 继承。
DRY_RUN: bool = False


class _FakeWriteResult:
    """bulk_write / update_one 的假返回值，避免调用方 .matched_count 报错。"""
    matched_count  = 0
    modified_count = 0
    upserted_count = 0


def _col_write(collection, ops_or_doc, filter_=None, upsert=False, ordered=False):
    """
    统一写入代理：
      - DRY_RUN=True  → 打印完整内容，不写库，返回假结果
      - DRY_RUN=False → 正常写入
    ops_or_doc 可以是：
      list[UpdateOne]  → 走 bulk_write
      dict             → 走 update_one（需传 filter_）
    """
    if DRY_RUN:
        import json
        col_name = getattr(collection, "name", str(collection))
        sep = "─" * 60

        if isinstance(ops_or_doc, list):
            print(f"\n{sep}")
            print(f"[DRY-RUN] bulk_write → 集合: {col_name}  共 {len(ops_or_doc)} 条")
            print(sep)
            for i, op in enumerate(ops_or_doc):
                # UpdateOne 内部存在 _filter / _doc
                f = getattr(op, "_filter", None)
                d = getattr(op, "_doc", None) or {}
                set_doc = d.get("$set", d)
                print(f"  [{i+1}] filter={f}")
                try:
                    print(f"       $set={json.dumps(set_doc, ensure_ascii=False, default=str, indent=6)}")
                except Exception:
                    print(f"       $set={set_doc}")
        else:
            print(f"\n{sep}")
            print(f"[DRY-RUN] update_one → 集合: {col_name}")
            print(f"  filter : {filter_}")
            try:
                print(f"  $set   : {json.dumps(ops_or_doc, ensure_ascii=False, default=str, indent=4)}")
            except Exception:
                print(f"  $set   : {ops_or_doc}")
        print(sep)
        return _FakeWriteResult()

    if isinstance(ops_or_doc, list):
        return collection.bulk_write(ops_or_doc, ordered=ordered)
    else:
        return collection.update_one(filter_, {"$set": ops_or_doc}, upsert=upsert)


# =============================================================================
# 全局常量 & Mongo 集合
# =============================================================================
SITE = "ml_mx"
DB   = f"main_{SITE}"

visual_plus_col    = get_collection(DB, SITE, "visual_plus")
sku_col            = get_collection(DB, SITE, "sku")
price_segment_col  = get_collection(DB, SITE, "price_segment")
every_day_sku_col  = get_collection(DB, SITE, "every_day_sku")
big_Excel_col      = get_collection(DB, SITE, "_big_Excel")
visualize_table_col= get_collection(DB, SITE, "visualize_table")
bi_category_col    = get_collection(DB, SITE, "bi_category_data")
scrapy_buffer_col  = get_collection("slave_ml_mx", "ml_scrapy_buffer", "ml_mx")

ERP_URI = "mongodb://erp:Damai20230214*.*@42.193.215.253:27017/?authMechanism=DEFAULT&authSource=erp&directConnection=true"

# 进程/线程全局（工作进程中由 init_worker_globals 覆盖）
TIMES_4   = "2512"   # YYMM
_time_map  = {}       # {YYMM: "20YYMM"}

# =============================================================================
# ─── 通用工具 ────────────────────────────────────────────────────────────────
# =============================================================================

def get_last_n_months(times_4: str, n: int = 13) -> list[str]:
    """返回含 times_4 在内向前 n 个月的 YYMM 列表（从旧到新）"""
    year  = int("20" + times_4[:2])
    month = int(times_4[2:])
    months = []
    for _ in range(n):
        months.append(f"{str(year)[2:]}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(months))


def make_time_map(months_yymm: list[str]) -> dict[str, str]:
    return {m: "20" + m for m in months_yymm}


def safe_div(a, b):
    return a / b if b else 0


def safe_float(v, default=0.0):
    try:
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


def get_nested(d: dict, key: str, default=0):
    cur = d
    for k in key.split("."):
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def prev_month_int(yyyymm: int) -> int:
    year, month = divmod(yyyymm, 100)
    return (year - 1) * 100 + 12 if month == 1 else year * 100 + (month - 1)


def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


# =============================================================================
# ─── 功能 1：生成字段说明 xlsx ──────────────────────────────────────────────
# =============================================================================

def task_generate_schema(output_path="大表格字段说明.xlsx"):
    data = [
        ["cat_id", "int/str", "类目ID"],
        ["time", "int", "当前月份（整数）"],
        ["2025_gmv_growth_order_ratio", "float", "2025年GMV增长率"],
        ["top10_commodity_sku_id_order", "int", "top10商品销量总和"],
        ["top10_commodity_sku_id_order_ratio", "float", "top10商品销量占比"],
        ["top1_seller_sales_order_ratio_sum_ratio", "float", "top1店铺销量占比"],
        ["top3_seller_sales_order_ratio_sum_ratio", "float", "top3店铺销量占比"],
        ["top1_brand_sales_order_ratio_sum_ratio", "float", "top1品牌销量占比"],
        ["top1_brand_sales_order_ratio_name", "str", "top1品牌名称"],
        ["top3_brand_sales_order_ratio_sum_ratio", "float", "top3品牌销量占比"],
        # top50_sku_new_stock_stats ci/ful/nor × 30/90/180 × count/order/order_ratio
        *[
            [f"top50_sku_new_stock_stats_{st}_new{d}_{m}", t,
             f"top50新品{'数量' if m=='count' else '销量' if m=='order' else '销量占比'}（{st}，{d}天）"]
            for st in ("ci", "ful", "nor")
            for d in (30, 90, 180)
            for m, t in [("count", "int"), ("order", "int"), ("order_ratio", "float")]
        ],
        # all_new_stock_stats ci/nor × 30/90/180 × count/order/order_ratio
        *[
            [f"all_new_stock_stats_{st}_new{d}_{m}", t,
             f"所有新品{'数量' if m=='count' else '销量' if m=='order' else '销量占比'}（{st}，{d}天）"]
            for st in ("ci", "nor")
            for d in (30, 90, 180)
            for m, t in [("count", "int"), ("order", "int"), ("order_ratio", "float")]
        ],
        # all_new_stock_stats ful（含 price）
        *[
            [f"all_new_stock_stats_ful_new{d}_{m}", t,
             f"所有新品{'数量' if m=='count' else '销量' if m=='order' else '销量占比' if m=='order_ratio' else '均价'}（ful，{d}天）"]
            for d in (30, 90, 180)
            for m, t in [("count", "int"), ("order", "int"), ("order_ratio", "float"), ("price", "float")]
        ],
    ]
    df = pd.DataFrame(data, columns=["字段名", "类型/范围", "描述"])
    df.to_excel(output_path, index=False)
    print(f"生成完成：{output_path}")


# =============================================================================
# ─── 功能 2：全量类目报表（brank_range_price / 测试）────────────────────────
# =============================================================================

LOG_FILE_REPORT = "processed_cat.log"


def load_processed_cats(log_file: str, month_yyyymm: str) -> set:
    processed = set()
    if not os.path.exists(log_file):
        return processed
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2 and parts[0] == month_yyyymm:
                processed.add(parts[1])
    return processed


def save_processed_cat(log_file: str, month_yyyymm: str, cat_id: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{month_yyyymm}\t{cat_id}\t{ts}\n")


# ── 价格带 ──────────────────────────────────────────────────────────────────

def get_price_ranges(cat_id: str) -> list[tuple]:
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
    ranges.sort(key=lambda x: x[0])
    return ranges


def build_price_index(price_ranges):
    mins   = [mn for mn, _, _ in price_ranges]
    maxs   = [mx for _, mx, _ in price_ranges]
    labels = [lb for _, _, lb in price_ranges]
    return mins, maxs, labels


def price_to_label(price_index, price):
    mins, maxs, labels = price_index
    i = bisect_right(mins, price) - 1
    if i < 0 or price >= maxs[i]:
        return None
    return labels[i]


def init_price_stats(months_keys, price_ranges, top_brands):
    return {
        mk: {label: {b: {"order": 0} for b in top_brands}
             for _, _, label in price_ranges}
        for mk in months_keys
    }


# ── TopN ─────────────────────────────────────────────────────────────────────

def get_top_n(d: dict, n=10) -> dict:
    return dict(heapq.nlargest(n, d.items(), key=lambda x: x[1]))


def build_top_skus(sku_orders: dict, top_n=10) -> dict:
    total = sum(sku_orders.values()) or 0
    top = heapq.nlargest(top_n, sku_orders.items(), key=lambda x: x[1])
    return {
        sku: {"order": order, "order_ratio": safe_div(order, total)}
        for sku, order in top
    }


# ── 新品销量（单次 Mongo 查询同时算 30/90/180）────────────────────────────────

def batch_new_sales_multi(sku_infos, sku_ids, month_start, month_end,
                           deltas=(30, 90, 180)):
    deltas = tuple(sorted(deltas))
    start_int = int(month_start.strftime("%Y%m%d"))
    end_int   = int(month_end.strftime("%Y%m%d"))

    sku_limits = {}
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
            if end_dt >= month_start:
                limits[dd] = int(end_dt.strftime("%Y%m%d"))
        if limits:
            sku_limits[sku] = limits

    if not sku_limits:
        return {d: {} for d in deltas}, {d: {} for d in deltas}

    cursor = every_day_sku_col.find(
        {"sl": {"$in": list(sku_limits.keys())},
         "dT": {"$gte": start_int, "$lte": end_int}},
        {"sl": 1, "oD": 1, "dT": 1, "pR": 1}
    )
    totals_map = {d: defaultdict(int) for d in deltas}
    gmvs_map   = {d: defaultdict(float) for d in deltas}

    for doc in cursor:
        sku = doc["sl"]
        dT  = doc.get("dT", 0) or 0
        od  = doc.get("oD", 0) or 0
        pr  = doc.get("pR", 0) or 0
        limits = sku_limits.get(sku)
        if not limits:
            continue
        for dd in deltas:
            limit_dt = limits.get(dd)
            if limit_dt and dT <= limit_dt:
                totals_map[dd][sku] += od
                gmvs_map[dd][sku]   += od * pr

    return ({d: dict(v) for d, v in totals_map.items()},
            {d: dict(v) for d, v in gmvs_map.items()})


# ── 新品按仓统计（共用）─────────────────────────────────────────────────────

def _calc_new_stock_stats(z_order, sku_ids, stock_map,
                           totals_map, gmvs_map, deltas=(30, 90, 180)) -> dict:
    stats = defaultdict(lambda: {
        f"new{d}_count": 0 for d in deltas
    } | {
        f"new{d}_order": 0 for d in deltas
    } | {
        f"new{d}_price": 0 for d in deltas
    } | {
        f"new{d}_order_ratio": 0 for d in deltas
    })

    for sku in sku_ids:
        st = stock_map.get(sku, "unknown")
        for d in deltas:
            if totals_map[d].get(sku, 0) > 0:
                stats[st][f"new{d}_count"] += 1
                stats[st][f"new{d}_order"] += totals_map[d][sku]

    for st in ["ful", "ci", "nor"]:
        for d in deltas:
            stats[st][f"new{d}_order_ratio"] = safe_div(stats[st][f"new{d}_order"], z_order)

    for st, v in stats.items():
        for d in deltas:
            if v[f"new{d}_order"] > 0:
                gmv = sum(gmvs_map[d].get(s, 0) for s in sku_ids
                          if stock_map.get(s) == st)
                v[f"new{d}_price"] = round(safe_div(gmv, v[f"new{d}_order"]), 2)

    return dict(stats)


# ── SKU 聚合（Pass1）────────────────────────────────────────────────────────

def scan_sku_aggregates_and_current(cats, months, cur_month):
    months_set = set(months)
    agg = {
        m: {"brand_total": defaultdict(int),
            "brand_total_gmv": defaultdict(float),
            "seller_total": defaultdict(int)}
        for m in months
    }
    cur_sku_infos  = {}
    cur_stock_map  = {}
    cur_sku_orders = defaultdict(int)

    cursor = sku_col.find(
        {"category_id": {"$in": cats}},
        {"_id": 0, "sku_id": 1, "brand": 1, "sellerName": 1,
         "monthly_sale_trend": 1, "active_price": 1,
         "puton_date": 1, "stock_type": 1}
    ).batch_size(2000)

    for d in cursor:
        trend = d.get("monthly_sale_trend") or {}
        if not trend:
            continue
        sku_id = d.get("sku_id")
        price  = d.get("active_price")
        if not sku_id or price is None:
            continue
        brand      = d.get("brand") or "Unknown"
        seller     = d.get("sellerName") or "UnknownSeller"
        stock_type = d.get("stock_type") or "unknown"

        for m, order in trend.items():
            if m not in months_set or not order:
                continue
            a = agg[m]
            a["brand_total"][brand]     += order
            a["brand_total_gmv"][brand] += order * price
            a["seller_total"][seller]   += order
            if m == cur_month:
                cur_sku_orders[sku_id]  += order
                cur_sku_infos[sku_id]    = d
                cur_stock_map[sku_id]    = stock_type

    for m in months:
        for k in agg[m]:
            agg[m][k] = dict(agg[m][k])
    return agg, dict(cur_sku_orders), cur_sku_infos, cur_stock_map


def scan_price_stats_for_top_brands(cats, months, top_brands, price_ranges, price_index):
    months_set  = set(months)
    months_keys = [_time_map[m] for m in months]
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
        label = price_to_label(price_index, price)
        if label is None:
            continue
        trend = d.get("monthly_sale_trend") or {}
        for m, order in trend.items():
            if m not in months_set or not order:
                continue
            mk = _time_map[m]
            price_stats[mk][label][brand]["order"] += order

    return price_stats


# ── 年度 GMV ─────────────────────────────────────────────────────────────────

def calculate_year_gmv(agg, months, year_str) -> float:
    return sum(
        sum(agg[m]["brand_total_gmv"].values())
        for m in months if m.startswith(year_str) and agg.get(m)
    )


# ── 写入 _big_Excel（汇总字段）──────────────────────────────────────────────

def import_big_Excel(cat_id, report, month_key: str):
    def g(d, *path, default=0):
        cur = d
        for p in path:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(p)
        return cur if cur is not None else default

    top50   = report.get("top50_sku_new_stock_stats", {}).get(month_key, {})
    all_new = report.get("all_new_stock_stats", {}).get(month_key, {})

    top10_com = report.get("top10_commodity", {}).get(month_key, {})
    top10_com_order = sum(v.get("order", 0) or 0 for v in top10_com.values())
    top10_com_ratio = sum(v.get("order_ratio", 0) or 0 for v in top10_com.values())

    seller_data = report.get("top10_seller_sales", {}).get(month_key, {}).get(month_key, {})
    top1_seller = top3_seller = 0.0
    for i, (_, v) in enumerate(seller_data.items(), 1):
        ratio = v.get("order_ratio", 0) or 0
        if i == 1:
            top1_seller += ratio
        top3_seller += ratio
        if i >= 3:
            break

    brand_data = report.get("top10_brand_sales", {}).get(month_key, {}).get(month_key, {})
    top1_brand = top3_brand = 0.0
    top1_brand_name = ""
    for i, (name, v) in enumerate(brand_data.items(), 1):
        ratio = v.get("order_ratio", 0) or 0
        if i == 1:
            top1_brand      += ratio
            top3_brand      += ratio
            top1_brand_name  = name
        else:
            top3_brand += ratio
        if i >= 3:
            break

    result = {
        "cat_id": cat_id,
        "time":   int(month_key),
        "2025_gmv_growth_order_ratio":        report.get("gmv_growth", {}).get("2025", {}).get("order_ratio", 0),
        "top10_commodity_sku_id_order":        top10_com_order,
        "top10_commodity_sku_id_order_ratio":  top10_com_ratio,
        "top1_seller_sales_order_ratio_sum_ratio": top1_seller,
        "top3_seller_sales_order_ratio_sum_ratio": top3_seller,
        "top1_brand_sales_order_ratio_sum_ratio":  top1_brand,
        "top1_brand_sales_order_ratio_name":       top1_brand_name,
        "top3_brand_sales_order_ratio_sum_ratio":  top3_brand,
    }

    # top50 / all_new 展平
    for prefix, src in [("top50_sku_new_stock_stats", top50),
                         ("all_new_stock_stats",       all_new)]:
        for st in ("ci", "ful", "nor"):
            for d in (30, 90, 180):
                for m in ("count", "order", "order_ratio"):
                    k = f"{prefix}_{st}_new{d}_{m}"
                    result[k] = g(src, st, f"new{d}_{m}")
                if prefix == "all_new_stock_stats" and st == "ful":
                    result[f"{prefix}_{st}_new{d}_price"] = g(src, st, f"new{d}_price")

    _col_write(big_Excel_col, result,
               filter_={"cat_id": cat_id, "time": int(month_key)}, upsert=True)


# ── 主报表构建 ───────────────────────────────────────────────────────────────

def build_category_report(cat_id: str) -> dict:
    cat_doc = visual_plus_col.find_one({"cat_id": cat_id}, {"cat": 1, "order": 1})
    if not cat_doc:
        return {}

    order_map = cat_doc.get("order", {}) or {}
    cats = cat_doc.get("cat", [])
    if not cats:
        return {}

    months    = get_last_n_months(TIMES_4, 13)
    month_key = _time_map[TIMES_4]
    z_order   = (order_map.get(month_key, {}) or {}).get("monthly", 0) or 0

    price_ranges = get_price_ranges(cat_id)
    price_index  = build_price_index(price_ranges)

    agg, cur_sku_orders, cur_sku_infos, cur_stock_map = \
        scan_sku_aggregates_and_current(cats, months, TIMES_4)

    top10_brand  = get_top_n(agg[TIMES_4]["brand_total"])
    top10_seller = get_top_n(agg[TIMES_4]["seller_total"])
    total_seller = len(agg[TIMES_4]["seller_total"])

    price_stats = scan_price_stats_for_top_brands(
        cats, months, top10_brand, price_ranges, price_index)

    report = {
        "total_seller_count":        {month_key: total_seller},
        "top10_brand_sales":         {month_key: {}},
        "top10_brand_price_stats":   {month_key: {}},
        "top10_seller_sales":        {month_key: {}},
        "top10_commodity":           {month_key: {}},
        "top50_sku_new_stock_stats": {month_key: {}},
        "all_new_stock_stats":       {month_key: {}},
    }

    for m in months:
        mk        = _time_map[m]
        z_order_m = (order_map.get(mk, {}) or {}).get("monthly", 0) or 0

        report["top10_brand_sales"][month_key][mk] = {
            b: {
                "order":       agg[m]["brand_total"].get(b, 0),
                "order_ratio": safe_div(agg[m]["brand_total"].get(b, 0), z_order_m),
                "gmv":         agg[m]["brand_total_gmv"].get(b, 0),
                "price":       safe_div(agg[m]["brand_total_gmv"].get(b, 0),
                                        agg[m]["brand_total"].get(b, 0)),
            } for b in top10_brand
        }
        report["top10_brand_price_stats"][month_key][mk] = price_stats.get(mk, {})
        report["top10_seller_sales"][month_key][mk] = {
            s: {
                "order":       agg[m]["seller_total"].get(s, 0),
                "order_ratio": safe_div(agg[m]["seller_total"].get(s, 0), z_order_m),
            } for s in top10_seller
        }

    report["top10_commodity"][month_key] = build_top_skus(cur_sku_orders, top_n=10)

    year_n, month_n = int("20" + TIMES_4[:2]), int(TIMES_4[2:])
    month_start = datetime(year_n, month_n, 1)
    month_end   = datetime(year_n, month_n, calendar.monthrange(year_n, month_n)[1])
    sku_ids     = list(cur_sku_infos.keys())

    totals_map, gmvs_map = batch_new_sales_multi(
        cur_sku_infos, sku_ids, month_start, month_end)

    report["all_new_stock_stats"][month_key] = _calc_new_stock_stats(
        z_order, sku_ids, cur_stock_map, totals_map, gmvs_map)

    top50_ids = [s for s, _ in heapq.nlargest(50, cur_sku_orders.items(), key=lambda x: x[1])]
    t50_totals, t50_gmvs = batch_new_sales_multi(
        cur_sku_infos, top50_ids, month_start, month_end)
    report["top50_sku_new_stock_stats"][month_key] = _calc_new_stock_stats(
        z_order, top50_ids, cur_stock_map, t50_totals, t50_gmvs)

    gmv_2023 = calculate_year_gmv(agg, months, "23")
    gmv_2024 = calculate_year_gmv(agg, months, "24")
    gmv_2025 = calculate_year_gmv(agg, months, "25")
    report["gmv_growth"] = {
        "2024": {"order": gmv_2024,
                 "order_ratio": round(safe_div(gmv_2024 - gmv_2023, gmv_2023), 4)},
        "2025": {"order": gmv_2025,
                 "order_ratio": round(safe_div(gmv_2025 - gmv_2024, gmv_2024), 4)},
    }

    # 写 visual_plus（使用点路径更新）
    report_set = {
        f"total_seller_count.{month_key}":      report["total_seller_count"][month_key],
        f"top10_brand_sales.{month_key}":        report["top10_brand_sales"][month_key],
        f"top10_brand_price_stats.{month_key}":  report["top10_brand_price_stats"][month_key],
        f"top10_seller_sales.{month_key}":       report["top10_seller_sales"][month_key],
        f"top10_commodity.{month_key}":          report["top10_commodity"][month_key],
        f"top50_sku_new_stock_stats.{month_key}": report["top50_sku_new_stock_stats"][month_key],
        f"all_new_stock_stats.{month_key}":      report["all_new_stock_stats"][month_key],
        "gmv_growth": report["gmv_growth"],
    }
    _col_write(visual_plus_col, report_set,
               filter_={"cat_id": cat_id}, upsert=True)
    import_big_Excel(cat_id, report, month_key)
    return report


def _worker_report(cat_id, log_file, month_yyyymm):
    try:
        build_category_report(cat_id)
        save_processed_cat(log_file, month_yyyymm, cat_id)
        return True, cat_id, None
    except Exception as e:
        return False, cat_id, str(e)


def init_worker_globals(times_4, months_yymm, dry_run=False):
    global TIMES_4, _time_map, DRY_RUN
    TIMES_4   = times_4
    _time_map = make_time_map(months_yymm)
    DRY_RUN   = dry_run


def task_build_report(times_4: str, test_cat_id: str | None = None):
    global TIMES_4, _time_map
    TIMES_4   = times_4
    months    = get_last_n_months(TIMES_4, 13)
    _time_map = make_time_map(months)
    month_yyyymm = _time_map[TIMES_4]

    mp.set_start_method("spawn", force=True)
    processed = load_processed_cats(LOG_FILE_REPORT, month_yyyymm)

    datas = visual_plus_col.find(
        {"cat_id": test_cat_id} if test_cat_id else {"cat_id": {"$nin": list(processed)}},
        {"cat_id": 1, "_id": 0}, no_cursor_timeout=True
    )
    cat_list = [d["cat_id"] for d in datas if d.get("cat_id")]
    datas.close()
    print(f"Month={month_yyyymm}, 待处理: {len(cat_list)}")

    stop_event = mp.Event()
    signal.signal(signal.SIGINT, lambda s, f: (print("\n停止中..."), stop_event.set()))

    n_proc = max(mp.cpu_count() - 1, 1)
    worker_fn = partial(_worker_report, log_file=LOG_FILE_REPORT, month_yyyymm=month_yyyymm)

    with mp.Pool(n_proc, initializer=init_worker_globals,
                 initargs=(TIMES_4, months, DRY_RUN)) as pool:
        it = pool.imap_unordered(worker_fn, cat_list, chunksize=10)
        with tqdm(total=len(cat_list), desc="类目报表", unit="cat") as pbar:
            for success, cat_id, error in it:
                if stop_event.is_set():
                    break
                if not success:
                    tqdm.write(f"[ERROR] {cat_id}: {error}")
                pbar.update(1)
        pool.terminate()
    print("报表构建完成。")


# =============================================================================
# ─── 功能 3：2025 GMV 增长率回填 ────────────────────────────────────────────
# =============================================================================

def task_gmv2025():
    months_str = [f"{m:02d}" for m in range(1, 13)]
    datas  = visualize_table_col.find({}, {"monthly_gmv_trend": 1, "cat_id": 1})
    ops    = []
    batch  = 1000

    for doc in tqdm(datas, desc="2025 GMV"):
        cat_id = doc.get("cat_id")
        gmv    = doc.get("monthly_gmv_trend", {})
        if not cat_id or not gmv:
            continue
        gmv_2024 = sum(gmv.get(f"2024{m}", 0) for m in months_str)
        gmv_2025 = sum(gmv.get(f"2025{m}", 0) for m in months_str)
        ratio = safe_div(gmv_2025, gmv_2024)
        ops.append(UpdateOne({"cat_id": cat_id},
                              {"$set": {"2025_gmv_growth_order_ratio": ratio}},
                              upsert=False))
        if len(ops) >= batch:
            _col_write(big_Excel_col, ops)
            ops = []
    if ops:
        _col_write(big_Excel_col, ops)
    print("2025 GMV 回填完成。")


# =============================================================================
# ─── 功能 4：ERP bi_category_data 同步 ──────────────────────────────────────
# =============================================================================

def task_sync_erp(months: list[str]):
    client = MongoClient(ERP_URI)
    src_col = client["erp"]["bi_category_data"]
    grand_total = 0

    for month in tqdm(months, desc="ERP 月份同步"):
        cursor = src_col.find({"month": month}, no_cursor_timeout=True).batch_size(1000)
        ops = []
        total = 0
        try:
            for doc in tqdm(cursor, desc=f"  {month}"):
                doc.pop("_id", None)
                cat_id = doc.get("cat_id")
                if not cat_id:
                    continue
                ops.append(UpdateOne({"cat_id": cat_id, "month": month},
                                      {"$set": doc}, upsert=True))
                if len(ops) >= 1000:
                    _col_write(bi_category_col, ops)
                    total += len(ops)
                    ops = []
            if ops:
                _col_write(bi_category_col, ops)
                total += len(ops)
        finally:
            cursor.close()
        print(f"  {month} 同步完成，写入 {total} 条")
        grand_total += total
    client.close()
    print(f"ERP 同步完成，总写入 {grand_total} 条。")


# =============================================================================
# ─── 功能 5：退款率 / 广告数据回填 visual_plus ─────────────────────────────
# =============================================================================

def task_refund(months: list[str]):
    erp_client = MongoClient(ERP_URI)
    src_col    = erp_client["erp"]["bi_category_data"]

    FLUSH_SIZE = 2000
    stats = defaultdict(int)
    update_map: dict[str, dict] = defaultdict(dict)

    cursor = src_col.find(
        {"month": {"$in": months}},
        {"_id": 0, "month": 1, "cat_id": 1, "clicks": 1,
         "cost": 1, "cpc": 1, "refund_rate": 1},
        no_cursor_timeout=True
    ).batch_size(3000)

    def _flush(m):
        if not m:
            return
        ops = [UpdateOne({"cat_id": cid}, {"$set": fields}, upsert=False)
               for cid, fields in m.items()]
        try:
            res = _col_write(visual_plus_col, ops)
            stats["matched"]   += res.matched_count
            stats["modified"]  += res.modified_count
        except BulkWriteError as e:
            details = e.details or {}
            stats["matched"]  += details.get("nMatched", 0)
            stats["modified"] += details.get("nModified", 0)
            stats["errors"]   += 1
        stats["batches"] += 1
        m.clear()

    try:
        for d in tqdm(cursor, desc="退款率/广告回填"):
            stats["total"] += 1
            cat_id = d.get("cat_id")
            month  = str(d.get("month", ""))
            if not cat_id or month not in months:
                stats["skipped"] += 1
                continue
            update_map[cat_id][f"ad_click_rate2.{month}"]    = safe_float(d.get("clicks"))
            update_map[cat_id][f"avg_ad_cost_ratio2.{month}"]= safe_float(d.get("cost"))
            update_map[cat_id][f"avg_cpc_usd2.{month}"]      = safe_float(d.get("cpc"))
            update_map[cat_id][f"avg_refund_rate2.{month}"]  = safe_float(d.get("refund_rate"))
            if len(update_map) >= FLUSH_SIZE:
                _flush(update_map)
        _flush(update_map)
    finally:
        cursor.close()
    erp_client.close()
    print("退款率回填完成：", dict(stats))


# =============================================================================
# ─── 功能 6：Top50 SKU + offersInf 统计 ─────────────────────────────────────
# =============================================================================

def _safe_eval(x, default):
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except Exception:
            return default
    return x if x is not None else default


def task_top50sku(times_list: list[int],
                   time_map: dict[int, tuple],
                   success_log: str = "top50_skus_2.txt",
                   error_log:   str = "top50_skus_error_2.txt",
                   max_workers: int = 50):
    slog = Path(success_log); slog.touch(exist_ok=True)
    elog = Path(error_log);   elog.touch(exist_ok=True)
    processed = {line.split()[0] for line in slog.read_text("utf-8").splitlines() if line.strip()}

    def _process(cat_id: str):
        if cat_id in processed:
            return None, None
        try:
            cat_doc = visual_plus_col.find_one({"cat_id": cat_id}, {"cat": 1})
            if not cat_doc or "cat" not in cat_doc:
                raise ValueError("visual_plus 中未找到 cat 列表")
            cat_ids_sub = cat_doc["cat"]

            top50_heaps  = {t: [] for t in times_list}
            top100_heaps = {t: [] for t in times_list}
            count_follow     = {t: 0 for t in times_list}
            order_follow     = {t: 0 for t in times_list}
            count_non_follow = {t: 0 for t in times_list}
            order_non_follow = {t: 0 for t in times_list}

            for sub_id in cat_ids_sub:
                cursor = sku_col.find(
                    {"category_id": sub_id},
                    {"sku_id": 1, "monthly_sale_trend": 1, "offersInf": 1}
                ).batch_size(5000)
                for row in cursor:
                    trend     = _safe_eval(row.get("monthly_sale_trend", {}), {})
                    offersInf = _safe_eval(row.get("offersInf", []), [])
                    sku_id    = row.get("sku_id")
                    offers_len = len(offersInf)

                    for t in times_list:
                        time_key = time_map[t][0]
                        sale = trend.get(str(time_key), 0)

                        if offers_len > 0:
                            count_follow[t]  += 1; order_follow[t]  += sale
                        else:
                            count_non_follow[t] += 1; order_non_follow[t] += sale

                        heapq.heappush(top50_heaps[t], (sale, offers_len, sku_id))
                        if len(top50_heaps[t]) > 50:
                            heapq.heappop(top50_heaps[t])
                        heapq.heappush(top100_heaps[t], (sale, offers_len, sku_id))
                        if len(top100_heaps[t]) > 100:
                            heapq.heappop(top100_heaps[t])

            upd_excel = {}
            upd_plus  = {}
            for t in times_list:
                sorted50  = sorted(top50_heaps[t],  key=lambda x: (-x[0], -x[1]))
                sorted100 = sorted(top100_heaps[t], key=lambda x: (-x[0], -x[1]))
                upd_excel[f"top50_skus.{t}"]  = {s: {"offersInf_len": ol, "order": sl} for sl, ol, s in sorted50}
                upd_excel[f"top100_skus.{t}"] = {s: {"offersInf_len": ol, "order": sl} for sl, ol, s in sorted100}

                total_order = order_non_follow[t] + order_follow[t]
                for k, cnt, ord_, ratio in [
                    ("non_follow", count_non_follow[t], order_non_follow[t],
                     safe_div(order_non_follow[t], total_order)),
                    ("follow",     count_follow[t],     order_follow[t],
                     safe_div(order_follow[t], total_order)),
                ]:
                    upd_plus[f"offersInf.{t}.{k}.count"] = cnt
                    upd_plus[f"offersInf.{t}.{k}.order"] = ord_
                    upd_plus[f"offersInf.{t}.{k}.ratio"] = ratio

            _col_write(big_Excel_col,    upd_excel, filter_={"cat_id": cat_id}, upsert=True)
            _col_write(visual_plus_col,  upd_plus,  filter_={"cat_id": cat_id}, upsert=True)
            return cat_id, None
        except Exception as e:
            return None, f"{cat_id} error: {e}"

    all_cats    = [c["cat_id"] for c in visual_plus_col.find({}, {"cat_id": 1}) if c.get("cat_id")]
    success_log_list, error_log_list = [], []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_process, cid): cid for cid in all_cats}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Top50 SKU"):
            cid, err = fut.result()
            if err:
                error_log_list.append(f"{err}\n")
            elif cid:
                success_log_list.append(f"{cid}\n")

    slog.open("a", encoding="utf-8").writelines(success_log_list)
    elog.open("a", encoding="utf-8").writelines(error_log_list)
    print("Top50 SKU 统计完成。")


# =============================================================================
# ─── 功能 7：Top50 SKU 详细指标 ─────────────────────────────────────────────
# =============================================================================

def _extract_volume_weight(attributes):
    length = width = height = weight = None
    for attr in attributes or []:
        aid, val = attr.get("id"), attr.get("v_name_en")
        if not aid or not val:
            continue
        nums = re.findall(r"[\d.]+", val)
        if not nums:
            continue
        try:
            num = float(nums[0])
        except Exception:
            continue
        val_l = val.lower()
        if aid == "PACKAGE_LENGTH":
            length = num * (0.01 if "cm" in val_l else 0.001 if "mm" in val_l else 1)
        elif aid == "PACKAGE_WIDTH":
            width  = num * (0.01 if "cm" in val_l else 0.001 if "mm" in val_l else 1)
        elif aid == "PACKAGE_HEIGHT":
            height = num * (0.01 if "cm" in val_l else 0.001 if "mm" in val_l else 1)
        elif aid == "PACKAGE_WEIGHT":
            weight = num / 1000 if "g" in val_l else num
    volume = length * width * height if length and width and height else 0
    return volume, weight or 0


def _process_cat_month_top50(cat_id: str, month: int):
    data = big_Excel_col.find_one({"cat_id": cat_id}, {"top50_skus": 1, "_id": 0})
    if not data or "top50_skus" not in data:
        return None
    month_data = data["top50_skus"].get(str(month))
    if not month_data:
        return None

    sku_ids   = list(month_data.keys())[:50]
    year, mon = int(str(month)[:4]), int(str(month)[4:])
    m_start   = datetime(year, mon, 1)
    m_end     = datetime(year, mon, days_in_month(year, mon))

    sku_infos = {
        d["sku_id"]: d for d in sku_col.find(
            {"sku_id": {"$in": sku_ids}},
            {"sku_id": 1, "active_price": 1, "stock_type": 1, "sellerName": 1,
             "brand": 1, "puton_date": 1, "offersInf": 1,
             "conversion_all_order": 1, "conversion_all_view": 1,
             "attributes": 1, "_id": 0}
        )
    }

    m_start_tz = datetime(m_start.year, m_start.month, 1, tzinfo=timezone.utc)
    m_end_tz   = datetime(m_end.year,   m_end.month,   m_end.day, tzinfo=timezone.utc)
    rating_map = {
        d["_id"]: d["avg"]
        for d in scrapy_buffer_col.aggregate([
            {"$match": {"sku_id": {"$in": sku_ids},
                        "t": {"$gte": m_start_tz, "$lte": m_end_tz}}},
            {"$group": {"_id": "$sku_id", "avg": {"$avg": "$rating_avg"}}}
        ])
    }

    # 新品销量（复用 batch_new_sales_multi）
    totals_map, _ = batch_new_sales_multi(sku_infos, sku_ids, m_start, m_end)
    new30, new90, new180 = totals_map[30], totals_map[90], totals_map[180]

    st = defaultdict(float)
    st.update({"shop_set": set(), "brand_set": set()})

    for sku_id in sku_ids:
        info = sku_infos.get(sku_id)
        if not info:
            continue
        sku_month = month_data.get(sku_id, {})
        order = sku_month.get("order", 0)

        st["month_sales"]  += order
        st["month_gmv"]    += order * int(info.get("active_price") or 0)
        st["sku_cnt"]      += 1
        st["conversion_all_order"] += int(info.get("conversion_all_order") or 0)
        st["conversion_all_view"]  += int(info.get("conversion_all_view") or 0)
        if info.get("sellerName"): st["shop_set"].add(info["sellerName"])
        if info.get("brand"):      st["brand_set"].add(info["brand"])
        if info.get("stock_type") == "ful":
            st["full_sales"] += order; st["full_sku_cnt"] += 1
        if info.get("stock_type") == "nor":
            st["nor_sales"] += order; st["nor_sku_cnt"] += 1
        if info.get("offersInf"):
            st["offers_sku_cnt"] += 1; st["offers_sales"] += order

        vol, wgt = _extract_volume_weight(info.get("attributes"))
        if vol > 0: st["volume_m3_sum"] += vol; st["volume_m3_cnt"] += 1
        if wgt > 0: st["volume_weight_sum"] += wgt; st["volume_weight_cnt"] += 1

        if new30.get(sku_id, 0) > 0:
            st["new_30_cnt"] += 1; st["new_30_sales"] += new30[sku_id]
        if new90.get(sku_id, 0) > 0:
            st["new_90_cnt"] += 1; st["new_90_sales"] += new90[sku_id]
        if new180.get(sku_id, 0) > 0:
            st["new_180_cnt"] += 1; st["new_180_sales"] += new180[sku_id]

        r = rating_map.get(sku_id)
        if r: st["rating_sum"] += r; st["rating_cnt"] += 1

    ms = st["month_sales"]
    return {
        "cat_id": cat_id, "time": month,
        "top50_month_sales":            ms,
        "top50_month_gmv":              st["month_gmv"],
        "top50_avg_order_price":        safe_div(st["month_gmv"], ms),
        "top50_avg_product_sales":      safe_div(ms, st["sku_cnt"]),
        "top50_sales_growth_index":     0,
        "top50_full_sales_ratio":       safe_div(st["full_sales"], ms),
        "top50_avg_volume_m3":          safe_div(st["volume_m3_sum"], st["volume_m3_cnt"]),
        "top50_avg_volume_weight_kg":   safe_div(st["volume_weight_sum"], st["volume_weight_cnt"]),
        "top50_offers_sku_count":       int(st["offers_sku_cnt"]),
        "top50_offers_sales":           st["offers_sales"],
        "top50_offers_sales_ratio":     safe_div(st["offers_sales"], ms),
        "top50_seller_count":           len(st["shop_set"]),
        "top50_brand_count":            len(st["brand_set"]),
        "top50_full_sku_count":         int(st["full_sku_cnt"]),
        "top50_full_sku_ratio":         safe_div(st["full_sku_cnt"], st["sku_cnt"]),
        "top50_full_last30d_sales":     st["full_sales"],
        "top50_full_avg_sales":         safe_div(st["full_sales"], st["full_sku_cnt"]),
        "top50_nor_sku_count":          int(st["nor_sku_cnt"]),
        "top50_nor_last30d_sales":      st["nor_sales"],
        "top50_nor_avg_sales":          safe_div(st["nor_sales"], st["nor_sku_cnt"]),
        "top50_new_30d_count":          int(st["new_30_cnt"]),
        "top50_new_30d_sales":          st["new_30_sales"],
        "top50_new_30d_sales_ratio":    safe_div(st["new_30_sales"], ms),
        "top50_new_90d_count":          int(st["new_90_cnt"]),
        "top50_new_90d_sales":          st["new_90_sales"],
        "top50_new_90d_sales_ratio":    safe_div(st["new_90_sales"], ms),
        "top50_new_180d_count":         int(st["new_180_cnt"]),
        "top50_new_180d_sales":         st["new_180_sales"],
        "top50_new_180d_sales_ratio":   safe_div(st["new_180_sales"], ms),
        "top50_avg_rating":             round(safe_div(st["rating_sum"], st["rating_cnt"]), 2),
        "top50_avg_conversion_rate":    safe_div(st["conversion_all_order"], st["conversion_all_view"]),
    }


def task_top50detail(time_list: list[int],
                      success_log: str = "top50.txt",
                      max_workers: int = 12):
    slog = success_log

    def load_success() -> set:
        s = set()
        if os.path.exists(slog):
            with open(slog, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        cat, mo = line.split(",")
                        s.add((cat, int(mo)))
        return s

    def append_success(cat_id, month):
        with open(slog, "a", encoding="utf-8") as f:
            f.write(f"{cat_id},{month}\n")

    success_set = load_success()
    cat_ids = [d["cat_id"] for d in visual_plus_col.find({}, {"cat_id": 1}) if d.get("cat_id")]

    for cat in tqdm(cat_ids, desc="Top50 详细指标"):
        months_todo = [m for m in time_list if (cat, m) not in success_set]
        if not months_todo:
            continue
        bulk_ops = []
        with ThreadPoolExecutor(max_workers=min(max_workers, len(months_todo))) as ex:
            futures = {ex.submit(_process_cat_month_top50, cat, m): m for m in months_todo}
            for fut in as_completed(futures):
                mo = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"[ERROR] {cat}-{mo}: {e}")
                    continue
                if res:
                    bulk_ops.append(UpdateOne({"cat_id": cat, "time": mo},
                                               {"$set": res}, upsert=True))
                    append_success(cat, mo)
                if len(bulk_ops) >= 5:
                    _col_write(big_Excel_col, bulk_ops)
                    bulk_ops.clear()
        if bulk_ops:
            _col_write(big_Excel_col, bulk_ops)
    print("Top50 详细指标完成。")


# =============================================================================
# ─── 功能 8：_big_Excel → visual_plus 字段同步 ─────────────────────────────
# =============================================================================

_plus_logger = None

def _init_plus_logger(log_path="写入plus2.log"):
    global _plus_logger
    if _plus_logger:
        return
    _plus_logger = logging.getLogger("visual_plus_sync")
    _plus_logger.setLevel(logging.INFO)
    h = RotatingFileHandler(log_path, maxBytes=50*1024*1024, backupCount=5, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"))
    _plus_logger.addHandler(h)


def _process_one_cat_plus(cat_id: str, time_list: list[int]) -> bool:
    try:
        bulk = []
        for t in time_list:
            cur = big_Excel_col.find_one(
                {"cat_id": cat_id, "time": t},
                {"_id": 0,
                 "ad_click_rate": 1, "avg_cpc_usd": 1, "avg_ad_cost_ratio": 1,
                 "avg_refund_rate": 1, "total_gmv_usd": 1,
                 "top50_new_30d_sales_ratio": 1, "top50_new_90d_sales_ratio": 1,
                 "top50_new_180d_sales_ratio": 1, "top50_avg_order_price": 1,
                 "top50_seller_count": 1, "top50_brand_count": 1,
                 "full_stock_avg_price": 1, "local_seller_avg_price": 1,
                 "crossborder_seller_avg_price": 1,
                 "seller_concentration": 1, "brand_concentration": 1}
            )
            if not cur:
                continue
            try:
                set_fields = {
                    f"ad_click_rate2.{t}":                cur["ad_click_rate"],
                    f"avg_cpc_usd2.{t}":                  cur["avg_cpc_usd"],
                    f"avg_ad_cost_ratio2.{t}":            cur["avg_ad_cost_ratio"],
                    f"avg_refund_rate2.{t}":              cur["avg_refund_rate"],
                    f"total_gmv_usd.{t}":                 cur["total_gmv_usd"],
                    f"top50_new_30d_sales_ratio.{t}":     (cur["top50_new_30d_sales_ratio"]  or 0) * 100,
                    f"top50_new_90d_sales_ratio.{t}":     (cur["top50_new_90d_sales_ratio"]  or 0) * 100,
                    f"top50_new_180d_sales_ratio.{t}":    (cur["top50_new_180d_sales_ratio"] or 0) * 100,
                    f"top50_avg_order_price.{t}":         cur["top50_avg_order_price"],
                    f"top50_seller_count.{t}":            cur["top50_seller_count"],
                    f"top50_brand_count.{t}":             cur["top50_brand_count"],
                    f"full_stock_avg_price.{t}":          cur["full_stock_avg_price"],
                    f"local_seller_avg_price.{t}":        cur["local_seller_avg_price"],
                    f"crossborder_seller_avg_price.{t}":  cur["crossborder_seller_avg_price"],
                    f"seller_concentration.{t}":          cur["seller_concentration"],
                    f"brand_concentration.{t}":           cur["brand_concentration"],
                }
            except KeyError:
                continue
            bulk.append(UpdateOne({"cat_id": cat_id}, {"$set": set_fields}))

        if bulk:
            res = _col_write(visual_plus_col, bulk)
            if _plus_logger:
                _plus_logger.info(f"cat_id={cat_id} matched={res.matched_count} modified={res.modified_count}")
        return True
    except Exception:
        if _plus_logger:
            _plus_logger.exception(f"cat_id={cat_id} failed")
        return False


def task_sync_plus(time_list: list[int], max_workers: int = 8):
    _init_plus_logger()
    cat_ids = [d["cat_id"] for d in visual_plus_col.find({}, {"cat_id": 1}) if d.get("cat_id")]
    success = failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_process_one_cat_plus, cid, time_list): cid for cid in cat_ids}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="同步 plus 字段"):
            (success if fut.result() else failed) and None
            if fut.result():
                success += 1
            else:
                failed += 1
    print(f"plus 同步完成：success={success}, failed={failed}")


# =============================================================================
# ─── 功能 9：全量 _big_Excel 主指标计算（all.py）───────────────────────────
# =============================================================================

_all_success_lock = Lock()


def _fetch_and_compute_metrics(t: int, bak_col, cat_id: str):
    doc     = bak_col.find_one({"cat_id": cat_id}) or {}
    gmv_doc = visualize_table_col.find_one(
        {"cat_id": cat_id}, {"monthly_sale_trend": 1, "monthly_gmv_trend": 1, "_id": 0}) or {}

    gmv_t = gmv_doc.get("monthly_gmv_trend", {})
    ord_t = gmv_doc.get("monthly_sale_trend", {})

    p = visual_plus_col.find_one(
        {"cat_id": cat_id},
        {"offersInf": 1, "stock_type": 1, "stock_type_avg_price": 1, "_id": 0}
    ) or {}

    stock_type       = (p.get("stock_type") or {}).get(str(t), {})
    stock_avg_price  = (p.get("stock_type_avg_price") or {}).get(str(t), {})
    offers_inf       = (p.get("offersInf") or {}).get(str(t), {})

    bi_doc      = bi_category_col.find_one({"cat_id": cat_id, "month": str(t)}) or {}
    total_order = ord_t.get(str(t), 0)
    total_prod  = doc.get("total_prod_num") or 1

    follow_order     = get_nested(offers_inf, "follow.order", 0)
    non_follow_order = get_nested(offers_inf, "non_follow.order", 0)
    sum_order        = follow_order + non_follow_order

    result = {
        "time": t, "cat_id": cat_id,
        "total_sales":               total_order,
        "total_gmv_usd":             gmv_t.get(str(t), 0),
        "total_avg_price":           safe_div(gmv_t.get(str(t), 0), total_order),
        "total_avg_product_sales":   safe_div(total_order, total_prod),
        "total_sales_growth_index":  get_nested(doc, "increase_relative_ratio"),
        "full_stock_avg_price":          stock_avg_price.get("ful"),
        "local_seller_avg_price":        stock_avg_price.get("nor"),
        "crossborder_seller_avg_price":  stock_avg_price.get("ci"),
        "local_seller_sales":            stock_type.get("nor"),
        "crossborder_seller_sales":      stock_type.get("ci"),
        "active_product_count":          get_nested(doc, "active_product_num.order30d"),
        "local_seller_avg_sales":        get_nested(doc, "stock_info.nor_avg_sales"),
        "active_product_ratio":          get_nested(doc, "index_active"),
        "follow_sales_ratio":            safe_div(follow_order, sum_order),
        "product_concentration":         get_nested(doc, "product_centralize_ratio.ratio"),
        "brand_concentration":           get_nested(doc, "market_centralize_ratio.ratio"),
        "seller_concentration":          get_nested(doc, "seller_centralize_data.ratio"),
        "seller_count":                  get_nested(doc, "seller_info.seller_num"),
        "brand_count":                   get_nested(doc, "brand_num"),
        "full_stock_sales_ratio":        get_nested(doc, "stock_info.fbm_order30d_ratio"),
        "full_stock_product_count":      get_nested(doc, "stock_info.fbm_number"),
        "full_stock_product_ratio":      get_nested(doc, "stock_info.fbm_ratio"),
        "full_stock_active_count":       get_nested(doc, "stock_info.fbm_count30d"),
        "full_stock_turnover_rate":      get_nested(doc, "stock_info.full_stock_turnover_rate"),
        "full_stock_last30d_sales":      get_nested(doc, "stock_info.fbm_order30d"),
        "full_stock_avg_sales":          get_nested(doc, "stock_info.full_avg_sales"),
        "new_30d_count":                 get_nested(doc, "new_product_info.p30d_count30d"),
        "new_30d_sales":                 get_nested(doc, "new_product_info.p30d_order30d"),
        "new_30d_sales_ratio":           get_nested(doc, "new_product_info.p30d_order30d_ratio"),
        "new_90d_count":                 get_nested(doc, "new_product_info.p90d_count30d"),
        "new_90d_sales":                 get_nested(doc, "new_product_info.p90d_order30d"),
        "new_90d_sales_ratio":           get_nested(doc, "new_product_info.p90d_order30d_ratio"),
        "new_180d_count":                get_nested(doc, "new_product_info.p180d_count30d"),
        "new_180d_sales":                get_nested(doc, "new_product_info.p180d_order30d"),
        "new_180d_sales_ratio":          get_nested(doc, "new_product_info.p180d_order30d_ratio"),
        "ad_click_rate":                 bi_doc.get("ctr", 0),
        "avg_cpc_usd":                   bi_doc.get("cpc", 0),
        "avg_ad_cost_ratio":             bi_doc.get("acos", 0),
        "avg_refund_rate":               bi_doc.get("refund_rate", 0),
    }
    _col_write(big_Excel_col, result,
               filter_={"cat_id": cat_id, "time": t}, upsert=True)
    return result


def _all_process_one(cat_id, t, collection, success_set):
    if (cat_id, t) in success_set:
        return "skip", cat_id, t
    try:
        _fetch_and_compute_metrics(t, collection, cat_id)
        with _all_success_lock:
            pass  # caller writes log
        return "success", cat_id, t
    except Exception as e:
        return "fail", cat_id, t, str(e)


def task_bigexcel(time_list: list[int],
                   success_log: str = "all_2.txt",
                   max_workers: int = 8):
    def load_success():
        s = set()
        if os.path.exists(success_log):
            with open(success_log, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        cat, mo = line.split(",")
                        s.add((cat, int(mo)))
        return s

    def append_success(cat_id, mo):
        with _all_success_lock:
            with open(success_log, "a", encoding="utf-8") as f:
                f.write(f"{cat_id},{mo}\n")

    success_set = load_success()
    cat_ids = [d["cat_id"] for d in visual_plus_col.find({}, {"cat_id": 1}) if d.get("cat_id")]
    print(f"已成功记录数: {len(success_set)}")

    for t in time_list:
        print(f"\n===== Processing {t} =====")
        col = get_collection(DB, SITE, f"visualize_table_bak_{t}28")
        tasks = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for cat_id in cat_ids:
                if (cat_id, t) not in success_set:
                    tasks.append(ex.submit(_all_process_one, cat_id, t, col, success_set))
            for fut in as_completed(tasks):
                res = fut.result()
                if res[0] == "success":
                    append_success(res[1], res[2])
                elif res[0] == "fail":
                    print(f"失败: {res[1]}, {res[2]}, {res[3]}")
    print("全量指标计算完成。")


# =============================================================================
# ─── 入口 ────────────────────────────────────────────────────────────────────
# =============================================================================

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="看板数据整合脚本")
    p.add_argument("--task",  default="all",
                   choices=["all", "schema", "report", "gmv2025",
                             "sync_erp", "refund", "top50sku",
                             "top50detail", "sync_plus", "bigexcel"])
    p.add_argument("--month", default="2602",
                   help="统计月份 YYMM，例如 2602")
    p.add_argument("--test_cat", default=None,
                   help="单测类目 ID，仅 report 任务生效")
    p.add_argument("--dry-run", action="store_true",
                   help="测试模式：只读取数据，不写入 MongoDB，打印将要写入的内容")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    TASK = args.task

    # ── 测试模式 ─────────────────────────────────────────────────────────
    DRY_RUN = args.dry_run
    if DRY_RUN:
        print("=" * 60)
        print("[DRY-RUN] 测试模式已开启，不写入 MongoDB")
        print("=" * 60)

    # ── 时间配置（可按需调整）───────────────────────────────────────────
    MONTH_YYMM  = args.month
    TIME_INT    = int("20" + MONTH_YYMM)          # e.g. 202602
    ERP_MONTHS  = ["202602", "202601", "202512"]  # ERP 同步月份
    REFUND_MONTHS = [
        "202501","202502","202503","202504","202505","202506",
        "202507","202508","202509","202510","202511","202512","202601",
    ]
    TIME_LIST_INT = [202602]                       # 整数月份列表
    TOP50SKU_TIME_MAP = {202602: (2602, 2601, 2502)}
    # ────────────────────────────────────────────────────────────────────

    if TASK in ("all", "schema"):
        task_generate_schema()

    if TASK in ("all", "report"):
        task_build_report(MONTH_YYMM, test_cat_id=args.test_cat)

    if TASK in ("all", "gmv2025"):
        task_gmv2025()

    if TASK in ("all", "sync_erp"):
        task_sync_erp(ERP_MONTHS)

    if TASK in ("all", "refund"):
        task_refund(REFUND_MONTHS)

    if TASK in ("all", "top50sku"):
        task_top50sku(TIME_LIST_INT, TOP50SKU_TIME_MAP)

    if TASK in ("all", "top50detail"):
        task_top50detail(TIME_LIST_INT)

    if TASK in ("all", "sync_plus"):
        task_sync_plus(TIME_LIST_INT)

    if TASK in ("all", "bigexcel"):
        task_bigexcel(TIME_LIST_INT)

    print("\n全部完成。")
