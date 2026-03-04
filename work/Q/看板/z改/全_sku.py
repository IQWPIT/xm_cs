# -*- coding: utf-8 -*-
import os
import sys
import ast
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne
# =====================================================
# 环境 & MongoDB
# =====================================================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
visualize_table = get_collection("main_ml_mx", "ml_mx", "visualize_table")
cat_col = get_collection("main_ml_mx", "ml_mx", "cat")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
# =====================================================
# 配置
# =====================================================
ERP_URI = ("mongodb://erp:Damai20230214*.*@42.193.215.253:27017/"
           "?authMechanism=DEFAULT&authSource=erp&directConnection=true")

FIELDS = [
    "cat_id",
    "sale_trend",
    "gmv_trend",
    "monthly_sale_trend",
    "monthly_gmv_trend"
]

TIMES_LIST = [
    202510, 202509, 202508, 202507, 202506, 202505,
    202504, 202503, 202502, 202501,
    202412, 202411, 202410,
    202511,
    202512
]

TIME_MAP = {
    202510:(2510,2509,2410),
    202509:(2509,2508,2409),
    202508:(2508,2507,2408),
    202507:(2507,2506,2407),
    202506:(2506,2505,2406),
    202505:(2505,2504,2405),
    202504:(2504,2503,2404),
    202503:(2503,2502,2403),
    202502:(2502,2501,2402),
    202501:(2501,2412,2401),
    202412:(2412,2411,2312),
    202411:(2411,2410,2311),
    202410:(2410,2409,2310),
    202511:(2511,2510,2411),
    202512: (202512, 202511, 202412)
}

BINS_DIR = r"D:/gg_xm/Q/看板/z改/价格段"
TOP10_DIR = r"D:/gg_xm/Q/看板/z改/top10品牌"
LOG_FILE = r"D:/gg_xm/Q/看板/z改/日志/stock_type_top10_log.txt"

COLLECTION_NAME = "top10_brand_range_price"
COLLECTION_STOCK = "stock_type_top10_seller"

MAX_WORKERS = 4

# =====================================================
# 工具函数
# =====================================================
def rnd(x):
    return round(float(x), 2) if x else 0

def safe_div(a, b):
    return a / b if b else 0

def bar(i, t, p="进度"):
    l = 40
    f = int(l * i / t) if t else 0
    sys.stdout.write(f"\r{p}: |{'█'*f}{'-'*(l-f)}| {i}/{t}")
    sys.stdout.flush()

def safe_dict(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str) and x.strip():
        try:
            v = ast.literal_eval(x)
            if isinstance(v, dict):
                return v
        except Exception:
            pass
    return {}

# =====================================================
# 总 cat_id / 子 cat_id 获取
# =====================================================
def get_total_and_sub_cats():
    cat_mapping = {}
    for doc in visual_plus.find({"cat_id":"MLM4620"}, {"cat": 1,"cat_id":1}):
        cat_id = doc.get("cat_id")
        sub_cats = doc.get("cat")
        if sub_cats:
            cat_mapping[cat_id] = sub_cats
        else:
            cat_mapping[cat_id] = [cat_id]
    return cat_mapping

cat_mapping = get_total_and_sub_cats()

# =====================================================
# 单月指标计算
# =====================================================
def calc_month(d, current, last_month, last_year):
    last_month = int("20"+str(last_month))
    last_year = int("20"+str(last_year))
    st, gt = d.get("sale_trend", {}), d.get("gmv_trend", {})
    ms, mg = d.get("monthly_sale_trend", {}), d.get("monthly_gmv_trend", {})

    o = {"monthly": 0, "day": {}, "average_day": 0, "year": 0, "chain": 0}
    g = {"monthly": 0, "day": {}, "average_day": 0, "year": 0, "chain": 0}
    p = {"monthly": 0, "day": {}, "year": 0, "chain": 0}

    to = tg = dcnt = 0
    m_str = "20"+str(current)
    current = int(m_str)
    for day, ov in st.items():
        if not day.startswith(m_str):
            continue
        gv = gt.get(day, 0)
        o["day"][day] = rnd(ov)
        g["day"][day] = rnd(gv)
        p["day"][day] = rnd(safe_div(gv, ov))
        to += ov
        tg += gv
        dcnt += 1

    if dcnt:
        o["monthly"] = rnd(to)
        o["average_day"] = rnd(to / dcnt)
        g["monthly"] = rnd(tg)
        g["average_day"] = rnd(tg / dcnt)

    o["year"] = rnd(safe_div(ms.get(str(current)), ms.get(str(last_month))))
    o["chain"] = rnd(safe_div(ms.get(str(current)), ms.get(str(last_year))))
    g["year"] = rnd(safe_div(mg.get(str(current)), mg.get(str(last_month))))
    g["chain"] = rnd(safe_div(mg.get(str(current)), mg.get(str(last_year))))

    pn = safe_div(mg.get(str(current)), ms.get(str(current)))
    p["monthly"] = rnd(pn)
    p["year"] = rnd(safe_div(pn, safe_div(mg.get(str(last_month)), ms.get(str(last_month)))))
    p["chain"] = rnd(safe_div(pn, safe_div(mg.get(str(last_year)), ms.get(str(last_year)))))

    return o, g, p

# =====================================================
# listing_prices计算
# =====================================================
def update_listing_prices(cat_id):
    cat_doc = cat_col.find_one({"id": cat_id}, {"listing_prices": 1})
    if not cat_doc or not cat_doc.get("listing_prices"):
        return
    listing = cat_doc["listing_prices"][0]
    return {
        "name": listing.get("listing_type_name"),
        "sale_fee_ratio": listing.get("sale_fee_ratio")
    }

# =====================================================
# top10_brand_range_price 功能 (MongoDB 版)
# =====================================================
def compute_bins(cat_id):
    bins_file = os.path.join(BINS_DIR, f"{cat_id}.pkl")
    if os.path.exists(bins_file):
        with open(bins_file, "rb") as f:
            return pickle.load(f)

    prices = []
    sub_cats = cat_mapping.get(cat_id, [cat_id])
    for sub_cat_id in sub_cats:
        for doc in sku_col.find({"category_id": sub_cat_id}, {"active_price": 1}):
            if "active_price" in doc and doc["active_price"] is not None:
                prices.append(float(doc["active_price"]))

    bins = np.array([999999.0]) if not prices else np.unique(np.floor(np.quantile(prices, np.linspace(0,1,11))/100)*100)[1:]

    with open(bins_file, "wb") as f:
        pickle.dump(bins, f)
    return bins

def compute_top10(cat_id):
    top10_file = os.path.join(TOP10_DIR, f"top10_{cat_id}.pkl")
    if os.path.exists(top10_file):
        with open(top10_file, "rb") as f:
            return pickle.load(f)

    brand_sales = defaultdict(int)
    sub_cats = cat_mapping.get(cat_id, [cat_id])
    for sub_cat_id in sub_cats:
        for doc in sku_col.find({"category_id": sub_cat_id}, {"brand":1, "monthly_sale_trend":1}):
            ms = safe_dict(doc.get("monthly_sale_trend", {}))
            order = ms.get(str(TIMES_LIST[0])[2::], 0)  # 用 TIMES_LIST 第一个月作为基础
            brand = doc.get("brand")
            if brand:
                brand_sales[brand] += order

    top10 = [b for b,_ in sorted(brand_sales.items(), key=lambda x:x[1], reverse=True)[:10]]
    with open(top10_file, "wb") as f:
        pickle.dump(top10, f)
    return top10

def compute_top10_bins(cat_id):
    bins = compute_bins(cat_id)
    top10 = compute_top10(cat_id)

    data = {str(t):{} for t in TIMES_LIST}
    for t in TIMES_LIST:
        data[str(t)] = {}
        for b in top10 + ["others"]:
            data[str(t)][b] = {
                "price":0,
                "range_price":{str(int(r)):{ "order":0,"gmv":0 } for r in bins}
            }

    sub_cats = cat_mapping.get(cat_id, [cat_id])
    for sub_cat_id in sub_cats:
        for doc in sku_col.find({"category_id": sub_cat_id}, {"brand":1, "active_price":1, "monthly_sale_trend":1}):
            ms = safe_dict(doc.get("monthly_sale_trend", {}))
            price = float(doc.get("active_price",0))
            brand = doc.get("brand")
            for t in TIMES_LIST:
                order = ms.get(str(t)[2::],0)
                gmv = price * order
                bname = brand if brand in top10 else "others"
                for br in bins:
                    if price <= br:
                        key = str(int(br))
                        data[str(t)][bname]["range_price"][key]["order"] += int(order)
                        data[str(t)][bname]["range_price"][key]["gmv"] += gmv
                        break

    for t in TIMES_LIST:
        for b in top10 + ["others"]:
            total_o = sum(v["order"] for v in data[str(t)][b]["range_price"].values())
            total_g = sum(v["gmv"] for v in data[str(t)][b]["range_price"].values())
            data[str(t)][b]["price"] = rnd(total_g / total_o) if total_o else 0

    return data

# =====================================================
# stock_type_top10_seller 功能 (MongoDB 版)
# =====================================================
def calc_stock_type_top10_seller_total(total_cat_id:str):
    try:
        acc = defaultdict(lambda: defaultdict(int))
        sub_cats = cat_mapping.get(total_cat_id, [total_cat_id])
        for cat_id in sub_cats:
            for doc in sku_col.find({"category_id": cat_id}, {"stock_type":1, "sellerName":1, "monthly_sale_trend":1}):
                ms = safe_dict(doc.get("monthly_sale_trend", {}))
                for t in TIMES_LIST:
                    t_4 = str(t)[2::]
                    order = ms.get(t_4,0)
                    stock = doc.get("stock_type")
                    seller = doc.get("sellerName")
                    if stock and seller and order:
                        acc[(t,stock)][seller] += int(order)

        result = {}
        result[str(t)] = {}
        for (t, stock_type), sellers in acc.items():
            top10 = sorted(sellers.items(), key=lambda x:x[1], reverse=True)[:10]
            if stock_type not in result:
                result[str(t)][stock_type] = {}
            result[str(t)][stock_type] = {s:o for s,o in top10}

        if result:
            visual_plus.update_one(
                {"cat_id": total_cat_id},
                {"$set": {f"{COLLECTION_STOCK}.{a}":b for a,b in result.items()}},
                upsert=True
            )
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"{total_cat_id}\n")

    except Exception as e:
        print(f"{total_cat_id} 失败: {e}")

# =====================================================
# ERP 同步
# =====================================================
def sync_bi_ops(cat_ids):
    erp_col = MongoClient(ERP_URI)["erp"]["bi_category_data"]
    ops = []
    for cat_id in cat_ids:
        for r in erp_col.find({"cat_id": cat_id, "month":{"$in":[str(t) for t in TIMES_LIST]}}):
            m = r["month"]
            ops.append(UpdateOne(
                {"cat_id": cat_id},
                {"$set": {f"bi_category_data.{m}": {k:r.get(k) for k in ["acos","cost","cpc","ctr","refund_rate"]}} },
                upsert=True
            ))
    return ops

# =====================================================
# 主流程
# =====================================================
def main():
    total_cat_ids = list(cat_mapping.keys())
    ops = []

    # stock_type_top10_seller 并行处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        pool.map(calc_stock_type_top10_seller_total, total_cat_ids)

    total = len(total_cat_ids)
    for i, total_cat_id in enumerate(total_cat_ids, 1):
        upd = {"cat_id": total_cat_id}

        # 月度指标 & top10 & listing_prices
        vt = visualize_table.find_one({"cat_id": total_cat_id}, {f:1 for f in FIELDS})
        if vt:
            top10_data = compute_top10_bins(total_cat_id)
            for t in TIMES_LIST:
                current, last_month, last_year = TIME_MAP[t]
                o, g, p = calc_month(vt, current, last_month, last_year)
                upd[f"order.20{current}"] = o
                upd[f"gmv.20{current}"] = g
                upd[f"price.20{current}"] = p

                upd[f"{COLLECTION_NAME}.20{current}"] = top10_data[str(t)]

        listing_prices = update_listing_prices(total_cat_id)
        upd["listing_prices"] = listing_prices

        ops.append(UpdateOne({"cat_id": total_cat_id}, {"$set": upd}, upsert=True))
        bar(i, total, "计算进度")

    # ERP 同步
    ops.extend(sync_bi_ops(total_cat_ids))

    if ops:
        visual_plus.bulk_write(ops, ordered=False)
        print("\n全部数据写入完成 ✅")

    print("\n🎉 全流程完成")

# =====================================================
# 入口
# =====================================================
if __name__ == "__main__":
    main()
