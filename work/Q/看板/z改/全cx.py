# -*- coding: utf-8 -*-
import os
import sys
import ast
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import duckdb
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
    202412, 202411, 202410, 202511,202512
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
    202512: (2512, 2511, 2412)
}

DUCKDB_PATH = r"D:\gg_xm\Q\DuckDB\cx_all.duckdb"
BINS_DIR = r"D:/gg_xm/Q/看板/z改/价格段"
TOP10_DIR = r"D:/gg_xm/Q/看板/z改/top10品牌"
LOG_FILE = r"D:/gg_xm/Q/看板/z改/日志/stock_type_top10_log.txt"

COLLECTION_NAME = "top10_brand_range_price"
COLLECTION_STOCK = "stock_type_top10_seller"

MAX_WORKERS = 4
CHUNK_SIZE = 50000

# =====================================================
# DuckDB
# =====================================================
con_duck = duckdb.connect(DUCKDB_PATH, read_only=True)

# =====================================================
# 工具函数
# =====================================================
def rnd(x): return round(float(x), 2) if x else 0
def safe_div(a, b): return a / b if b else 0

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
# cat 映射
# =====================================================
def get_total_and_sub_cats():
    mp = {}
    for doc in visual_plus.find({"cat_id": "MLM437507"}, {"cat": 1, "cat_id": 1}):
        mp[doc["cat_id"]] = doc.get("cat") or [doc["cat_id"]]
    return mp

cat_mapping = get_total_and_sub_cats()

# =====================================================
# 月度指标
# =====================================================
def calc_month(d, m, lm, ly):
    m, lm, ly = str(m), str(lm), str(ly)
    st, gt = d.get("sale_trend", {}), d.get("gmv_trend", {})
    ms, mg = d.get("monthly_sale_trend", {}), d.get("monthly_gmv_trend", {})

    o = {"monthly":0,"day":{}, "average_day":0,"year":0,"chain":0}
    g = {"monthly":0,"day":{}, "average_day":0,"year":0,"chain":0}
    p = {"monthly":0,"day":{}, "year":0,"chain":0}

    to = tg = dcnt = 0
    for day, ov in st.items():
        if not day.startswith(m): continue
        gv = gt.get(day, 0)
        o["day"][day] = rnd(ov)
        g["day"][day] = rnd(gv)
        p["day"][day] = rnd(safe_div(gv, ov))
        to += ov; tg += gv; dcnt += 1

    if dcnt:
        o["monthly"] = rnd(to)
        o["average_day"] = rnd(to/dcnt)
        g["monthly"] = rnd(tg)
        g["average_day"] = rnd(tg/dcnt)

    o["year"]  = rnd(safe_div(ms.get(m), ms.get(lm)))
    o["chain"] = rnd(safe_div(ms.get(m), ms.get(ly)))
    g["year"]  = rnd(safe_div(mg.get(m), mg.get(lm)))
    g["chain"] = rnd(safe_div(mg.get(m), mg.get(ly)))

    pn = safe_div(mg.get(m), ms.get(m))
    p["monthly"] = rnd(pn)
    p["year"]  = rnd(safe_div(pn, safe_div(mg.get(lm), ms.get(lm))))
    p["chain"] = rnd(safe_div(pn, safe_div(mg.get(ly), ms.get(ly))))

    return o, g, p

# =====================================================
# listing_prices
# =====================================================
def update_listing_prices(cat_id):
    doc = cat_col.find_one({"id": cat_id}, {"listing_prices": 1})
    if not doc or not doc.get("listing_prices"):
        return None
    l = doc["listing_prices"][0]
    return {
        "name": l.get("listing_type_name"),
        "sale_fee_ratio": l.get("sale_fee_ratio")
    }

# =====================================================
# top10 brand & price bins
# =====================================================
def compute_bins(cat_id):
    pkl = os.path.join(BINS_DIR, f"{cat_id}.pkl")
    if os.path.exists(pkl):
        return pickle.load(open(pkl, "rb"))

    prices = []
    for cid in cat_mapping.get(cat_id, [cat_id]):
        total = con_duck.execute(f"SELECT COUNT(*) FROM {cid}").fetchone()[0]
        for off in range(0, total, CHUNK_SIZE):
            df = con_duck.execute(
                f"SELECT active_price FROM {cid} LIMIT {CHUNK_SIZE} OFFSET {off}"
            ).df()
            prices.extend(df["active_price"].dropna().astype(float).tolist())

    bins = np.array([999999.0]) if not prices else \
        np.unique(np.floor(np.quantile(prices, np.linspace(0,1,11))/100)*100)[1:]

    pickle.dump(bins, open(pkl, "wb"))
    return bins

def compute_top10(cat_id, target_month):
    sales = defaultdict(int)
    for cid in cat_mapping.get(cat_id, [cat_id]):
        total = con_duck.execute(f"SELECT COUNT(*) FROM {cid}").fetchone()[0]
        for off in range(0, total, CHUNK_SIZE):
            df = con_duck.execute(
                f"SELECT brand, monthly_sale_trend FROM {cid} LIMIT {CHUNK_SIZE} OFFSET {off}"
            ).df()
            df["monthly_sale_trend"] = df["monthly_sale_trend"].apply(safe_dict)
            for b, o in zip(df["brand"],
                            df["monthly_sale_trend"].apply(lambda x:x.get(str(target_month),0))):
                if pd.notna(b):
                    sales[b] += o

    return [b for b,_ in sorted(sales.items(), key=lambda x:x[1], reverse=True)[:10]]

def compute_top10_bins(cat_id, target_month):
    bins = compute_bins(cat_id)
    top10 = compute_top10(cat_id, target_month)
    m = str(target_month)

    data = {m:{}}
    for b in top10 + ["others"]:
        data[m][b] = {
            "price":0,
            "range_price":{str(int(r)):{ "order":0,"gmv":0 } for r in bins}
        }

    for cid in cat_mapping.get(cat_id, [cat_id]):
        total = con_duck.execute(f"SELECT COUNT(*) FROM {cid}").fetchone()[0]
        for off in range(0, total, CHUNK_SIZE):
            df = con_duck.execute(
                f"SELECT brand, active_price, monthly_sale_trend FROM {cid} LIMIT {CHUNK_SIZE} OFFSET {off}"
            ).df()
            df["monthly_sale_trend"] = df["monthly_sale_trend"].apply(safe_dict)
            df["order"] = df["monthly_sale_trend"].apply(lambda x:x.get(m,0))
            df["active_price"] = df["active_price"].fillna(0).astype(float)
            df["gmv"] = df["order"] * df["active_price"]

            for _, r in df.iterrows():
                b = r["brand"] if r["brand"] in top10 else "others"
                for br in bins:
                    if r["active_price"] <= br:
                        k = str(int(br))
                        data[m][b]["range_price"][k]["order"] += int(r["order"])
                        data[m][b]["range_price"][k]["gmv"] += float(r["gmv"])
                        break

    for b in top10 + ["others"]:
        o = sum(v["order"] for v in data[m][b]["range_price"].values())
        g = sum(v["gmv"] for v in data[m][b]["range_price"].values())
        data[m][b]["price"] = rnd(g/o) if o else 0

    return data

# =====================================================
# stock_type_top10_seller
# =====================================================
def calc_stock_type_top10_seller_total(cat_id, target_month):
    acc = defaultdict(lambda: defaultdict(int))
    m = str(target_month)

    for cid in cat_mapping.get(cat_id, [cat_id]):
        total = con_duck.execute(f"SELECT COUNT(*) FROM {cid}").fetchone()[0]
        for off in range(0, total, CHUNK_SIZE):
            df = con_duck.execute(
                f"SELECT stock_type, sellerName, monthly_sale_trend FROM {cid} "
                f"LIMIT {CHUNK_SIZE} OFFSET {off}"
            ).df()
            df["monthly_sale_trend"] = df["monthly_sale_trend"].apply(safe_dict)
            df["order"] = df["monthly_sale_trend"].apply(lambda x:x.get(m,0))

            for s, seller, o in zip(df["stock_type"], df["sellerName"], df["order"]):
                if s and seller and o:
                    acc[s][seller] += int(o)

    res = {
        s:{k:v for k,v in sorted(d.items(), key=lambda x:x[1], reverse=True)[:10]}
        for s,d in acc.items()
    }

    if res:
        visual_plus.update_one(
            {"cat_id": cat_id},
            {"$set": {f"{COLLECTION_STOCK}.20{m}": res}},
            upsert=True
        )

# =====================================================
# 主流程
# =====================================================
def main():
    total_cat_ids = list(cat_mapping.keys())
    ops = []

    for show_month in TIMES_LIST:
        target, lm, ly = TIME_MAP[show_month]
        print(f"\n===== 处理月份 {show_month} =====")

           # with ThreadPoolExecutor(MAX_WORKERS) as pool:
        #     pool.map(lambda cid: calc_stock_type_top10_seller_total(cid, target), total_cat_ids)

        for cid in total_cat_ids:
            upd = {"cat_id": cid}
            vt = visualize_table.find_one({"cat_id": cid}, {f: 1 for f in FIELDS})

            if vt:
                o,g,p = calc_month(vt, target, lm, ly)
                upd[f"order.{show_month}"] = o
                upd[f"gmv.{show_month}"] = g
                upd[f"price.{show_month}"] = p

            upd[f"{COLLECTION_NAME}.{show_month}"] = \
                compute_top10_bins(cid, target)[str(target)]

            upd["listing_prices"] = update_listing_prices(cid)

            ops.append(UpdateOne({"cat_id": cid}, {"$set": upd}, upsert=True))

    if ops:
        visual_plus.bulk_write(ops, ordered=False)

    print("\n🎉 批量时间全流程完成")

# =====================================================
if __name__ == "__main__":
    main()
