# -*- coding: utf-8 -*-
"""
MongoDB + 批量月份 + 缓存价格段 + Top10 品牌（模块化）【修正版】
"""
import os
import ast
import pickle
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import pandas as pd
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
visual_plus_2 = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
MAX_WORKERS = 6

# =================================================
# 配置
# =================================================
SHOW_MONTHS = [
    # 202510, 202509, 202508, 202507, 202506, 202505,
    # 202504, 202503, 202502, 202501,
    # 202412, 202411, 202410,
    202511,
    202512
]

TIME_MAP = {
    # 202510:2510, 202509:2509, 202508:2508, 202507:2507,
    # 202506:2506, 202505:2505, 202504:2504, 202503:2503,
    # 202502:2502, 202501:2501, 202412:2412, 202411:2411,
    # 202410:2410,
    202511:2511,
    202512:2512,
}

CACHE_BASE = Path(r"D:\gg_xm\Q\看板\z改")
PRICE_BIN_DIR = CACHE_BASE / "价格段"
TOP10_DIR = CACHE_BASE / "top10品牌"
PRICE_BIN_DIR.mkdir(parents=True, exist_ok=True)
TOP10_DIR.mkdir(parents=True, exist_ok=True)

# =================================================
# 工具函数
# =================================================
def safe_eval(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except:
            return {}
    return {}

def load_pkl(path: Path):
    if path.exists():
        with open(path, "rb") as f:
            return pickle.load(f)
    return None

def save_pkl(path: Path, data):
    with open(path, "wb") as f:
        pickle.dump(data, f)

# =================================================
# 类目映射
# =================================================
def get_cat_mapping():
    mp = {}
    for doc in visual_plus.find({"cat_id": "MLM4620"}, {"cat":1, "cat_id":1}):
        mp[doc["cat_id"]] = doc.get("cat") or [doc["cat_id"]]
    return mp

# =================================================
# 数据加载（MongoDB版）
# =================================================
def load_cat_df_mongo(sub_cats, target_month):
    sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
    m = str(target_month)
    dfs = []

    query = {"category_id": {"$in": sub_cats}}
    projection = {
        "brand": 1, "sku_id": 1, "sellerType": 1,
        "active_price": 1, "monthly_sale_trend": 1,
        "conversion_all_order": 1, "conversion_all_view": 1
    }

    for doc in sku_col.find(query, projection):
        monthly_trend = safe_eval(doc.get("monthly_sale_trend", {}))
        sale_m = monthly_trend.get(m, 0)
        price = pd.to_numeric(doc.get("active_price", 0), errors="coerce")
        if price <= 0:
            continue
        dfs.append({
            "brand": doc.get("brand") or "未知品牌",
            "sku_id": doc.get("sku_id"),
            "sellerType": doc.get("sellerType") or "others",
            "active_price": price,
            f"sale_{m}": sale_m,
            "conversion_all_order": pd.to_numeric(doc.get("conversion_all_order", 0), errors="coerce"),
            "conversion_all_view": pd.to_numeric(doc.get("conversion_all_view", 0), errors="coerce")
        })
    return pd.DataFrame(dfs)

# =================================================
# 价格段处理
# =================================================
def build_price_bins(df, cat_id):
    cache_file = PRICE_BIN_DIR / f"{cat_id}.pkl"
    bins = load_pkl(cache_file)
    if bins is None:
        bins = np.unique(np.floor(df["active_price"].quantile(np.linspace(0,1,11))/100)*100)
        bins[0] = 0.001
        save_pkl(cache_file, bins.tolist())
    df["price_bin"] = pd.cut(df["active_price"], bins=bins, include_lowest=True)
    return df

# =================================================
# Top10品牌
# =================================================
def get_top10_brand_names(df, cat_id):
    cache_file = TOP10_DIR / f"top10_{cat_id}.pkl"
    top10 = load_pkl(cache_file)
    if top10:
        return top10
    brand_sum = df.groupby("brand", observed=False).size().sort_values(ascending=False)
    top10 = brand_sum.head(10).index.tolist()
    save_pkl(cache_file, top10)
    return top10

# =================================================
# 指标计算
# =================================================
def calc_sellerType_price(df, target_month):
    m = str(target_month)
    out = {}
    for st, g in df.groupby("sellerType", observed=False):
        bin_sum = g.groupby("price_bin", observed=False)[f"sale_{m}"].sum()
        out[st] = {str(int(iv.right)): int(v) for iv, v in bin_sum.items()}
    return out

def calc_range_price(df, target_month):
    m = str(target_month)
    bin_sum = df.groupby("price_bin", observed=False)[f"sale_{m}"].sum()
    return {str(int(iv.right)): int(v) for iv, v in bin_sum.items()}

def calc_conversion(df):
    grp = df.groupby("price_bin", observed=False)[["conversion_all_order","conversion_all_view"]].sum()
    out = {}
    for iv, r in grp.iterrows():
        order = int(r["conversion_all_order"])
        view = int(r["conversion_all_view"])
        out[str(int(iv.right))] = {
            "conversion_all_order": order,
            "conversion_all_view": view,
            "conversion_ratio": round(order/view,4) if view else None
        }
    return out

# =================================================
# 单Cat处理
# =================================================
def process_cat(cat_id, sub_cats, target_month):
    try:
        df = load_cat_df_mongo(sub_cats, target_month)
        if df.empty:
            return None
        df = build_price_bins(df, cat_id)
        top10_names = get_top10_brand_names(df, cat_id)
        m = str(target_month)

        brand_sum = df.groupby("brand", observed=False)[f"sale_{m}"].sum()
        top10 = {b: int(brand_sum.get(b,0)) for b in top10_names}
        others = int(brand_sum[~brand_sum.index.isin(top10_names)].sum())
        if others>0: top10["others"] = others

        return {
            "cat_id": cat_id,
            "top10_brand": top10,
            "sellerType_price": calc_sellerType_price(df, target_month),
            "range_price": calc_range_price(df, target_month),
            "conversion": calc_conversion(df)
        }
    except Exception as e:
        print(f"Cat {cat_id} 处理失败:", e)
        return None

# =================================================
# 主流程
# =================================================
def main():
    results = []
    cat_mapping = get_cat_mapping()


    for show_month in SHOW_MONTHS:
        target_month = TIME_MAP[show_month]
        print(f"▶ 处理月份 {target_month}")

        with ProcessPoolExecutor(MAX_WORKERS) as exe:
            futures = [exe.submit(process_cat, cat_id, sub_cats, target_month)
                       for cat_id, sub_cats in cat_mapping.items()]
            for f in as_completed(futures):
                r = f.result()
                if not r:
                    continue
                visual_plus_2.update_one(
                    {"cat_id": r["cat_id"]},
                    {"$set": {
                        f"top10_brand.{show_month}.order": r["top10_brand"],
                        f"sellerType_price.{show_month}": r["sellerType_price"],
                        f"range_price.{show_month}": r["range_price"],
                        f"conversion.{show_month}": r["conversion"],
                    }},
                    upsert=True
                )
                results.append(r)
    return results

if __name__=="__main__":
    all_results = main()
    for r in all_results[:5]:
        print(r["cat_id"], r["top10_brand"])
        print(r["cat_id"], r["sellerType_price"])
