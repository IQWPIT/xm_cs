# -*- coding: utf-8 -*-
"""
=================================================
 DuckDB + 批量月份 + 缓存价格段 + Top10 品牌（模块化）
=================================================
"""
import os
import ast
import pickle
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import duckdb
import numpy as np
import pandas as pd
# =================================================
# 环境 & DuckDB 初始化
# =================================================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
DUCKDB_PATH = r"D:/data/看板/sku总表/cx_all.duckdb"
MAX_WORKERS = 6
CHUNK_SIZE = 100_000
con = duckdb.connect(DUCKDB_PATH, read_only=True)

# =================================================
# 配置
# =================================================
SHOW_MONTHS = [
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
    202512:(2512,2511,2412)
}

CACHE_BASE = Path(r"D:\gg_xm\Q\看板\z改")
PRICE_BIN_DIR = CACHE_BASE / "价格段"
TOP10_DIR = CACHE_BASE / "top10品牌"
PRICE_BIN_DIR.mkdir(parents=True, exist_ok=True)
TOP10_DIR.mkdir(parents=True, exist_ok=True)

visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")

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
# 数据加载
# =================================================
def get_cat_mapping(total_cat_id="MLM4620"):
    mp = {}
    for doc in visual_plus.find({"cat_id": total_cat_id}, {"cat": 1, "cat_id": 1}):
        mp[doc["cat_id"]] = doc.get("cat") or [doc["cat_id"]]
    return mp

def load_cat_df(sub_cats, target_month):
    m = str(target_month)
    dfs = []

    for table in sub_cats:
        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for offset in range(0, total, CHUNK_SIZE):
            df = con.execute(f"""
                SELECT brand, sku_id, sellerType, active_price, monthly_sale_trend,
                       conversion_all_order, conversion_all_view
                FROM {table}
                LIMIT {CHUNK_SIZE} OFFSET {offset}
            """).df()

            df["monthly_sale_trend"] = df["monthly_sale_trend"].apply(safe_eval)
            df[f"sale_{m}"] = df["monthly_sale_trend"].apply(lambda x: x.get(m, 0) if isinstance(x, dict) else 0)
            df["active_price"] = pd.to_numeric(df["active_price"], errors="coerce")
            df = df[df["active_price"] > 0]
            df["brand"] = df["brand"].fillna("未知品牌")
            df["sellerType"] = df["sellerType"].fillna("others")
            dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

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
    # 确保是数值
    df["conversion_all_order"] = pd.to_numeric(df["conversion_all_order"], errors="coerce").fillna(0)
    df["conversion_all_view"] = pd.to_numeric(df["conversion_all_view"], errors="coerce").fillna(0)

    grp = df.groupby("price_bin", observed=False)[["conversion_all_order","conversion_all_view"]].sum()
    out = {}
    for iv, r in grp.iterrows():
        order = int(r["conversion_all_order"])
        view = int(r["conversion_all_view"])
        out[str(int(iv.right))] = {
            "conversion_all_order": order,
            "conversion_all_view": view,
            "conversion_ratio": round(order/view, 4) if view else None
        }
    return out

# =================================================
# 单Cat处理
# =================================================
def process_cat(cat_id, sub_cats, target_month):
    df = load_cat_df(sub_cats, target_month)
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
        "top10_brand": {"top10_brand": top10},
        "sellerType_price": calc_sellerType_price(df, target_month),
        "range_price": calc_range_price(df, target_month),
        "conversion": calc_conversion(df)
    }

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
                visual_plus.update_one(
                    {"cat_id": r["cat_id"]},
                    {"$set": {
                        f"top10_brand.{show_month}": r["top10_brand"],
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
