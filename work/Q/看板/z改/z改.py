import os
import ast
import shutil
import traceback
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import pymongo
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
every_day_sku = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
cat_col = get_collection("main_ml_mx", "ml_mx", "cat")
BASE_DIR = Path(r"D:\data\看板\20260104")
BASE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = BASE_DIR / "processed_top10_log_cx.txt"
EMPTY_DIR = Path(r"D:\data\看板\not_processed_改")
EMPTY_DIR.mkdir(exist_ok=True)
TIMES_LIST = [202510, 202511, 202512]
TIME_MAP = {
    202510: (2510, 2509, 2410),
    202511: (2511, 2510, 2411),
    202512: (2512, 2511, 2412)
}
MAX_WORKERS = 8
CSV_CHUNK = 100_000


def safe_eval(x, default):
    if pd.isna(x) or x in ("", "nan", None):
        return default
    try:
        return ast.literal_eval(x)
    except Exception:
        return default


def stringify_keys(obj):
    if isinstance(obj, dict):
        return {str(k): stringify_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [stringify_keys(i) for i in obj]
    return obj


def safe_get_weight(pinfo, fallback):
    if isinstance(pinfo, str):
        try:
            pinfo = ast.literal_eval(pinfo)
        except Exception:
            return fallback
    if isinstance(pinfo, dict):
        w = pinfo.get("weight")
        try:
            return float(w)
        except Exception:
            return fallback
    return fallback

def get_price(weight, price):
    table = [
        (0, 0.1, 3.4, 1.1), (0.1, 0.2, 4.59, 1.1),
        (0.2, 0.3, 5.83, 1.1), (0.3, 0.4, 7.28, 1.9),
        (0.4, 0.5, 8.48, 1.9), (0.5, 0.6, 10.1, 3.7),
        (0.6, 0.7, 11.46, 3.7), (0.7, 0.8, 12.62, 3.7),
        (0.8, 0.9, 13.88, 6), (0.9, 1, 14.75, 6),
        (1, 1.5, 18.64, 10), (1.5, 2, 25.8, 20),
        (2, 3, 44.23, 30), (3, 4, 51.72, 40),
        (4, 5, 60.33, 60.33), (5, 6, 76.23, 76.23),
        (6, 7, 92.12, 92.12), (7, 8, 108.01, 108.01),
        (8, 9, 123.9, 129.9), (9, 10, 139.79, 139.79),
        (10, 100000, 219.25, 219.25),
    ]
    for mn, mx, hi, lo in table:
        if mn <= weight < mx:
            return hi if price > 299 else lo
    return 0


def get_fee(weight, price):
    table = [
        (0, 0.3, 91.7, 52.4, 65.5),
        (0.3, 0.5, 98, 56, 70),
        (0.5, 1, 104.3, 59.6, 74.5),
        (1, 2, 118.3, 67.6, 84.5),
        (2, 100000, 226.1, 129.2, 161.5),
    ]
    for mn, mx, p1, p2, p3 in table:
        if mn <= weight < mx:
            if price < 299:
                return p1
            elif price <= 499:
                return p2
            return p3
    return 0


def fetch_sku_day_map(sku_ids, t):
    start, end = int(f"{t}01"), int(f"{t}31")
    cursor = every_day_sku.find(
        {"sl": {"$in": sku_ids}, "dT": {"$gte": start, "$lte": end}},
        {"sl": 1, "dT": 1, "oD": 1, "pR": 1},
    )
    res = defaultdict(dict)
    for d in cursor:
        res[d["sl"]][d["dT"]] = {
            "oD": d.get("oD", 0),
            "pR": d.get("pR", 0),
            "gmv": d.get("oD", 0) * d.get("pR", 0),
        }
    return res


def process_csv_worker(args):
    file_path, processed_files = args
    cat_id = Path(file_path).stem

    if cat_id in processed_files:
        return None

    if os.path.getsize(file_path) < 2048:
        shutil.move(file_path, EMPTY_DIR / Path(file_path).name)
        return None

    cfg = visual_plus.find_one({"cat_id": cat_id}, {"listing_prices": 1})
    if not cfg:
        return None

    sale_fee_ratio = cfg["listing_prices"]["sale_fee_ratio"]
    result = {"cat_id": cat_id, "updates": {}}
    top10_brands_list = []

    try:
        for chunk in pd.read_csv(file_path, chunksize=CSV_CHUNK, low_memory=False):
            chunk["monthly_sale_trend"] = chunk.get("monthly_sale_trend", pd.Series([{}]*len(chunk))).apply(lambda x: safe_eval(x, {}))
            chunk["offersInf"] = chunk.get("offersInf", pd.Series([[]]*len(chunk))).apply(lambda x: safe_eval(x, []))
            chunk["brand"] = chunk.get("brand", pd.Series(["未知"]*len(chunk))).fillna("未知")
            chunk["sellerType"] = chunk.get("sellerType", pd.Series(["others"]*len(chunk))).fillna("others")
            chunk["stock_type"] = chunk.get("stock_type", "未知")
            chunk["active_price"] = chunk.get("active_price", 0)

            for t in TIMES_LIST:
                time_key = TIME_MAP[t][0]
                chunk[f"sale_{time_key}"] = chunk["monthly_sale_trend"].apply(lambda x: x.get(str(time_key), 0))
                chunk[f"gmv_{time_key}"] = chunk[f"sale_{time_key}"] * chunk["active_price"]

                # sellerType / stock_type
                sellerType_sales = chunk.groupby("sellerType")[f"sale_{time_key}"].sum().to_dict()
                stock_type_sales = chunk.groupby("stock_type")[f"sale_{time_key}"].sum().to_dict()

                # Top10 brand
                brand_group = chunk.groupby("brand")[f"sale_{time_key}"].sum().sort_values(ascending=False)
                if not top10_brands_list and t == TIMES_LIST[0]:
                    top10_brands_list = list(brand_group.head(10).index)
                top10_sale = brand_group.reindex(top10_brands_list, fill_value=0).to_dict()
                top10_sale["others"] = int(brand_group.drop(top10_brands_list, errors="ignore").sum())
                brand_gmv_group = chunk.groupby("brand")[f"gmv_{time_key}"].sum().reindex(top10_brands_list, fill_value=0)
                top10_gmv = brand_gmv_group.to_dict()
                top10_gmv["others"] = float(chunk.groupby("brand")[f"gmv_{time_key}"].sum().drop(top10_brands_list, errors="ignore").sum())

                # Top100 SKU
                top100_skus_df = chunk.nlargest(100, f"sale_{time_key}")
                top100_skus_dict = {}
                top100_summary_dict = {}
                offersInf_summary = {}

                # offersInf flag
                top100_skus_df["follow_flag"] = top100_skus_df["offersInf"].apply(lambda x: 0 if len(x)==0 else 1)
                follow_count = int((top100_skus_df["follow_flag"]==0).sum())
                non_follow_count = int((top100_skus_df["follow_flag"]==1).sum())
                follow_sales = int(top100_skus_df.loc[top100_skus_df["follow_flag"]==0, f"sale_{time_key}"].sum())
                non_follow_sales = int(top100_skus_df.loc[top100_skus_df["follow_flag"]==1, f"sale_{time_key}"].sum())
                offersInf_summary = {
                    "offersInf": {
                        "non_follow": {"count": follow_count, "order": follow_sales},
                        "follow": {"count": non_follow_count, "order": non_follow_sales}
                    }
                }

                # top100_summary
                offers_stats = top100_skus_df.groupby(top100_skus_df["offersInf"].apply(len)).agg(
                    sku_count=("sku_id", "count"),
                    total_order=(f"sale_{time_key}", "sum")
                ).reset_index()
                for _, row in offers_stats.iterrows():
                    top100_summary_dict[int(row["offersInf"])] = {
                        "count": int(row["sku_count"]),
                        "order": int(row["total_order"])
                    }

                # top100_skus 日数据
                top100_sku_ids = top100_skus_df["sku_id"].tolist()
                sku_day_map = fetch_sku_day_map(top100_sku_ids, t)
                for _, row in top100_skus_df.iterrows():
                    sku = row["sku_id"]
                    top100_skus_dict[sku] = {
                        "order": int(row[f"sale_{time_key}"]),
                        "offersInf_len": len(row["offersInf"]),
                        "day": sku_day_map.get(sku, {})
                    }

                # top100_stock_type_price
                top100_stock_type_price = {}
                for stock_type, grp in top100_skus_df.groupby("stock_type"):
                    grp_order = float(grp[f"sale_{time_key}"].sum())
                    grp_gmv = float(grp[f"gmv_{time_key}"].sum())
                    top100_stock_type_price[stock_type] = {
                        "order": grp_order,
                        "gmv": grp_gmv,
                        "price": grp_gmv/grp_order if grp_order!=0 else 0
                    }

                # 汇总结果
                result["updates"][t] = {
                    "top10_brand": {"order": top10_sale, "gmv": top10_gmv},
                    "stock_type": stock_type_sales,
                    "sellerType": sellerType_sales,
                    "top100_stock_type_price": top100_stock_type_price,
                    "offersInf_summary": offersInf_summary,
                    "top100_summary": top100_summary_dict,
                    "top100_skus": top100_skus_dict
                }

    except Exception as e:
        print(f"[ERROR] CSV处理失败: {cat_id}, {e}")
        traceback.print_exc()
        return None
    return result
def flush_bulk(tasks):
    ops = []
    for task in tasks:
        cat_id = task["cat_id"]
        doc = {}
        for t, v in task["updates"].items():
            doc[f"top10_brand.{t}"] = stringify_keys(v["top10_brand"])
            doc[f"stock_type.{t}"] = stringify_keys(v["stock_type"])
            doc[f"sellerType.{t}"] = stringify_keys(v["sellerType"])
            doc[f"top100_stock_type_price.{t}"] = stringify_keys(v["top100_stock_type_price"])
            doc[f"offersInf.{t}"] = stringify_keys(v["offersInf_summary"].get("offersInf", {}))
            doc[f"top100_summary.{t}"] = stringify_keys(v.get("top100_summary", {}))
            doc[f"top100_skus.{t}"] = stringify_keys(v.get("top100_skus", {}))
        ops.append(pymongo.UpdateOne({"cat_id": cat_id}, {"$set": doc}, upsert=True))

    if ops:
        visual_plus.bulk_write(ops, ordered=False)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            for t in tasks:
                f.write(f"{t['cat_id']}\n")
def main():
    processed_files = set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()
    csv_files = list(BASE_DIR.glob("*.csv"))
    tasks = [(str(f), processed_files) for f in csv_files]

    results = []
    with ProcessPoolExecutor(MAX_WORKERS) as exe:
        for res in tqdm(exe.map(process_csv_worker, tasks), total=len(tasks)):
            if res:
                results.append(res)
                if len(results) >= 20:
                    flush_bulk(results)
                    results.clear()

    if results:
        flush_bulk(results)

    print("✅ 全部完成")


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
