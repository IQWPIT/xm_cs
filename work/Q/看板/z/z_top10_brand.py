import pandas as pd
from pathlib import Path
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import pymongo
import traceback
import shutil
import ast
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
cat = get_collection("main_ml_mx", "ml_mx", "cat")
folder_path = Path(r"D:\data\看板\\202512_sku_p_info_2")
log_file = folder_path / "0.txt"
folder_path.mkdir(parents=True, exist_ok=True)

# ========================== 工具函数 ==========================
def get_price(weight, price):
    table = [
        (0, 0.1, 3.4, 1.1), (0.1, 0.2, 4.59, 1.1), (0.2, 0.3, 5.83, 1.1),
        (0.3, 0.4, 7.28, 1.9), (0.4, 0.5, 8.48, 1.9), (0.5, 0.6, 10.1, 3.7),
        (0.6, 0.7, 11.46, 3.7), (0.7, 0.8, 12.62, 3.7), (0.8, 0.9, 13.88, 6),
        (0.9, 1, 14.75, 6), (1, 1.5, 18.64, 10), (1.5, 2, 25.8, 20),
        (2, 3, 44.23, 30), (3, 4, 51.72, 40), (4, 5, 60.33, 60.33),
        (5, 6, 76.23, 76.23), (6, 7, 92.12, 92.12), (7, 8, 108.01, 108.01),
        (8, 9, 123.9, 129.9), (9, 10, 139.79, 139.79), (10, 11, 155.69, 155.69),
        (11, 12, 171.58, 171.58), (12, 13, 187.47, 187.47), (13, 14, 203.36, 203.36),
        (14, 10000000, 219.25, 219.25)
    ]
    for min_w, max_w, price_greater, price_less in table:
        if min_w <= weight < max_w:
            return price_greater if price > 299 else price_less
    return None

def get_fee(weight, price):
    table = [
        (0, 0.3, 131, 91.70, 52.40, 65.50), (0.3, 0.5, 140, 98.00, 56.00, 70.00),
        (0.5, 1, 149, 104.30, 59.60, 74.50), (1, 2, 169, 118.30, 67.60, 84.50),
        (2, 3, 190, 133.00, 76.00, 95.00), (3, 4, 206, 144.20, 82.40, 103.00),
        (4, 5, 220, 154.00, 88.00, 110.00), (5, 7, 245, 171.50, 98.00, 122.50),
        (7, 9, 279, 195.30, 111.60, 139.50), (9, 12, 323, 226.10, 129.20, 161.50),
        (12, 15, 380, 266.00, 152.00, 190.00), (15, 20, 445, 311.50, 178.00, 222.50),
        (20, 30, 563, 394.10, 225.20, 281.50), (23, 40, 698, 488.60, 279.20, 349.00),
        (40, 50, 903, 632.10, 361.20, 451.50), (50, 60, 1014, 709.80, 405.60, 507.00),
        (60, 70, 1041, 728.70, 416.40, 520.50), (70, 80, 1084, 758.80, 433.60, 542.00),
        (80, 90, 1219, 853.30, 487.60, 609.50), (90, 100, 1406, 984.20, 562.40, 703.00),
        (110, 125, 1593, 1115.10, 637.20, 796.50), (125, 150, 2115, 1480.50, 846.00, 1057.50),
        (150, 175, 2637, 1845.90, 1054.80, 1318.50), (175, 200, 3159, 2211.30, 1263.60, 1579.50),
        (200, 225, 3681, 2576.70, 1472.40, 1840.50), (225, 250, 4203, 2942.10, 1681.20, 2101.50),
        (250, 275, 4725, 3307.50, 1890.00, 2362.50), (275, 300, 5246, 3672.20, 2098.40, 2623.00),
        (300, 325, 5770, 4039.00, 2308.00, 2885.00), (325, 10000000, 6292, 4404.40, 2516.80, 3146.00)
    ]
    for min_w, max_w, fee_base, fee1, fee2, fee3 in table:
        if min_w <= weight < max_w:
            if price < 299:
                return fee1
            elif price <= 499:
                return fee2
            else:
                return fee3
    return None

def safe_eval(x, default):
    if pd.isna(x) or x in ("", "nan"):
        return default
    try:
        return ast.literal_eval(x)
    except:
        return default

def stringify_keys(d):
    if isinstance(d, dict):
        return {str(k): stringify_keys(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [stringify_keys(i) for i in d]
    else:
        return d

def safe_get_weight(pinfo, cat_weight):
    if isinstance(pinfo, str):
        try:
            pinfo = ast.literal_eval(pinfo)
        except:
            pinfo = {}
    if isinstance(pinfo, dict):
        w = pinfo.get("weight", None)
        if w not in (None, "", "nan", "None"):
            try:
                return float(w)
            except:
                pass
    return cat_weight

# ========================== CSV Worker ==========================
def process_csv_worker(args):
    file_path, times_list, time_map, processed_files = args
    file_path = Path(file_path)
    cat_id = file_path.stem
    sale_fee_ratio_doc = visual_plus.find_one({"cat_id": cat_id}, {"listing_prices": 1})
    if not sale_fee_ratio_doc or "listing_prices" not in sale_fee_ratio_doc:
        print(f"[WARN] {cat_id} not found listing_prices")
        return None
    sale_fee_ratio = sale_fee_ratio_doc["listing_prices"]["sale_fee_ratio"]
    if cat_id in processed_files:
        return None

    result = {"cat_id": cat_id, "updates": {}}
    top10_brands_202510 = []

    try:
        chunksize = 100_000
        if os.path.getsize(file_path) < 2*1024:
            target_dir = r"D:\data\看板\not_processed_改"
            os.makedirs(target_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(target_dir, os.path.basename(file_path)))
            print(f"[EMPTY] moved: {file_path}")
            return

        with tqdm(total=1, desc=f"Processing {cat_id}", unit="rows") as pbar:
            for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
                pbar.update(len(chunk))

                chunk["monthly_sale_trend"] = [safe_eval(x, {}) for x in chunk.get("monthly_sale_trend", pd.Series([{}]*len(chunk)))]
                chunk['offersInf'] = [ast.literal_eval(x) if isinstance(x, str) else [] for x in chunk.get('offersInf', ["[]"]*len(chunk))]
                chunk["brand"] = chunk.get("brand", pd.Series(["未知品牌"]*len(chunk))).fillna("未知品牌")
                chunk["sellerType"] = chunk.get("sellerType", pd.Series(["others"]*len(chunk))).fillna("others")
                chunk["stock_type"] = chunk.get("stock_type", "未知")
                chunk["active_price"] = chunk.get("active_price", 0)

                for t in times_list:
                    time, _, _ = time_map[t]
                    chunk[f"sale_{time}"] = [x.get(f"{time}", 0) for x in chunk["monthly_sale_trend"]]
                    chunk[f"gmv_{time}"] = chunk[f"sale_{time}"] * chunk["active_price"]

                    sellerCbt_sales = {str(k) if not pd.isna(k) else "未知": v
                                       for k, v in chunk.groupby("sellerCbt")[f"sale_{time}"].sum().items()} if "sellerCbt" in chunk.columns else {}
                    sellerType_sales = {str(k) if not pd.isna(k) else "未知": v
                                        for k, v in chunk.groupby("sellerType")[f"sale_{time}"].sum().items()} if "sellerType" in chunk.columns else {}
                    stock_sales = chunk.groupby("stock_type")[f"sale_{time}"].sum().to_dict()

                    brand_group = chunk.groupby("brand").agg(
                        sale_total=(f"sale_{time}", "sum"),
                        gmv_total=(f"gmv_{time}", "sum")
                    )

                    if t == 202510 and not top10_brands_202510:
                        brand_group_sorted = brand_group.sort_values("sale_total", ascending=False)
                        top10_sale = brand_group_sorted["sale_total"].head(10).to_dict()
                        top10_sale["others"] = int(brand_group_sorted["sale_total"].iloc[10:].sum())
                        top10_gmv = brand_group_sorted["gmv_total"].head(10).to_dict()
                        top10_gmv["others"] = float(brand_group_sorted["gmv_total"].iloc[10:].sum())
                        top10_brands_202510 = list(brand_group_sorted.index[:10])
                    else:
                        tmp_sale = brand_group["sale_total"].reindex(top10_brands_202510, fill_value=0).to_dict()
                        tmp_sale["others"] = int(brand_group["sale_total"].drop(top10_brands_202510, errors="ignore").sum())
                        tmp_gmv = brand_group["gmv_total"].reindex(top10_brands_202510, fill_value=0).to_dict()
                        tmp_gmv["others"] = float(brand_group["gmv_total"].drop(top10_brands_202510, errors="ignore").sum())
                        top10_sale = tmp_sale
                        top10_gmv = tmp_gmv

                    # ==== top100 SKU / offersInf 统计 ====
                    top100_skus = chunk.nlargest(100, f"sale_{time}")
                    top100_offers_length = top100_skus[['sku_id','offersInf', f'sale_{time}']].copy()
                    top100_offers_length['offers_length'] = top100_offers_length['offersInf'].apply(len)
                    top100_sku_ids = top100_skus['sku_id'].tolist()

                    top100_offers_dict = {}
                    for _, row in top100_offers_length.iterrows():
                        sku = row['sku_id']
                        top100_offers_dict[sku] = {
                            "order": int(row[f'sale_{time}']),
                            "offersInf_len": int(row['offers_length'])
                        }

                    # stock_type top100 汇总
                    top100_stock_type_price = {}
                    for stock_type, group in top100_skus.groupby("stock_type"):
                        sum_profit = 0
                        top100_group = group.nlargest(100, f"sale_{time}")
                        sku_details = {}
                        for _, row in top100_group.iterrows():
                            oD_sum = int(row[f'sale_{time}'])
                            gmv_sum = float(row[f'gmv_{time}'])
                            pR = gmv_sum / oD_sum if oD_sum else 0
                            try:
                                pinfo = row.get("p_info_2", None)
                                weight = safe_get_weight(pinfo, 0)
                                wkg = weight / 1000 if weight else 0
                                freight = get_fee(wkg, pR) if stock_type=="ful" else get_price(wkg, pR)*18.2669
                            except:
                                weight = 0
                                freight = 0
                            profit = (pR*(1-sale_fee_ratio)-freight)*oD_sum
                            sum_profit += profit
                            sku_details[row['sku_id']] = {
                                "oD": oD_sum,
                                "gmv": gmv_sum,
                                "pR": pR,
                                "weight": weight,
                                "freight": freight,
                                "profit": profit,
                                "sale_fee_ratio": sale_fee_ratio
                            }
                        order = float(top100_group[f'sale_{time}'].sum())
                        gmv = float(top100_group[f'gmv_{time}'].sum())
                        ratio = gmv / order if order != 0 else 0
                        top100_stock_type_price[stock_type] = {
                            "order": order,
                            "gmv": gmv,
                            "price": ratio,
                            "profit": sum_profit,
                            "avg_profit": sum_profit / 100,
                            "skus": sku_details
                        }

                    total_order = int(top100_skus[f'sale_{time}'].sum())
                    total_gmv = float(top100_skus[f'gmv_{time}'].sum())
                    total_ratio = total_gmv / total_order if total_order else 0
                    top100_stock_type_price["all"] = {
                        "order": total_order,
                        "gmv": total_gmv,
                        "price": total_ratio
                    }

                    offers_stats = top100_offers_length.groupby("offers_length").agg(
                        sku_count=("sku_id", "count"),
                        total_order=(f"sale_{time}", "sum")
                    ).reset_index()
                    offers_stats_dict = {int(row["offers_length"]): {"count": int(row["sku_count"]), "order": int(row["total_order"])}
                                         for _, row in offers_stats.iterrows()}

                    result["updates"][t] = {
                        "top10_brand": {"order": top10_sale, "gmv": top10_gmv},
                        # "cbt_sales": sellerCbt_sales,
                        # "stock_type": stock_sales,
                        # "sellerType": sellerType_sales,
                        "top100_stock_type_price": top100_stock_type_price,
                        "offersInf_summary": {
                        #     "offersInf": {
                        #         "non_follow": {"count": int((chunk['offersInf'].apply(len)==0).sum()),
                        #                        "order": int(chunk.loc[chunk['offersInf'].apply(len)==0, f"sale_{time}"].sum())},
                        #         "follow": {"count": int((chunk['offersInf'].apply(len)>0).sum()),
                        #                    "order": int(chunk.loc[chunk['offersInf'].apply(len)>0, f"sale_{time}"].sum())}
                        #     },
                            "top100_summary": offers_stats_dict,
                            "top100_skus": top100_offers_dict
                        }
                    }

    except Exception as e:
        print(f"报错：{e}")
        traceback.print_exc()
        return None

    if result["updates"]:
        flush_bulk([result], visual_plus, log_file)
        print(f"[DONE] {cat_id} 已写入 MongoDB")
    return result

# ========================== Mongo 写入 ==========================
def flush_bulk(buffer, visual_plus, log_file):
    ops = []
    for task in buffer:
        cat_id = task["cat_id"]
        update_doc = {}
        for t, v in task["updates"].items():
            # top100_stock_type_price 去掉 day
            stock_type_no_day = {}
            for k, val in v["top100_stock_type_price"].items():
                stock_type_no_day[k] = {key: value for key, value in val.items() if key != "day"}
            # update_doc[f"top100_stock_type_price.{t}"] = stock_type_no_day

            update_doc[f"top10_brand.{t}"] = stringify_keys(v["top10_brand"])
            # update_doc[f"cbt_sales.{t}"] = stringify_keys(v["cbt_sales"])
            # update_doc[f"stock_type.{t}"] = stringify_keys(v["stock_type"])
            # update_doc[f"sellerType.{t}"] = stringify_keys(v["sellerType"])
            # update_doc[f"offersInf.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("offersInf", {}))
            # update_doc[f"top100_summary.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("top100_summary", {}))
            # update_doc[f"top100_skus.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("top100_skus", {}))

        visual_plus.update_one({"cat_id": cat_id}, {"$set": update_doc}, upsert=True)
        # with open(log_file, "a", encoding="utf-8") as f:
        #     f.write(f"{cat_id}\n")


# ==========================================================
#                     主程序
# ==========================================================
def main():
    folder_path = Path(r"D:\data\看板\\202512_sku_p_info_2")
    log_file = folder_path / "0.txt"
    folder_path.mkdir(parents=True, exist_ok=True)

    # 时间配置
    times_list = [
        202510,
        # 202509,
        # 202508,
        # 202507,
        # 202506,
        # 202505,
        # 202504,
        # 202503,
        # 202502,
        # 202501,
        # 202412,
        # 202411,
        # 202410,
        202511,
    ]

    time_map = {
        202510:(2510,2509,2410),
        # 202509:(2509,2508,2409),
        # 202508:(2508,2507,2408),
        # 202507:(2507,2506,2407),
        # 202506:(2506,2505,2406),
        # 202505:(2505,2504,2405),
        # 202504:(2504,2503,2404),
        # 202503:(2503,2502,2403),
        # 202502:(2502,2501,2402),
        # 202501:(2501,2412,2401),
        # 202412:(2412,2411,2312),
        # 202411:(2411,2410,2311),
        # 202410:(2410,2409,2310),
        202511: (2511, 2510, 2411),
    }

    processed_files = set()
    if log_file.exists():
        processed_files = {x.strip() for x in log_file.read_text().splitlines()}

    csv_list = list(folder_path.glob("*.csv"))
    # csv_list = ["D:/data/看板/202512_sku_p_info_2/MLM1055.csv"]

    # 准备多进程参数
    args_list = [(str(f), times_list, time_map, processed_files) for f in csv_list]

    # 多进程处理 CSV，每处理完一个立即写入 MongoDB
    with ProcessPoolExecutor(max_workers=8) as exe:
        for res in tqdm(exe.map(process_csv_worker, args_list), total=len(args_list)):
            if res:
                print(0)
                # flush_bulk([res], visual_plus, log_file)  # 每次只写入一个 CSV 的结果

    print("全部完成！")
# ==========================================================
#                Windows 入口保护
# ==========================================================
if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()