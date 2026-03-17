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
log_file = folder_path / "processed_top10_log_cx2.txt"
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
                        "cbt_sales": sellerCbt_sales,
                        "stock_type": stock_sales,
                        "sellerType": sellerType_sales,
                        "top100_stock_type_price": top100_stock_type_price,
                        "offersInf_summary": {
                            "offersInf": {
                                "non_follow": {"count": int((chunk['offersInf'].apply(len)==0).sum()),
                                               "order": int(chunk.loc[chunk['offersInf'].apply(len)==0, f"sale_{time}"].sum())},
                                "follow": {"count": int((chunk['offersInf'].apply(len)>0).sum()),
                                           "order": int(chunk.loc[chunk['offersInf'].apply(len)>0, f"sale_{time}"].sum())}
                            },
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
            update_doc[f"top100_stock_type_price.{t}"] = stock_type_no_day

            update_doc[f"top10_brand.{t}"] = stringify_keys(v["top10_brand"])
            update_doc[f"cbt_sales.{t}"] = stringify_keys(v["cbt_sales"])
            update_doc[f"stock_type.{t}"] = stringify_keys(v["stock_type"])
            update_doc[f"sellerType.{t}"] = stringify_keys(v["sellerType"])
            update_doc[f"offersInf.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("offersInf", {}))
            update_doc[f"top100_summary.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("top100_summary", {}))
            update_doc[f"top100_skus.{t}"] = stringify_keys(v.get("offersInf_summary", {}).get("top100_skus", {}))

        visual_plus.update_one({"cat_id": cat_id}, {"$set": update_doc}, upsert=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"{cat_id}\n")


# ==========================================================
#                     主程序
# ==========================================================
def main():
    folder_path = Path(r"D:\data\看板\\202512_sku_p_info_2")
    log_file = folder_path / "processed_top10_log_cx2.txt"
    folder_path.mkdir(parents=True, exist_ok=True)

    # 时间配置
    times_list = [
        202511,
        202510,
        202509,
        202508,
        202507,
        202506,
        202505,
        202504,
        202503,
        202502,
        202501,
        202412,
        202411,
        202410,
    ]

    time_map = {
        202511:(2511,2510,2411),
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
        202410:(2410,2409,2310)
    }

    processed_files = set()
    if log_file.exists():
        processed_files = {x.strip() for x in log_file.read_text().splitlines()}

    csv_list = list(folder_path.glob("*.csv"))
    # csv_list = ["D:/data/看板/202512_sku_p_info_2/MLM1055.csv"]
    a = ['MLM1747', 'MLM92153', 'MLM437507', 'MLM45864', 'MLM437561', 'MLM437563', 'MLM437562', 'MLM1367', 'MLM1039',
         'MLM1000', 'MLM456046', 'MLM422167', 'MLM455985', 'MLM440238', 'MLM455976', 'MLM413971', 'MLM271637',
         'MLM5320', 'MLM22621', 'MLM172372', 'MLM92149', 'MLM180901', 'MLM8531', 'MLM179724', 'MLM92474', 'MLM180902',
         'MLM456111', 'MLM1748', 'MLM2239', 'MLM179617', 'MLM1403', 'MLM194325', 'MLM455932', 'MLM178700', 'MLM1423',
         'MLM194324', 'MLM1071', 'MLM1100', 'MLM189310', 'MLM189530', 'MLM456927', 'MLM1117', 'MLM1081', 'MLM429307',
         'MLM1091', 'MLM1072', 'MLM1111', 'MLM1105', 'MLM1372', 'MLM1861', 'MLM1806', 'MLM1384', 'MLM423151', 'MLM5702',
         'MLM429651', 'MLM421311', 'MLM39965', 'MLM187792', 'MLM1392', 'MLM5360', 'MLM429683', 'MLM5362', 'MLM1396',
         'MLM429704', 'MLM1385', 'MLM1246', 'MLM187817', 'MLM194072', 'MLM1253', 'MLM1263', 'MLM43673', 'MLM194452',
         'MLM187663', 'MLM126070', 'MLM1248', 'MLM194417', 'MLM1051', 'MLM3813', 'MLM1058', 'MLM192051', 'MLM194341',
         'MLM437210', 'MLM7502', 'MLM1648', 'MLM191082', 'MLM430598', 'MLM10848', 'MLM1691', 'MLM1700', 'MLM182235',
         'MLM430687', 'MLM439434', 'MLM36845', 'MLM1655', 'MLM1651', 'MLM438450', 'MLM10736', 'MLM1718', 'MLM1723',
         'MLM182456', 'MLM1144', 'MLM8232', 'MLM438578', 'MLM438579', 'MLM1041', 'MLM1049', 'MLM5849', 'MLM168281',
         'MLM430403', 'MLM10372', 'MLM437477', 'MLM430989', 'MLM1276', 'MLM2480', 'MLM1309', 'MLM1357', 'MLM191799',
         'MLM1292', 'MLM8969', 'MLM1362', 'MLM438178', 'MLM437767', 'MLM421369', 'MLM1338', 'MLM1285', 'MLM1302',
         'MLM1342', 'MLM438532', 'MLM189113', 'MLM438546', 'MLM18308', 'MLM410723', 'MLM174624', 'MLM4349', 'MLM432691',
         'MLM69803', 'MLM437079', 'MLM182735', 'MLM421387', 'MLM1328', 'MLM438393', 'MLM4682', 'MLM6144', 'MLM440904',
         'MLM422147', 'MLM191639', 'MLM1575', 'MLM438597', 'MLM438282', 'MLM438451', 'MLM158828', 'MLM158840',
         'MLM438284', 'MLM1576', 'MLM4887', 'MLM431414', 'MLM438078', 'MLM189492', 'MLM4914', 'MLM145906', 'MLM1004',
         'MLM189967', 'MLM4900', 'MLM2830', 'MLM1574', 'MLM1631', 'MLM1613', 'MLM8179', 'MLM194414', 'MLM1582',
         'MLM436380', 'MLM436414', 'MLM107711', 'MLM436246', 'MLM1499', 'MLM437317', 'MLM2102', 'MLM1182', 'MLM3004',
         'MLM438365', 'MLM435173', 'MLM194141', 'MLM3005', 'MLM434816', 'MLM194155', 'MLM434786', 'MLM3022', 'MLM3937',
         'MLM123220', 'MLM404419', 'MLM1431', 'MLM1442', 'MLM117517', 'MLM1132', 'MLM433060', 'MLM2961', 'MLM438116',
         'MLM432873', 'MLM11229', 'MLM433069', 'MLM432988', 'MLM437165', 'MLM436922', 'MLM189993', 'MLM432818',
         'MLM3655', 'MLM352344', 'MLM10811', 'MLM187708', 'MLM189879', 'MLM437237', 'MLM433047', 'MLM432871',
         'MLM44011', 'MLM179227', 'MLM179242', 'MLM179243', 'MLM435284', 'MLM187772', 'MLM5395', 'MLM6556', 'MLM10217',
         'MLM6567', 'MLM174912', 'MLM438156', 'MLM1953', 'MLM194494', 'MLM1740', 'MLM189470', 'MLM413990', 'MLM413972',
         'MLM456033', 'MLM456116', 'MLM413999', 'MLM413987', 'MLM455961', 'MLM438216', 'MLM92132', 'MLM92130',
         'MLM162992', 'MLM458439', 'MLM437817', 'MLM372429', 'MLM438008', 'MLM438040', 'MLM191902', 'MLM180468',
         'MLM8532', 'MLM176118', 'MLM176122', 'MLM437503', 'MLM171483', 'MLM177353', 'MLM177661', 'MLM174920',
         'MLM437696', 'MLM179786', 'MLM437694', 'MLM438862', 'MLM164783', 'MLM180599', 'MLM438606', 'MLM158111',
         'MLM438669', 'MLM161232', 'MLM62458', 'MLM458594', 'MLM159159', 'MLM458709', 'MLM458739', 'MLM438823',
         'MLM164739', 'MLM22580', 'MLM458349', 'MLM438863', 'MLM458376', 'MLM458331', 'MLM2228', 'MLM458231',
         'MLM458247', 'MLM438834', 'MLM459291', 'MLM163938', 'MLM45843', 'MLM458931', 'MLM458933', 'MLM158133',
         'MLM158086', 'MLM458676', 'MLM458682', 'MLM458678', 'MLM458686', 'MLM458904', 'MLM458955', 'MLM164129',
         'MLM458806', 'MLM458945', 'MLM458453', 'MLM164019', 'MLM160019', 'MLM458087', 'MLM458146', 'MLM458138',
         'MLM458094', 'MLM458086', 'MLM458145', 'MLM440285', 'MLM440289', 'MLM179644', 'MLM437673', 'MLM179627',
         'MLM179618', 'MLM179706', 'MLM179793', 'MLM194289', 'MLM189607', 'MLM194523', 'MLM194310', 'MLM194316',
         'MLM194308', 'MLM194315', 'MLM194320', 'MLM194328', 'MLM194313', 'MLM194317', 'MLM194306', 'MLM194309',
         'MLM194318', 'MLM194311', 'MLM1101', 'MLM85006', 'MLM1122', 'MLM1123', 'MLM434779', 'MLM126125', 'MLM1086',
         'MLM434784', 'MLM1093', 'MLM32646', 'MLM434760', 'MLM434771', 'MLM11060', 'MLM1076', 'MLM126287', 'MLM434762',
         'MLM8036', 'MLM434764', 'MLM434852', 'MLM85072', 'MLM437492', 'MLM6652', 'MLM439621', 'MLM6661', 'MLM439630',
         'MLM417836', 'MLM429665', 'MLM440833', 'MLM40396', 'MLM187682', 'MLM421312', 'MLM429684', 'MLM185376',
         'MLM15046', 'MLM82314', 'MLM430318', 'MLM172004', 'MLM437092', 'MLM158069', 'MLM194082', 'MLM1257',
         'MLM457133', 'MLM1254', 'MLM194455', 'MLM180910', 'MLM187664', 'MLM455944', 'MLM4596', 'MLM418966',
         'MLM187829', 'MLM432598', 'MLM418971', 'MLM418968', 'MLM1252', 'MLM1251', 'MLM180961', 'MLM1249', 'MLM425049',
         'MLM431636', 'MLM436011', 'MLM432437', 'MLM5069', 'MLM432435', 'MLM432436', 'MLM2910', 'MLM1052', 'MLM189424',
         'MLM437923', 'MLM438858', 'MLM438859', 'MLM438857', 'MLM438922', 'MLM438856', 'MLM16126', 'MLM193855',
         'MLM438852', 'MLM438853', 'MLM438854', 'MLM437546', 'MLM5017', 'MLM430796', 'MLM430916', 'MLM1696', 'MLM2676',
         'MLM430794', 'MLM191050', 'MLM438680', 'MLM2141', 'MLM10190', 'MLM182237', 'MLM414248', 'MLM437482',
         'MLM131444', 'MLM430639', 'MLM3308', 'MLM437653', 'MLM85838', 'MLM120342', 'MLM437570', 'MLM3358', 'MLM420016',
         'MLM430339', 'MLM189230', 'MLM127823', 'MLM430367', 'MLM430404', 'MLM438189', 'MLM6508', 'MLM2481',
         'MLM410992', 'MLM7409', 'MLM1935', 'MLM1934', 'MLM12379', 'MLM438372', 'MLM438378', 'MLM1979', 'MLM438203',
         'MLM6040', 'MLM438383', 'MLM438397', 'MLM438398', 'MLM437769', 'MLM438461', 'MLM438426', 'MLM438428',
         'MLM438427', 'MLM438429', 'MLM8960', 'MLM438176', 'MLM3098', 'MLM438174', 'MLM438160', 'MLM126120',
         'MLM130213', 'MLM438468', 'MLM418341', 'MLM1344', 'MLM1343', 'MLM438534', 'MLM438293', 'MLM189118', 'MLM8004',
         'MLM438547', 'MLM436924', 'MLM131737', 'MLM438580', 'MLM39115', 'MLM438625', 'MLM5506', 'MLM438308',
         'MLM438937', 'MLM411154', 'MLM438599', 'MLM103850', 'MLM159011', 'MLM119121', 'MLM438287', 'MLM1621',
         'MLM437359', 'MLM436106', 'MLM438286', 'MLM1021', 'MLM38971', 'MLM6189', 'MLM10720', 'MLM9394', 'MLM438448',
         'MLM189437', 'MLM149101', 'MLM437691', 'MLM436440', 'MLM179173', 'MLM1632', 'MLM1616', 'MLM116380',
         'MLM437239', 'MLM12269', 'MLM412384', 'MLM443824', 'MLM193786', 'MLM436298', 'MLM194102', 'MLM436269',
         'MLM436296', 'MLM436280', 'MLM436302', 'MLM436273', 'MLM167542', 'MLM8166', 'MLM187572', 'MLM436100',
         'MLM156011', 'MLM194411', 'MLM194445', 'MLM187569', 'MLM194421', 'MLM171804', 'MLM1612', 'MLM437200',
         'MLM436392', 'MLM436384', 'MLM436382', 'MLM436389', 'MLM431869', 'MLM436416', 'MLM187643', 'MLM437572',
         'MLM429182', 'MLM31551', 'MLM1609', 'MLM436248', 'MLM436250', 'MLM26548', 'MLM438345', 'MLM435174',
         'MLM435208', 'MLM194144', 'MLM2987', 'MLM194163', 'MLM194139', 'MLM7715', 'MLM1841', 'MLM12175', 'MLM432273',
         'MLM7932', 'MLM187076', 'MLM433105', 'MLM414136', 'MLM437228', 'MLM437293', 'MLM189590', 'MLM7764',
         'MLM432825', 'MLM432828', 'MLM432832', 'MLM438014', 'MLM420077', 'MLM179574', 'MLM194396', 'MLM178499',
         'MLM435037', 'MLM420667', 'MLM417634', 'MLM180917', 'MLM435064', 'MLM435074', 'MLM438155', 'MLM438289',
         'MLM44105', 'MLM409093', 'MLM100821', 'MLM435076', 'MLM180939', 'MLM435080', 'MLM168074', 'MLM435068',
         'MLM435071', 'MLM146238', 'MLM393758', 'MLM191372', 'MLM422169', 'MLM179849', 'MLM437618', 'MLM180527',
         'MLM22611', 'MLM62402', 'MLM437671', 'MLM437755', 'MLM177049', 'MLM176133', 'MLM176135', 'MLM178277',
         'MLM437929', 'MLM437881', 'MLM429226', 'MLM456122', 'MLM437486', 'MLM177357', 'MLM177359', 'MLM437697',
         'MLM437698', 'MLM437735', 'MLM437709', 'MLM179789', 'MLM164730', 'MLM165434', 'MLM158088', 'MLM438570',
         'MLM165593', 'MLM438608', 'MLM145889', 'MLM458511', 'MLM158052', 'MLM165684', 'MLM438681', 'MLM189724',
         'MLM159220', 'MLM440144', 'MLM163960', 'MLM438903', 'MLM164724', 'MLM438874', 'MLM164713', 'MLM458864',
         'MLM458865', 'MLM100673', 'MLM458848', 'MLM438729', 'MLM438798', 'MLM438802', 'MLM163079', 'MLM439034',
         'MLM164027', 'MLM432521', 'MLM458116', 'MLM458103', 'MLM375063', 'MLM440291', 'MLM440290', 'MLM440314',
         'MLM194329', 'MLM189660', 'MLM194312', 'MLM434790', 'MLM434791', 'MLM434835', 'MLM434836', 'MLM434834',
         'MLM126295', 'MLM434767', 'MLM434766', 'MLM194740', 'MLM434781', 'MLM1075', 'MLM434855', 'MLM439623',
         'MLM187683', 'MLM433716', 'MLM432119', 'MLM39210', 'MLM118500', 'MLM39119', 'MLM8490', 'MLM6135', 'MLM191202',
         'MLM8449', 'MLM118506', 'MLM10208', 'MLM8485', 'MLM11052', 'MLM39118', 'MLM191155', 'MLM118532', 'MLM167572',
         'MLM4377', 'MLM8500', 'MLM187310', 'MLM12937', 'MLM118494', 'MLM191138', 'MLM439517', 'MLM430605', 'MLM430797',
         'MLM430785', 'MLM193864', 'MLM3579', 'MLM1670', 'MLM4701', 'MLM9739', 'MLM191672', 'MLM438413', 'MLM371425',
         'MLM39201', 'MLM69695', 'MLM131439', 'MLM187262', 'MLM69809', 'MLM8159', 'MLM438205', 'MLM12201', 'MLM438375',
         'MLM438385', 'MLM438473', 'MLM120089', 'MLM456043', 'MLM438297', 'MLM438291', 'MLM438245', 'MLM10723',
         'MLM18053', 'MLM4334', 'MLM417764', 'MLM436306', 'MLM436305', 'MLM375455', 'MLM194103', 'MLM392274',
         'MLM118809', 'MLM436277', 'MLM436291', 'MLM31542', 'MLM437818', 'MLM168681', 'MLM194442', 'MLM194446',
         'MLM436386', 'MLM435210', 'MLM194165', 'MLM194167', 'MLM194166', 'MLM434923', 'MLM1846', 'MLM1842', 'MLM1844',
         'MLM190045', 'MLM180911', 'MLM417907', 'MLM413158', 'MLM44103', 'MLM189641', 'MLM377674', 'MLM1805', 'MLM4708',
         'MLM432164', 'MLM29966', 'MLM438872', 'MLM458840', 'MLM438883', 'MLM458819', 'MLM438882', 'MLM458800',
         'MLM438884', 'MLM164717', 'MLM120051', 'MLM434361', 'MLM18114', 'MLM439528', 'MLM7753', 'MLM43197',
         'MLM439070', 'MLM439072', 'MLM439615', 'MLM439610', 'MLM439056', 'MLM439165', 'MLM131443', 'MLM189815',
         'MLM371932', 'MLM371933', 'MLM438373', 'MLM44626', 'MLM438077', 'MLM438079', 'MLM1368', 'MLM1945', 'MLM459590',
         'MLM455857', 'MLM457647', 'MLM5166', 'MLM2136', 'MLM438529', 'MLM436810', 'MLM436814', 'MLM436812',
         'MLM438411', 'MLM1430', 'MLM115562', 'MLM5208', 'MLM3964', 'MLM120666', 'MLM437535', 'MLM3122', 'MLM437215',
         'MLM437217', 'MLM1456', 'MLM432031', 'MLM432032', 'MLM194111', 'MLM194112', 'MLM430317', 'MLM432040',
         'MLM377249', 'MLM417671', 'MLM2818', 'MLM2791', 'MLM2736', 'MLM194021', 'MLM2763', 'MLM437336', 'MLM2779',
         'MLM419936', 'MLM438041', 'MLM455304', 'MLM455301', 'MLM455303', 'MLM186863', 'MLM455307', 'MLM455419',
         'MLM455429', 'MLM455041', 'MLM189757', 'MLM454777', 'MLM454379', 'MLM430630', 'MLM447778', 'MLM455108',
         'MLM455059', 'MLM455125', 'MLM179906', 'MLM455106', 'MLM1500', 'MLM438191', 'MLM189340', 'MLM438718',
         'MLM455449', 'MLM455443', 'MLM438820', 'MLM189241', 'MLM456536', 'MLM411938', 'MLM178685', 'MLM145907',
         'MLM438807', 'MLM191631', 'MLM455173', 'MLM455182', 'MLM455178', 'MLM455180', 'MLM454704', 'MLM437173',
         'MLM436140', 'MLM456754', 'MLM454690', 'MLM454692', 'MLM454701', 'MLM2103', 'MLM455054', 'MLM455052',
         'MLM455104', 'MLM187742', 'MLM454906', 'MLM438242', 'MLM454898', 'MLM454793', 'MLM454785', 'MLM5182',
         'MLM5160', 'MLM454900', 'MLM437434', 'MLM454914', 'MLM454916', 'MLM454904', 'MLM454908', 'MLM178354',
         'MLM455231', 'MLM455417', 'MLM454731', 'MLM455040', 'MLM2335', 'MLM412364', 'MLM456417', 'MLM437870',
         'MLM412436', 'MLM437918', 'MLM412365', 'MLM437849', 'MLM456290', 'MLM456294', 'MLM412432', 'MLM437844',
         'MLM437840', 'MLM456456', 'MLM456463', 'MLM437943', 'MLM437877', 'MLM456216', 'MLM456208', 'MLM31586',
         'MLM438028', 'MLM438020', 'MLM194319', 'MLM431640', 'MLM123324', 'MLM1713', 'MLM2048', 'MLM392406',
         'MLM455100', 'MLM455102', 'MLM120631', 'MLM438447', 'MLM190783', 'MLM455407', 'MLM455404', 'MLM438446',
         'MLM430653', 'MLM187689', 'MLM193931', 'MLM438809', 'MLM439041', 'MLM438720', 'MLM438728', 'MLM438738',
         'MLM189275', 'MLM443822', 'MLM411939', 'MLM439015', 'MLM2526', 'MLM439030', 'MLM439014', 'MLM439016',
         'MLM435291', 'MLM31511', 'MLM438742', 'MLM455445', 'MLM438748', 'MLM455324', 'MLM455328', 'MLM455317',
         'MLM455315', 'MLM455321', 'MLM455319', 'MLM455326', 'MLM437683', 'MLM436142', 'MLM168469', 'MLM454938',
         'MLM454896', 'MLM454934', 'MLM454936', 'MLM429212', 'MLM454918', 'MLM454798', 'MLM454794', 'MLM454789',
         'MLM454787', 'MLM412676', 'MLM187729', 'MLM49290', 'MLM454775', 'MLM454771', 'MLM454779', 'MLM454773',
         'MLM454769', 'MLM194503', 'MLM454922', 'MLM454928', 'MLM454924', 'MLM454930', 'MLM454926', 'MLM193841',
         'MLM454782', 'MLM454784', 'MLM454902', 'MLM193851', 'MLM10590', 'MLM433012', 'MLM455044', 'MLM412441',
         'MLM434751', 'MLM431016', 'MLM438723', 'MLM438722', 'MLM189284', 'MLM411950', 'MLM439035', 'MLM439021',
         'MLM438333', 'MLM429291', 'MLM438335', 'MLM189823', 'MLM432820', 'MLM1941', 'MLM191179', 'MLM438819',
         'MLM380107', 'MLM432426', 'MLM432123', 'MLM6505', 'MLM193845', 'MLM454932', 'MLM252528', 'MLM194671',
         'MLM2527', 'MLM455255', 'MLM455260', 'MLM455249', 'MLM455252', 'MLM455257', 'MLM455259', 'MLM455277',
         'MLM455275', 'MLM455278', 'MLM443024', 'MLM455276', 'MLM455279', 'MLM438970', 'MLM151557', 'MLM31538',
         'MLM412763', 'MLM455236', 'MLM151548', 'MLM455201', 'MLM455200', 'MLM455198', 'MLM455207', 'MLM455206',
         'MLM455212', 'MLM455204', 'MLM454950', 'MLM454952', 'MLM454948', 'MLM454957', 'MLM189258', 'MLM456761',
         'MLM31532', 'MLM31536', 'MLM9202', 'MLM455210', 'MLM438908', 'MLM120114', 'MLM445854', 'MLM189265',
         'MLM189268', 'MLM431013', 'MLM439044', 'MLM440953', 'MLM438897', 'MLM438836', 'MLM438835', 'MLM438837',
         'MLM413212', 'MLM122014', 'MLM438825', 'MLM438830', 'MLM438841', 'MLM438963', 'MLM191746', 'MLM455425',
         'MLM455432', 'MLM455292', 'MLM454707', 'MLM454791', 'MLM454801', 'MLM437862', 'MLM454980', 'MLM438591',
         'MLM417299', 'MLM412099', 'MLM392350', 'MLM454975', 'MLM454885', 'MLM438025', 'MLM454738', 'MLM454992',
         'MLM454858', 'MLM454873', 'MLM392345', 'MLM438592', 'MLM454806', 'MLM454845', 'MLM455263', 'MLM454875',
         'MLM455412', 'MLM454809', 'MLM455007', 'MLM454829', 'MLM392009', 'MLM400189', 'MLM454910', 'MLM438593',
         'MLM455245', 'MLM454995', 'MLM455008', 'MLM454945', 'MLM448169', 'MLM455434', 'MLM1310', 'MLM455135',
         'MLM438930', 'MLM454724', 'MLM448316', 'MLM454920', 'MLM455528', 'MLM417479', 'MLM437387', 'MLM191074',
         'MLM417943', 'MLM417480', 'MLM191075', 'MLM455530', 'MLM418348', 'MLM438415', 'MLM277586', 'MLM49294',
         'MLM455572', 'MLM2524', 'MLM187107', 'MLM455388', 'MLM455490', 'MLM455488', 'MLM6140', 'MLM437407',
         'MLM455492', 'MLM455522', 'MLM126068', 'MLM455396', 'MLM455398', 'MLM157897', 'MLM455369', 'MLM455386',
         'MLM455380', 'MLM436376', 'MLM438217', 'MLM455582', 'MLM447346', 'MLM455502', 'MLM455506', 'MLM417454',
         'MLM455558', 'MLM8626', 'MLM455617', 'MLM455335', 'MLM157922', 'MLM457044', 'MLM417842', 'MLM436371',
         'MLM187843', 'MLM455390', 'MLM455371', 'MLM455370', 'MLM157896', 'MLM455394', 'MLM455391', 'MLM455382',
         'MLM455383', 'MLM373498', 'MLM432807', 'MLM432834', 'MLM438224', 'MLM411921', 'MLM6203', 'MLM437524',
         'MLM187112', 'MLM455332', 'MLM455333', 'MLM455334', 'MLM455337', 'MLM433516', 'MLM445876', 'MLM189094',
         'MLM157923', 'MLM429076', 'MLM440954', 'MLM373654', 'MLM2522', 'MLM417843', 'MLM418200', 'MLM157907',
         'MLM455637', 'MLM437347', 'MLM416546', 'MLM455633', 'MLM379645', 'MLM429584', 'MLM455602', 'MLM436816',
         'MLM432891', 'MLM455751', 'MLM455749', 'MLM436793', 'MLM455746', 'MLM455745', 'MLM455755', 'MLM455767',
         'MLM455753', 'MLM194657', 'MLM439018', 'MLM439024', 'MLM439022', 'MLM388334', 'MLM439358', 'MLM455721',
         'MLM457052', 'MLM457111', 'MLM455174', 'MLM455804', 'MLM194419', 'MLM455806', 'MLM455802', 'MLM455814',
         'MLM455823', 'MLM455825', 'MLM455827', 'MLM455831', 'MLM194096', 'MLM180912', 'MLM456149', 'MLM430487',
         'MLM455895', 'MLM455899', 'MLM458209', 'MLM455874', 'MLM455930', 'MLM445765', 'MLM437166', 'MLM455946',
         'MLM455965', 'MLM455977', 'MLM455989', 'MLM456013', 'MLM456004', 'MLM456048', 'MLM126146', 'MLM413657',
         'MLM457171', 'MLM456592', 'MLM456637', 'MLM458468', 'MLM457417', 'MLM456115', 'MLM456119', 'MLM456130',
         'MLM456153', 'MLM456176', 'MLM456190', 'MLM456194', 'MLM456211', 'MLM437914', 'MLM435307', 'MLM456231',
         'MLM456257', 'MLM456245', 'MLM437855', 'MLM456276', 'MLM456283', 'MLM456293', 'MLM456303', 'MLM456341',
         'MLM457316', 'MLM456372', 'MLM456387', 'MLM456389', 'MLM456383', 'MLM456400', 'MLM456403', 'MLM456422',
         'MLM456407', 'MLM456444', 'MLM456580', 'MLM456585', 'MLM456600', 'MLM456594', 'MLM456608', 'MLM456615',
         'MLM456624', 'MLM456635', 'MLM456621', 'MLM457326', 'MLM456663', 'MLM456674', 'MLM456668', 'MLM456689',
         'MLM456694', 'MLM456704', 'MLM456709', 'MLM456722', 'MLM456720', 'MLM193836', 'MLM442345', 'MLM442346',
         'MLM457064', 'MLM454448', 'MLM457147', 'MLM456833', 'MLM442343', 'MLM456826', 'MLM457072', 'MLM446799',
         'MLM446801', 'MLM456820', 'MLM193837', 'MLM457022', 'MLM456929', 'MLM442414', 'MLM442396', 'MLM430444',
         'MLM442351', 'MLM442365', 'MLM457633', 'MLM457159', 'MLM456875', 'MLM457581', 'MLM457621', 'MLM999179',
         'MLM457256', 'MLM457259', 'MLM446791', 'MLM456835', 'MLM456886', 'MLM456795', 'MLM456817', 'MLM456858',
         'MLM456848', 'MLM456873', 'MLM456876', 'MLM456866', 'MLM456877', 'MLM456874', 'MLM456899', 'MLM456903',
         'MLM456921', 'MLM457264', 'MLM457055', 'MLM456933', 'MLM457573', 'MLM457010', 'MLM457018', 'MLM457136',
         'MLM457127', 'MLM456882', 'MLM437932', 'MLM456291', 'MLM456650', 'MLM3025', 'MLM457428', 'MLM457790',
         'MLM457521', 'MLM457539', 'MLM417504', 'MLM438128', 'MLM352399', 'MLM437263', 'MLM437297', 'MLM438574',
         'MLM445758', 'MLM437695', 'MLM417119', 'MLM439447', 'MLM437733', 'MLM455763', 'MLM442344', 'MLM455566',
         'MLM437904', 'MLM1540', 'MLM10514', 'MLM92281', 'MLM9141', 'MLM92303', 'MLM56666', 'MLM92312', 'MLM1541',
         'MLM1229', 'MLM9004', 'MLM93880', 'MLM92327', 'MLM1898', 'MLM455241', 'MLM456349', 'MLM413994', 'MLM271719',
         'MLM419937', 'MLM433054', 'MLM433042', 'MLM440152', 'MLM439549', 'MLM429614', 'MLM439511', 'MLM439525',
         'MLM438543', 'MLM439548', 'MLM433370', 'MLM445767', 'MLM439050', 'MLM454819', 'MLM432523', 'MLM455469',
         'MLM439524', 'MLM445766', 'MLM445752', 'MLM455949', 'MLM456457', 'MLM457940', 'MLM456828', 'MLM456883',
         'MLM457084', 'MLM457077', 'MLM457584', 'MLM457839', 'MLM457844', 'MLM457840', 'MLM457697', 'MLM457646',
         'MLM457971', 'MLM457667', 'MLM457841', 'MLM457754', 'MLM457599', 'MLM457694', 'MLM457593', 'MLM457656',
         'MLM457727', 'MLM457700', 'MLM457710', 'MLM457660', 'MLM457576', 'MLM457781', 'MLM457811', 'MLM457566',
         'MLM457675', 'MLM457749', 'MLM457638', 'MLM457725', 'MLM457661', 'MLM458256', 'MLM458211', 'MLM458435',
         'MLM458450', 'MLM458227', 'MLM458442', 'MLM458482', 'MLM458461', 'MLM458446', 'MLM458248', 'MLM458656',
         'MLM458501', 'MLM458533', 'MLM458744', 'MLM458762', 'MLM458541', 'MLM458534', 'MLM458095', 'MLM458122',
         'MLM458077', 'MLM458977', 'MLM458516', 'MLM458355', 'MLM458156', 'MLM458413', 'MLM458495', 'MLM458619',
         'MLM458566', 'MLM458588', 'MLM458852', 'MLM458890', 'MLM458633', 'MLM458570', 'MLM458234', 'MLM458646',
         'MLM458575', 'MLM459260', 'MLM458859', 'MLM458641', 'MLM458901', 'MLM458088', 'MLM458636', 'MLM458882',
         'MLM458243', 'MLM458958', 'MLM458795', 'MLM458668', 'MLM458961', 'MLM458694', 'MLM458834', 'MLM458967',
         'MLM458698', 'MLM458971', 'MLM458213', 'MLM459043', 'MLM459107', 'MLM458984', 'MLM459044', 'MLM459076',
         'MLM458689', 'MLM458813', 'MLM459117', 'MLM459008', 'MLM459127', 'MLM459009', 'MLM459000', 'MLM459081',
         'MLM459095', 'MLM459082', 'MLM459087', 'MLM459122', 'MLM459205', 'MLM458276', 'MLM459167', 'MLM459199',
         'MLM459245', 'MLM459177', 'MLM459213', 'MLM459142', 'MLM459222', 'MLM459136', 'MLM459155', 'MLM458598',
         'MLM458549', 'MLM458277', 'MLM459244', 'MLM459276', 'MLM459295', 'MLM459312', 'MLM459116', 'MLM459267',
         'MLM459271', 'MLM459261', 'MLM1459', 'MLM1478', 'MLM1743', 'MLM1907', 'MLM126939', 'MLM126952', 'MLM92310',
         'MLM164000', 'MLM1542', 'MLM92277', 'MLM126094', 'MLM126988', 'MLM126940', 'MLM126993', 'MLM127008',
         'MLM163997', 'MLM164624', 'MLM126945', 'MLM126090', 'MLM127001', 'MLM9145', 'MLM1168', 'MLM92320', 'MLM126093',
         'MLM126095', 'MLM92285', 'MLM126105', 'MLM164006', 'MLM92299', 'MLM92319', 'MLM1545', 'MLM41706', 'MLM182155',
         'MLM126082', 'MLM1892', 'MLM50751', 'MLM50743', 'MLM182154', 'MLM1466', 'MLM1472', 'MLM1480', 'MLM1487',
         'MLM50722', 'MLM126941', 'MLM1496', 'MLM1468', 'MLM92307', 'MLM1785', 'MLM50753', 'MLM1493', 'MLM52797',
         'MLM126967', 'MLM1489', 'MLM9151', 'MLM163994', 'MLM163982', 'MLM163981', 'MLM126997', 'MLM9122', 'MLM459424',
         'MLM459256', 'MLM459374', 'MLM459440', 'MLM459483', 'MLM459506', 'MLM459464', 'MLM459465', 'MLM459352',
         'MLM459405', 'MLM459400', 'MLM459387', 'MLM50754', 'MLM459449', 'MLM458477', 'MLM459430', 'MLM459397',
         'MLM459517', 'MLM459533', 'MLM459513', 'MLM459419']
    csv_list = []
    for i in a:
        c = f"D:/data/看板/202512_sku_p_info_2/{i}.csv"
        csv_list.append(c)

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