import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
import os
import ast
from pathlib import Path
from tqdm import tqdm
import warnings
from collections import defaultdict

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")

# ===============================
# 配置路径
# ===============================
time = 2511 # monthly_sale_trend 取值
data_folder = Path(r"D:\data\看板\202512_sku_p_info_2")
bins_folder = Path(r"D:/data/看板/range_price")
log_file = Path(rf"D:\data\看板\日志\top10_brand_range_price_{time}.txt")
error_log = Path(rf"D:\data\看板\日志\top10_brand_range_price_errors_{time}.txt")
csv_files = list(data_folder.glob("*.csv"))
file_names = [f.stem for f in csv_files]
file_names = ["MLM73299"]

# 读取已处理文件
if log_file.exists():
    with open(log_file, "r", encoding="utf-8") as f:
        processed_files = set(line.strip() for line in f)
else:
    processed_files = set()

# 打开错误日志（追加）
def log_error(msg):
    with open(error_log, "a", encoding="utf-8") as ef:
        ef.write(f"{datetime.now().isoformat()} {msg}\n")

# ===============================
# 功能：安全转换 monthly_sale_trend
# ===============================
def safe_dict(x):
    if isinstance(x, dict):
        return x
    elif isinstance(x, str) and x.strip():
        try:
            return ast.literal_eval(x)
        except Exception:
            return {}
    else:
        return {}

# ===============================
# 功能：品牌价格段统计（与之前保持一致）
# ===============================
def process_brand(df_brand, bins_right):
    df_brand = df_brand.copy()
    if df_brand.empty:
        return {'price': 0, 'range_price': {}}

    # 如果 active_price 全为 NaN，直接返回
    if df_brand['active_price'].dropna().empty:
        return {'price': 0, 'range_price': {}}

    avg_price = float(df_brand['active_price'].mean())
    bins_for_cut = np.concatenate([[0], bins_right])
    df_brand.loc[:, 'price_segment'] = pd.cut(df_brand['active_price'], bins=bins_for_cut, right=False)

    seg_group = df_brand.groupby('price_segment', observed=False).agg(
        order=('total_sale', 'sum'),
        gmv=('gmv', 'sum')
    ).sort_values('order', ascending=False)

    range_price_dict = seg_group.head(10).to_dict(orient='index')
    if len(seg_group) > 10:
        others_order = int(seg_group['order'].iloc[10:].sum())
        others_gmv = float(seg_group['gmv'].iloc[10:].sum())
        range_price_dict['others'] = {'order': others_order, 'gmv': others_gmv}

    # 转字符串 key，避免 nan
    range_price_dict_fmt = {}
    for k, v in range_price_dict.items():
        if isinstance(k, pd.Interval):
            key_str = f"{k.right:.2f}"
        else:
            key_str = str(k)
        if key_str.lower() == 'nan':
            continue
        range_price_dict_fmt[key_str] = {'order': int(v['order']), 'gmv': float(round(v['gmv'], 2))}

    return {'price': round(avg_price, 2), 'range_price': range_price_dict_fmt}

# ===============================
# 功能：stock_type × 店铺 Top10（接受 df_sellers）
# ===============================
def get_top10_seller_by_month_stocktype(df):
    if df is None or df.empty:
        return {}
    rows = []
    for _, r in df.iterrows():
        trend = r.get("monthly_sale_trend", {})
        if isinstance(trend, dict):
            try:
                total_sale = sum(trend.values())
            except Exception:
                total_sale = 0
            rows.append({
                "stock_type": r.get("stock_type", ""),
                "sellerName": r.get("sellerName", ""),
                "sale": total_sale
            })

    df2 = pd.DataFrame(rows)
    if df2.empty:
        return {}

    grouped = df2.groupby(["stock_type", "sellerName"], as_index=False).agg(
        total_sale=("sale", "sum")
    )

    result = {}
    for st, df_st in grouped.groupby("stock_type"):
        df_top10 = df_st.sort_values("total_sale", ascending=False).head(10)
        result[str(st)] = {str(row["sellerName"]): {"order": int(row["total_sale"])} for _, row in df_top10.iterrows()}
    return result

# ===============================
# CSV 块读取 + 字节进度条
# ===============================
def read_csv_in_chunks(csv_file, chunksize=200000):
    total_bytes = os.path.getsize(csv_file)
    pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc=f"Reading {csv_file.name}")
    try:
        for chunk in pd.read_csv(csv_file, chunksize=chunksize, iterator=True):
            chunk_bytes = chunk.memory_usage(deep=True).sum()
            pbar.update(chunk_bytes)
            yield chunk
    except Exception as e:
        log_error(f"Failed reading chunks for {csv_file.name}: {repr(e)}")
    finally:
        pbar.close()

# ===============================
# 主流程：分块处理并跳过问题行/块
# ===============================
for cat_id in tqdm(file_names, desc="Processing files"):
    if cat_id in processed_files:
        print(f"跳过已处理文件: {cat_id}")
        continue

    csv_file = data_folder / f"{cat_id}.csv"
    if not csv_file.exists():
        log_error(f"File not found: {csv_file}")
        continue

    print("处理文件:", csv_file.name)

    brand_data_list = []
    top10_seller_rows = []

    # 逐 chunk 处理
    for chunk_idx, chunk in enumerate(read_csv_in_chunks(csv_file)):
        # 检查必需列
        required_cols = {'brand', 'active_price', 'monthly_sale_trend', 'stock_type', 'sellerName'}
        missing = required_cols - set(chunk.columns)
        if missing:
            # 如果某些列缺失，记录并跳过这个 chunk（你要求“这些错误跳过”）
            log_error(f"{csv_file.name} chunk {chunk_idx}: missing columns {missing}, skipping chunk")
            continue

        # 转换 monthly_sale_trend（每行安全转换）
        try:
            chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].apply(safe_dict)
        except Exception as e:
            log_error(f"{csv_file.name} chunk {chunk_idx}: safe_dict apply failed: {repr(e)}")
            # 尝试逐行处理，跳过无法解析的行
            fixed_rows = []
            for i, val in enumerate(chunk['monthly_sale_trend'].values):
                try:
                    fixed_rows.append(safe_dict(val))
                except Exception as e2:
                    log_error(f"{csv_file.name} chunk {chunk_idx} row {i}: safe_dict failed: {repr(e2)}")
                    fixed_rows.append({})
            chunk['monthly_sale_trend'] = fixed_rows

        # 计算 total_sale 与 gmv（如果 active_price 或 monthly_sale_trend 有异常，逐行处理并跳过出错行）
        total_sales = []
        gmvs = []
        brands = []
        active_prices = []
        seller_rows_to_add = []

        for row_idx, row in chunk.iterrows():
            try:
                monthly = row['monthly_sale_trend'] if isinstance(row['monthly_sale_trend'], dict) else {}
                # total_sale 取指定 month key（time）
                total_sale = int(monthly.get(f"{time}", 0)) if monthly else 0
                active_price = float(row['active_price']) if pd.notna(row['active_price']) else 0.0
                gmv = total_sale * active_price

                total_sales.append(total_sale)
                gmvs.append(gmv)
                brands.append(row.get('brand', None))
                active_prices.append(active_price)

                # 累计用于 top10 seller（保留 monthly dict 以便 get_top10）
                try:
                    # ensure we store a safe copy
                    seller_rows_to_add.append({
                        "stock_type": row.get("stock_type", "") or "",
                        "sellerName": row.get("sellerName", "") or "",
                        "monthly_sale_trend": monthly
                    })
                except Exception as e:
                    log_error(f"{csv_file.name} chunk {chunk_idx} row {row_idx}: seller row pack failed: {repr(e)}")
            except Exception as e:
                # 跳过单行并记录
                log_error(f"{csv_file.name} chunk {chunk_idx} row {row_idx}: row processing failed: {repr(e)}")
                continue

        # 如果没有成功解析到任何行，跳过
        if len(total_sales) == 0:
            log_error(f"{csv_file.name} chunk {chunk_idx}: no valid rows parsed, skipping chunk")
            continue

        # 构建精简 DataFrame 用于品牌累积（避免保存整个 chunk）
        df_small = pd.DataFrame({
            'brand': brands,
            'total_sale': total_sales,
            'gmv': gmvs,
            'active_price': active_prices
        })

        brand_data_list.append(df_small)
        top10_seller_rows.extend(seller_rows_to_add)

    # 如果完全没有品牌数据，记录并跳过文件
    if not brand_data_list:
        log_error(f"{csv_file.name}: no brand data collected for file, skipping file")
        # 仍记录该文件为已处理以避免反复尝试（可根据需要改）
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(cat_id + "\n")
        continue

    # 汇总品牌数据（使用逐步聚合，避免一次性大 concat）
    try:
        df_brands = pd.concat(brand_data_list, ignore_index=True)
    except Exception as e:
        log_error(f"{csv_file.name}: concat brand_data_list failed: {repr(e)}")
        # 尝试逐步 concat 少量块
        tmp = []
        for part in brand_data_list:
            try:
                tmp.append(part)
            except Exception as e2:
                log_error(f"{csv_file.name}: skipping a bad brand part: {repr(e2)}")
        if tmp:
            df_brands = pd.concat(tmp, ignore_index=True)
        else:
            log_error(f"{csv_file.name}: no usable brand parts, skipping file")
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(cat_id + "\n")
            continue

    # 汇总品牌统计
    try:
        brand_group = df_brands.groupby('brand', dropna=True).agg(
            sale_total=('total_sale', 'sum'),
            gmv_total=('gmv', 'sum')
        ).sort_values('sale_total', ascending=False)
    except Exception as e:
        log_error(f"{csv_file.name}: brand_group aggregation failed: {repr(e)} - skipping file")
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(cat_id + "\n")
        continue

    # top10 brands（过滤 nan）
    top10_brands = [str(b) for b in brand_group.index[:10] if pd.notna(b)]
    df_top10 = df_brands[df_brands['brand'].isin(top10_brands)]
    df_others = df_brands[~df_brands['brand'].isin(top10_brands)]

    # top10 seller 聚合（采用安全函数）
    try:
        df_sellers = pd.DataFrame(top10_seller_rows)
        top10_seller_dict = get_top10_seller_by_month_stocktype(df_sellers)
    except Exception as e:
        log_error(f"{csv_file.name}: get_top10_seller failed: {repr(e)}")
        top10_seller_dict = {}

    # 生成或加载价格段
    bins_file = bins_folder / f"{csv_file.stem}.npy"
    try:
        if bins_file.exists():
            bins_right = np.load(bins_file)
            print("加载已存在价格段:", bins_file)
        else:
            # 若 df_top10 或 df_others 没有 active_price，跳过生成，设置默认 bins_right
            prices_for_bins = []
            if 'active_price' in df_top10.columns:
                prices_for_bins.extend(df_top10['active_price'].dropna().tolist())
            if 'active_price' in df_others.columns:
                prices_for_bins.extend(df_others['active_price'].dropna().tolist())

            if not prices_for_bins:
                # 没有价格数据时使用默认单一段
                bins_right = np.array([999999.0])
                log_error(f"{csv_file.name}: no prices for bins, using default bins_right")
            else:
                all_prices_arr = np.array(prices_for_bins)
                quantiles = np.linspace(0, 1, 11)
                bins = np.quantile(all_prices_arr, quantiles)
                bins_floor = np.floor(bins / 100) * 100
                bins_ceil = np.ceil(bins / 100) * 100
                bins_rounded = bins_floor.copy()
                bins_rounded[-1] = bins_ceil[-1]
                bins_rounded[0] = 0.001
                bins_rounded = np.unique(bins_rounded)
                bins_right = bins_rounded[1:]
                os.makedirs(bins_folder, exist_ok=True)
                np.save(bins_file, bins_right)
                print("生成价格段文件:", bins_file)
    except Exception as e:
        log_error(f"{csv_file.name}: bins generation/loading failed: {repr(e)}")
        bins_right = np.array([999999.0])

    # 构建品牌输出（确保 key 为字符串且跳过 nan 品牌）
    output_dict = {}
    for brand in top10_brands:
        try:
            df_brand = df_top10[df_top10['brand'] == brand]
            if df_brand.empty:
                continue
            output_dict[str(brand)] = process_brand(df_brand, bins_right)
        except Exception as e:
            log_error(f"{csv_file.name}: process_brand failed for brand {brand}: {repr(e)}")
            continue

    # others
    try:
        output_dict['others'] = process_brand(df_others, bins_right)
    except Exception as e:
        log_error(f"{csv_file.name}: process_brand failed for others: {repr(e)}")
        output_dict['others'] = {'price': 0, 'range_price': {}}

    # 写入 MongoDB（保护性转换：所有 key 都转成字符串并把 numpy 类型转成 python 类型）
    try:
        # convert nested numpy types to python builtins
        def convert(obj):
            if isinstance(obj, dict):
                return {str(k): convert(v) for k, v in obj.items() if str(k).lower() != 'nan'}
            elif isinstance(obj, list):
                return [convert(i) for i in obj]
            elif isinstance(obj, np.generic):
                return obj.item()
            else:
                return obj

        safe_output = convert(output_dict)
        safe_seller = convert(top10_seller_dict)

        visual_plus.update_one(
            {"cat_id": str(csv_file.stem)},
            {
                "$set": {
                    f"top10_brand_range_price.20{time}": safe_output,
                    f"top10_sellername_stock_type.20{time}": safe_seller
                }
            },
            upsert=True
        )
        print(f"已写入 MongoDB: {csv_file.stem}")
    except Exception as e:
        log_error(f"{csv_file.name}: mongo update failed: {repr(e)}")
        # 仍把文件写入已处理，避免重复失败循环（如果不想这样可移除）
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(cat_id + "\n")
        continue

    # 写入已处理日志
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(cat_id + "\n")

print("所有 CSV 处理完成！")
