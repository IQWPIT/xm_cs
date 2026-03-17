import pandas as pd
import ast
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

def safe_eval(x, default):
    if pd.isna(x) or x in ("", "nan"):
        return default
    try:
        return ast.literal_eval(x)
    except:
        return default

# ---------------- CSV 处理 worker ----------------
def process_csv_worker(args):
    file_path, times_list, time_map = args
    file_path = Path(file_path)
    cat_id = file_path.stem

    chunksize = 500000
    # 按 month -> brand 聚合
    month_brand_dict = {t: {} for t in times_list}

    for chunk in pd.read_csv(file_path, low_memory=False, chunksize=chunksize):
        # 基础字段
        chunk["monthly_sale_trend"] = chunk.get(
            "monthly_sale_trend",
            pd.Series([{}]*len(chunk))
        ).apply(lambda x: safe_eval(x, {}))
        chunk["brand"] = chunk.get("brand", pd.Series(["未知品牌"]*len(chunk))).fillna("未知品牌")
        chunk["active_price"] = chunk.get("active_price", pd.Series([0]*len(chunk)))

        for t in times_list:
            time, _, _ = time_map[t]
            chunk["sale"] = chunk["monthly_sale_trend"].apply(lambda x: x.get(f"{time}", 0))
            chunk["gmv"] = chunk["sale"] * chunk["active_price"]

            # 按品牌累计销量
            for brand, group in chunk.groupby("brand"):
                sale_sum = group["sale"].sum()
                price_sum = group["gmv"].sum()
                if brand in month_brand_dict[t]:
                    month_brand_dict[t][brand]["sale"] += sale_sum
                    month_brand_dict[t][brand]["gmv"] += price_sum
                else:
                    month_brand_dict[t][brand] = {"sale": sale_sum, "gmv": price_sum}

    # 处理 top10
    result_rows = []
    for t in times_list:
        brand_data = pd.DataFrame.from_dict(month_brand_dict[t], orient="index").reset_index()
        brand_data = brand_data.rename(columns={"index": "brand"})
        top10 = brand_data.sort_values("sale", ascending=False).head(10)
        top10["month"] = t
        result_rows.append(top10[["month", "brand", "sale", "gmv"]])

    df_result = pd.concat(result_rows, ignore_index=True)
    return cat_id, df_result

# ---------------- 导出 Excel ----------------
def export_all_categories_excel(results, save_path):
    with pd.ExcelWriter(save_path, engine="openpyxl") as writer:
        for cat_id, df in results:
            df.to_excel(writer, sheet_name=cat_id, index=False)
    print(f"Excel 已保存 → {save_path}")

# ---------------- 主程序 ----------------
def main():
    folder_path = Path(r"D:\任务")
    folder_path.mkdir(parents=True, exist_ok=True)

    times_list = [
        202510, 202509, 202508, 202507, 202506,
        202505, 202504, 202503, 202502, 202501,
        202412, 202411, 202410
    ]

    time_map = {
        202510: (2510, 2509, 2410),
        202509: (2509, 2508, 2409),
        202508: (2508, 2507, 2408),
        202507: (2507, 2506, 2407),
        202506: (2506, 2505, 2406),
        202505: (2505, 2504, 2405),
        202504: (2504, 2503, 2404),
        202503: (2503, 2502, 2403),
        202502: (2502, 2501, 2402),
        202501: (2501, 2412, 2401),
        202412: (2412, 2411, 2312),
        202411: (2411, 2410, 2311),
        202410: (2410, 2409, 2310)
    }

    csv_list = [
        "D:/data/临时/20251205_sku_p_info_2/MLB1646.csv",
        "D:/data/临时/20251205_sku_p_info_2/MLB72503.csv",
        # "D:/data/看板/202512_sku_p_info_2/MLM27576.csv",
        # "D:/data/看板/202512_sku_p_info_2/MLM1002.csv"
    ]
    args_list = [(str(f), times_list, time_map) for f in csv_list]

    results = []
    with ProcessPoolExecutor(max_workers=4) as exe:
        for res in tqdm(exe.map(process_csv_worker, args_list), total=len(args_list)):
            if res:
                results.append(res)

    save_path = folder_path / "top10_brand_summary.xlsx"
    export_all_categories_excel(results, save_path)

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
