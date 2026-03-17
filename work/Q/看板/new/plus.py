import math
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
import os
from dm.connector.mongo.manager3 import get_collection
from tqdm import tqdm
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

TIMES_LIST = [
    # 202601, 202512, 202511, 202510, 202509, 202508, 202507,
    # 202506, 202505, 202504, 202503, 202502, 202501, 202412,
    202602
]

# ----------------------------------------------------------------------------------------------------------------#
# 计算价格段函数（保持原逻辑不变）
# ----------------------------------------------------------------------------------------------------------------#
def get_price_segment(cat_id, price_scatter_data, step=100, mongo_db="main_ml_mx", mongo_coll="price_segment"):
    # 在子进程中创建 collection
    from dm.connector.mongo.manager3 import get_collection
    price_seg_col = get_collection(mongo_db, "ml_mx", mongo_coll)

    saved_doc = price_seg_col.find_one({"cat_id": cat_id})
    if saved_doc:
        range_list = saved_doc["price_ranges"]
    else:
        df = pd.DataFrame([{
            "price": v["price"],
            "order": v["order30d"],
            "gmv": v["gmv30d"],
            "count": v["count30d"]
        } for v in price_scatter_data.values()]).sort_values("price").reset_index(drop=True)

        total_count = df["count"].sum()
        thresholds = [total_count * i / 10 for i in range(1, 11)]
        acc, seg, seg_list = 0, 1, []
        for c in df["count"]:
            acc += c
            while seg <= 10 and acc > thresholds[seg - 1]:
                seg += 1
            seg_list.append(min(seg, 10))
        df["price_segment"] = seg_list

        result = df.groupby("price_segment", as_index=False).agg(
            price_min=("price", "min"),
            price_max=("price", "max"),
            order=("order", "sum"),
            gmv=("gmv", "sum"),
            count=("count", "sum")
        )

        range_list = []
        prev_max = None
        for _, row in result.iterrows():
            min_p = prev_max if prev_max is not None else int(math.floor(row.price_min / step) * step)
            max_p = max(min_p + step, int(math.ceil(row.price_max / step) * step))
            range_list.append(f"{min_p}-{max_p}")
            prev_max = max_p

        price_seg_col.update_one({"cat_id": cat_id}, {"$set": {"price_ranges": range_list}}, upsert=True)

    # 映射价格段
    df = pd.DataFrame([{
        "price": v["price"],
        "order": v["order30d"],
        "gmv": v["gmv30d"],
        "count": v["count30d"]
    } for v in price_scatter_data.values()]).sort_values("price").reset_index(drop=True)

    def map_price(price, segments):
        for i, seg in enumerate(segments):
            min_p, max_p = map(int, seg.split('-'))
            if min_p <= price < max_p or (i == len(segments) - 1 and price <= max_p):
                return i
        return len(segments) - 1

    df["seg_idx"] = df["price"].apply(lambda x: map_price(x, range_list))
    grouped = df.groupby("seg_idx", as_index=False).agg(
        order=("order", "sum"),
        gmv=("gmv", "sum"),
        count=("count", "sum")
    )
    range_data_dict = {}
    for idx, row in grouped.iterrows():
        max_p = int(range_list[int(row["seg_idx"])].split('-')[1])
        range_data_dict[str(max_p)] = {
            "order": int(row["order"]),
            "gmv": float(row["gmv"]),
            "count": int(row["count"])
        }
    return range_list, range_data_dict

# ----------------------------------------------------------------------------------------------------------------#
# 日期处理函数
# ----------------------------------------------------------------------------------------------------------------#
def get_prev_month_and_last_year(time: int):
    dt = datetime.strptime(str(time), "%Y%m")
    prev_month = int((dt - relativedelta(months=1)).strftime("%Y%m"))
    last_year = int((dt - relativedelta(years=1)).strftime("%Y%m"))
    return prev_month, last_year

def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]

def safe_div(a, b):
    return a / b if b != 0 else 0

# ----------------------------------------------------------------------------------------------------------------#
# 工具
# ----------------------------------------------------------------------------------------------------------------#
def process_cat(cat_dict):
    """每个进程处理一个类目"""
    cat_id = cat_dict.get("cat_id")
    if not cat_id:
        return
    for t in TIMES_LIST:
        process_time(cat_id, t)
# ----------------------------------------------------------------------------------------------------------------#
# 单个时间点处理函数（用于多进程）
# ----------------------------------------------------------------------------------------------------------------#
def process_time(visual_plus_cat_id, time):
    try:
        # 子进程内创建 MongoCollection
        visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
        visualize_table_col = get_collection("main_ml_mx", "ml_mx", "visualize_table")

        visualize_table_datas = visualize_table_col.find_one({"cat_id": visual_plus_cat_id})
        if not visualize_table_datas:
            print(f"{visual_plus_cat_id}-{time}: visualize_table_datas missing")
            return

        prev_month, last_year = get_prev_month_and_last_year(time)
        year, month = divmod(time, 100)
        data_sum = days_in_month(year, month)

        # 安全获取集合数据
        def get_safe_collection(name):
            col = get_collection("main_ml_mx", "ml_mx", name)
            return col.find_one({"cat_id": visual_plus_cat_id}) or {}

        visualize_table_time = get_safe_collection(f"visualize_table_bak_{time}28")
        visualize_table_time_prev = get_safe_collection(f"visualize_table_bak_{prev_month}28")
        visualize_table_time_last = get_safe_collection(f"visualize_table_bak_{last_year}28")

        monthly_sale_trend = visualize_table_datas.get("monthly_sale_trend", {}).get(str(time))
        if monthly_sale_trend is None:
            print(f"{visual_plus_cat_id}-{time}: monthly_sale_trend missing")
            return

        monthly_sale_trend_prev = visualize_table_datas.get("monthly_sale_trend", {}).get(str(prev_month), 1)
        monthly_sale_trend_last = visualize_table_datas.get("monthly_sale_trend", {}).get(str(last_year), 1)
        order = {
            "monthly": monthly_sale_trend,
            "average_day": monthly_sale_trend / data_sum,
            "year": safe_div(monthly_sale_trend, monthly_sale_trend_prev),
            "chain": safe_div(monthly_sale_trend, monthly_sale_trend_last)
        }

        # stock_info
        stock_info = visualize_table_time.get("stock_info", {})
        stock_info_prev = visualize_table_time_prev.get("stock_info", {})
        stock_info_last = visualize_table_time_last.get("stock_info", {})

        fbm_order30d = stock_info.get("fbm_order30d", 0)
        nor_order30d = stock_info.get("nor_order30d", 0)
        ci_order30d = stock_info.get("ci_order30d", 0)
        stock_type = {"ful": fbm_order30d, "nor": nor_order30d, "ci": ci_order30d}

        fbm_gmv30d = stock_info.get("fbm_gmv30d", 0)
        nor_gmv30d = stock_info.get("nor_gmv30d", 0)
        ci_gmv30d = stock_info.get("ci_gmv30d", 0)
        all_gmv30d = fbm_gmv30d + nor_gmv30d + ci_gmv30d

        fbm_gmv30d_prev = stock_info_prev.get("fbm_gmv30d", 0)
        nor_gmv30d_prev = stock_info_prev.get("nor_gmv30d", 0)
        ci_gmv30d_prev = stock_info_prev.get("ci_gmv30d", 0)
        all_gmv30d_prev = fbm_gmv30d_prev + nor_gmv30d_prev + ci_gmv30d_prev

        fbm_gmv30d_last = stock_info_last.get("fbm_gmv30d", 0)
        nor_gmv30d_last = stock_info_last.get("nor_gmv30d", 0)
        ci_gmv30d_last = stock_info_last.get("ci_gmv30d", 0)
        all_gmv30d_last = fbm_gmv30d_last + nor_gmv30d_last + ci_gmv30d_last

        gmv = {
            "monthly": all_gmv30d,
            "average_day": all_gmv30d / data_sum,
            "year": safe_div(all_gmv30d, all_gmv30d_prev),
            "chain": safe_div(all_gmv30d, all_gmv30d_last)
        }

        avg_price = visualize_table_time.get("avg_price", 0)
        avg_price_prev = visualize_table_time_prev.get("avg_price", 1)
        avg_price_last = visualize_table_time_last.get("avg_price", 1)
        price = {
            "monthly": avg_price,
            "average_day": avg_price,
            "year": safe_div(avg_price, avg_price_prev),
            "chain": safe_div(avg_price, avg_price_last)
        }

        stock_type_gmv = {"ful": fbm_gmv30d, "nor": nor_gmv30d, "ci": ci_gmv30d}
        stock_type_avg_price = {
            "ful": safe_div(fbm_gmv30d, fbm_order30d),
            "nor": safe_div(nor_gmv30d, nor_order30d),
            "ci": safe_div(ci_gmv30d, ci_order30d)
        }

        seller_concentration = visualize_table_time.get("seller_centralize_data", {}).get("ratio", 0)
        brand_concentration = visualize_table_time.get("market_centralize_ratio", {}).get("ratio", 0)
        new_30d_sales_ratio = visualize_table_time.get("new_product_info", {}).get("p30d_order30d", 0)
        new_90d_sales_ratio = visualize_table_time.get("new_product_info", {}).get("p90d_order30d", 0)
        new_180d_sales_ratio = visualize_table_time.get("new_product_info", {}).get("p180d_order30d", 0)
        top10 = visualize_table_time.get("competition_brands", [])[::10]
        top10_brand = {"order": {}, "gmv": {}, "avg_price": {}, "count": {}}
        for brand in top10:
            top10_brand["order"][brand["brand"]] = brand["order30d"]
            top10_brand["gmv"][brand["brand"]] = brand["gmv30d"]
            top10_brand["avg_price"][brand["brand"]] = brand["avg_price"]
            top10_brand["count"][brand["brand"]] = brand["count30d"]

        price_scatter_data = visualize_table_time.get("price_scatter_data", {})
        range_list, range_dict = get_price_segment(visual_plus_cat_id, price_scatter_data, step=100)

        visual_plus.update_one(
            {"cat_id": visual_plus_cat_id},
            {"$set": {
                f"order.{time}": order,
                f"gmv.{time}": gmv,
                f"price.{time}": price,
                f"stock_type_avg_price.{time}": stock_type_avg_price,
                f"stock_type.{time}": stock_type,
                f"stock_type_gmv.{time}": stock_type_gmv,
                f"seller_concentration.{time}": seller_concentration,
                f"brand_concentration.{time}": brand_concentration,
                f"new_30d_sales_ratio.{time}": new_30d_sales_ratio,
                f"new_90d_sales_ratio.{time}": new_90d_sales_ratio,
                f"new_180d_sales_ratio.{time}": new_180d_sales_ratio,
                f"range_price.{time}": range_dict,
                f"top10_brand.{time}": top10_brand,
            }},
            upsert=True
        )

    except Exception:
        print(f"Error processing {visual_plus_cat_id}-{time}")
        traceback.print_exc()

# ----------------------------------------------------------------------------------------------------------------#
# 主函数，多进程执行每个时间点
# ----------------------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    import multiprocessing
    multiprocessing.set_start_method("spawn")  # ⚡ 避免 fork 问题

    # 获取所有类目 ID
    visualize_table_col = get_collection("main_ml_mx", "ml_mx", "visualize_table")
    visual_plus_cat_ids = list(visualize_table_col.find({}, {"cat_id": 1}))

    # 按类目多进程
    with ProcessPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(process_cat, d) for d in visual_plus_cat_ids]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing categories"):
            f.result()  # 捕获异常
