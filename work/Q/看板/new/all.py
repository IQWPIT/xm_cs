import os
from dm.connector.mongo.manager3 import get_collection
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

success_lock = Lock()  # ✅ success_set / 日志写入锁

# ===================== Mongo 集合 =====================
visualize_table = get_collection("main_ml_mx", "ml_mx", "visualize_table")
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
_big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")

bi_site_data = get_collection("main_ml_mx", "ml_mx", "bi_category_data")

# ===================== 时间 & cat_id =====================
time_list = [
    # 202411,202412,202501,202502,202503,202504,202505,202506,202507,202508,202509,
    # 202510,
    # 202511,
    202512,
    202601,
    202602
]
SUCCESS_LOG = "all_2.txt"

def process_one(cat_id, t, collection, success_set):
    """
    单个 (cat_id, t) 任务
    """
    if (cat_id, t) in success_set:
        return "skip", cat_id, t

    try:
        fetch_and_compute_metrics(t, collection, cat_id)

        # ✅ 成功后写 success
        with success_lock:
            append_success(cat_id, t)
            success_set.add((cat_id, t))

        return "success", cat_id, t

    except Exception as e:
        return "fail", cat_id, t, str(e)

def load_success_set():
    s = set()
    if os.path.exists(SUCCESS_LOG):
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                cat, month = line.split(",")
                s.add((cat, int(month)))
    return s

def append_success(cat_id, month):
    with open(SUCCESS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{cat_id},{month}\n")


# ===================== 工具函数 =====================
def get_nested(d, key, default=0):
    cur = d
    for k in key.split("."):
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def to_round(v):
    return v if v is not None else 0

def to_percent_str(v):
    return v if v is not None else 0

def fetch_and_compute_metrics(t, collection, cat_id):
    doc = collection.find_one({"cat_id": cat_id}) or {}
    gmv_doc = visualize_table.find_one({"cat_id": cat_id}, {"monthly_sale_trend":1,"monthly_gmv_trend": 1, "_id": 0}) or {}

    gmv = gmv_doc.get("monthly_gmv_trend", {})
    order = gmv_doc.get("monthly_sale_trend", {})

    p = visual_plus.find_one({"cat_id": cat_id},
                             {"offersInf": 1, "_id": 0,
                              "stock_type":1,
                              # "stock_type_gmv":1,
                              "stock_type_avg_price":1}) or {}

    stock_type = p["stock_type"][f"{t}"]
    stock_type_ci = stock_type.get("ci")
    stock_type_nor = stock_type.get("nor")

    # stock_type_gmv = p["stock_type_gmv"][f"{t}"]
    stock_type_avg_price = p["stock_type_avg_price"].get(f"{t}", {})

    stock_type_avg_price_ci = stock_type_avg_price.get("ci")
    stock_type_avg_price_nor = stock_type_avg_price.get("nor")
    stock_type_avg_price_ful = stock_type_avg_price.get("ful")

    offers_inf = p.get("offersInf", {}).get(str(t), {})

    bi_doc = bi_site_data.find_one({"cat_id": cat_id, "month": str(t)}) or {}
    ctr = bi_doc.get("ctr", 0)
    cpc = bi_doc.get("cpc", 0)
    acos = bi_doc.get("acos", 0)
    refund_rate = bi_doc.get("refund_rate", 0)

    follow_order = get_nested(offers_inf, "follow.order", 0)
    non_follow_order = get_nested(offers_inf, "non_follow.order", 0)
    sum_order = follow_order + non_follow_order
    follow_p = follow_order / sum_order if sum_order else 0

    total_prod_num = doc.get("total_prod_num") or 1

    result_doc = {
        "time": t,  # 时间
        "cat_id": cat_id,  # cat_id

        # ===== Basic =====
        "total_sales": to_round(order.get(str(t), 0)),  # 全量_销量
        "total_gmv_usd": to_round(gmv.get(str(t), 0)),  # 全量_销售额(比索)
        "total_avg_price": to_round(gmv.get(str(t), 0)/order.get(str(t)) if order.get(str(t)) else 0),  # 全量_成交平均价
        "total_avg_product_sales": to_round(order.get(str(t), 0) / total_prod_num),  # 全量_商品平均销量
        "total_sales_growth_index": to_round(get_nested(doc, "increase_relative_ratio")),  # 全量_销量增长指数
        "full_stock_avg_price":stock_type_avg_price_ful,        #       全量FULL仓均价
        "local_seller_avg_price":stock_type_avg_price_nor,      #       全量本土自发货均价
        "crossborder_seller_avg_price":stock_type_avg_price_ci, #       全量跨境自发货均价
        "local_seller_sales":stock_type_nor,                    #       全量本土自发货销量
        "crossborder_seller_sales":stock_type_ci,               #       全量跨境自发货销量

        "active_product_count":to_round(get_nested(doc, "active_product_num.order30d")),   #    全量活跃商品数
        "local_seller_avg_sales":to_round(get_nested(doc, "stock_info.nor_avg_sales")),    #    全量本土自发货动销平均销量

        # ===== Ratios / Percentages =====
        "active_product_ratio": to_percent_str(get_nested(doc, "index_active")),  # 全量_活跃商品占比
        "follow_sales_ratio": to_percent_str(follow_p),  # 全量_跟卖销量占比
        "product_concentration": to_percent_str(get_nested(doc, "product_centralize_ratio.ratio")),  # 全量_产品集中度
        "brand_concentration": to_percent_str(get_nested(doc, "market_centralize_ratio.ratio")),  # 全量_品牌集中度
        "seller_concentration": to_percent_str(get_nested(doc, "seller_centralize_data.ratio")),  # 全量_店铺集中度

        # ===== Sellers & Brands =====
        "seller_count": to_round(get_nested(doc, "seller_info.seller_num")),  # 全量_店铺数
        "brand_count": to_round(get_nested(doc, "brand_num")),  # 全量_品牌数

        # ===== FULL Stock =====
        "full_stock_sales_ratio": to_percent_str(get_nested(doc, "stock_info.fbm_order30d_ratio")),  # 全量_FULL仓销量占比
        "full_stock_product_count": to_round(get_nested(doc, "stock_info.fbm_number")),  # 全量_FULL仓商品数
        "full_stock_product_ratio": to_percent_str(get_nested(doc, "stock_info.fbm_ratio")),  # 全量_FULL仓商品数量占比
        "full_stock_active_count": to_round(get_nested(doc, "stock_info.fbm_count30d")),  # 全量_FULL仓活跃商品数
        "full_stock_turnover_rate": to_percent_str(get_nested(doc, "stock_info.full_stock_turnover_rate")),
        # 全量_FULL仓动销率
        "full_stock_last30d_sales": to_round(get_nested(doc, "stock_info.fbm_order30d")),  # 全量_FULL仓近30天销量
        "full_stock_avg_sales": to_round(get_nested(doc, "stock_info.full_avg_sales")),  # 全量_FULL仓动销平均销量

        # ===== New Products =====
        "new_30d_count": to_round(get_nested(doc, "new_product_info.p30d_count30d")),  # 全量_近30天新品数量
        "new_30d_sales": to_round(get_nested(doc, "new_product_info.p30d_order30d")),  # 全量_近30天新品销量
        "new_30d_sales_ratio": to_percent_str(get_nested(doc, "new_product_info.p30d_order30d_ratio")),  # 全量_近30天新品销量占比
        "new_90d_count": to_round(get_nested(doc, "new_product_info.p90d_count30d")),  # 全量_近90天新品数量
        "new_90d_sales": to_round(get_nested(doc, "new_product_info.p90d_order30d")),  # 全量_近90天新品销量
        "new_90d_sales_ratio": to_percent_str(get_nested(doc, "new_product_info.p90d_order30d_ratio")),  # 全量_近90天新品销量占比
        "new_180d_count": to_round(get_nested(doc, "new_product_info.p180d_count30d")),  # 全量_近180天新品数量
        "new_180d_sales": to_round(get_nested(doc, "new_product_info.p180d_order30d")),  # 全量_近180天新品销量
        "new_180d_sales_ratio": to_percent_str(get_nested(doc, "new_product_info.p180d_order30d_ratio")),
        # 全量_近180天新品销量占比

        # ===== Ads & After-Sales =====
        "ad_click_rate": to_percent_str(ctr),  # 全量_广告点击率
        "avg_cpc_usd": to_round(cpc),  # 全量_平均单词点击广告费(美元)
        "avg_ad_cost_ratio": acos,  # 全量_平均广告销售成本比
        "avg_refund_rate": to_percent_str(refund_rate),  # 全量_平均退款率
    }
    _big_Excel.update_one(
        {"cat_id": cat_id,"time": t,},
        {"$set": result_doc},
        upsert=True
    )

    return result_doc

# ===================== 主流程 =====================
if __name__ == "__main__":
    cat_ids = [
        doc["cat_id"]
        for doc in visual_plus.find({}, {"cat_id": 1})
        if doc.get("cat_id")
    ]

    success_set = load_success_set()
    print(f"总类目数: {len(cat_ids)}  已成功处理记录数: {len(success_set)}")

    MAX_WORKERS = 8

    for t in time_list:
        print(f"\n===== Processing {t} =====")

        collection_name = f"visualize_table_bak_{t}28"
        collection = get_collection("main_ml_mx", "ml_mx", collection_name)

        tasks = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for cat_id in cat_ids:
                if (cat_id, t) in success_set:
                    continue

                tasks.append(
                    executor.submit(process_one, cat_id, t, collection, success_set)
                )

            for future in as_completed(tasks):
                res = future.result()

                if res[0] == "fail":
                    print(f"❌ 失败: {res[1]}, {res[2]}, {res[3]}")
