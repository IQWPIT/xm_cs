import os
import calendar
from datetime import datetime, timedelta
from collections import defaultdict
import heapq
from pprint import pprint
# =============================
# 环境变量 & 集合
# =============================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus_cx = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
cat_id = "MLM437510"
month = "202601"
report = visual_plus_cx.find_one({"cat_id":cat_id})
def import_big_Excel(cat_id,report,month):
    #25年GMV增长率
    gmv_growth_order_ratio = report["gmv_growth"]["2025"]["order_ratio"]
    #top10商品销量 top10商品销量占比
    top10_commodity_sku_id_order = report["top10_commodity"][month]
    top10_commodity_sku_id_order_sum = 0
    top10_commodity_sku_id_order_sum_ratio = 0
    for sku_id,orders in top10_commodity_sku_id_order.items():
        a = orders["order"]
        b = orders["order_ratio"]
        top10_commodity_sku_id_order_sum += a
        top10_commodity_sku_id_order_sum_ratio += b
    #top1店铺销量占比 top3店铺销量占比
    top3_seller_sales_order_ratio_data = report["top10_seller_sales"][month][month]
    top3_seller_sales_order_ratio_sum_ratio = 0
    top1_seller_sales_order_ratio_sum_ratio = 0
    i = 0
    for seller_sales,orders in top3_seller_sales_order_ratio_data.items():
        i = i + 1
        if i == 1:
            top1_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top3_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
        else:
            top3_seller_sales_order_ratio_sum_ratio += orders["order_ratio"]
            if i>3:
                break
    # top品牌销量占比
    top10_brand_sales_order_ratio_data = report["top10_brand_sales"][month][month]
    top1_brand_sales_order_ratio_sum_ratio = 0
    top1_brand_sales_order_ratio_name = ""
    top3_brand_sales_order_ratio_sum_ratio = 0
    i = 0
    for brand_sales,orders in top10_brand_sales_order_ratio_data.items():
        i = i + 1
        if i == 1:
            top1_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top3_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            top1_brand_sales_order_ratio_name = brand_sales
        else:
            top3_brand_sales_order_ratio_sum_ratio += orders["order_ratio"]
            if i>3:
                break
    # top50新品数据
    top50_sku_new_stock_stats_ci_new30_count = report["top50_sku_new_stock_stats"][month]["ci"]["new30_count"]
    top50_sku_new_stock_stats_ci_new90_count = report["top50_sku_new_stock_stats"][month]["ci"]["new90_count"]
    top50_sku_new_stock_stats_ci_new180_count = report["top50_sku_new_stock_stats"][month]["ci"]["new180_count"]
    top50_sku_new_stock_stats_ci_new30_order = report["top50_sku_new_stock_stats"][month]["ci"]["new30_order"]
    top50_sku_new_stock_stats_ci_new90_order = report["top50_sku_new_stock_stats"][month]["ci"]["new90_order"]
    top50_sku_new_stock_stats_ci_new180_order = report["top50_sku_new_stock_stats"][month]["ci"]["new180_order"]
    top50_sku_new_stock_stats_ci_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new30_order_ratio"]
    top50_sku_new_stock_stats_ci_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new90_order_ratio"]
    top50_sku_new_stock_stats_ci_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["ci"]["new180_order_ratio"]

    top50_sku_new_stock_stats_ful_new30_count = report["top50_sku_new_stock_stats"][month]["ful"]["new30_count"]
    top50_sku_new_stock_stats_ful_new90_count = report["top50_sku_new_stock_stats"][month]["ful"]["new90_count"]
    top50_sku_new_stock_stats_ful_new180_count = report["top50_sku_new_stock_stats"][month]["ful"]["new180_count"]
    top50_sku_new_stock_stats_ful_new30_order = report["top50_sku_new_stock_stats"][month]["ful"]["new30_order"]
    top50_sku_new_stock_stats_ful_new90_order = report["top50_sku_new_stock_stats"][month]["ful"]["new90_order"]
    top50_sku_new_stock_stats_ful_new180_order = report["top50_sku_new_stock_stats"][month]["ful"]["new180_order"]
    top50_sku_new_stock_stats_ful_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new30_order_ratio"]
    top50_sku_new_stock_stats_ful_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new90_order_ratio"]
    top50_sku_new_stock_stats_ful_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["ful"][
        "new180_order_ratio"]


    top50_sku_new_stock_stats_nor_new30_count = report["top50_sku_new_stock_stats"][month]["nor"]["new30_count"]
    top50_sku_new_stock_stats_nor_new90_count = report["top50_sku_new_stock_stats"][month]["nor"]["new90_count"]
    top50_sku_new_stock_stats_nor_new180_count = report["top50_sku_new_stock_stats"][month]["nor"]["new180_count"]
    top50_sku_new_stock_stats_nor_new30_order = report["top50_sku_new_stock_stats"][month]["nor"]["new30_order"]
    top50_sku_new_stock_stats_nor_new90_order = report["top50_sku_new_stock_stats"][month]["nor"]["new90_order"]
    top50_sku_new_stock_stats_nor_new180_order = report["top50_sku_new_stock_stats"][month]["nor"]["new180_order"]
    top50_sku_new_stock_stats_nor_new30_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new30_order_ratio"]
    top50_sku_new_stock_stats_nor_new90_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new90_order_ratio"]
    top50_sku_new_stock_stats_nor_new180_order_ratio = report["top50_sku_new_stock_stats"][month]["nor"]["new180_order_ratio"]

    # all新品数据
    all_new_stock_stats_ci_new30_count = report["all_new_stock_stats"][month]["ci"]["new30_count"]
    all_new_stock_stats_ci_new90_count = report["all_new_stock_stats"][month]["ci"]["new90_count"]
    all_new_stock_stats_ci_new180_count = report["all_new_stock_stats"][month]["ci"]["new180_count"]
    all_new_stock_stats_ci_new30_order = report["all_new_stock_stats"][month]["ci"]["new30_order"]
    all_new_stock_stats_ci_new90_order = report["all_new_stock_stats"][month]["ci"]["new90_order"]
    all_new_stock_stats_ci_new180_order = report["all_new_stock_stats"][month]["ci"]["new180_order"]
    all_new_stock_stats_ci_new30_order_ratio = report["all_new_stock_stats"][month]["ci"]["new30_order_ratio"]
    all_new_stock_stats_ci_new90_order_ratio = report["all_new_stock_stats"][month]["ci"]["new90_order_ratio"]
    all_new_stock_stats_ci_new180_order_ratio = report["all_new_stock_stats"][month]["ci"]["new180_order_ratio"]

    all_new_stock_stats_ful_new30_count = report["all_new_stock_stats"][month]["ful"]["new30_count"]
    all_new_stock_stats_ful_new90_count = report["all_new_stock_stats"][month]["ful"]["new90_count"]
    all_new_stock_stats_ful_new180_count = report["all_new_stock_stats"][month]["ful"]["new180_count"]
    all_new_stock_stats_ful_new30_order = report["all_new_stock_stats"][month]["ful"]["new30_order"]
    all_new_stock_stats_ful_new90_order = report["all_new_stock_stats"][month]["ful"]["new90_order"]
    all_new_stock_stats_ful_new180_order = report["all_new_stock_stats"][month]["ful"]["new180_order"]
    all_new_stock_stats_ful_new30_order_ratio = report["all_new_stock_stats"][month]["ful"]["new30_order_ratio"]
    all_new_stock_stats_ful_new90_order_ratio = report["all_new_stock_stats"][month]["ful"]["new90_order_ratio"]
    all_new_stock_stats_ful_new180_order_ratio = report["all_new_stock_stats"][month]["ful"]["new180_order_ratio"]
    all_new_stock_stats_ful_new30_price = report["all_new_stock_stats"][month]["ful"]["new30_price"]
    all_new_stock_stats_ful_new90_price = report["all_new_stock_stats"][month]["ful"]["new90_price"]
    all_new_stock_stats_ful_new180_price = report["all_new_stock_stats"][month]["ful"]["new180_price"]

    all_new_stock_stats_nor_new30_count = report["all_new_stock_stats"][month]["nor"]["new30_count"]
    all_new_stock_stats_nor_new90_count = report["all_new_stock_stats"][month]["nor"]["new90_count"]
    all_new_stock_stats_nor_new180_count = report["all_new_stock_stats"][month]["nor"]["new180_count"]
    all_new_stock_stats_nor_new30_order = report["all_new_stock_stats"][month]["nor"]["new30_order"]
    all_new_stock_stats_nor_new90_order = report["all_new_stock_stats"][month]["nor"]["new90_order"]
    all_new_stock_stats_nor_new180_order = report["all_new_stock_stats"][month]["nor"]["new180_order"]
    all_new_stock_stats_nor_new30_order_ratio = report["all_new_stock_stats"][month]["nor"]["new30_order_ratio"]
    all_new_stock_stats_nor_new90_order_ratio = report["all_new_stock_stats"][month]["nor"]["new90_order_ratio"]
    all_new_stock_stats_nor_new180_order_ratio = report["all_new_stock_stats"][month]["nor"]["new180_order_ratio"]


    result = {
    "cat_id": cat_id,  # 类目ID
    "time": int(month),  # 当前月份

    "2025_gmv_growth_order_ratio": gmv_growth_order_ratio,  # 25年GMV增长率
    "top10_commodity_sku_id_order": top10_commodity_sku_id_order_sum,  # top10商品销量
    "top10_commodity_sku_id_order_ratio": top10_commodity_sku_id_order_sum_ratio,  # top10商品销量占比
    "top1_seller_sales_order_ratio_sum_ratio": top1_seller_sales_order_ratio_sum_ratio,  # top1店铺销量占比
    "top3_seller_sales_order_ratio_sum_ratio": top3_seller_sales_order_ratio_sum_ratio,  # top3店铺销量占比
    "top1_brand_sales_order_ratio_sum_ratio": top1_brand_sales_order_ratio_sum_ratio,  # top1品牌销量占比
    "top1_brand_sales_order_ratio_name": top1_brand_sales_order_ratio_name,  # top1品牌名称
    "top3_brand_sales_order_ratio_sum_ratio": top3_brand_sales_order_ratio_sum_ratio,  # top3品牌销量占比

    # top50新品商品数量 ci
    "top50_sku_new_stock_stats_ci_new30_count": top50_sku_new_stock_stats_ci_new30_count,
    "top50_sku_new_stock_stats_ci_new90_count": top50_sku_new_stock_stats_ci_new90_count,
    "top50_sku_new_stock_stats_ci_new180_count": top50_sku_new_stock_stats_ci_new180_count,
    # top50新品商品销量 ci
    "top50_sku_new_stock_stats_ci_new30_order": top50_sku_new_stock_stats_ci_new30_order,
    "top50_sku_new_stock_stats_ci_new90_order": top50_sku_new_stock_stats_ci_new90_order,
    "top50_sku_new_stock_stats_ci_new180_order": top50_sku_new_stock_stats_ci_new180_order,
    # top50新品商品销量占比 ci
    "top50_sku_new_stock_stats_ci_new30_order_ratio": top50_sku_new_stock_stats_ci_new30_order_ratio,
    "top50_sku_new_stock_stats_ci_new90_order_ratio": top50_sku_new_stock_stats_ci_new90_order_ratio,
    "top50_sku_new_stock_stats_ci_new180_order_ratio": top50_sku_new_stock_stats_ci_new180_order_ratio,

    # top50新品商品数量 ful
    "top50_sku_new_stock_stats_ful_new30_count": top50_sku_new_stock_stats_ful_new30_count,
    "top50_sku_new_stock_stats_ful_new90_count": top50_sku_new_stock_stats_ful_new90_count,
    "top50_sku_new_stock_stats_ful_new180_count": top50_sku_new_stock_stats_ful_new180_count,
    # top50新品商品销量 ful
    "top50_sku_new_stock_stats_ful_new30_order": top50_sku_new_stock_stats_ful_new30_order,
    "top50_sku_new_stock_stats_ful_new90_order": top50_sku_new_stock_stats_ful_new90_order,
    "top50_sku_new_stock_stats_ful_new180_order": top50_sku_new_stock_stats_ful_new180_order,
    # top50新品商品销量占比 ful
    "top50_sku_new_stock_stats_ful_new30_order_ratio": top50_sku_new_stock_stats_ful_new30_order_ratio,
    "top50_sku_new_stock_stats_ful_new90_order_ratio": top50_sku_new_stock_stats_ful_new90_order_ratio,
    "top50_sku_new_stock_stats_ful_new180_order_ratio": top50_sku_new_stock_stats_ful_new180_order_ratio,

    # top50新品商品数量 nor
    "top50_sku_new_stock_stats_nor_new30_count": top50_sku_new_stock_stats_nor_new30_count,
    "top50_sku_new_stock_stats_nor_new90_count": top50_sku_new_stock_stats_nor_new90_count,
    "top50_sku_new_stock_stats_nor_new180_count": top50_sku_new_stock_stats_nor_new180_count,
    # top50新品商品销量 nor
    "top50_sku_new_stock_stats_nor_new30_order": top50_sku_new_stock_stats_nor_new30_order,
    "top50_sku_new_stock_stats_nor_new90_order": top50_sku_new_stock_stats_nor_new90_order,
    "top50_sku_new_stock_stats_nor_new180_order": top50_sku_new_stock_stats_nor_new180_order,
    # top50新品商品销量占比 nor
    "top50_sku_new_stock_stats_nor_new30_order_ratio": top50_sku_new_stock_stats_nor_new30_order_ratio,
    "top50_sku_new_stock_stats_nor_new90_order_ratio": top50_sku_new_stock_stats_nor_new90_order_ratio,
    "top50_sku_new_stock_stats_nor_new180_order_ratio": top50_sku_new_stock_stats_nor_new180_order_ratio,

    # all新品数据 ci
    "all_new_stock_stats_ci_new30_count": all_new_stock_stats_ci_new30_count,  # ci 新品30天数量
    "all_new_stock_stats_ci_new90_count": all_new_stock_stats_ci_new90_count,  # ci 新品90天数量
    "all_new_stock_stats_ci_new180_count": all_new_stock_stats_ci_new180_count,  # ci 新品180天数量
    "all_new_stock_stats_ci_new30_order": all_new_stock_stats_ci_new30_order,  # ci 新品30天销量
    "all_new_stock_stats_ci_new90_order": all_new_stock_stats_ci_new90_order,  # ci 新品90天销量
    "all_new_stock_stats_ci_new180_order": all_new_stock_stats_ci_new180_order,  # ci 新品180天销量
    "all_new_stock_stats_ci_new30_order_ratio": all_new_stock_stats_ci_new30_order_ratio,  # ci 新品30天销量占比
    "all_new_stock_stats_ci_new90_order_ratio": all_new_stock_stats_ci_new90_order_ratio,  # ci 新品90天销量占比
    "all_new_stock_stats_ci_new180_order_ratio": all_new_stock_stats_ci_new180_order_ratio,  # ci 新品180天销量占比

    # all新品数据 ful
    "all_new_stock_stats_ful_new30_count": all_new_stock_stats_ful_new30_count,  # ful 新品30天数量
    "all_new_stock_stats_ful_new90_count": all_new_stock_stats_ful_new90_count,  # ful 新品90天数量
    "all_new_stock_stats_ful_new180_count": all_new_stock_stats_ful_new180_count,  # ful 新品180天数量
    "all_new_stock_stats_ful_new30_order": all_new_stock_stats_ful_new30_order,  # ful 新品30天销量
    "all_new_stock_stats_ful_new90_order": all_new_stock_stats_ful_new90_order,  # ful 新品90天销量
    "all_new_stock_stats_ful_new180_order": all_new_stock_stats_ful_new180_order,  # ful 新品180天销量
    "all_new_stock_stats_ful_new30_order_ratio": all_new_stock_stats_ful_new30_order_ratio,  # ful 新品30天销量占比
    "all_new_stock_stats_ful_new90_order_ratio": all_new_stock_stats_ful_new90_order_ratio,  # ful 新品90天销量占比
    "all_new_stock_stats_ful_new180_order_ratio": all_new_stock_stats_ful_new180_order_ratio,  # ful 新品180天销量占比
    "all_new_stock_stats_ful_new30_price": all_new_stock_stats_ful_new30_price,  # ful 新品30天均价
    "all_new_stock_stats_ful_new90_price": all_new_stock_stats_ful_new90_price,  # ful 新品90天均价
    "all_new_stock_stats_ful_new180_price": all_new_stock_stats_ful_new180_price,  # ful 新品180天均价

    # all新品数据 nor
    "all_new_stock_stats_nor_new30_count": all_new_stock_stats_nor_new30_count,  # nor 新品30天数量
    "all_new_stock_stats_nor_new90_count": all_new_stock_stats_nor_new90_count,  # nor 新品90天数量
    "all_new_stock_stats_nor_new180_count": all_new_stock_stats_nor_new180_count,  # nor 新品180天数量
    "all_new_stock_stats_nor_new30_order": all_new_stock_stats_nor_new30_order,  # nor 新品30天销量
    "all_new_stock_stats_nor_new90_order": all_new_stock_stats_nor_new90_order,  # nor 新品90天销量
    "all_new_stock_stats_nor_new180_order": all_new_stock_stats_nor_new180_order,  # nor 新品180天销量
    "all_new_stock_stats_nor_new30_order_ratio": all_new_stock_stats_nor_new30_order_ratio,  # nor 新品30天销量占比
    "all_new_stock_stats_nor_new90_order_ratio": all_new_stock_stats_nor_new90_order_ratio,  # nor 新品90天销量占比
    "all_new_stock_stats_nor_new180_order_ratio": all_new_stock_stats_nor_new180_order_ratio,  # nor 新品180天销量占比
}
    print(result)
    print(len(result))
    big_Excel.update_one({"cat_id": cat_id,"time":int(month)}, {"$set": result}, upsert=True)


import_big_Excel(cat_id,report,month)