import pandas as pd

# 定义表格数据
data = [
    ["cat_id", "int/str", "类目ID"],
    ["time", "int", "当前月份（整数）"],
    ["2025_gmv_growth_order_ratio", "float", "2025年GMV增长率"],
    ["top10_commodity_sku_id_order", "int", "top10商品销量总和"],
    ["top10_commodity_sku_id_order_ratio", "float", "top10商品销量占比"],
    ["top1_seller_sales_order_ratio_sum_ratio", "float", "top1店铺销量占比"],
    ["top3_seller_sales_order_ratio_sum_ratio", "float", "top3店铺销量占比"],
    ["top1_brand_sales_order_ratio_sum_ratio", "float", "top1品牌销量占比"],
    ["top1_brand_sales_order_ratio_name", "str", "top1品牌名称"],
    ["top3_brand_sales_order_ratio_sum_ratio", "float", "top3品牌销量占比"],

    # top50_sku_new_stock_stats_ci
    ["top50_sku_new_stock_stats_ci_new30_count", "int", "top50新品数量（ci，30天）"],
    ["top50_sku_new_stock_stats_ci_new90_count", "int", "top50新品数量（ci，90天）"],
    ["top50_sku_new_stock_stats_ci_new180_count", "int", "top50新品数量（ci，180天）"],
    ["top50_sku_new_stock_stats_ci_new30_order", "int", "top50新品销量（ci，30天）"],
    ["top50_sku_new_stock_stats_ci_new90_order", "int", "top50新品销量（ci，90天）"],
    ["top50_sku_new_stock_stats_ci_new180_order", "int", "top50新品销量（ci，180天）"],
    ["top50_sku_new_stock_stats_ci_new30_order_ratio", "float", "top50新品销量占比（ci，30天）"],
    ["top50_sku_new_stock_stats_ci_new90_order_ratio", "float", "top50新品销量占比（ci，90天）"],
    ["top50_sku_new_stock_stats_ci_new180_order_ratio", "float", "top50新品销量占比（ci，180天）"],

    # top50_sku_new_stock_stats_ful
    ["top50_sku_new_stock_stats_ful_new30_count", "int", "top50新品数量（ful，30天）"],
    ["top50_sku_new_stock_stats_ful_new90_count", "int", "top50新品数量（ful，90天）"],
    ["top50_sku_new_stock_stats_ful_new180_count", "int", "top50新品数量（ful，180天）"],
    ["top50_sku_new_stock_stats_ful_new30_order", "int", "top50新品销量（ful，30天）"],
    ["top50_sku_new_stock_stats_ful_new90_order", "int", "top50新品销量（ful，90天）"],
    ["top50_sku_new_stock_stats_ful_new180_order", "int", "top50新品销量（ful，180天）"],
    ["top50_sku_new_stock_stats_ful_new30_order_ratio", "float", "top50新品销量占比（ful，30天）"],
    ["top50_sku_new_stock_stats_ful_new90_order_ratio", "float", "top50新品销量占比（ful，90天）"],
    ["top50_sku_new_stock_stats_ful_new180_order_ratio", "float", "top50新品销量占比（ful，180天）"],

    # top50_sku_new_stock_stats_nor
    ["top50_sku_new_stock_stats_nor_new30_count", "int", "top50新品数量（nor，30天）"],
    ["top50_sku_new_stock_stats_nor_new90_count", "int", "top50新品数量（nor，90天）"],
    ["top50_sku_new_stock_stats_nor_new180_count", "int", "top50新品数量（nor，180天）"],
    ["top50_sku_new_stock_stats_nor_new30_order", "int", "top50新品销量（nor，30天）"],
    ["top50_sku_new_stock_stats_nor_new90_order", "int", "top50新品销量（nor，90天）"],
    ["top50_sku_new_stock_stats_nor_new180_order", "int", "top50新品销量（nor，180天）"],
    ["top50_sku_new_stock_stats_nor_new30_order_ratio", "float", "top50新品销量占比（nor，30天）"],
    ["top50_sku_new_stock_stats_nor_new90_order_ratio", "float", "top50新品销量占比（nor，90天）"],
    ["top50_sku_new_stock_stats_nor_new180_order_ratio", "float", "top50新品销量占比（nor，180天）"],

    # all_new_stock_stats_ci
    ["all_new_stock_stats_ci_new30_count", "int", "所有新品数量（ci，30天）"],
    ["all_new_stock_stats_ci_new90_count", "int", "所有新品数量（ci，90天）"],
    ["all_new_stock_stats_ci_new180_count", "int", "所有新品数量（ci，180天）"],
    ["all_new_stock_stats_ci_new30_order", "int", "所有新品销量（ci，30天）"],
    ["all_new_stock_stats_ci_new90_order", "int", "所有新品销量（ci，90天）"],
    ["all_new_stock_stats_ci_new180_order", "int", "所有新品销量（ci，180天）"],
    ["all_new_stock_stats_ci_new30_order_ratio", "float", "所有新品销量占比（ci，30天）"],
    ["all_new_stock_stats_ci_new90_order_ratio", "float", "所有新品销量占比（ci，90天）"],
    ["all_new_stock_stats_ci_new180_order_ratio", "float", "所有新品销量占比（ci，180天）"],

    # all_new_stock_stats_ful
    ["all_new_stock_stats_ful_new30_count", "int", "所有新品数量（ful，30天）"],
    ["all_new_stock_stats_ful_new90_count", "int", "所有新品数量（ful，90天）"],
    ["all_new_stock_stats_ful_new180_count", "int", "所有新品数量（ful，180天）"],
    ["all_new_stock_stats_ful_new30_order", "int", "所有新品销量（ful，30天）"],
    ["all_new_stock_stats_ful_new90_order", "int", "所有新品销量（ful，90天）"],
    ["all_new_stock_stats_ful_new180_order", "int", "所有新品销量（ful，180天）"],
    ["all_new_stock_stats_ful_new30_order_ratio", "float", "所有新品销量占比（ful，30天）"],
    ["all_new_stock_stats_ful_new90_order_ratio", "float", "所有新品销量占比（ful，90天）"],
    ["all_new_stock_stats_ful_new180_order_ratio", "float", "所有新品销量占比（ful，180天）"],
    ["all_new_stock_stats_ful_new30_price", "float", "所有新品均价（ful，30天）"],
    ["all_new_stock_stats_ful_new90_price", "float", "所有新品均价（ful，90天）"],
    ["all_new_stock_stats_ful_new180_price", "float", "所有新品均价（ful，180天）"],

    # all_new_stock_stats_nor
    ["all_new_stock_stats_nor_new30_count", "int", "所有新品数量（nor，30天）"],
    ["all_new_stock_stats_nor_new90_count", "int", "所有新品数量（nor，90天）"],
    ["all_new_stock_stats_nor_new180_count", "int", "所有新品数量（nor，180天）"],
    ["all_new_stock_stats_nor_new30_order", "int", "所有新品销量（nor，30天）"],
    ["all_new_stock_stats_nor_new90_order", "int", "所有新品销量（nor，90天）"],
    ["all_new_stock_stats_nor_new180_order", "int", "所有新品销量（nor，180天）"],
    ["all_new_stock_stats_nor_new30_order_ratio", "float", "所有新品销量占比（nor，30天）"],
    ["all_new_stock_stats_nor_new90_order_ratio", "float", "所有新品销量占比（nor，90天）"],
    ["all_new_stock_stats_nor_new180_order_ratio", "float", "所有新品销量占比（nor，180天）"],
]

# 生成 DataFrame
df = pd.DataFrame(data, columns=["字段名", "类型/范围", "描述"])

# 输出 Excel
df.to_excel("大表格字段说明.xlsx", index=False)
print("生成完成：大表格字段说明.xlsx")