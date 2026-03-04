result_doc = {
        "time": time,  # 时间
        "cat_id": visual_plus_cat_id,  # cat_id

        # ===== Basic =====
        "total_sales": monthly_sale_trend  # 全量_销量
        "total_gmv_usd": all_gmv30d,  # 全量_销售额(比索)
        "total_avg_price": avg_price,  # 全量_成交平均价
        "total_avg_product_sales": monthly_sale_trend/,  # 全量_商品平均销量

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