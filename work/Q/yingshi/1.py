from connector.m import M
m = M("ml_mx")

df_202408 = m.c_monthly_sku.find(
        {"month": {"$in": 202408}, "brand":{"$regex": "^(tp-link|tapo|steren)$", "$options": "i"}},
        {
            "sku_id": 1,
            "item_id": 1,
            "month": 1,
            "category_id": 1,
            "monthlyorder": 1,
            "monthlygmv": 1,
            "lanjing_source": 1,
        },
    )

df_202409 = m.c_monthly_sku.find(
        {"month": {"$in": 202409}, "brand":{"$regex": "^(tp-link|tapo|steren)$", "$options": "i"}},
        {
            "sku_id": 1,
            "item_id": 1,
            "month": 1,
            "category_id": 1,
            "monthlyorder": 1,
            "monthlygmv": 1,
            "lanjing_source": 1,
        },
    )

df_202410 = m.c_monthly_sku.find(
        {"month": {"$in": 202410}, "brand":{"$regex": "^(tp-link|tapo|steren)$", "$options": "i"}},
        {
            "sku_id": 1,
            "item_id": 1,
            "month": 1,
            "category_id": 1,
            "monthlyorder": 1,
            "monthlygmv": 1,
            "lanjing_source": 1,
        },
    )

