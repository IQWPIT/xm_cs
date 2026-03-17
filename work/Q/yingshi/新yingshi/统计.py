import os
import re
from collections import defaultdict
import pandas as pd
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

sites = [
    # "ml_cl",
    # "ml_co",
    # "ml_mx",
    "ml_br",
    # "ml_ar"
]
times = [
    # 202401,
    # 202411,
    # 202412,
    # 202501,
    # # 202507,
    # # 202508,
    # # 202509,
    # # 202510,
    # 202511,
    202512,
    # 202601
]

brands = ["EZVIZ"]      # 👉 [] 或 None = 全量（不按品牌拆）
# brands = []            # ← 切到全量模式

brand_regex = [re.compile(f"^{b}$", re.IGNORECASE) for b in brands]

result = []

for site in sites:
    c_monthly_sku = get_collection("yingshi", "yingshi", f"{site}_monthly_sku")

    for month in times:
        query = {"month": month}
        if brand_regex:
            query["brand"] = {"$in": brand_regex}

        cursor = c_monthly_sku.find(
            query,
            {
                "monthlyorder": 1,
                "monthlygmv": 1,
                "category_id": 1,
                "brand": 1,
                "sku_id":1,
            }
        )

        # 🔑 根据是否有品牌，决定聚合 key
        agg = defaultdict(lambda: {"order": 0, "gmv": 0, "sku_count": 0})

        for d in cursor:
            cat_id = d.get("category_id")
            if not cat_id:
                continue

            try:
                order = int(d.get("monthlyorder") or 0)
                gmv = int(d.get("monthlygmv") or 0)
            except Exception:
                continue

            if brand_regex:
                brand_v = d.get("brand")
                if not brand_v:
                    continue
                key = (site, month, cat_id, brand_v)
            else:
                key = (site, month, cat_id)

            agg[key]["order"] += order
            agg[key]["gmv"] += gmv
            agg[key]["sku_count"] += 1   # ✅ 增加 SKU 数统计

        # 输出
        for key, v in agg.items():
            order = v["order"]
            gmv = v["gmv"]
            sku_count = v["sku_count"]

            row = {
                "site": site,
                "month": month,
                "category_id": key[2],
                "order": order,
                "gmv": gmv,
                "avg_price": round(gmv / order, 2) if order else 0.0,
                "sku_count": sku_count        # ✅ 输出 SKU 数量
            }

            if brand_regex:
                row["brand"] = key[3]
            else:
                row["brand"] = "ALL"   # 或者直接不加这个字段

            result.append(row)


df = pd.DataFrame(result)

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", None)

print(
    df.sort_values(
        by=["category_id", "month", "site", "brand"],
        ascending=[True, True, True, True]
    ).to_string(index=False)
)
