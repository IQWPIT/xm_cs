import sys
import os
from pymongo import UpdateOne
import pandas as pd

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

sku = get_collection("main_ml_mx", "ml_mx", "sku")

data = sku.find(
    {},
    {"_id": 1, "category_id": 1, "brand": 1, "monthly_sale_trend.2510": 1}
)

m = {}
n = 0

for i in data:
    n += 1
    print(n)

    cat = i.get("category_id")
    brand = i.get("brand")
    sale_2510 = i.get("monthly_sale_trend", {}).get("2510")

    if sale_2510 is None:
        print("缺少 monthly_sale_trend.2510:", i["_id"])
        continue

    sale_2510 = int(sale_2510)

    # 初始化分类
    if cat not in m:
        m[cat] = {}

    # 累计品牌值
    m[cat][brand] = m[cat].get(brand, 0) + sale_2510

# -------- 保存为 Excel --------
rows = []
for cat, brands in m.items():
    for brand, value in brands.items():
        print(cat, brand, value)
        rows.append({
            "category_id": cat,
            "brand": brand,
            "sale_2510": value
        })

df = pd.DataFrame(rows)

output_path = "sku_trend_result.xlsx"
df.to_excel(output_path, index=False)

print("保存成功:", output_path)
