# -*- coding: utf-8 -*-
import os
import pandas as pd
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection


def to_int(v):
    try:
        return int(float(v))
    except Exception:
        return 0


_lj_data = get_collection("yingshi", "yingshi", "_lj_data")

MONTHS = [202507, 202508, 202509, 202510, 202511]

brand_stats = {}

cursor = _lj_data.find(
    {},
    {
        "_id": 0,
        "商品ID": 1,
        "202507月销量": 1,
        "202508月销量": 1,
        "202509月销量": 1,
        "202510月销量": 1,
        "202511月销量": 1,
        "品牌":1,
    },
    no_cursor_timeout=True,
).batch_size(1000)

try:
    for data in tqdm(cursor, desc="统计品牌数据"):
        brand = data.get("品牌")
        sku_id = data.get("商品ID")

        month_sales = {
            202507: to_int(data.get("202507月销量", 0)),
            202508: to_int(data.get("202508月销量", 0)),
            202509: to_int(data.get("202509月销量", 0)),
            202510: to_int(data.get("202510月销量", 0)),
            202511: to_int(data.get("202511月销量", 0)),
        }
        total_order = sum(month_sales.values())

        if brand not in brand_stats:
            brand_stats[brand] = {
                "brand": brand,
                "record_count": 0,
                "sku_set": set(),
                "total_order": 0,
                "202507_order": 0,
                "202508_order": 0,
                "202509_order": 0,
                "202510_order": 0,
                "202511_order": 0,
            }

        brand_stats[brand]["record_count"] += 1
        if sku_id:
            brand_stats[brand]["sku_set"].add(sku_id)
        brand_stats[brand]["total_order"] += total_order
        brand_stats[brand]["202507_order"] += month_sales[202507]
        brand_stats[brand]["202508_order"] += month_sales[202508]
        brand_stats[brand]["202509_order"] += month_sales[202509]
        brand_stats[brand]["202510_order"] += month_sales[202510]
        brand_stats[brand]["202511_order"] += month_sales[202511]

finally:
    cursor.close()

result = []
for brand, stat in brand_stats.items():
    sku_count = len(stat["sku_set"])
    total_order = stat["total_order"]
    avg_order_per_sku = round(total_order / sku_count, 2) if sku_count else 0

    result.append({
        "brand": stat["brand"],
        "record_count": stat["record_count"],   # 记录数
        "sku_count": sku_count,                 # 去重SKU数
        "total_order": total_order,             # 5个月总销量
        "avg_order_per_sku": avg_order_per_sku, # 单SKU平均销量
        "202507_order": stat["202507_order"],
        "202508_order": stat["202508_order"],
        "202509_order": stat["202509_order"],
        "202510_order": stat["202510_order"],
        "202511_order": stat["202511_order"],
    })

df = pd.DataFrame(result)
df = df.sort_values(["total_order", "sku_count"], ascending=[False, False]).reset_index(drop=True)

print(df)

output_file = "lj_brand_stats.xlsx"
df.to_excel(output_file, index=False)
print(f"已导出: {output_file}")