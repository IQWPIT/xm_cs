import sys
import os
from pymongo import UpdateOne

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection


def rnd(x):
    """安全保留 2 位小数"""
    try:
        return round(float(x), 2)
    except:
        return 0.00


def safe_div(a, b):
    """安全除法，避免除零"""
    try:
        return a / b if b else 0
    except:
        return 0


TARGET_MONTH = "202512"
last_month = "202511"
last_year = "202412"
fields = [
    "_id",
    "cat_id",
    "monthly_sale_trend",
    "sale_trend",
    "gmv_trend",
    "monthly_gmv_trend"
]

visualize_table = get_collection("main_ml_mx", "ml_mx", "visualize_table")
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# csv_files = list(source_folder.glob("*.csv"))
# file_names = [f.stem for f in csv_files]

# file_names = [
#     "MLM1747","MLM189530","MLM1403","MLM1367","MLM1368","MLM1384",
#     "MLM1246","MLM1051","MLM1648","MLM1144","MLM1500","MLM1039",
#     "MLM1276","MLM1575","MLM1000","MLM186863","MLM1574","MLM1499",
#     "MLM1182","MLM3937","MLM1132","MLM3025","MLM1071","MLM1953",
#     "MLM44011","MLM1430","MLM187772"
# ]
# file_names = ["MLM1055"]
# datas = visualize_table.find(
#     {"cat_id": {"$in": file_names}},
#     {f: 1 for f in fields}
# )
datas = visualize_table.find(
    {},
    {f: 1 for f in fields}
)

total = 13787  # 数据总量，可根据实际情况修改
result = []
n = 0

for idx, data in enumerate(datas, 1):
    _id = data["_id"]
    cat_id = data["cat_id"]

    sale_trend = data.get("sale_trend", {})
    gmv_trend = data.get("gmv_trend", {})
    monthly_sale = data.get("monthly_sale_trend", {})
    monthly_gmv = data.get("monthly_gmv_trend", {})

    order_dict = {"monthly": 0, "day": {}, "average_day": 0, "year": 0, "chain": 0}
    gmv_dict = {"monthly": 0, "day": {}, "average_day": 0, "year": 0, "chain": 0}
    price_dict = {"monthly": 0, "day": {}, "year": 0, "chain": 0}

    total_order = 0
    total_gmv = 0
    days_count = 0

    # 日数据处理
    for day, order in sale_trend.items():
        if not day.startswith(TARGET_MONTH):
            continue
        gmv = gmv_trend.get(day, 0)
        order_dict["day"][day] = rnd(order)
        gmv_dict["day"][day] = rnd(gmv)
        price_dict["day"][day] = rnd(safe_div(gmv, order))
        total_order += order
        total_gmv += gmv
        days_count += 1

    # 月/日均值
    if days_count > 0:
        order_dict["monthly"] = rnd(total_order)
        order_dict["average_day"] = rnd(safe_div(total_order, days_count))
        gmv_dict["monthly"] = rnd(total_gmv)
        gmv_dict["average_day"] = rnd(safe_div(total_gmv, days_count))

    # 年度/环比/价格
    try:
        order_dict["year"] = rnd(safe_div(monthly_sale.get(TARGET_MONTH, 0), monthly_sale.get(last_month, 0)))
        order_dict["chain"] = rnd(safe_div(monthly_sale.get(TARGET_MONTH, 0), monthly_sale.get(last_year, 0)))

        gmv_dict["year"] = rnd(safe_div(monthly_gmv.get(TARGET_MONTH, 0), monthly_gmv.get(last_month, 0)))
        gmv_dict["chain"] = rnd(safe_div(monthly_gmv.get(TARGET_MONTH, 0), monthly_gmv.get(last_year, 0)))

        price_this_month = safe_div(monthly_gmv.get(TARGET_MONTH, 0), monthly_sale.get(TARGET_MONTH, 0))
        price_last_month = safe_div(monthly_gmv.get(last_month, 0), monthly_sale.get(last_month, 0))
        price_last_year = safe_div(monthly_gmv.get(last_year, 0), monthly_sale.get(last_year, 0))

        price_dict["monthly"] = rnd(price_this_month)
        price_dict["year"] = rnd(safe_div(price_this_month, price_last_month))
        price_dict["chain"] = rnd(safe_div(price_this_month, price_last_year))
        n += 1
    except Exception as e:
        print("错误:", e)
        print(f"cat_id:{cat_id}")
        print(n)
        continue

    result.append({
        "_id": _id,
        "cat_id": cat_id,
        "order": {TARGET_MONTH: order_dict},
        "gmv": {TARGET_MONTH: gmv_dict},
        "price": {TARGET_MONTH: price_dict}
    })

    # 打印处理进度条
    bar_len = 50
    filled_len = int(bar_len * idx / total)
    bar = '█' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write(f"\r处理进度: |{bar}| {idx}/{total}")
    sys.stdout.flush()

print(f"\n数据处理完成，开始写入数据库...\n共处理 {n} 条有效数据")

# 批量写入
ops = []
for item in result:
    ops.append(UpdateOne(
        {"_id": item["_id"]},
        {"$set": {
            "cat_id": item["cat_id"],
            f"order.{TARGET_MONTH}": item["order"][TARGET_MONTH],
            f"gmv.{TARGET_MONTH}": item["gmv"][TARGET_MONTH],
            f"price.{TARGET_MONTH}": item["price"][TARGET_MONTH],
        }},
        upsert=True
    ))

# 写入进度条
for i, op in enumerate(ops, 1):
    visual_plus.bulk_write([op], ordered=False)
    bar_len = 50
    filled_len = int(bar_len * i / len(ops))
    bar = '█' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write(f"\r写入进度: |{bar}| {i}/{len(ops)}")
    sys.stdout.flush()

print("\n写入完成")
