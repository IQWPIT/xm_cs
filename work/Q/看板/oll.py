import os
from pymongo import UpdateOne
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

TARGET_MONTH = "202511"
a2 = "202510"
a3 = "202411"
m = [TARGET_MONTH,a2,a3]
file_names = [
    "MLM1747","MLM189530","MLM1403","MLM1367","MLM1368","MLM1384",
    "MLM1246","MLM1051","MLM1648","MLM1144","MLM1500","MLM1039",
    "MLM1276","MLM1575","MLM1000","MLM186863","MLM1574","MLM1499",
    "MLM1182","MLM3937","MLM1132","MLM3025","MLM1071","MLM1953",
    "MLM44011","MLM1430","MLM187772"
]

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
_c_visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")


def process_cat_id(cat_id):
    """获取单个 cat_id 的月度和日数据，不累加汇总"""
    fields = ["gmv", "order", "price"]
    data = visual_plus.find_one({"cat_id": cat_id}, {f: 1 for f in fields})
    if not data:
        return None

    gmv_monthly = {}
    order_monthly = {}
    price_monthly = {}
    gmv_day = defaultdict(float)
    order_day = defaultdict(float)

    for month in m:
        gmv_monthly[month] = data["gmv"][f"{month}"]["monthly"]
        order_monthly[month] = data["order"][month]["monthly"]
        price_monthly[month] = gmv_monthly[month] / order_monthly[month] if order_monthly[month] else 0

    for day, gmv_val in data["gmv"][f"{TARGET_MONTH}"]["day"].items():
        gmv_day[day] += gmv_val
        order_day[day] += data["order"][f"{TARGET_MONTH}"]["day"].get(day, 0)

    return {
        "gmv_monthly": gmv_monthly,
        "order_monthly": order_monthly,
        "price_monthly": price_monthly,
        "gmv_day": gmv_day,
        "order_day": order_day
    }


# ---- 多线程抓取数据 ----
max_workers = 8
results = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(process_cat_id, cat_id): cat_id for cat_id in file_names}
    for future in as_completed(futures):
        res = future.result()
        if res:
            results.append(res)

# ---- 主线程累加汇总 ----
gmv_monthly_total = defaultdict(float)
order_monthly_total = defaultdict(float)
price_monthly_total = defaultdict(float)
gmv_day_total = defaultdict(float)
order_day_total = defaultdict(float)
price_day_total = {}

for res in results:
    for month in m:
        gmv_monthly_total[month] += res["gmv_monthly"][month]
        order_monthly_total[month] += res["order_monthly"][month]

    for day, val in res["gmv_day"].items():
        gmv_day_total[day] += val
        order_day_total[day] += res["order_day"].get(day, 0)

# 月均价
for month in ["202511","202510","202411"]:
    price_monthly_total[month] = gmv_monthly_total[month] / order_monthly_total[month] if order_monthly_total[month] else 0

# 日均价
for day in gmv_day_total:
    price_day_total[day] = gmv_day_total[day] / order_day_total[day] if order_day_total[day] else 0

# 年同比 & 环比
gmv_year = gmv_monthly_total["202511"] / gmv_monthly_total["202510"] if gmv_monthly_total["202510"] else 0
gmv_chain = gmv_monthly_total["202511"] / gmv_monthly_total["202411"] if gmv_monthly_total["202411"] else 0
order_year = order_monthly_total["202511"] / order_monthly_total["202510"] if order_monthly_total["202510"] else 0
order_chain = order_monthly_total["202511"] / order_monthly_total["202411"] if order_monthly_total["202411"] else 0
price_year = price_monthly_total["202511"] / price_monthly_total["202510"] if price_monthly_total["202510"] else 0
price_chain = price_monthly_total["202511"] / price_monthly_total["202411"] if price_monthly_total["202411"] else 0

# ---- 构建汇总更新文档 ----
update_doc = {
    f"gmv.202511": {
        "monthly": gmv_monthly_total["202511"],
        "day": dict(gmv_day_total),
        "average_day": gmv_monthly_total["202511"]/30,
        "year": gmv_year,
        "chain": gmv_chain,
    },
    f"order.202511": {
        "monthly": order_monthly_total["202511"],
        "day": dict(order_day_total),
        "average_day": order_monthly_total["202511"]/30,
        "year": order_year,
        "chain": order_chain,
    },
    f"price.202511": {
        "monthly": price_monthly_total["202511"],
        "day": dict(price_day_total),
        "average_day": price_monthly_total["202511"]/30,
        "year": price_year,
        "chain": price_chain,
    }
}

# ---- 写入 MongoDB ----
visual_plus.update_one({"cat_id": "all"}, {"$set": update_doc}, upsert=True)

print("汇总更新完成")
