import os
from collections import defaultdict, OrderedDict
from pymongo import UpdateOne
import pandas as pd

# ================== 环境变量 ==================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# ================== 连接集合 ==================
src_col = get_collection("ml", "customize_common", "wu_custom_made_shopee")

# ================== 读取 Excel 产品表 ==================
df = pd.read_excel("选产品详情-20260225141512 (1).xlsx", header=1)
product_name_map = dict(zip(df["产品ID"].astype(str), df["产品名称"]))

# ================== 遍历 MongoDB 数据 ==================
datas = src_col.find({}, no_cursor_timeout=True)
bulk_ops = []

for data in datas:
    _id = data["_id"]
    product_id = data.get("Data", {}).get("ProductId")
    if not product_id:
        continue

    # ---- 获取滚动总销量 ----
    total_trend = data.get("Data", {}).get("SaleTotalCountTrend", {})
    if not total_trend:
        continue

    # ---- 按月份汇总总销量（月底减上月末或月初） ----
    monthly_sale = {}
    all_dates = sorted(total_trend.keys())
    prev_month_end_val = None  # 上月末总销量

    for month in sorted(set(date[:7] for date in all_dates)):
        month_dates = sorted([d for d in all_dates if d.startswith(month)])
        if not month_dates:
            continue
        try:
            # 本月最后一天总销量
            month_end_val = int(float(total_trend[month_dates[-1]]))

            if prev_month_end_val is None:
                # 如果没有上月末数据，用本月最早一天替代
                month_start_val = int(float(total_trend[month_dates[0]]))
            else:
                month_start_val = prev_month_end_val

            monthly_sale[month] = max(0, month_end_val - month_start_val)
            prev_month_end_val = month_end_val  # 更新上月末值
        except:
            continue

    monthly_sale = dict(sorted(monthly_sale.items()))

    # ---- 获取产品名称 ----
    product_name = product_name_map.get(str(product_id))

    # ---- 准备批量更新操作 ----
    update_doc = {"monthly_sale": monthly_sale}
    if product_name:
        update_doc["product_name"] = product_name

    bulk_ops.append(UpdateOne({"_id": _id}, {"$set": update_doc}))

    # 分批写入 MongoDB
    if len(bulk_ops) >= 1000:
        try:
            src_col.bulk_write(bulk_ops)
        except Exception as e:
            print(f"MongoDB 批量更新出错: {e}")
        bulk_ops = []

# 写回剩余数据
if bulk_ops:
    try:
        src_col.bulk_write(bulk_ops)
    except Exception as e:
        print(f"MongoDB 批量更新出错: {e}")

print("MongoDB 更新完成")