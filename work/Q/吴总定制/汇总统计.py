import os
from collections import defaultdict
import matplotlib.pyplot as plt
# ================== 环境变量 ==================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体显示中文
plt.rcParams['axes.unicode_minus'] = False    # 正确显示负号

# 连接集合
src_col = get_collection("ml", "customize_common", "wu_custom_made_shopee")

datas = src_col.find({}, no_cursor_timeout=True)  # 防止大数据超时

# 用于汇总每个月总销量
total_monthly_sale = defaultdict(int)

for data in datas:
    monthly_sale = data.get("monthly_sale", {})  # 直接使用 monthly_sale 字段
    if not monthly_sale:
        continue

    for month, count in monthly_sale.items():
        try:
            total_monthly_sale[month] += int(count)
        except:
            continue

# 按月份排序
total_monthly_sale = dict(sorted(total_monthly_sale.items()))

# 打印汇总
print("每个月总销量:")
for month, count in total_monthly_sale.items():
    print(f"{month}: {count}")

# 绘制柱状图
plt.figure(figsize=(15, 6))
plt.bar(total_monthly_sale.keys(), total_monthly_sale.values(), color='skyblue')
plt.xticks(rotation=45)
plt.xlabel("月份")
plt.ylabel("总销量")
plt.title("每月总销量柱状图")
plt.tight_layout()
plt.show()