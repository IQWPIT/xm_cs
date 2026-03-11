import os
from collections import defaultdict
from loguru import logger
import json
import csv

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =========================
# 配置
# =========================
site = "ml_br"
cat_id = ["MLB7073"]
MONTHS = [202507, 202508, 202509, 202510, 202511, 202512]  # 按顺序排列
TOP_N = 30  # 输出前 N 个品牌
CSV_FILE = "brand_monthlyorder.csv"
JSON_FILE = "brand_monthlyorder.json"

# =========================
# 集合
# =========================
c_monthly_sku = get_collection("yingshi", "yingshi", f"{site}_monthly_sku")
# c_monthly_sku = get_collection("yingshi", "yingshi", f"ml_br_monthly_sku_backup_20260305")
# =========================
# brand → month → order
# =========================
brand_month_map = defaultdict(lambda: defaultdict(int))

# =========================
# 查询月表
# =========================
import re
brand = ["EZVIZ", "Hikvision", "Intelbras", "TP-Link"]

brand_regex = [re.compile(f"^{b}$", re.I) for b in brand]

cursor = c_monthly_sku.find(
    {
        "month": {"$in": MONTHS},
        "category_id": {"$in": cat_id},
        "brand": {"$in": brand_regex},
    },
    {
        "month": 1,
        "brand": 1,
        "monthlyorder": 1,
        "monthlygmv":1,
    }
)

# =========================
# 累加 monthlyorder
# =========================
for d in cursor:
    brand = d.get("brand")
    month = d.get("month")
    # order = d.get("monthlyorder", 0)
    order = d.get("monthlygmv",0)
    if not brand or not month:
        continue
    brand_month_map[brand][month] = round(brand_month_map[brand][month] + order, 2)

# =========================
# 排序（按最新月份）
# =========================
sort_month = max(MONTHS)
prev_month = MONTHS[-2]  # 倒数第二个月
latest_month = MONTHS[-1]  # 最后一个月

sorted_brands = sorted(
    brand_month_map.items(),
    key=lambda x: x[1].get(sort_month, 0),
    reverse=True
)

# =========================
# 输出表格
# =========================
rows = sorted_brands[:TOP_N]

# 计算列宽（加一列 diff）
brand_width = max(len("brand"), max(len(brand) for brand, _ in rows))
month_width = {}
for m in MONTHS:
    month_width[m] = max(
        len(str(m)),
        max(len(str(month_data.get(m, 0))) for _, month_data in rows)
    )
diff_width = max(len("diff"), max(
    len(str(month_data.get(latest_month, 0) - month_data.get(prev_month, 0))) for _, month_data in rows))


def sep_line():
    line = "+" + "-" * (brand_width + 2)
    for m in MONTHS:
        line += "+" + "-" * (month_width[m] + 2)
    line += "+" + "-" * (diff_width + 2)
    return line + "+"


# 表头
print(sep_line())
header = f"| {'brand'.ljust(brand_width)} "
for m in MONTHS:
    header += f"| {str(m).ljust(month_width[m])} "
header += f"| {'diff'.ljust(diff_width)} "
print(header + "|")
print(sep_line())

# 表体 & 导出数据准备
csv_rows = []
json_data = {}
for brand, month_data in rows:
    row = f"| {brand.ljust(brand_width)} "
    for m in MONTHS:
        row += f"| {str(month_data.get(m, 0)).ljust(month_width[m])} "
    diff_value = month_data.get(latest_month, 0) - month_data.get(prev_month, 0)
    row += f"| {str(diff_value).ljust(diff_width)} "
    print(row + "|")

    # CSV 行
    csv_row = [brand] + [month_data.get(m, 0) for m in MONTHS] + [diff_value]
    csv_rows.append(csv_row)

    # JSON 数据
    json_data[brand] = {str(m): month_data.get(m, 0) for m in MONTHS}
    json_data[brand]["diff"] = diff_value

print(sep_line())

# =========================
# CSV 导出
# =========================
with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["brand"] + [str(m) for m in MONTHS] + ["diff"])
    writer.writerows(csv_rows)

logger.info(f"CSV 导出成功：{CSV_FILE}")

# =========================
# JSON 导出
# =========================
output_json = {
    "meta": {
        "site": site,
        "months": MONTHS,
        "latest_month": latest_month,
        "prev_month": prev_month,
        "unit": "monthlyorder"
    },
    "data": json_data
}

with open(JSON_FILE, "w", encoding="utf-8") as f:
    json.dump(output_json, f, ensure_ascii=False, indent=2)

logger.info(f"JSON 导出成功：{JSON_FILE}")
