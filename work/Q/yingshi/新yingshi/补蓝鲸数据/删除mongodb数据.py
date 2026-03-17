# -*- coding: utf-8 -*-
import os

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =========================
# 配置
# =========================
site = "ml_br"
cat_id = ["MLB7073"]

MONTHS = [
    202507,
    202508,
    202509,
    202510,
    202511,
]

TARGET_BRANDS = [
    "Intelbras",
    "TP-Link",
    "Hikvision",
    "Ezviz"
]

# =========================
# collection
# =========================
c_monthly_sku = get_collection(
    "yingshi",
    "yingshi",
    "ml_br_monthly_sku"
)

# =========================
# 构造忽略大小写匹配
# =========================
brand_regex = [
    {"brand": {"$regex": f"^{b}$", "$options": "i"}}
    for b in TARGET_BRANDS
]

query = {
    "category_id": {"$in": cat_id},
    "month": {"$in": MONTHS},
    "$or": brand_regex
}

# =========================
# 先统计影响数据量
# =========================
count = c_monthly_sku.count_documents(query)

print("================================")
print("即将修改的数据量:", count)
print("品牌:", TARGET_BRANDS)
print("月份:", MONTHS)
print("类目:", cat_id)
print("================================")

# =========================
# 更新数据
# =========================
update = {
    "$set": {
        "monthlyorder": 0,
        "monthlygmv": 0
    }
}

result = c_monthly_sku.update_many(query, update)

print("================================")
print("更新完成")
print("匹配数量:", result.matched_count)
print("修改数量:", result.modified_count)
print("================================")