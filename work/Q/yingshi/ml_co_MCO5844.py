import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# --------------------------
# 1. 连接 MongoDB
# --------------------------
m = get_collection("yingshi", "yingshi", "ml_co_monthly_sku")
_ml_br_l = get_collection("yingshi", "yingshi", "_ml_co_MCO5844")

# --------------------------
# 2. 安全转换为整数
# --------------------------
def safe_int(value, default=0):
    """安全转换为整数，不能转换时返回默认值"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

# --------------------------
# 3. 读取 _ml_mx_l 数据
# --------------------------
cursor = list(_ml_br_l.find({}, {'_id': 0, '商品ID': 1, '202510月销量': 1}))
print(len(cursor))
sku_ids = [x["商品ID"] for x in cursor]
print(len(sku_ids))

# --------------------------
# 4. 批量读取 ml_mx_monthly_sku 数据
# --------------------------
docs = list(m.find(
    {
        "$or": [
            {"sku_id": {"$in": sku_ids}},
            {"item_id": {"$in": sku_ids}}
        ],
        "month":202510,
        "category_id":"MCO5844"
    },
    {'_id': 0, 'sku_id': 1, 'item_id': 1, 'monthlyorder': 1}
))
print(len(docs))

# --------------------------
# 5. 构建字典映射，加速查询
# --------------------------
monthlyorder_map = {}
for d in docs:
    if d.get("sku_id"):
        monthlyorder_map[d["sku_id"]] = safe_int(d.get("monthlyorder"))
    if d.get("item_id"):
        monthlyorder_map[d["item_id"]] = safe_int(d.get("monthlyorder"))

# --------------------------
# 6. 聚合统计差异
# --------------------------
n = sum_diff = sum_202510 = sum_monthly = diff_large = diff_zero = 0
updates = []
n2 = 0
for row in cursor:
    ID = row["商品ID"]
    _202510 = safe_int(row.get("202510月销量"))
    monthly_order = monthlyorder_map.get(ID, 0)

    sum_202510 += _202510
    sum_monthly += monthly_order

    diff = _202510 - monthly_order

    # 差异超过 100 的计数
    if abs(diff) > 0:
        n += 1
        sum_diff += diff

    w = 100
    # if monthly_order != 0 and abs(diff) > w:
    if monthly_order != 0 and _202510 == 0:
        diff_large += diff
        print([ID, monthly_order, _202510], diff)
        # m.update_many({"sku_id": ID,
        #                "month":202510,
        #                 "category_id":"MCO5844"},
        #               {"$set": {"monthlyorder": _202510}})
        # m.update_many({"item_id": ID,
        #                "month": 202510,
        #                "category_id": "MCO5844"
        #                }, {"$set": {"monthlyorder": _202510}})

        a = ID
        updates.append(a)
        n2 = n2+1
        # break

print(n2)
print(len(updates))

# --------------------------
# 7. 输出统计结果
# --------------------------
print("差异超过100的数量:", n)
print("差异总和:", sum_diff)
print("202510月销量总和:", sum_202510)
print("monthlyorder总和:", sum_monthly)
print(f"差异大于{w}的总和 (monthly_order != 0):", diff_large)

