import os
from itertools import chain

# ================== 环境变量 ==================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# ================== MongoDB collection ==================
ml_mx_new_buffer_v2 = get_collection("task_buffer", "task_buffer", "ml_mx_new_buffer_v2")
every_day_offer = get_collection("main_ml_mx", "ml_mx", "every_day_offer")

# ================== 查询数据 ==================
cursor = ml_mx_new_buffer_v2.find(
    {"category_id":"MLM455251"},
    {"ysLastSortItem": 1, "_id": 0}
)

# ================== 扁平化 ysLastSortItem ==================
# 使用 itertools.chain.from_iterable 更高效地扁平化嵌套列表
ys_items = list(chain.from_iterable(item.get("ysLastSortItem", []) for item in cursor))

# ================== 输出 ==================
print(*ys_items, sep="\n")  # 每个元素单独换行打印
print("Total items:", len(ys_items))
# ================== 查询 every_day_offer 表 ==================
# 使用 distinct 获取存在的唯一 pI
if ys_items:
    existing_item_ids = every_day_offer.distinct("pI", {"pI": {"$in": list(ys_items)}})
    existing_count = len(existing_item_ids)
else:
    existing_count = 0

print("Number of unique item_ids existing in every_day_offer.pI:", existing_count)
print(existing_item_ids)