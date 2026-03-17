import os
from tqdm import tqdm
from pymongo import UpdateOne
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
_c_visual_plus = get_collection("main_ml_mx", "ml_mx", "_c_visual_plus")
cat = get_collection("main_ml_mx", "ml_mx", "cat")


# 1️⃣ 一次性把 cat 集合全部加载到内存
cat_docs = cat.find({}, {"id": 1, "path": 1})
cat_map = {}
for doc in cat_docs:
    if "path" in doc:
        cat_map[doc["id"]] = doc["path"]  # path 转成列表，按顺序存储

# 2️⃣ 构建父子关系
c = {}

cat_ids_cursor = visual_plus.find({}, {"cat_id": 1})

for doc in tqdm(cat_ids_cursor, desc="Processing cat_ids"):
    cat_id = doc["cat_id"]

    if cat_id not in cat_map:
        continue

    path_values = cat_map[cat_id]

    if cat_id not in path_values:
        continue

    idx = path_values.index(cat_id)
    parent_ids = path_values[:idx]

    for parent_id in parent_ids:
        if parent_id not in c:
            c[parent_id] = []
        c[parent_id].append(cat_id)

print(c["MLM1000"])

# -------------------------
# 写入 visual_plus 集合
# -------------------------
batch_size = 1000
operations = []
a = []
for parent_id, child_list in tqdm(c.items(), desc="Preparing update operations"):
    print("父子关系构建完成，总父类数:", parent_id,len(c[f"{parent_id}"]))
    a.append(parent_id)
    operations.append(
        UpdateOne(
            {"cat_id": parent_id},
            {"$set": {"cat": child_list}},  # 可以改字段名，比如 "cat"
            upsert=True  # 如果不存在父类文档，则创建
        )
    )

    if len(operations) >= batch_size:
        _c_visual_plus.bulk_write(operations)
        print(f"写入成功1{parent_id}")
        operations = []


# 写入剩余操作
if operations:
    _c_visual_plus.bulk_write(operations)
    print(f"写入成功2{parent_id}")

print(a)
print(len(a))