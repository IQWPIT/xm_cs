import os
from pymongo import UpdateOne
from tqdm import tqdm

# ===================== 环境变量 =====================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

_big_Excel = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
cat = get_collection("main_ml_mx", "ml_mx", "cat")

time_list = [
    202411, 202412, 202501, 202502, 202503, 202504, 202505, 202506,
    202507, 202508, 202509, 202510, 202511, 202512,202601
]

ops = []
BATCH_SIZE = 1000

cursor = cat.find({}, {"path": 1, "id": 1,"leaf":1})

for i in tqdm(cursor, desc="Processing cat"):
    cat_id = i["id"]
    print(cat_id)
    path = i.get("leaf")
    paths = i.get("path")
    for j in time_list:
        ops.append(
            UpdateOne(
                {"cat_id": cat_id, "time": j},
                {"$set": {
                    "leaf": path,
                    "path": paths
                }},
                upsert=True
            )
        )

    # 分批写入
    if len(ops) >= BATCH_SIZE:
        _big_Excel.bulk_write(ops, ordered=False)
        ops.clear()

# 写入剩余
if ops:
    _big_Excel.bulk_write(ops, ordered=False)

print("✅ 更新完成")
