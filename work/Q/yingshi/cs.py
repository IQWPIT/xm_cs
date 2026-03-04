import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection
c_v = get_collection("task_buffer","task_buffer","ml_mx_new_buffer_v2")

sku_id = "MLM19949526"
doc = c_v.find_one(
    {"sku_id": sku_id},
    {"ysLastSortItem": 1, "_id": 0}
)

item = doc.get("ysLastSortItem") if doc else None

print(item)