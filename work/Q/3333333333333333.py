import os

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# ================== MongoDB collection ==================
ml_mx_new_buffer_v2 = get_collection("task_buffer", "task_buffer", "ml_mx_new_buffer_v2")
every_day_offer = get_collection("main_ml_mx", "ml_mx", "every_day_offer")

# ================== 获取 202511 的 item_id 并去重 ==================
cursor = ml_mx_new_buffer_v2.find(
    {"category_id": "MLM455251"},
    {"hisOffers": 1, "_id": 0}
)

item_ids_set = set()

for doc in cursor:
    his_offers = doc.get("hisOffers", {})
    for date_key, date_value in his_offers.items():
        if not date_key.startswith("202511"):
            continue
        if isinstance(date_value, list):
            for entry in date_value:
                if isinstance(entry, dict) and "item_id" in entry:
                    item_ids_set.add(entry["item_id"])
        elif isinstance(date_value, dict):
            for idx_dict in date_value.values():
                if isinstance(idx_dict, dict) and "item_id" in idx_dict:
                    item_ids_set.add(idx_dict["item_id"])

print("Total unique item_ids from 202511:", len(item_ids_set))

# ================== 查询 every_day_offer 表 ==================
# 使用 distinct 获取存在的唯一 pI
if item_ids_set:
    existing_item_ids = every_day_offer.distinct("pI", {"pI": {"$in": list(item_ids_set)}})
    existing_count = len(existing_item_ids)
else:
    existing_count = 0

print("Number of unique item_ids existing in every_day_offer.pI:", existing_count)
print(existing_item_ids)
