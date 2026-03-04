import os
from pymongo import UpdateOne
from tqdm import tqdm
from pymongo import MongoClient
# ===================== 环境变量 =====================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

bi_category_data = get_collection("main_ml_mx", "ml_mx", "bi_category_data")
mongo_uri = "mongodb://erp:Damai20230214*.*@42.193.215.253:27017/?authMechanism=DEFAULT&authSource=erp&directConnection=true"
client = MongoClient(mongo_uri)
db = client["erp"]
bi_site_data = db["bi_category_data"]

bulk_ops = []
BATCH_SIZE = 1000

for d in tqdm(bi_site_data.find({})):
    bulk_ops.append(
        UpdateOne(
            {"_id": d["_id"]},
            {"$set": d},
            upsert=True
        )
    )

    if len(bulk_ops) >= BATCH_SIZE:
        bi_category_data.bulk_write(bulk_ops, ordered=False)
        bulk_ops.clear()

if bulk_ops:
    bi_category_data.bulk_write(bulk_ops, ordered=False)