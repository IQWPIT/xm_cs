import os
import argparse
from pymongo import UpdateOne
from tqdm import tqdm
from loguru import logger
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

yingshi_sku = get_collection("yingshi","yingshi","ml_br_monthly_sku")
datas = yingshi_sku.find({
  "item_id": { "$ne": None },"sku_id": { "$ne": None },
  "month": 202512
})
for data in tqdm(datas):
    item_id = data["item_id"]
    sku_id = data["sku_id"]
    _id =  data["_id"]
    if _id == None:
        continue
    yingshi_sku.update_one(
        {
            "_id": _id,
            "month": 202512,
            "item_id": item_id,
        },
        {
            "$set": {
                "sku_id":None
            }
        }
    )