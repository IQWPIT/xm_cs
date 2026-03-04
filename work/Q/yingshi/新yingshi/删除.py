import os
import argparse
from pymongo import UpdateOne
from tqdm import tqdm
from loguru import logger
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
sites = [
    "ml_cl",
    "ml_co",
    "ml_mx",
    # "ml_br"
]
for site in tqdm(sites):

    c_monthly_sku = get_collection("yingshi", "yingshi", f"{site}_monthly_sku")


    filter_ = {
        # "j": 0,
        "month":202512
    }

    # 1️⃣ 先看会删多少
    count = c_monthly_sku.count_documents(filter_)
    print(f"{site}即将删除 {count} 条数据")

    # 2️⃣ 再删
    res = c_monthly_sku.delete_many(filter_)
    print(f"实际删除 {res.deleted_count} 条数据")