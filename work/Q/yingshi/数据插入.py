import pathlib

import pandas as pd
from pymongo import UpdateOne
import os
from connector.m import M
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

cat_id = "MLM420128"
name = "_ml_br"
_ml_mx_l = get_collection("yingshi","yingshi",name)
df = pd.concat([pd.read_excel(i) for i in pathlib.Path("MLM420128").glob("*.xlsx")], ignore_index=True)
print(df.shape)
df = df.drop_duplicates(subset=["商品ID"])
req = []
for ind, row in df.iterrows():
    row_item = row.to_dict()
    update_doc = {
        "cat_id": cat_id,
        **row_item,
    }
    req.append(UpdateOne(filter={"商品ID": row_item["商品ID"]}, update={"$set":update_doc,}, upsert=True))
    if len(req) > 1000:
        _ml_mx_l.bulk_write(req)
        req.clear()
    # break
if req:
    _ml_mx_l.bulk_write(req)