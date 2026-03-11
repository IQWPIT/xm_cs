# -*- coding: utf-8 -*-
import os
import json
from collections import defaultdict

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

DST_MONTH = 202507

cat_ids = {
    'MLB7073': "ml_br",
    # 'MLB455251': "ml_br",
    # 'MLM437575': "ml_mx",
    # 'MLM455251': "ml_mx",
    # 'MLM420128': "ml_mx",
    # 'MLC5713': "ml_cl",
    # 'MCO5844': "ml_co",
    # "MLA5959": "ml_ar",
    # "MLA417835": "ml_ar",
}
cat_id_s = []
c_s = []
v = []
a = {}
for cat_id, site in cat_ids.items():

    col = get_collection("yingshi", "yingshi", f"{site}_monthly_sku")

    cursor = col.find(
        {
            "month": DST_MONTH,

        },
        {"category_id": 1, "categories": 1}
    )
    for doc in cursor:
        _id = doc["_id"]
        cat_id_q = doc["category_id"]
        c = doc["categories"]
        if cat_id_q not in cat_id_s:
            cat_id_s.append(cat_id_q)
        if c not in c_s:
            c_s.append(c)
        if cat_id_q == None:
            # col.update_one(
            #     {"_id": _id},
            # {"$set": {"category_id": "MLB7073"}},)
            v.append(doc)
        a[cat_id_q] = c
print(cat_id_s)
print(c_s)
print(v)
for i in v:
    print(i)
print(a)
print(len(a))