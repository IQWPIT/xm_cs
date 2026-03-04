import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
from bson import ObjectId
import pandas as pd
from loguru import logger
import datetime
from connector.m import M

m = M("ml_mx")
c_monthly_sku = m.c_sku
count = c_monthly_sku.find_one({
        "categories.cat_1": "Hogar, Muebles y Jardín",
        "outstock":0,
        "order30d":{"$gt": 5},
        "p_info_2.weight": {"$exists": True}
    })
print(count)
