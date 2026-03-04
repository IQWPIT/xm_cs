import os
import ast
import shutil
import traceback
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import pymongo
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
every_day_sku = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
cat_col = get_collection("main_ml_mx", "ml_mx", "cat")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")

TIMES_LIST = [202510, 202511, 202512]
TIME_MAP = {
    202510: 2510,
    202511: 2511,
    202512: 2512,
}
def get_cat_ids(cat_id):
    cat_ids = visual_plus.find_one({"cat_id":cat_id},{"cat":1})
    return cat_ids["cat"]

visual_plus_cat_ids = visual_plus.find({},{"cat_id":1})
for d in visual_plus_cat_ids:
    for time in TIMES_LIST:
        visual_plus_cat_id = d["cat_id"]
        cat_ids = get_cat_ids(visual_plus_cat_id)
        sku_datas = sku_col.find(
        {
                "category_id": {"$in": cat_ids},
            },
            {
                "_id": 0,
                "sku_id": 1,  # 商品id
                "category_id": 1,  # 类目id
                "brand": 1,  # 品牌
                "sellerCbt": 1,  #
                "sellerID": 1,
                "sellerName": 1,  # 店铺名称
                "sellerType": 1,  # 店铺类型
                "stock_type": 1,  # 发货仓
                "offersInf": 1,  # 跟卖数
                "monthly_sale_trend": 1,  # 月份销量
                "active_price": 1,  # 均价
                "conversion_all_order": 1,  # 访问销量
                "conversion_all_view": 1,  # 访问量
                "rating_avg": 1,  # 评分
                "date_created": 1,  # 上架时间
                "puton_date": 1,  # 上架时间

            }
        )
        for sku_data in sku_datas:
            sku_id = sku_data["sku_id"]
            brand = sku_data["brand"]
            sellerName = sku_data["sellerName"]
            sellerType = sku_data["sellerType"]
            stock_type = sku_data["stock_type"]
            offersInf = sku_data["offersInf"]
            monthly_sale_trend = sku_data["monthly_sale_trend"]
            active_price = sku_data["active_price"]
            conversion_all_order = sku_data["conversion_all_order"]
            conversion_all_view = sku_data["conversion_all_view"]
            rating_avg = sku_data["rating_avg"]
            date_created = sku_data["date_created"]
            puton_date = sku_data["puton_date"]







