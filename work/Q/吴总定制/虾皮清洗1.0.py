from pymongo import MongoClient, UpdateOne
import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# MongoDB URI
uri = "mongodb://erp:Damai20230214*.*@localhost:37043/?authMechanism=SCRAM-SHA-1&directConnection=true&readPreference=primary&authSource=admin"

client = MongoClient(uri)
db_name = "damai_shelf"
dst_col = get_collection("ml","customize_common","vivo_shopee_parsed_2")

times = [
    #202505,
    202506,202507,202508,202509,202510,202511,202512,
         202601
         ]
TIME = {
    202505:"_5",202506:"_6",202507:"_7",202508:"_8",202509:"_9",
    202510:"_10",202511:"_11",202512:"_12",202601:"_1",
}

price_ranges = [
    (0,1000), (1000,1500), (1500,2000), (2000,2500), (2500,3000),
    (3000,4000), (4000,5000), (5000,6000), (6000,8000), (8000,10000)
]
price_labels = [
    "0~1000","1000~1500","1500~2000","2000~2500","2500~3000",
    "3000~4000","4000~5000","5000~6000","6000~8000","8000~10000","10000+"
]

def get_price_range_index(price):
    for idx, (low, high) in enumerate(price_ranges):
        if low <= price < high:
            return idx
    return len(price_labels) - 1

def default_price_trend():
    return [{"od":0,"gmv":0} for _ in range(11)]

db = client[db_name]

for t in times:
    print(t)
    src_db_name = f"shopee_data_br{TIME[t]}"
    # src_db_name = f"amazon_data_br{TIME[t]}"
    print(src_db_name)
    updates = []

    for data in db[src_db_name].find():
        #虾皮
        shopId = data.get("shopId")
        productId = data.get("productId")
        price = data.get("price_number", 0)
        name = data.get("name", "")
        order30d = data.get("order30d", 0)
        total_gmv = int(price * order30d)
        # #亚马逊
        # shopId = data.get("asin")
        # productId = data.get("uuid")
        # price = data.get("price_number", 0)
        # name = data.get("name", "")
        # order30d = data.get("order30d", 0)
        # total_gmv = int(price*order30d)

        price_idx = get_price_range_index(price)
        month_str = str(t)

        trend = default_price_trend()
        trend[price_idx]["od"] = order30d
        trend[price_idx]["gmv"] = total_gmv

        bInfo = {

                "order": order30d,
                "gmv": total_gmv,
                "priceTrend": trend

        }

        output = {
            "sku_id": productId,
            "spu_title": name,
            "brand": data.get("brand", ""),
            "sellerID": shopId,
            f"price_l.{str(t)}": price,
            f"order30d_l.{str(t)}":order30d,
            f"bInfo.{str(t)}": bInfo,
            "price":price,

            "picture": data.get("imageUrl", ""),
            "URL": data.get("productUrl", ""),
            "score_count": data.get("discount", 0),
            "score": data.get("rating", 0),
        }

        updates.append(UpdateOne(
            {"sellerID": shopId, "sku_id": productId},
            {"$set": output}, upsert=True
        ))

    if updates:
        dst_col.bulk_write(updates)