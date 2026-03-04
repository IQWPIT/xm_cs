from pymongo import MongoClient, UpdateOne
import os
from tqdm import tqdm
# ====== 环境变量 ======
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
# ====== MongoDB 连接 ======
mongo_uri = "mongodb://erp:Damai20230214*.*@42.193.215.253:27017/?authMechanism=DEFAULT&authSource=erp&directConnection=true"
client = MongoClient(mongo_uri)
db = client['erp']  # 对应 authSource

# 测试连接
print("数据库列表:", client.list_database_names())

# ====== 选择集合 ======
collection = db['bi_site_data']  # 源数据集合
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")  # 目标集合

# ====== 查询 cat_id ======
cat_data = visual_plus.find({}, {"cat_id": 1})


# ====== 批量更新操作 ======
operations = []

for i in tqdm(cat_data, desc="Processing cat_id"):
    print(i)
    cat_id = i["cat_id"]

    # 查询 bi_category_data 中每个 cat_id 的月度指标
    avg_cursor = collection.find(
        {"cat_id": cat_id},
        {
            # "site_id": 1,
            "month": 1,
            "acos": 1,
            "cost": 1,
            "cpc": 1,
            "ctr": 1,
            "refund_rate": 1
        }
    )
    print(avg_cursor)

    for j in avg_cursor:
        month = j.get("month")
        if not month:
            continue  # 跳过没有 month 的数据

        operations.append(
            UpdateOne(
                {"cat_id": cat_id},
                {"$set": {
                    f"bi_category_data.{month}": {
                        "avg_acos": j.get("acos"),
                        "avg_cost": j.get("cost"),
                        "avg_cpc": j.get("cpc"),
                        "avg_ctr": j.get("ctr"),
                        "refund_rate": j.get("refund_rate")
                    }
                }},
                upsert=True
            )
        )

# ====== 批量写入 ======
if operations:
    print(f"总更新操作数: {len(operations)}")
    result = visual_plus.bulk_write(operations)
    print(f"匹配文档数: {result.matched_count}")
    print(f"修改文档数: {result.modified_count}")
    print(f"插入文档数: {result.upserted_count}")
else:
    print("没有需要更新的数据")
