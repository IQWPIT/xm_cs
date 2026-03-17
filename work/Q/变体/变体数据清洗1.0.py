import os
from collections import defaultdict
import pymysql
import pandas as pd
from pymongo import MongoClient

# --------------------------------------------------------------------------------------------------
# 环境变量
# --------------------------------------------------------------------------------------------------
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
# --------------------------------------------------------------------------------------------------
# MySQL 连接函数
# --------------------------------------------------------------------------------------------------
def get_mysql_conn():
    return pymysql.connect(
        host="114.132.88.73",
        user="root",
        password="damaitoken@2026#",
        database="DM_spiders",
        port=3306,
        charset="utf8mb4"
    )

def update_task_status(task_id, status, _id, src_col):
    """更新 MySQL 任务状态"""
    try:
        conn = get_mysql_conn()
        with conn.cursor() as cursor:
            sql = "UPDATE lj_variation_task SET status=%s WHERE id=%s"
            cursor.execute(sql, (status, task_id))
        conn.commit()
        src_col.update_one({"_id": _id}, {"$set": {"status": status}})
    except Exception as e:
        print(f"MySQL 更新失败 task_id={task_id} status={status} 错误：{e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------------------------
# MySQL 拉取任务
# --------------------------------------------------------------------------------------------------
conn = get_mysql_conn()
sql = """
SELECT
    id,
    item_id,
    site_id,
    state,
    status,
    create_time,
    update_time
FROM lj_variation_task
WHERE state = 1 
ORDER BY update_time DESC
"""
df = pd.read_sql(sql, conn)
conn.close()

print(f"MySQL 读取任务数：{len(df)}")

# --------------------------------------------------------------------------------------------------
# MongoDB 连接
# --------------------------------------------------------------------------------------------------
MONGO_URI = (
    "mongodb://common:ase5yDFHG%24%25FDSdif%40%23GH@localhost:10001/lj"
    "?authMechanism=SCRAM-SHA-1&authSource=admin&directConnection=true"
)
client = MongoClient(MONGO_URI)
db = client["lj"]
src_col = db["lj_variation_info"]

SITE_ID_MAP = {
    "MLM": "ml_mx",
    "MLB": "ml_br",
    "MLC": "ml_cl",
    "MCO": "ml_co",
    "MLA": "ml_la"
}

# --------------------------------------------------------------------------------------------------
# 主处理逻辑
# --------------------------------------------------------------------------------------------------
for _, row in df.iterrows():
    task_id = row["id"]
    item_id = row["item_id"]
    site_code = row["site_id"]
    site = SITE_ID_MAP.get(site_code)

    print(f"\n处理 item_id={item_id} site={site}")

    # ----------------------------------------------------------------------------------------------
    # 查询 MongoDB，只取最新插入的一条
    # ----------------------------------------------------------------------------------------------
    doc = src_col.find_one(
        {"mysql_task_id": task_id, "item_id": item_id, "status": {"$exists": False}},
        sort=[("_id", -1)]
    )
    # doc = src_col.find_one(
    #     {"mysql_task_id": task_id, "item_id": item_id},
    #     sort=[("_id", -1)]
    # )
    if doc is None:
        print("task_id:", task_id,"item_id:", item_id)
        continue
    _id = doc["_id"]

    if not doc:
        print("Mongo 未找到数据，标记任务失败")
        update_task_status(task_id, -1, _id, src_col)
        continue

    try:
        variationInfo = doc["response"]["data"]["data"]["variationInfo"]
    except Exception:
        print("数据结构异常，标记任务失败")
        update_task_status(task_id, -1, _id, src_col)
        continue

    # ----------------------------------------------------------------------------------------------
    # 统计结构
    # ----------------------------------------------------------------------------------------------
    attr_stat = defaultdict(lambda: defaultdict(lambda: {
        "soldQuantity": 0,
        "sold_true": 0,
        "picture_ids": []
    }))
    combo_stat = defaultdict(lambda: {
        "soldQuantity": 0,
        "sold_true": 0,
        "picture_ids": []
    })

    total_sold_true = 0
    total_soldQuantity = 0

    # ----------------------------------------------------------------------------------------------
    # variation 聚合
    # ----------------------------------------------------------------------------------------------
    for v in variationInfo:
        soldQuantity = v.get("soldQuantity", 0)
        sold_true = v.get("sold_true", 0)
        picture_ids = v.get("pictures", [])
        attrs = v.get("attrs", [])

        total_sold_true += sold_true
        total_soldQuantity += soldQuantity

        combo_key_parts = []
        for attr in attrs:
            name = attr.get("name")
            value = attr.get("value_name")
            if not name or not value:
                continue

            attr_stat[name][value]["soldQuantity"] += soldQuantity
            attr_stat[name][value]["sold_true"] += sold_true
            attr_stat[name][value]["picture_ids"].extend(picture_ids)

            combo_key_parts.append(f"{name}:{value}")

        if combo_key_parts:
            combo_key = "|".join(combo_key_parts)
            combo_stat[combo_key]["soldQuantity"] += soldQuantity
            combo_stat[combo_key]["sold_true"] += sold_true
            combo_stat[combo_key]["picture_ids"].extend(picture_ids)

    # ----------------------------------------------------------------------------------------------
    # 生成 variationInfo
    # ----------------------------------------------------------------------------------------------
    variations3 = []

    # ① 多属性组合
    combo_list = []
    for combo_key, data in combo_stat.items():
        sold_true_ratio = round(data["sold_true"] / total_sold_true, 4) if total_sold_true else 0
        soldQuantity_ratio = round(data["soldQuantity"] / total_soldQuantity, 4) if total_soldQuantity else 0

        # 正确拼接组合属性值
        attr_value = "/".join([part.split(":", 1)[1] for part in combo_key.split("|")])

        combo_list.append({
            "attr_name": "Color/Talla",
            "attr_value": attr_value,
            "pictures": list(set(data["picture_ids"])),
            "soldQuantity": data["soldQuantity"],
            "sold_true": data["sold_true"],
            "sold_true_ratio": sold_true_ratio,
            "soldQuantity_ratio": soldQuantity_ratio
        })
    variations3.append(combo_list)

    # ② 单属性
    single_attr_lists = []
    for attr_name, values in attr_stat.items():
        cur_list = []
        for attr_value, data in values.items():
            sold_true_ratio = round(data["sold_true"] / total_sold_true, 4) if total_sold_true else 0
            soldQuantity_ratio = round(data["soldQuantity"] / total_soldQuantity, 4) if total_soldQuantity else 0

            cur_list.append({
                "attr_name": attr_name,
                "attr_value": attr_value,
                "pictures": list(set(data["picture_ids"])),
                "soldQuantity": data["soldQuantity"],
                "sold_true": data["sold_true"],
                "sold_true_ratio": sold_true_ratio,
                "soldQuantity_ratio": soldQuantity_ratio
            })
        single_attr_lists.append(cur_list)

    # 固定顺序（Talla → Color）
    for name in ["Talla", "Color"]:
        for lst in single_attr_lists:
            if lst and lst[0]["attr_name"] == name:
                variations3.append(lst)
                break

    # ----------------------------------------------------------------------------------------------
    # 输出
    # ----------------------------------------------------------------------------------------------
    result = {
        "item_id": item_id,
        "site": site,
        "variationInfo": variations3
    }
    print(result)
    # ----------------------------------------------------------------------------------------------
    # 更新 MySQL 状态：成功
    # ----------------------------------------------------------------------------------------------
    update_task_status(task_id, 1, _id, src_col)
    main_ = get_collection(f"main_{site}",f"{site}","sku")
    main_.update_one({"item_id": item_id}, {"$set": {"variations3": variations3}})

print(f"\n处理完成，共 {len(df)} 条任务")
