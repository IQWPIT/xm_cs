# -*- coding: utf-8 -*-
import os
import json
import re
import pandas as pd
from pymongo import UpdateOne
from loguru import logger
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

ml_br_monthly_sku = get_collection("yingshi", "yingshi", "_lj_data")

# =====================
# 配置
# =====================
folder_path = r"D:\https\work\Q\yingshi\新yingshi\补蓝鲸数据\蓝鲸数据\ml_br\EZVIZ_蓝鲸数据_202507_202511"

# 注意：这里改成“目录”
mla_json_dir = r"D:\https\work\Q\yingshi\新yingshi\补蓝鲸数据\蓝鲸数据\ml_br\EZVIZ_蓝鲸数据_202507_202511"

BATCH_SIZE = 2000

def bulk_write_in_batches(col, ops, batch_size=BATCH_SIZE, desc="bulk_write"):
    if not ops:
        return 0
    total = 0
    for i in tqdm(range(0, len(ops), batch_size), desc=desc):
        batch = ops[i:i + batch_size]
        col.bulk_write(batch, ordered=False)
        total += len(batch)
    return total

# =====================
# 1) 导入目录下 Excel/CSV -> upsert
# =====================
ops_import = []
files = [f for f in os.listdir(folder_path) if f.endswith((".xlsx", ".csv"))]
logger.info(f"发现数据文件数: {len(files)}")

for file in tqdm(files, desc="读取数据文件"):
    file_path = os.path.join(folder_path, file)
    try:
        if file.endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)

        df = df.where(pd.notnull(df), None)
        records = df.to_dict("records")

        for r in records:
            sku_id = r.get("商品ID")
            if not sku_id:
                continue

            ops_import.append(
                UpdateOne(
                    {"商品ID": sku_id},
                    {"$set": r},
                    upsert=True
                )
            )
    except Exception as e:
        logger.exception(f"读取失败: {file_path} err={e}")

written_cnt = bulk_write_in_batches(ml_br_monthly_sku, ops_import, desc="导入数据 upsert")
logger.info(f"导入写入完成: {written_cnt}")

# =====================
# 2) 遍历 JSON 目录 -> 解析 跟卖ID -> 更新 Mongo
# =====================
pattern_id = r"(ML[A-Z]\d+)"  # MLB / MLA / MLC / MLM / MCO... 通用

json_files = []
if os.path.isdir(mla_json_dir):
    json_files = [
        os.path.join(mla_json_dir, f)
        for f in os.listdir(mla_json_dir)
        if f.lower().endswith(".json")
    ]
else:
    logger.warning(f"mla_json_dir 不是目录或不存在: {mla_json_dir}")

logger.info(f"发现 json 文件数: {len(json_files)}")

# 用 dict 做去重：后读到的覆盖先读到的（你想保留第一次就改逻辑）
mapping = {}  # 商品ID -> 被跟卖商品ID

for json_path in tqdm(json_files, desc="读取json目录"):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            continue

        for text in data:
            if not isinstance(text, str):
                continue
            if "被跟卖商品ID" not in text:
                continue

            follow_m = re.search(rf"被跟卖商品ID.*?{pattern_id}", text)
            sku_m = re.search(rf"商品ID.*?{pattern_id}", text)

            if not follow_m or not sku_m:
                continue

            sku_id = sku_m.group(1)
            follow_id = follow_m.group(1)

            mapping[sku_id] = follow_id  # 覆盖式去重：同 sku_id 取最后一次

    except Exception as e:
        logger.exception(f"解析失败: {json_path} err={e}")

logger.info(f"汇总到 商品ID->被跟卖商品ID 映射数: {len(mapping)}")

ops_follow = [
    UpdateOne(
        {"商品ID": sku_id},
        {"$set": {"被跟卖商品ID": follow_id}},
        upsert=False   # 只更新已存在商品；需要不存在也插入就改 True
    )
    for sku_id, follow_id in mapping.items()
]

updated_cnt = bulk_write_in_batches(ml_br_monthly_sku, ops_follow, desc="更新 被跟卖商品ID")
logger.info(f"被跟卖商品ID 更新完成: {updated_cnt}")

print("全部完成 ✅")