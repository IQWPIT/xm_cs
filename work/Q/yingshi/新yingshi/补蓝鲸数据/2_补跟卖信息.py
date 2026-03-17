import os
import json
import pathlib
import re
from loguru import logger
from pymongo import UpdateOne
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# ======================
# 参数
# ======================
site = "ml_ar"
cat_id = "MLA417835"
time = 202512
BATCH_SIZE = 1000

c_yingshi_lanjing_rawdata = get_collection(
    "yingshi", "yingshi", f"{time}_{site}_{cat_id}"
)

# ======================
# 读取 JSON
# ======================
data = []
for path in pathlib.Path(f"{site}_datas/{cat_id}").glob("*.json"):
    with open(path, encoding="utf-8") as f:
        data.extend(json.load(f))

logger.info(f"load records: {len(data)}")

# ======================
# 解析商品ID关系
# ======================
res = []
商品ID_set = set()

for text in data:
    if "被跟卖商品ID" not in text:
        continue

    try:
        m1 = re.search(r"被跟卖商品ID[^A-Z]*(MLA\d+)", text)
        m2 = re.search(r"商品ID[^A-Z]*(MLA\d+)", text)
        if not m1 or not m2:
            continue

        被跟卖商品ID = m1.group(1)
        商品ID = m2.group(1)

        if 商品ID in 商品ID_set:
            continue

        商品ID_set.add(商品ID)
        res.append({
            "商品ID": 商品ID,
            "被跟卖商品ID": 被跟卖商品ID
        })

    except Exception as e:
        logger.warning(f"parse failed: {e!r}, {text!r}")

logger.info(f"valid relations: {len(res)}")

# ======================
# 批量更新 MongoDB
# ======================
req = []
for i in res:
    req.append(
        UpdateOne(
            {
                "商品ID": i["商品ID"],
                "month": time
            },
            {
                "$set": {
                    "被跟卖商品ID": i["被跟卖商品ID"]
                }
            }
        )
    )

    if len(req) >= BATCH_SIZE:
        c_yingshi_lanjing_rawdata.bulk_write(req, ordered=False)
        req.clear()

if req:
    c_yingshi_lanjing_rawdata.bulk_write(req, ordered=False)

logger.info("bulk update finished")
