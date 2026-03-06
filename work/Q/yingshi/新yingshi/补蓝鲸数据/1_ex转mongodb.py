import os
import pathlib
import pandas as pd
from loguru import logger
from pymongo import UpdateOne
from tqdm import tqdm

# =========================
# 环境 & Mongo
# =========================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

# =========================
# 配置
# =========================
SITE = "ml_ar"
CAT_ID = "MLA417835"
TIME = 202512
BATCH_SIZE = 1000

DATA_DIR = pathlib.Path(f"{SITE}_datas/{CAT_ID}")

# =========================
# MongoDB collections
# =========================
c_yingshi_lanjing_rawdata = get_collection(
    "yingshi", "yingshi", f"{TIME}_{SITE}_{CAT_ID}"
)

# =========================
# 读取 Excel
# =========================
# 忽略 Excel 临时文件（~$开头）
excel_files = [fp for fp in DATA_DIR.glob("*.xlsx") if not fp.name.startswith("~$")]

if not excel_files:
    raise FileNotFoundError(f"No xlsx files found in {DATA_DIR}")

logger.info(f"Loading {len(excel_files)} excel files")

dfs = []
for fp in excel_files:
    try:
        dfs.append(pd.read_excel(fp))
    except Exception as e:
        logger.warning(f"Failed to read {fp}: {e}")

df = pd.concat(dfs, ignore_index=True)
logger.info(f"Total rows loaded: {len(df)}")

# =========================
# 查看数据示例
# =========================
print(df[["商品ID", "202512月销量"]].head(10))

# =========================
# 批量写入 Mongo
# =========================
requests: list[UpdateOne] = []

for row in tqdm(df.itertuples(index=False, name=None), total=len(df)):
    # 用 zip(df.columns, row) 保留原始列名
    row_dict = dict(zip(df.columns, row))

    商品ID = row_dict.get("商品ID")
    if not 商品ID:
        continue

    requests.append(
        UpdateOne(
            {"商品ID": 商品ID},
            {"$set": row_dict},
            upsert=True
        )
    )

    if len(requests) == BATCH_SIZE:
        try:
            c_yingshi_lanjing_rawdata.bulk_write(requests, ordered=False)
        except Exception as e:
            logger.exception(f"Bulk write failed, batch size={len(requests)}")
        finally:
            requests.clear()

# =========================
# 写入剩余
# =========================
if requests:
    try:
        c_yingshi_lanjing_rawdata.bulk_write(requests, ordered=False)
    except Exception as e:
        logger.exception(f"Final bulk write failed, batch size={len(requests)}")

logger.success("Excel data successfully written to MongoDB")
