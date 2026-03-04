import requests
import base64
import gzip
import json
import os
import pandas as pd
from tqdm import tqdm
# ================== 环境变量 ==================
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection



# ================== Mongo ==================
src_col = get_collection("ml", "customize_common", "wu_custom_made_shopee")

# ================== API 配置 ==================
url = "https://standardapi.sorftime.com/api/ProductTrend?domain=208"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Authorization": "BasicAuth atdomjnkzk5ymlgxz2jjcfe2tjvtut09"
}

# ================== 工具函数 ==================

def trend_list_to_dict(trend_list):
    if not trend_list:
        return {}
    return dict(zip(trend_list[::2], trend_list[1::2]))


def decode_response(response):
    content = response.content
    try:
        compressed_bytes = base64.b64decode(content)
        try:
            decompressed_bytes = gzip.decompress(compressed_bytes)
        except:
            decompressed_bytes = compressed_bytes
    except:
        try:
            decompressed_bytes = gzip.decompress(content)
        except:
            decompressed_bytes = content

    return decompressed_bytes.decode("utf-8")


def replace_image_url(url):
    """
    替换图片链接前缀
    """
    if pd.isna(url):
        return url

    return url.replace(
        "https://down-br.img.susercontent.com/file/",
        "https://save.sorftime.com/toolexcleimage/"
    )


# ================== 读取 Excel ==================

file_path = "选产品详情-20260225141512 (1).xlsx"  # 改成你的路径

df = pd.read_excel(file_path, header=1)  # 列名在第二行

# 替换主图链接
if "主图" in df.columns:
    df["主图"] = df["主图"].apply(replace_image_url)

# 读取产品ID
product_ids = df["产品ID"].dropna().astype(str).tolist()

print("读取到产品数量:", len(product_ids))


# ================== 批量调用 API ==================

for product_id in tqdm(product_ids):
    try:
        payload = {
            "ProductId": product_id
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code != 200:
            print("请求失败:", product_id)
            continue

        decompressed_str = decode_response(response)
        data = json.loads(decompressed_str)

        if data.get("Code") != 0:
            print("API错误:", product_id)
            continue

        # 转趋势结构
        trend_fields = [
            "SaleCountTrend",
            "SaleTotalCountTrend",
            "PriceTrend",
            "ReviewCountTrend",
            "StarTrend"
        ]

        for field in trend_fields:
            if field in data["Data"]:
                data["Data"][field] = trend_list_to_dict(data["Data"][field])

        # 写入 Mongo
        src_col.update_one(
            {"item_id": product_id},
            {"$set": data},
            upsert=True
        )

    except Exception as e:
        print("异常:", product_id, str(e))

print("全部完成")