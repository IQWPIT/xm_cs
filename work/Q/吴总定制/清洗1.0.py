# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
import numpy as np
from pymongo import MongoClient
from model_patterns import model_patterns
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
# =========================
# 配置
# =========================
BRAND_FILE = r"品牌字典.csv"
OUTPUT_FILE = r"vivo_shopee_解析.xlsx"

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"


DB_NAME = "customize_common"
SRC_COLLECTION = "vivo_shopee"
DST_COLLECTION = "vivo_shopee_parsed"

# =========================
# MongoDB 连接
# =========================
src_col = get_collection("ml",DB_NAME,SRC_COLLECTION)
dst_col = get_collection("ml",DB_NAME,DST_COLLECTION)
# =========================
# 1. 拉取数据
# =========================
cursor = src_col.find(
    {
        # "itemid":23294059720
    },
    {
        "_id": 0,
        "itemid": 1,
        "item_basic.brand": 1,
        "item_basic.sold": 1,
        "item_basic.price": 1,
        "item_basic.shop_name": 1,
        "item_basic.name": 1,
    }
)

data = list(cursor)
if not data:
    raise RuntimeError("Mongo 中无数据")

df_raw = pd.DataFrame([d.get("item_basic", {}) for d in data])
df_raw["itemid"] = [d.get("itemid") for d in data]

# =========================
# 2. 字段标准化
# =========================
df = pd.DataFrame({
    "item_id": df_raw["itemid"],
    "name": df_raw.get("name", ""),
    "brand": df_raw.get("brand"),
    "shop": df_raw.get("shop_name"),
    "sales": df_raw.get("sold", 0),
    "price": df_raw.get("price", 0) / 100000,
})

# =========================
# 3. 品牌字典（只增不减）
# =========================
def append_new_brands(brands, brand_file):
    brands = {str(b).strip() for b in brands if b and str(b).strip()}
    if not brands:
        return

    exist_brands = set()
    if os.path.exists(brand_file):
        try:
            df_exist = pd.read_csv(brand_file)
            if not df_exist.empty:
                col = "品牌" if "品牌" in df_exist.columns else df_exist.columns[0]
                exist_brands = set(df_exist[col].dropna().astype(str).str.strip())
        except Exception as e:
            print(f"⚠️ 读取 brand.csv 失败，忽略历史品牌：{e}")

    new_brands = brands - exist_brands
    if not new_brands:
        return

    df_new = pd.DataFrame(sorted(new_brands), columns=["品牌"])
    write_header = not os.path.exists(brand_file)
    df_new.to_csv(brand_file, mode="a", header=write_header, index=False, encoding="utf-8-sig")
    print(f"🆕 新增品牌写入 brand.csv：{len(new_brands)} 个")

append_new_brands(df["brand"].dropna(), BRAND_FILE)

# =========================
# 4. 填充空品牌
# =========================
brand_dict = pd.read_csv(BRAND_FILE, encoding="utf-8-sig")["品牌"].dropna().astype(str).tolist()
brand_pattern = re.compile("|".join(rf"\b{re.escape(b)}\b" for b in brand_dict), re.IGNORECASE)

def fill_brand(row):
    if row["brand"] and str(row["brand"]).strip():
        return row["brand"]
    m = brand_pattern.search(str(row["name"]))
    return m.group(0) if m else "Other"

df["brand"] = df.apply(fill_brand, axis=1)

# =========================
# 5. 型号解析（限制正则范围，避免错匹配）
# =========================
def parse_model(name):
    name = str(name).upper()
    # 限定 vivo/iQOO/V/Y/X/S/T 型号
    patterns = [r"\bIQOO\s?\d+\b", r"\bV\d+\b", r"\bY\d+\b",
                r"\bX\d+\b", r"\bS\d+\b", r"\bT\d+\b"]
    for p in patterns:
        m = re.search(p, name)
        if m:
            return m.group(0)
    # fallback 用 model_patterns
    for p in model_patterns:
        m = re.search(p, name, re.IGNORECASE)
        if m:
            return m.group(1) if m.lastindex else m.group(0)
    return "Unknown"

df["model"] = df["name"].apply(parse_model)

# =========================
# 6. 存储 / 内存 / 网络解析（正则严格匹配）
# =========================
def parse_storage_ram_net(name):
    name = str(name)
    # 存储: 3位以上数字+GB 或 1位数字+T
    storage_match = re.search(
        r"((?:\d{1,2}[+/-])?\d{3,} *(?:GB|Gb|ROM|TB|T))",
        name, re.IGNORECASE
    )
    # 内存: 1-2位数字 + GB，后面可能带 RAM
    ram_match = re.search(r"\b([1-9]|[1-5]\d)\s*(GB|Gb)(?:\s*RAM)?\b", name, re.IGNORECASE)

    net_match = re.search(r"\b(4G|5G)\b", name, re.IGNORECASE)
    return (
        # storage_match.group(1).replace(" ", "") if storage_match else "未知",
        # ram_match.group(1).replace(" ", "") if ram_match else "未知",
        net_match.group(1) if net_match else "未知"
    )
df[["net_match"]] = df["name"].apply(lambda x: pd.Series(parse_storage_ram_net(x)))
# df[["storage", "ram", "network"]] = df["name"].apply(lambda x: pd.Series(parse_storage_ram_net(x)))

def gb_to_int(s):
    if s and isinstance(s, str):
        m = re.search(r"(\d+)", s)
        return int(m.group(1)) if m else None
    return None

# df["storage_gb"] = df["storage"].apply(gb_to_int)
# df["ram_gb"] = df["ram"].apply(gb_to_int)

# =========================
# 7. 价格段
# =========================
bins = [0,1000,1500,2000,2500,3000,4000,5000,6000,8000,10000,1e9]
labels = ["1000","1500","2000","2500","3000","4000","5000","6000","8000","10000","10000+"]
df["price_range"] = pd.cut(df["price"], bins=bins, labels=labels, right=False)

# =========================
# 8. 不过滤，保留所有数据
# =========================
df_final = df.copy()

# =========================
# 9. 导出 Excel
# =========================
df_final.to_excel(OUTPUT_FILE, index=False)
print(f"✅ 已导出 Excel：{OUTPUT_FILE}")
print(df_final.head())

# =========================
# 10. 写入 MongoDB
# =========================
df_final = df_final.loc[:, ~df_final.columns.duplicated()]
df_final = df_final.replace({np.nan: None})

records = df_final.to_dict("records")

try:
    dst_col.delete_many({})
    dst_col.insert_many(records)
    print(f"✅ Mongo 写入成功：{DB_NAME}.{DST_COLLECTION}")
    print(f"📊 写入条数：{len(records)}")
except Exception as e:
    print("❌ Mongo 写入失败：", e)
    import traceback
    traceback.print_exc()
