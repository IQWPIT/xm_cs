import pandas as pd
import numpy as np
import os
import re
import random
from pymongo import UpdateOne
from bson import ObjectId

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# =================================================
# 1. 读取 Mongo 数据
# =================================================
site = "ml_cl"
c_monthly_sku = get_collection("yingshi", "yingshi", f"{site}_monthly_sku")

year_months = [202409,202410,202507, 202508, 202509, 202510]
brank_mx = ["tp-link"]

df_monthly_sku = pd.DataFrame(
    c_monthly_sku.find(
        {"month": {"$in": year_months},
         "brand": {
    "$in": [re.compile(f"^{b}$", re.IGNORECASE) for b in brank_mx]
}
         },
        {
            "_id": 1,
            "sku_id": 1,
            "item_id": 1,
            "month": 1,
            "category_id": 1,
            "monthlyorder": 1,
            "monthlygmv": 1,
            "lanjing_source": 1,
            "brand": 1
        }
    ).batch_size(10000)
)

df4 = df_monthly_sku.copy()

# =================================================
# 2. 计算价格（避免除零，NaN用0）
# =================================================
df4["price"] = df4.apply(
    lambda x: x["monthlygmv"] / x["monthlyorder"] if x["monthlyorder"] not in [0, None] else 0,
    axis=1
)

# =================================================
# 3. 生成宽表
# =================================================
df4["monthlyorder_col"] = df4["month"].astype(str) + "monthlyorder"
df4["monthlygmv_col"] = df4["month"].astype(str) + "monthlygmv"
df4["price_col"] = df4["month"].astype(str) + "price"
df4["_id_col"] = df4["month"].astype(str) + "_id"

df_wide_order = df4.pivot(index=["sku_id", "item_id"], columns="monthlyorder_col", values="monthlyorder")
df_wide_gmv   = df4.pivot(index=["sku_id", "item_id"], columns="monthlygmv_col", values="monthlygmv")
df_wide_price = df4.pivot(index=["sku_id", "item_id"], columns="price_col", values="price")
df_wide_id    = df4.pivot(index=["sku_id", "item_id"], columns="_id_col", values="_id")

df_wide = pd.concat([df_wide_order, df_wide_gmv, df_wide_price, df_wide_id], axis=1).reset_index()

# 补齐缺失列 + NaN用0
for m in year_months:
    df_wide[f"{m}monthlyorder"] = df_wide.get(f"{m}monthlyorder", 0).fillna(0)
    df_wide[f"{m}monthlygmv"]    = df_wide.get(f"{m}monthlygmv", 0).fillna(0)
    df_wide[f"{m}price"]         = df_wide.get(f"{m}price", 0).fillna(0)
    df_wide[f"{m}_id"]           = df_wide.get(f"{m}_id", np.nan)

# =================================================
# 4. 保存修正前价格
# =================================================
df_wide["202409price_before"] = df_wide["202409price"].copy()

# =================================================
# 5. 修正 202409price（排除原本为空）
# =================================================
valid_mask = df_wide["202409price_before"] != 0
cond = valid_mask & ((df_wide["202410price"] - df_wide["202409price"]) > 1)

df_wide.loc[cond, "202409price"] = df_wide.loc[cond, "202410price"]+200 #random.randint(1, 50)

# 更新 GMV
df_wide["202409monthlygmv"] = df_wide["202409price"] * df_wide["202409monthlyorder"]
df_wide["202409price"] = df_wide["202409price"].round(2)
df_wide["202409monthlygmv"] = df_wide["202409monthlygmv"].round(2)

# =================================================
# 6. 对比修改前后价格
# =================================================
df_wide["202409price_change"] = df_wide["202409price"] - df_wide["202409price_before"]

df_changed = df_wide[df_wide["202409price_change"].abs() > 0.01][[
    "sku_id", "item_id", "202409price_before", "202409price", "202409price_change"
]]
print("\n===== 被修正的 SKU 列表 =====")
print(df_changed)

# =================================================
# 7. 月度汇总
# =================================================
summary = []
for m in year_months:
    total_order = df_wide[f"{m}monthlyorder"].sum()
    total_gmv   = df_wide[f"{m}monthlygmv"].sum()
    summary.append({
        "month": m,
        "total_order": total_order,
        "total_gmv": total_gmv,
        "avg_price": round(total_gmv / total_order, 2) if total_order > 0 else 0
    })
df_summary = pd.DataFrame(summary)
print("\n===== 月度汇总（修正后）=====")
print(df_summary)

# =================================================
# 8. 批量写回 MongoDB（只写需要修改的行）
# =================================================
# df_to_modify = df_wide[df_wide["202409price_change"].abs() > 0.01][["202409monthlygmv", "202409_id"]].dropna(subset=["202409_id"])
# operations = []
#
# for idx, row in df_to_modify.iterrows():
#     operations.append(
#         UpdateOne(
#             {"_id": row["202409_id"]},
#             {"$set": {"monthlygmv": float(row["202409monthlygmv"])}}
#         )
#     )
#
# if operations:
#     result = c_monthly_sku.bulk_write(operations)
#     print(f"✅ 批量更新完成，匹配 {result.matched_count} 条，修改 {result.modified_count} 条记录")
# else:
#     print("⚠️ 没有需要更新的记录")

# =================================================
# 10. 单条更新 MongoDB（示例）
# =================================================
# target_id = ObjectId("6718a269c32def096c1d9e1f")
# row = df_wide[df_wide["202409_id"] == target_id]
#
# if not row.empty:
#     new_gmv = float(row["202409monthlygmv"].values[0])
#     c_monthly_sku.update_one(
#         {"_id": target_id},
#         {"$set": {"monthlygmv": new_gmv}}
#     )
#     print(f"✅ 已更新 _id={target_id} 的 202409monthlygmv 为 {new_gmv}")
# else:
#     print(f"⚠️ 未找到 _id={target_id} 对应的行")
