import os
import pandas as pd

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection
time = 202510
yingshi = get_collection(
    "yingshi",
    "yingshi",
    f"ml_br_every_day_offer_{time}",
)
print("br",time)
df_monthly_sku = pd.DataFrame(
    yingshi.find(
        {},
        {
            "sI": 1,
            "dT": 1,
            "oD": 1,
            "_id": 0
        }
    )
)

# 同一 SKU + 日期 去重（保留第一条）
df_monthly_sku = df_monthly_sku.drop_duplicates(
    subset=["sI", "dT"]
)

# 确保 oD 是数值类型
df_monthly_sku["oD"] = pd.to_numeric(
    df_monthly_sku["oD"],
    errors="coerce"
)

# 按订单量倒序
df_monthly_sku = df_monthly_sku.sort_values(
    by="oD",
    ascending=False
)

print(df_monthly_sku.head())
# 打印前几行（你已经有）
print(df_monthly_sku.head())

# 只显示 oD 列
print(df_monthly_sku["oD"])

# 计算 oD 总和
total_od = df_monthly_sku["oD"].sum()
print("oD 总和：", total_od)
