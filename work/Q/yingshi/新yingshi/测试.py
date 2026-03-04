import requests
import pandas as pd

from yingshi.ml_mx_品牌 import site

# ==========================
# 配置
# ==========================
url = "http://119.91.253.91:3400/monthly-listing"
token = "yingshi20240419"
month = 202601
limit = 10000  # 每页条数

all_df = []
skip = 0
site = "ml_ar"
while True:
    params = {
        "skip": skip,
        "limit": limit,
        "token": token,
        "month": month,
        "site": site,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data_json = response.json()
    except Exception as e:
        print(f"请求失败 skip={skip}: {e}")
        break

    if not data_json:
        break

    # 转成 DataFrame，不展开 category
    if isinstance(data_json, dict):
        temp_df = pd.DataFrame([data_json])
    else:
        temp_df = pd.DataFrame(data_json)

    all_df.append(temp_df)
    print(f"已获取数据条数: {len(temp_df)} skip={skip}")
    skip += limit

# 合并所有页
df_final = pd.concat(all_df, axis=0, ignore_index=True)
print("抓取总数据量:", len(df_final))

# # ==========================
# # 筛选 cpid 不存在或为空的记录
# # ==========================
# df_no_cpid = df_final[
#     (~df_final.get('cpid', pd.Series([None]*len(df_final))).notna())]
# # |
# #     (df_final.get('cpid', pd.Series([""]*len(df_final))) == "")
# # ]
#
# print("cpid 缺失记录数:", len(df_no_cpid))
# print(df_no_cpid.head())
#
# # # 输出 CSV
# df_no_cpid.to_csv("no_cpid_data.csv", index=False, encoding="utf-8-sig")
# print("已输出 no_cpid_data.csv")
#
# # 假设你的最终 DataFrame 是 df_final
# target_url = "https://www.mercadolibre.com.ar/kit-seguridad-full-hd-4-camaras-hd-libercam-4hd-lc-infrarroja-hdmi-ip-consola-dvr/p/MLA27611472?pdp_filters=item_id%3AMLA1542055202#origin=share&sid=share&wid=MLA1542055202"
#
# # 筛选
# df_target = df_final[df_final['url'] == target_url]
#
# print("找到的记录数:", len(df_target))
# print(df_target)


import pandas as pd
from collections import Counter

# 假设你的 DataFrame 是 df_final
type_summary = {}

for col in df_final.columns:
    # 获取每一列的类型
    types = df_final[col].apply(lambda x: type(x).__name__)
    # 统计每种类型出现次数
    type_counts = dict(Counter(types))
    type_summary[col] = type_counts

# 输出结果
for col, counts in type_summary.items():
    print(f"列: {col}")
    for t, c in counts.items():
        print(f"  类型 {t}: {c} 条")
    print("-" * 50)