import os
import re
import pandas as pd
import glob
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
brank_mx = ["xiaomi"]
ml_cl_visits = get_collection("ml_reviews","ml_visits","ml_cl_visits")
ml_cl_monthly_sku = get_collection("yingshi","yingshi","ml_cl_monthly_sku")
sku = []
folder = r"D:\gg_xm\Q\yingshi\ml_cl_品牌"  # ⚠️改成你 Excel 所在目录
dfs = []

xlsx_files = glob.glob(os.path.join(folder, "*.xlsx"))

print(f"发现 Excel 文件数量: {len(xlsx_files)}")

for file in xlsx_files:
    try:
        df = pd.read_excel(file)  # 👈 关键：read_excel
    except Exception as e:
        print(f"❌ 读取失败: {os.path.basename(file)} | {e}")
        continue

    # 清理列名（非常重要）
    df.columns = df.columns.astype(str).str.strip()

    need_cols = ["商品ID", "202511月销量"]
    missing = [c for c in need_cols if c not in df.columns]

    if missing:
        print(f"❌ 跳过 {os.path.basename(file)}，缺少列: {missing}")
        print("实际列名：", df.columns.tolist())
        continue

    sub = df[need_cols].copy()
    sub["source_file"] = os.path.basename(file)

    dfs.append(sub)
    print(f"✅ 已处理: {os.path.basename(file)} 行数={len(sub)}")

# 🚨 防止 concat 炸
if not dfs:
    raise RuntimeError("没有任何 Excel 成功读取，请检查路径或列名")

final_df = pd.concat(dfs, ignore_index=True)

# 销量转整数
final_df["202511月销量"] = (
    pd.to_numeric(final_df["202511月销量"], errors="coerce")
    .fillna(0)
    .astype(int)
)

final_df.to_csv(
    "商品ID_202511月销量.csv",
    index=False,
    encoding="utf-8-sig"
)

# final_df 已经生成了

for idx, row in final_df.iterrows():
    # row["商品ID"] 和 row["202511月销量"] 分别访问
    if row['202511月销量']>=0:
        sl = "item_id"
        a = ml_cl_monthly_sku.find_one(
    {
        "item_id":row['商品ID'],
        "month":202511,
        # "brand": {"$in": [re.compile(f"^{b}$", re.IGNORECASE) for b in brank_mx]},
    },
    {
        "sku_id":1,
        "item_id":1,
        "monthlyorder":1,
        "monthlygmv":1
    })
        if a is None:
            a = ml_cl_monthly_sku.find_one(
                {
                    "sku_id": row['商品ID'],
                    "month": 202511,
                    # "brand": {"$in": [re.compile(f"^{b}$", re.IGNORECASE) for b in brank_mx]},
                },
                {
                    "sku_id": 1,
                    "item_id": 1,
                    "monthlyorder": 1,
                    "monthlygmv": 1
                })
            sl = "sku_id"
        if a is None:
            continue
        # print(a)
        monthlyorder = a["monthlyorder"]
        if monthlyorder >0 and row['202511月销量'] == 0:
            sku.append(row['商品ID'])
            print(f" {sl}  商品ID={row['商品ID']}, 202511月销量={row['202511月销量']},{monthlyorder}")


print(sku)
