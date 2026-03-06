import pandas as pd

# 读取 CSV
df1 = pd.read_csv("001.csv")
df2 = pd.read_csv("brand_monthlyorder.csv")

# 根据 q_id 合并（inner join，只保留匹配行）
merged_df = pd.merge(df1, df2, on="q_id", how="inner")

# 如果你想保留左表所有行（左连接）
# merged_df = pd.merge(df1, df2, on="q_id", how="left")

# 输出结果
print(merged_df)

# 保存到新 CSV
merged_df.to_csv("merged.csv", index=False)
