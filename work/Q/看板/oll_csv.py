from pathlib import Path
import os
import pandas as pd
from tqdm import tqdm

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

source_folder = Path(r"D:\data\看板\202512_sku_p_info_2")
target_folder = Path(r"D:\data\看板\not_processed_改")
target_folder.mkdir(exist_ok=True)

file_names = [
    "MLM1747",
    "MLM189530",
    "MLM1403",
    "MLM1367",
    "MLM1368",
    "MLM1384",
    "MLM1246",
    "MLM1051",
    "MLM1648",
    "MLM1144",
    "MLM1500",
    "MLM1039",
    "MLM1276",
    "MLM1575",
    "MLM1000",
    "MLM186863",
    "MLM1574",
    "MLM1499",
    "MLM1182",
    "MLM3937",
    "MLM1132",
    "MLM3025",
    "MLM1071",
    "MLM1953",
    "MLM44011",
    "MLM1430",
    "MLM187772"
]

all_dfs = []

for cat_id in tqdm(file_names, desc="Processing cat_id files"):
    cat_file = source_folder / f"{cat_id}.csv"
    if cat_file.exists():  # 文件存在才读取
        try:
            cat_df = pd.read_csv(cat_file)
            cat_df["cat_id"] = cat_id  # 添加 cat_id 列，方便区分来源
            all_dfs.append(cat_df)
        except Exception as e:
            print(f"Error reading {cat_file}: {e}")
    else:
        print(f"{cat_file} not found, skipping.")

# 合并所有 DataFrame
if all_dfs:
    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_file = target_folder / "all.csv"
    merged_df.to_csv(merged_file, index=False)
    print(f"Merged {len(all_dfs)} files into {merged_file}")
else:
    print("No files were merged.")


