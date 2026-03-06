import pandas as pd
import ast
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import heapq

# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "_big_Excel")

# ==== 文件夹 & 日志 ====
folder_path = Path(r"D:\data\看板\_改")
success_log = Path(r"D:\data\看板\日志\top50_skus.txt")
error_log = Path(r"D:\data\看板\日志\top50_skus_error2.txt")
success_log.touch(exist_ok=True)
error_log.touch(exist_ok=True)

# ==== 已处理文件列表 ====
processed_files = set()
if success_log.exists():
    with open(success_log, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                processed_files.add(line.split()[0])

# ==== 待处理 CSV 文件（跳过已处理） ====
csv_files = [f for f in folder_path.glob("*.csv") if f.stem not in processed_files]

# ==== 时间映射 ====
times_list = [
    202510, 202509, 202508, 202507, 202506, 202505,
    202504, 202503, 202502, 202501,
    202412, 202411, 202410, 202511, 202512
]

time_map = {
    202510:(2510,2509,2410),202509:(2509,2508,2409),202508:(2508,2507,2408),
    202507:(2507,2506,2407),202506:(2506,2505,2406),202505:(2505,2504,2405),
    202504:(2504,2503,2404),202503:(2503,2502,2403),202502:(2502,2501,2402),
    202501:(2501,2412,2401),202412:(2412,2411,2312),202411:(2411,2410,2311),
    202410:(2410,2409,2310),202511:(2511,2510,2411),202512:(2512,2511,2412)
}

chunksize = 10000  # 每次处理 10000 行

# ==== 单文件处理函数（纯 Python top50） ====
def process_file(file_path: Path):
    cat_id = file_path.stem
    try:
        # 每个时间点维护一个堆
        top50_heaps = {times: [] for times in times_list}  # 每个 heap 存 tuple: (销量, offersInf_len, sku_id)

        with tqdm(unit="rows", desc=f"Processing {cat_id}", leave=False) as pbar:
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].fillna("{}")
                chunk['offersInf'] = chunk['offersInf'].fillna("[]")
                chunk['brand'] = chunk['brand'].fillna("未知品牌")

                # 安全解析
                chunk['monthly_sale_trend'] = chunk['monthly_sale_trend'].apply(
                    lambda x: ast.literal_eval(x) if isinstance(x, str) else {}
                )
                chunk['offersInf'] = chunk['offersInf'].apply(
                    lambda x: ast.literal_eval(x) if isinstance(x, str) else []
                )

                # 遍历每行，更新每个时间点的 heap
                for _, row in chunk.iterrows():
                    for times in times_list:
                        time, _, _ = time_map[times]
                        sale = row['monthly_sale_trend'].get(str(time), 0)
                        offers_len = len(row['offersInf'])
                        sku_id = row['sku_id']

                        heap = top50_heaps[times]
                        heap_item = (sale, offers_len, sku_id)

                        if len(heap) < 50:
                            heapq.heappush(heap, heap_item)
                        else:
                            if heap_item > heap[0]:
                                heapq.heapreplace(heap, heap_item)

                pbar.update(len(chunk))

        # ==== 构建 Mongo 更新数据 ====
        update_data = {}
        for times in times_list:
            heap = top50_heaps[times]
            # 排序成从大到小
            sorted_top50 = sorted(heap, key=lambda x: (-x[0], -x[1]))
            top50_sku_dict = {sku: {"offersInf_len": offers_len, "order": sale} for sale, offers_len, sku in sorted_top50}
            update_data[f"top50_skus.{times}"] = top50_sku_dict

        visual_plus.update_one({"cat_id": cat_id}, {"$set": update_data}, upsert=True)

        # ==== 成功日志 ====
        with open(success_log, "a", encoding="utf-8") as f:
            f.write(f"{cat_id} ✅ 成功处理\n")

    except Exception as e:
        with open(error_log, "a", encoding="utf-8") as f:
            f.write(f"{cat_id} ❌ 出错：{e}\n")

# ==== 多线程处理（跳过已处理文件） ====
if csv_files:
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(process_file, f): f.stem for f in csv_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="总进度"):
            try:
                future.result()
            except Exception as e:
                cat_id = futures[future]
                with open(error_log, "a", encoding="utf-8") as f:
                    f.write(f"{cat_id} ❌ 线程出错：{e}\n")
else:
    print("所有文件都已处理过，无需重复处理。")
