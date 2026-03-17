# -*- coding: utf-8 -*-
import os
import time

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection
site = "ml_br"

def main():
    start_time = time.time()

    col = get_collection("task_buffer", "task_buffer", f"{site}_item_stock")

    exist_count = 0
    lt50_count = 0
    scanned = 0

    # 只取 lastSt，减少网络传输和内存占用
    cursor = col.find(
        {},
        {"_id": 0, "lastSt": 1},
        no_cursor_timeout=True
    ).batch_size(5000)
    # 只取 lastSt，减少网络传输和内存占用
    # cursor = col.find(
    #     {},
    #     {"_id": 0, "lastSt": 1},
    #     no_cursor_timeout=True
    # ).limit(1000000)

    try:
        for doc in cursor:
            scanned += 1

            if scanned % 100000 == 0:
                print(
                    f"已扫描: {scanned:,} | "
                    f"lastSt存在: {exist_count:,} | "
                    f"lastSt<50: {lt50_count:,} | "
                    f"比值: {lt50_count/exist_count:.2f} | "
                    f"耗时: {time.time() - start_time:.2f}s"
                )

            if "lastSt" not in doc:
                continue

            value = doc["lastSt"]
            if value is None:
                continue

            exist_count += 1

            # 防止有脏数据，比如字符串、对象等
            if isinstance(value, (int, float)) and value < 50:
                lt50_count += 1

    finally:
        cursor.close()

    ratio = lt50_count / exist_count if exist_count else 0

    print("=" * 80)
    print(f"站点:{site}")
    print(f"总扫描数量: {scanned:,}")
    print(f"lastSt 字段存在数量: {exist_count:,}")
    print(f"lastSt < 50 数量: {lt50_count:,}")
    print(f"占比: {ratio:.4%}")
    print(f"总耗时: {time.time() - start_time:.2f} 秒")
    print("=" * 80)


if __name__ == "__main__":
    main()