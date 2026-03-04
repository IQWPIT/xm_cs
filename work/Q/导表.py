import os
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

d = "20251205_sku_p_info_2"


# ========= 子进程执行函数 ========= #
def process_category(cat_id):
    """
    每个进程执行：查询数据、导出 CSV
    """
    try:
        # 重新初始化（子进程不能共享连接）
        sku = get_collection("main_ml_br", "ml_br", "sku")

        cursor = sku.find(
            {
                "category_id": cat_id,
                "brand": {"$exists": True, "$ne": ""},
                "monthly_sale_trend": {"$exists": True, "$ne": {}}
            },
            {
                "_id": 0,
                "sku_id":1,
                "category_id": 1,
                "brand": 1,
                "sellerCbt":1,
                "sellerID":1,
                "sellerName":1,
                "sellerType":1,
                "stock_type":1,
                "offersInf": 1,
                "price_trend": 1,
                "monthly_sale_trend": 1,
                "active_price":1,
                "conversion_all_order":1,
                "conversion_all_view":1,
                "p_info_2":1,
                # "monthly_sale_trend.2510": 1,
            }
        )

        rows = list(cursor)
        df = pd.DataFrame(rows)

        df.to_csv(f"D:/data/临时/{d}/{cat_id}.csv", index=False, encoding="utf-8-sig")


        return cat_id, len(rows), None   # None 表示无错误

    except Exception as e:
        return cat_id, 0, str(e)



# ========= 主程序 ========= #
def main():
    # 创建输出目录
    os.makedirs(f"D:/data/临时/{d}", exist_ok=True)

    # visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
    # cats = list(visual_plus.find({}, {"cat_id": 1, "_id": 0}))
    # cat_list = [c["cat_id"] for c in cats]
    cat_list = ["MLB1646","MLB72503"]

    print(f"共 {len(cat_list)} 个类目需要导出（多进程并行中...）")
    start = time.time()

    futures = []
    results = []

    # ======= 多进程执行 ======= #
    with ProcessPoolExecutor() as executor:
        for c in cat_list:
            out_file = f"D:/data/临时/{d}/{c}.csv"
            if os.path.exists(out_file):
                print(f"跳过 {c}（文件已存在）")
                continue
            futures.append(executor.submit(process_category, c))

        done = 0
        total = len(futures)

        for future in as_completed(futures):
            done += 1
            cat_id, count, error = future.result()

            if error:
                print(f"[{done}/{total}] 类目 {cat_id} ❌ 出错：{error}")
            else:
                print(f"[{done}/{total}] 类目 {cat_id} ✔ 导出 {count} 条数据")

    print(f"\n全部完成，总耗时：{time.time() - start:.2f} 秒")


if __name__ == "__main__":
    main()
