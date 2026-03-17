import os
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import shutil

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

d = "20260104"
empty_dir = f"D:/data/看板/{d}_empty"  # 0 条数据导出目录

# ========= 子进程执行函数 ========= #
def process_category(cat_id):
    """
    每个进程执行：查询数据、导出 CSV
    """
    try:
        # 重新初始化（子进程不能共享连接）
        sku = get_collection("main_ml_mx", "ml_mx", "sku")

        cursor = sku.find(
            {
                "category_id": cat_id,
            },
            {
                "_id": 0,
                "sku_id":1,                 #   商品id
                "category_id": 1,           #   类目id
                "brand": 1,                 #   品牌
                "sellerCbt":1,              #
                "sellerID":1,
                "sellerName":1,             #   店铺名称
                "sellerType":1,             #   店铺类型
                "stock_type":1,             #   发货仓
                "offersInf": 1,             #   跟卖数
                "price_trend": 1,
                "monthly_sale_trend": 1,    #   月份销量
                "active_price":1,           #   均价
                "conversion_all_order":1,   #   访问销量
                "conversion_all_view":1,    #   访问量
                "p_info_2":1,               #   商品属性
                "rating_avg":1,             #   评分
                "date_created":1,           #   上架时间
                "puton_date":1,             #   上架时间

            }
        )

        rows = list(cursor)
        df = pd.DataFrame(rows)

        return cat_id, df, None   # 返回 DataFrame 而不是直接保存

    except Exception as e:
        return cat_id, None, str(e)


# ========= 主程序 ========= #
def main():
    # 创建输出目录
    main_dir = f"D:/data/看板/{d}"
    os.makedirs(main_dir, exist_ok=True)
    os.makedirs(empty_dir, exist_ok=True)

    visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
    cats = list(visual_plus.find({}, {"cat_id": 1, "_id": 0}))
    cat_list = [c["cat_id"] for c in cats]
    # cat_list = ["MLM120630"]

    print(f"共 {len(cat_list)} 个类目需要导出（多进程并行中...）")
    start = time.time()

    futures = []

    # ======= 多进程执行 ======= #
    with ProcessPoolExecutor() as executor:
        for c in cat_list:
            out_file = f"{main_dir}/{c}.csv"
            if os.path.exists(out_file):
                print(f"跳过 {c}（文件已存在）")
                continue
            print(c)
            futures.append(executor.submit(process_category, c))

        done = 0
        total = len(futures)

        for future in as_completed(futures):
            done += 1
            if done == 596:
                print("000")
            cat_id, df, error = future.result()

            if error:
                print(f"[{done}/{total}] 类目 {cat_id} ❌ 出错：{error}")
                continue

            if df.empty:
                # 导出 0 条数据到 empty_dir
                df.to_csv(f"{empty_dir}/{cat_id}.csv", index=False, encoding="utf-8-sig")
                print(f"[{done}/{total}] 类目 {cat_id} ⚠️ 0 条数据，导出到 empty 文件夹")
            else:
                df.to_csv(f"{main_dir}/{cat_id}.csv", index=False, encoding="utf-8-sig")
                print(f"[{done}/{total}] 类目 {cat_id} ✔ 导出 {len(df)} 条数据")

    print(f"\n全部完成，总耗时：{time.time() - start:.2f} 秒")


if __name__ == "__main__":
    main()
