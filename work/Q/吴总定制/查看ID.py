from pathlib import Path
import pandas as pd
import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
src_col = get_collection("ml","customize_common","vivo_shopee_parsed")
data = src_col.find({},{"item_id":1})
item_ids = []
for item in data:
    item_ids.append(item["item_id"])
def read_product_ids_and_replace_images(folder_path, output_txt_path):
    folder = Path(folder_path)
    all_product_ids = set()  # 用 set 自动去重

    for file in folder.glob("*.xlsx"):
        print(f"正在读取: {file.name}")
        try:
            # 第二行是表头，所以 header=1
            df = pd.read_excel(file, header=1, dtype=str)

            # 清理列名空格和隐藏字符
            df.columns = df.columns.str.strip().str.replace(r"\s", "", regex=True)

            # 检查必须列
            if "产品ID" not in df.columns or "主图" not in df.columns:
                print(f"跳过: {file.name}，缺少产品ID或主图列")
                continue

            # 替换主图链接
            df["主图"] = df["主图"].apply(
                lambda x: f"https://save.sorftime.com/toolexcleimage?url={x}"
                if pd.notna(x) and str(x).startswith("http")
                else x
            )

            # 保存产品ID到 set（自动去重）
            ids = df["产品ID"].dropna().tolist()
            all_product_ids.update(ids)

            # 保存处理后的 Excel
            new_file = file.with_name(file.stem + "_已处理.xlsx")
            df.to_excel(new_file, index=False)
            print(f"已生成: {new_file.name}, 本文件产品数量: {len(ids)}")

        except Exception as e:
            print(f"读取失败: {file.name} -> {e}")

    # 去重后的产品ID列表
    all_product_ids_list = list(all_product_ids)
    print(f"总去重后产品ID数量: {len(all_product_ids_list)}")
    a = []
    print(all_product_ids_list)
    for i in item_ids:
        if str(i) not in all_product_ids_list:
            a.append(str(i))
            print(i)
    print(f"mangodb有不在总去重后产品ID: {len(a)}")
    print(a)


    # 输出到 TXT 文件
    with open(output_txt_path, "w", encoding="utf-8") as f:
        for pid in all_product_ids_list:
            f.write(f"{pid}\n")
    print(f"产品ID已保存到: {output_txt_path}")

    return all_product_ids_list


if __name__ == "__main__":
    folder_path = r"D:\gg_xm\Q\吴总定制\虾皮数据"
    output_txt_path = r"D:\gg_xm\Q\吴总定制\product_ids.txt"

    product_ids = read_product_ids_and_replace_images(folder_path, output_txt_path)