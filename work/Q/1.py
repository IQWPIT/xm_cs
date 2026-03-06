import os
import pandas as pd
from tqdm import tqdm
from connector.m import M

# 设置环境变量
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

# 初始化 MongoDB 连接
m = M("ml_br")

sku_s = m.c_sku
sku = sku_s.find_one({"sku_id":"MLB18983370"},{"spu_url":1})
print(sku)