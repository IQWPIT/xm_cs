# -*- coding: utf-8 -*-
import os
import datetime
from loguru import logger
from pymongo import UpdateOne
import requests  # 注意，这里应该是直接导入 requests 库，而不是重用 requests 作为变量名
import pandas as pd



# 假设你有一个 url 和 param，用于请求
url = "http://119.91.253.91:3400/monthly-listing?skip=0&limit=200&token=yingshi20240419&month=202512&site=ml_ar"
param = {"key": "categories"}

# 发起 HTTP 请求
response = requests.get(url=url, params=param, timeout=20)

# 解析 JSON 数据
re_text_js = response.json()

# 检查返回的数据结构
print(type(re_text_js))  # 检查返回的数据类型
print(re_text_js)  # 打印查看数据内容

# 将 JSON 数据转换为 DataFrame
temp = pd.DataFrame(re_text_js)
