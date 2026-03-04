import pandas as pd
from pathlib import Path
import os
import re
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import threading
import queue
import pymongo
import traceback
import shutil
import ast
from collections import defaultdict
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
brank_co = [
    # "ezviz","tapo","imou","hikvision",
    "xiaomi"]
from dm.connector.mongo.manager3 import get_collection
brank_mx = ["ezviz","tapo","imou","hikvision","xiaomi"]
ml_cl_visits = get_collection("ml_reviews","ml_visits","ml_co_visits")
ml_cl_monthly_sku = get_collection("yingshi","yingshi","ml_co_monthly_sku")
ml_cl_item_id = ml_cl_monthly_sku.find(
    {
        # "item_id":None,
        "month":202511,
        "brand": {"$in": [re.compile(f"^{b}$", re.IGNORECASE) for b in brank_mx]},
    },
    {
        "sku_id":1,
        "item_id":1,
        "monthlyorder":1,
        "monthlygmv":1
    })

# mlc_list = [
#     'MLC1400089577', 'MLC1737057060', 'MLC1248903794', 'MLC2954818006', 'MLC2994270190', 'MLC1628868307', 'MLC1402423524', 'MLC1629800757', 'MLC2873458172', 'MLC1312985386', 'MLC1650569121', 'MLC2964198370', 'MLC1390098037', 'MLC1543880059', 'MLC1544236389', 'MLC1628818433', 'MLC1496099171', 'MLC1432503279', 'MLC1630930443', 'MLC1457659327', 'MLC2903556040', 'MLC1268436635', 'MLC1505713683', 'MLC1393605967', 'MLC1482029655', 'MLC1605278451', 'MLC1521507379', 'MLC2496470090', 'MLC1627612529', 'MLC1628878905', 'MLC1825940454', 'MLC2892441866', 'MLC1678391103', 'MLC1889117882', 'MLC1710971399', 'MLC1698969265', 'MLC1411705141', 'MLC2891158732', 'MLC2772967768', 'MLC2911744942', 'MLC2898628014', 'MLC2117066792', 'MLC1584689947', 'MLC2740213940', 'MLC2457717312', 'MLC1612798099', 'MLC1359072957', 'MLC1393568329', 'MLC2903422224', 'MLC1628843653', 'MLC2935322272', 'MLC1589425511', 'MLC1341970311', 'MLC1062089115', 'MLC1409064689', 'MLC2873405998', 'MLC2764994526', 'MLC1491172655', 'MLC3044287810', 'MLC2855460212', 'MLC1393554571', 'MLC1391313075', 'MLC2448769706', 'MLC1578422633', 'MLC912445944', 'MLC2671499644', 'MLC2688634364', 'MLC2794534448', 'MLC1525393024', 'MLC1492910241', 'MLC2725185980', 'MLC2891027786', 'MLC2873588120', 'MLC3000159342', 'MLC2914784260', 'MLC1378645280', 'MLC1557198090', 'MLC1563503993', 'MLC2931969258', 'MLC2048550886', 'MLC1409012853', 'MLC1396704689', 'MLC2837395180', 'MLC1393599785', 'MLC1896718846', 'MLC1866224294', 'MLC1615133681', 'MLC1612860095', 'MLC1579126877', 'MLC1533119607', 'MLC3152776508', 'MLC3114559516', 'MLC969172942', 'MLC1561201081', 'MLC2911140508', 'MLC1539617254', 'MLC1008903265', 'MLC1536415649', 'MLC1430784805', 'MLC1527748200', 'MLC1268436614', 'MLC2460903582', 'MLC1233046216', 'MLC1408680359', 'MLC1475376381', 'MLC1052105920', 'MLC1583318047', 'MLC1776752380', 'MLC1355117925', 'MLC535101633', 'MLC1145662892', 'MLC1408953304', 'MLC2865057030', 'MLC1599226175', 'MLC1359136905', 'MLC1353108830', 'MLC1698388887', 'MLC2910386062', 'MLC1564413177', 'MLC1811422300', 'MLC2299426294', 'MLC2739611050', 'MLC2926071594', 'MLC1811874420', 'MLC1812017450', 'MLC2670997816', 'MLC3039777864', 'MLC2851884460', 'MLC2670997720', 'MLC2671204398', 'MLC1580386221', 'MLC2765478554', 'MLC1578945959', 'MLC1580398925', 'MLC2794115958', 'MLC2862486806', 'MLC1438531481', 'MLC3324947632', 'MLC1588163709', 'MLC2809702696', 'MLC1566847631', 'MLC1599676041', 'MLC1552740791', 'MLC2849234028', 'MLC2842072582', 'MLC1587928801', 'MLC3196051302', 'MLC2873639594', 'MLC1588285643', 'MLC528304185', 'MLC960923203', 'MLC1716342058', 'MLC1626066753', 'MLC3152932810', 'MLC983856394', 'MLC3177984412', 'MLC1628919695', 'MLC1586645103', 'MLC1370218685', 'MLC1408622547', 'MLC1406258485', 'MLC2506058238', 'MLC1458343515', 'MLC1410258811', 'MLC2563554568', 'MLC1325682415', 'MLC1623514169', 'MLC2873588092', 'MLC1357042917', 'MLC2538049246', 'MLC2048550850', 'MLC961037062', 'MLC1572045141', 'MLC2928836454', 'MLC2468635870', 'MLC1466566211', 'MLC1489692981', 'MLC1641622591', 'MLC1533107065', 'MLC2873431874', 'MLC2879447578', 'MLC1492379357', 'MLC2411435358', 'MLC2460972234', 'MLC1409061119', 'MLC2341507810', 'MLC1393591951', 'MLC1393593165', 'MLC1390096393']

item_id = {}
item = []
item_monthlyorder_sum = 0
item_monthlygmv_sum = 0

for i in ml_cl_item_id:
    a = 0
    b = "sku_id"
    if i["sku_id"] is None:
        sl = i["item_id"]
        b = "item_id"
    else:
        sl = i["sku_id"]
    a = ml_cl_visits.find_one({"sl":sl},{"_id":0,"visits":1})
    visits = a["visits"]
    monthlyorder =i["monthlyorder"]
    monthlygmv = i["monthlygmv"]
    c = 0
    time = 20251100
    time_end = 20251200
    for v,j in visits.items():
        if int(v) > time and int(v) < time_end:
            c += j
    # if sl in mlc_list:
    #     a = 1
    if monthlyorder>0:
        print(b, sl)
    # if (c <= 1 and monthlyorder != 0) or a==1 :
    #     item_id[sl] = monthlyorder
    #     item.append(sl)
    #     item_monthlyorder_sum = item_monthlyorder_sum + monthlyorder
    #     item_monthlygmv_sum = item_monthlygmv_sum + monthlygmv
    #     monthlyorder = 0
    #     monthlygmv = 0
    #     print(b, sl)


# print(item_monthlyorder_sum, item_monthlygmv_sum)
    # ml_cl_monthly_sku.update_one(
    #     {f"{b}":sl,"month":202511},
    #     {
    #         "$set":{
    #             "monthlyorder":monthlyorder,
    #             "monthlygmv":monthlygmv,
    #         }
    #     })

