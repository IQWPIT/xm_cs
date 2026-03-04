import os
from datetime import datetime, timedelta
from calendar import monthrange
from pymongo import MongoClient
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment
import re
from collections import defaultdict
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection


visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

data = visual_plus.find_one({"cat_id":"MLM455286"},{"_id":0,"cat":1})["cat"]
print(data)