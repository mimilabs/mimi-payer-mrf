# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest In-Network files

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

from pathlib import Path
from collections import Counter
import gzip
import json
from pprint import pprint
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col
import re
catalog = "mimi_ws_1"
schema = "payermrf"
table = "in_network_rates"
parent = "UnitedHealthcare"

# COMMAND ----------

files = []
for fpath in tqdm(Path("/Volumes/mimi_ws_1/payermrf/src/unh/gz/").glob("*in-network-rates.json.gz")):
    files.append((datetime.fromtimestamp(fpath.lstat().st_mtime), fpath))
sorted(files, key=lambda x: x[0], reverse=True)[:5]

# COMMAND ----------


