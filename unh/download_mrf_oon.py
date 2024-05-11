# Databricks notebook source
# MAGIC %md
# MAGIC ## Download OON files

# COMMAND ----------

!pip install tqdm pycurl

# COMMAND ----------

import requests
from pathlib import Path
from tqdm import tqdm
from itertools import product
from dateutil.parser import parse
import pandas as pd
import pycurl
import time
import socket
import urllib.parse
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the mrf files

# COMMAND ----------

catalog = "mimi_ws_1"
schema = "payermrf"
table = "unh_toc"
volumepath = "/Volumes/mimi_ws_1/payermrf/src/unh/gz"
max_retries = 5
retry_delay = 60 # 60 seconds - 1 min
urls = [x.location for x in (spark.read.table(f"{catalog}.{schema}.{table}")
                                .select("location")
                                .distinct()
                                .collect())]

# COMMAND ----------

def download_file2(url, filename, path):
    with open(f"{path}/{filename}", "wb") as fp:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, fp)
        c.setopt(c.SSL_VERIFYPEER, False)
        c.setopt(c.SSL_VERIFYHOST, False)
        c.perform()
        c.close()

# COMMAND ----------

urls_target = []
for url in urls: 
    if "_allowed-amounts.json.gz" in url:
        urls_target.append(url)

# COMMAND ----------

for url in tqdm(urls_target):
    tokens = url.split("&fn=")
    if len(tokens) > 1:
        filename = tokens[1]
    else:
        filename = url.split("/")[-1]
    filename = re.sub(r'[^\x00-\x7F]+', '', filename)

    if Path(f"{volumepath}/{filename}").exists():
        #print(f"{filename} already exists, skipping...")
        continue
    try:
        download_file2(urllib.parse.quote(url, safe="?=/:&"), 
                       filename, 
                       volumepath)
        # if success, then break
    except UnicodeEncodeError as e:
        print(f"Unicode Error: {filename}")
    except (pycurl.error, socket.error) as e:
        print(f"Retrying..., due to {e}")
        time.sleep(retry_delay)

# COMMAND ----------


