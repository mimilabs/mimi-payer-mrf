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
import pandas as pd
import pycurl
import time
import socket
from urllib import parse
from urllib.parse import urlparse, parse_qs
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

for url_raw in tqdm(urls_target):
    parsed_url = urlparse(url_raw)
    query_params = parse_qs(parsed_url.query)
    fd = query_params["fd"][0]
    fn = query_params["fn"][0]
    fn_ascii = re.sub(r'[^\x00-\x7F]+', '', fn)
    fn_pct_encoding = parse.quote(fn, safe="?=/:&")
    url = url_raw
    if fn_ascii != fn:
        # For the file names with Unicode, use the direct URL
        # As this URL uses "sig", we do not want to use this link for all the files; the sig may change over time
        # And we hope UNH will fix the Unicode URLs; in theory, we shouldn't use Unicode in URL
        url =  (f"https://mrfstorageprod.blob.core.windows.net/public-mrf/{fd}/{fn_pct_encoding}" +
                "?sp=r&st=2024-03-26T04:49:21Z&se=2025-03-25T12:49:21Z&spr=https&sv=2022-11-02&sr=c&sig=M0sm1qeV6LULQexjwJYsuupRKv1UpgsQLzpfLtZbzkk%3D")
        print(f"NOTE: Unicode file name changed to : {fn_ascii}")

    if Path(f"{volumepath}/{fn_ascii}").exists():
        #print(f"{filename} already exists, skipping...")
        continue
    try:
        download_file2(url, 
                       fn_ascii, 
                       volumepath)
        # if success, then break
    except UnicodeEncodeError as e:
        print(f"Unicode Error: {fn}")
    except (pycurl.error, socket.error) as e:
        print(f"Retrying..., due to {e}")
        time.sleep(retry_delay)

# COMMAND ----------


