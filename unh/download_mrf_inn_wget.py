# Databricks notebook source
# MAGIC %md
# MAGIC ## Download In-Network Files w WGET

# COMMAND ----------

# MAGIC %md
# MAGIC Ater trying a few different methods of downloading big in-network-rate files, we believe using the wget command is the most stable way. So, we will be using this approach for the in-network files.
# MAGIC
# MAGIC For the out-of-network files, we can still keep the requests or pycurl approach.

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from pathlib import Path
from urllib import parse
from urllib.parse import urlparse, parse_qs
import re
from tqdm import tqdm
from pyspark.sql.functions import col

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

url_inr = [url for url in urls if "_in-network-rates.json.gz" in url]
url_fn_pairs = []

# COMMAND ----------

for url_raw in tqdm(url_inr):
    parsed_url = urlparse(url_raw)
    query_params = parse_qs(parsed_url.query)
    fd = query_params["fd"][0]
    fn = query_params["fn"][0]
    fn_ascii = re.sub(r'[^\x00-\x7F]+', '', fn)
    fn_pct_encoding = parse.quote(fn, safe="?=/:&")
    url = url_raw
    
    #if fn_ascii != fn:
    if True:
        # NOTE: This may be faster...
        # For the file names with Unicode, use the direct URL
        # As this URL uses "sig", we do not want to use this link for all the files; the sig may change over time
        # And we hope UNH will fix the Unicode URLs; in theory, we shouldn't use Unicode in URL
        url = (f"https://mrfstorageprod.blob.core.windows.net/public-mrf/{fd}/{fn_pct_encoding}" +
                "?sp=r&st=2024-03-26T04:49:21Z&se=2025-03-25T12:49:21Z&spr=https&sv=2022-11-02&sr=c&sig=M0sm1qeV6LULQexjwJYsuupRKv1UpgsQLzpfLtZbzkk%3D")
        
    if Path(f"{volumepath}/{fn_ascii}").exists():
        #print(f"{filename} already exists, skipping...")
        continue
    
    url_fn_pairs.append((url, fn_pct_encoding))

# COMMAND ----------

len(url_fn_pairs)

# COMMAND ----------

with open("/Volumes/mimi_ws_1/sandbox/src/unh_wget_scripts.sh", "w") as fp:
    fp.write("#!/bin/sh\n\n")
    for url_fn in url_fn_pairs:
        fp.write(f'wget --progress=bar:force:noscroll -O \"{volumepath}/{url_fn[1]}\" \"{url_fn[0]}\"')
        fp.write("\n")

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC bash /Volumes/mimi_ws_1/sandbox/src/unh_wget_scripts.sh
