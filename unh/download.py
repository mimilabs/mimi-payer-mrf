# Databricks notebook source
# MAGIC %md
# MAGIC ## Download the index file for the Table of Contents

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from pathlib import Path
from tqdm import tqdm
from itertools import product
from dateutil.parser import parse
import pandas as pd

# COMMAND ----------

def download_indexfile(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# NOTE: The table of contents are javascript based; the list of files need to be scraped via Selenium...
#url = "https://transparency-in-coverage.uhc.com/"
#response = requests.get(url)
#response.raise_for_status()  # This will raise an error if the fetch fails
#soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the Table of Contents

# COMMAND ----------

catalog = "mimi_ws_1"
schema = "payermrf"
table = "unc_toc"
volumepath = "/Volumes/mimi_ws_1/payermrf/src/unh/index"
indexfile = sorted([x for x in Path(volumepath).glob("*")], key=lambda x: x.stem[-10:])[-1]

# COMMAND ----------

urls = []
with open(indexfile, "r") as fp:
    urls = fp.read().split("\n")

# COMMAND ----------

# https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/table-of-contents

index_data = []
index_header = ["filename", "date", "employer", "entity_name", "entity_type",
                   "plan_name", "plan_id", "plan_id_type",
                   "plan_market_type", "description", "location"]
for url in tqdm(urls):
    filename = url.split("/")[-1].split("?")[0]
    r = requests.get(url).json()
    date = parse(filename.split("_")[0]).date()
    employer = filename.split("_")[1]
    entity_name = r.get("reporting_entity_name", "")
    entity_type = r.get("reporting_entity_type", "")
    for item in r.get("reporting_structure", []):
        reporting_plans = item.get("reporting_plans", [])
        in_network_files = item.get("in_network_files", [])
        if "allowed_amount_file" in item:
            in_network_files += [item.get("allowed_amount_file", {})]
        for plan, file in product(reporting_plans, in_network_files):
            plan_name = plan.get("plan_name")
            plan_id = plan.get("plan_id")
            plan_id_type = plan.get("plan_id_type")
            plan_market_type = plan.get("plan_market_type")
            description = file.get("description")
            location = file.get("location")
            row = [filename, date, employer, entity_name, entity_type,
                   plan_name, plan_id, plan_id_type,
                   plan_market_type, description, location]
            index_data.append(row)
    if len(index_data) > 10000:
        df = spark.createDataFrame(pd.DataFrame(index_data, columns=index_header))
        (df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{catalog}.{schema}.{table}"))
        index_data = []

if len(index_data) > 0:
    df = spark.createDataFrame(pd.DataFrame(index_data, columns=index_header))
    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.{schema}.{table}"))
    index_data = []
    

# COMMAND ----------

spark.createDataFrame(pd.DataFrame(index_data, columns=index_header)).display()

# COMMAND ----------


