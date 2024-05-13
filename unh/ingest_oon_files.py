# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest OON files

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
table = "allowed_amounts"
parent = "UnitedHealthcare"

# COMMAND ----------

table_toc = "unh_toc"
pdf = (spark.read.table(f"{catalog}.{schema}.{table_toc}")
       .filter(col("description")=="allowed amount file")
       .toPandas())

# COMMAND ----------

metamap = {}
for _, row in pdf.iterrows():
    tokens = row["location"].split("&fn=")
    if len(tokens) > 1:
        filename = tokens[1]
    else:
        filename = row["location"].split("/")[-1]
    filename = re.sub(r'[^\x00-\x7F]+', '', filename)
    metamap[Path(filename).stem] = {"plan_id": row["plan_id"],
                                    "plan_id_type": row["plan_id_type"],
                                    "plan_market_type": row["plan_market_type"],
                                    "employer": row["employer"],
                                    "entity_name": row["entity_name"],
                                    "entity_type": row["entity_type"]}

# COMMAND ----------

files_with_errors = []

# COMMAND ----------

data = []
header = ["parent", "npi", "tin_type", "tin_value", 
        "billed_charge", "allowed_amount", 
        "service_code", "billing_class",
        "billing_code", "name", "description",
        "billing_code_type", "billing_code_type_version",
        "reporting_entity_name", "reporting_entity_type",
        "sourceSystem_plan", "version", 
        "npi_lst", "service_code_lst",
        "last_updated_on", "fn", 
        "plan_id", "plan_id_type", "plan_market_type",
        "employer", "entity_name", "entity_type"]
for fpath in tqdm(Path("/Volumes/mimi_ws_1/payermrf/src/unh/gz/").glob("*allowed-amounts.json.gz")):
    try:
        with gzip.open(str(fpath), "r") as fp:
            try:
                doc = json.load(fp)
            except json.JSONDecodeError: 
                files_with_errors.append(fpath.stem)
                continue
            fn = fpath.stem
            metainfo = metamap.get(fn, {})
            last_updated_on = doc.get('last_updated_on')
            reporting_entity_name = doc.get('reporting_entity_name')
            reporting_entity_type = doc.get('reporting_entity_type')
            sourceSystem_plan = doc.get('sourceSystem_plan')
            version = doc.get('version')
            for oon_object in doc.get("out_of_network", []):
                billing_code = oon_object.get('billing_code')
                billing_code_type = oon_object.get('billing_code_type')
                billing_code_type_version = oon_object.get('billing_code_type_version')
                description = oon_object.get('description')
                name = oon_object.get('name')
                for aa_object in oon_object.get("allowed_amounts", []):
                    
                    billing_class = aa_object.get("billing_class")
                    service_code = aa_object.get("service_code", [])
                    service_code0 = None
                    if len(service_code) > 0:
                        service_code0 = service_code[0]
                    tin_type = aa_object.get("tin", {}).get("type")
                    tin_value = aa_object.get("tin", {}).get("value")
                    tin_value = str(int(tin_value) if isinstance(tin_value, float) else tin_value)
                    if tin_value[-2:] == ".0":
                        tin_value = tin_value[:-2] # remove the trailing .0
                    
                    for payment in aa_object.get("payments", []):
                        allowed_amount = payment.get("allowed_amount")
                        for provider in payment.get("providers", []):
                            billed_charge = provider.get("billed_charge")
                            npi_lst = [str(int(npi) if isinstance(npi, float) else npi) 
                                    for npi in provider.get("npi", [])]
                            npi0 = None
                            if len(npi_lst) > 0:
                                npi0 = npi_lst[0]
                            row = [parent, 
                                npi0, tin_type, tin_value, 
                                billed_charge, allowed_amount, 
                                service_code0, billing_class,
                                billing_code, name, description,
                                billing_code_type, billing_code_type_version,
                                reporting_entity_name, reporting_entity_type,
                                sourceSystem_plan, version,
                                npi_lst, service_code,
                                datetime.strptime(last_updated_on, "%Y-%m-%d").date(), 
                                fn,
                                metainfo.get("plan_id"), 
                                metainfo.get("plan_id_type"), 
                                metainfo.get("plan_market_type"),
                                metainfo.get("employer"), 
                                metainfo.get("entity_name"), 
                                metainfo.get("entity_type")]
                            data.append(row)
        if len(data) > 20000:
            df = spark.createDataFrame(pd.DataFrame(data, columns = header))
            (df.write
                .format("delta")
                .mode("append")
                .saveAsTable(f"{catalog}.{schema}.{table}"))
            data = []
    except gzip.BadGzipFile:
        files_with_errors.append(fpath.stem)

if len(data) > 0:
    df = spark.createDataFrame(pd.DataFrame(data, columns = header))
    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.{schema}.{table}"))

# COMMAND ----------

print(files_with_errors)

# COMMAND ----------

for fpath in tqdm(Path("/Volumes/mimi_ws_1/payermrf/src/unh/gz/").glob("*allowed-amounts.json.gz")):
    if fpath.stem in files_with_errors:
        continue
    dbutils.fs.mv(str(fpath), 
        f"/Volumes/mimi_ws_1/payermrf/src/unh/archive_allowed_amounts/{fpath.name}")
