# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest In-Network files

# COMMAND ----------

!pip install tqdm json-stream

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
import subprocess
from urllib import parse
from urllib.parse import urlparse, parse_qs

catalog = "mimi_ws_1"
schema = "payermrf"
table = "in_network_rates"
parent = "UnitedHealthcare"
zip_path = "/Volumes/mimi_ws_1/payermrf/src/unh/gz/"
unzip_path = "/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates/"

# COMMAND ----------

urls = [x.location for x in (spark.read.table(f"{catalog}.{schema}.unh_toc")
                             .filter(col("employer").contains("DELTA-AIR"))
                                .select("location")
                                .distinct()
                                .collect())]

for url_raw in urls:
    parsed_url = urlparse(url_raw)
    query_params = parse_qs(parsed_url.query)
    fd = query_params["fd"][0]
    fn = Path(query_params["fn"][0])
    if "_in-network-rates.json" not in fn.stem:
        continue

    command = f"gzip -d -c \"/Volumes/mimi_ws_1/payermrf/src/unh/gz/{fn}\" > \"/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates/{fn.stem}\""
    print(command)
    print("")

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC gzip -d -c "/Volumes/mimi_ws_1/payermrf/src/unh/gz/2024-05-01_UMR--Inc-_Third-Party-Administrator_FUSION-HLTH-TENET-IN-MI-SANFORD-HLTH-PROGYNY-CSP-DCI_UNITEDHEALTHCARE-CHOICE-PLUS_-STS_0L_in-network-rates.json.gz" > "/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates/2024-05-01_UMR--Inc-_Third-Party-Administrator_FUSION-HLTH-TENET-IN-MI-SANFORD-HLTH-PROGYNY-CSP-DCI_UNITEDHEALTHCARE-CHOICE-PLUS_-STS_0L_in-network-rates.json"
# MAGIC
# MAGIC gzip -d -c "/Volumes/mimi_ws_1/payermrf/src/unh/gz/2024-05-01_UMR--Inc-_Third-Party-Administrator_FUSION-HEALTH-TENET-IN-MI-PROGYNY-CSP-DCI_UNITEDHEALTHCARE-CHOICE-PLUS_-TNS_0L_in-network-rates.json.gz" > "/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates/2024-05-01_UMR--Inc-_Third-Party-Administrator_FUSION-HEALTH-TENET-IN-MI-PROGYNY-CSP-DCI_UNITEDHEALTHCARE-CHOICE-PLUS_-TNS_0L_in-network-rates.json"
# MAGIC
# MAGIC gzip -d -c "/Volumes/mimi_ws_1/payermrf/src/unh/gz/2024-05-01_UMR--Inc-_Third-Party-Administrator_PROGYNY-SANFORD-TENET-IN-MICHIGAN-CSP-DCI_UNITED-HEALTH-CARE---OPTIONS-PPO_-STP_58_in-network-rates.json.gz" > "/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates/2024-05-01_UMR--Inc-_Third-Party-Administrator_PROGYNY-SANFORD-TENET-IN-MICHIGAN-CSP-DCI_UNITED-HEALTH-CARE---OPTIONS-PPO_-STP_58_in-network-rates.json"

# COMMAND ----------


