# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest In-Network files

# COMMAND ----------

!pip install tqdm json-stream

# COMMAND ----------

from pathlib import Path
import json_stream
import pandas as pd
from tqdm import tqdm

volumepath = "/Volumes/mimi_ws_1/payermrf/src/unh/unzipped_in_network_rates"
fn = "2024-05-01_UMR--Inc-_Third-Party-Administrator_FUSION-HEALTH-TENET-IN-MI-PROGYNY-CSP-DCI_UNITEDHEALTHCARE-CHOICE-PLUS_-TNS_0L_in-network-rates.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Provider Network Table

# COMMAND ----------

pdf_lst = []
with open(f"{volumepath}/{fn}", "r") as fp:
    data = json_stream.load(fp)
    for provider_reference_obj in tqdm(data["provider_references"]):
        dfraw = []
        for provider_obj in provider_reference_obj["provider_groups"]:
            npi_lst = [str(npi)[:10] for npi in provider_obj["npi"]]
            tin_obj = {k: str(v) for k, v in provider_obj["tin"].items()}
            dfraw.append([tin_obj["value"], tin_obj["type"], npi_lst])
        pdf = pd.DataFrame(dfraw, 
                     columns=["tin_value", "tin_type", "npi_lst"])
        pdf["provider_group_id"] = provider_reference_obj["provider_group_id"]
        pdf["source"] = fn
        pdf_lst.append(pdf)
df = spark.createDataFrame(pd.concat(pdf_lst))

# COMMAND ----------

(df.write
    .format("delta")
    .mode("append")
    .saveAsTable("mimi_ws_1.payermrf.provider_references"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest In-Network Rates

# COMMAND ----------

pdf_lst = []
header = ["negotiation_arrangement",
            "service_code",
            "billing_class",
            "billing_code", "name", "description",
            "billing_code_type", "billing_code_type_version",
            "billing_code_modifier",
            "negotiated_type",
            "negotiated_rate",
            "expiration_date",
            "additional_information",
            "provider_reference",
            "reporting_entity_name", 
            "reporting_entity_type",
            "source"]
dfraw = []
with open(f"{volumepath}/{fn}", "r") as fp:
    data = json_stream.load(fp)
    reporting_entity_name = data["reporting_entity_name"]
    reporting_entity_type = data["reporting_entity_type"]
    for in_network_obj in tqdm(data["in_network"]):
        negotiation_arrangement = in_network_obj["negotiation_arrangement"]
        name = in_network_obj["name"]
        billing_code_type = in_network_obj["billing_code_type"]
        billing_code_type_version = in_network_obj["billing_code_type_version"]
        billing_code = in_network_obj["billing_code"]
        description = in_network_obj["description"]
        for negotiated_rate_details_obj in in_network_obj["negotiated_rates"]:
            provider_references = [x for x in negotiated_rate_details_obj["provider_references"]]
            for negotiated_price_obj in negotiated_rate_details_obj["negotiated_prices"]:
                _obj = {k: v for k, v in negotiated_price_obj.persistent().items()}
                negotiated_type = _obj["negotiated_type"]
                negotiated_rate = _obj["negotiated_rate"]
                expiration_date = _obj["expiration_date"]
                billing_class = _obj["billing_class"]
                service_code = [x for x in _obj.get("service_code", [])]
                billing_code_modifier = [x for x in _obj.get("billing_code_modifier", [])]
                additional_information = _obj.get("additional_information")
                for provider_reference in provider_references:
                    dfraw.append([negotiation_arrangement,
                                ";".join(service_code),
                                billing_class,
                                billing_code, name, description,
                                billing_code_type, billing_code_type_version,
                                ";".join(billing_code_modifier),
                                negotiated_type,
                                negotiated_rate,
                                expiration_date,
                                additional_information,
                                provider_reference,
                                reporting_entity_name, 
                                reporting_entity_type,
                                fn])
        if len(dfraw) > 500000:
            df = spark.createDataFrame(pd.DataFrame(dfraw, columns=header))
            (df.write
                .format("delta")
                .mode("append")
                .saveAsTable("mimi_ws_1.payermrf.in_network_rates"))
            dfraw = []

if len(dfraw) > 0:
    df = spark.createDataFrame(pd.DataFrame(dfraw, columns=header))
    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable("mimi_ws_1.payermrf.in_network_rates"))
    dfraw = []
