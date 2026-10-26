# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Incremental FHIR API ingestion
# MAGIC Fetches Patient → Encounter → Observation → Condition, saves raw JSON Bundles, appends Bronze Delta tables, and audits API calls.
# MAGIC
# MAGIC Update PROJECT_ROOT and CONFIG_PATH for your Databricks workspace. Configure a Unity Catalog Volume for raw storage.

# COMMAND ----------


import os
import sys
import json
import importlib

PROJECT_ROOT = (
    "/Workspace/Users/chavalilakshmankumar@gmail.com/"
    "fhir_databricks_assignment"
)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
CONFIG_PATH = os.path.join(
    PROJECT_ROOT, "config", "config.example.json"
)

sys.path.insert(0, SRC_PATH)

import fhir_pipeline
importlib.reload(fhir_pipeline)

from fhir_pipeline import run_pipeline, ensure_tables

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

# Use the existing Unity Catalog Volume.
config["catalog"] = "workspace"
config["schema"] = "default"
config["raw_root"] = (
    "/Volumes/workspace/default/lakehouse_files/raw"
)
config["lookback_days"] = 3

ensure_tables(
    spark,
    config["catalog"],
    config["schema"]
)

result = run_pipeline(spark, config)
print(result)
