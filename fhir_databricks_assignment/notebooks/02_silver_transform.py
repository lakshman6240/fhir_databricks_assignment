# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Silver transformations and SCD Type 2
# MAGIC Reads Bronze Delta tables, deduplicates identical versions, extracts common FHIR fields, and maintains history/current tables.

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# CONFIGURATION
# ============================================================
CATALOG = "workspace"
SCHEMA = "default"

RESOURCE_TYPES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

print("Starting Silver transformations...")

# ============================================================
# PROCESS EACH BRONZE TABLE
# ============================================================
for resource_type in RESOURCE_TYPES:

    resource_lower = resource_type.lower()

    bronze_table = (
        f"{CATALOG}.{SCHEMA}.bronze_{resource_lower}"
    )

    silver_history_table = (
        f"{CATALOG}.{SCHEMA}.silver_{resource_lower}_history"
    )

    silver_current_view = (
        f"silver_{resource_lower}_current"
    )

    print(f"\nProcessing {resource_type}...")

    # Check that the Bronze table exists
    if not spark.catalog.tableExists(bronze_table):
        print(f"SKIPPED: Bronze table not found: {bronze_table}")
        continue

    bronze_df = spark.table(bronze_table)

    if bronze_df.limit(1).count() == 0:
        print(f"SKIPPED: No records in {bronze_table}")
        continue

    # Verify required columns
    required_columns = {
        "resource_id",
        "ingestion_timestamp",
        "record_hash",
        "raw_json"
    }

    missing_columns = required_columns - set(bronze_df.columns)

    if missing_columns:
        raise ValueError(
            f"{bronze_table} is missing columns: "
            f"{sorted(missing_columns)}"
        )

    # ========================================================
    # 1. REMOVE REPEATED COPIES OF THE SAME RESOURCE VERSION
    # ========================================================
    dedup_window = (
        Window
        .partitionBy("resource_id", "record_hash")
        .orderBy(F.col("ingestion_timestamp").asc())
    )

    versions = (
        bronze_df
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(dedup_window)
        )
        .filter(F.col("_duplicate_rank") == 1)
        .drop("_duplicate_rank")
    )

    # ========================================================
    # 2. BUILD SCD TYPE 2 VALIDITY PERIODS
    # ========================================================
    history_window = (
        Window
        .partitionBy("resource_id")
        .orderBy(
            F.col("ingestion_timestamp").asc(),
            F.col("record_hash").asc()
        )
    )

    history_df = (
        versions
        .withColumn(
            "valid_from",
            F.col("ingestion_timestamp")
        )
        .withColumn(
            "valid_to",
            F.lead("ingestion_timestamp").over(history_window)
        )
        .withColumn(
            "is_current",
            F.col("valid_to").isNull()
        )
    )

    # ========================================================
    # 3. WRITE SILVER HISTORY TABLE
    # ========================================================
    (
        history_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_history_table)
    )

    # ========================================================
    # 4. CREATE CURRENT-RECORD VIEW
    # ========================================================
    spark.sql(f"""
        CREATE OR REPLACE VIEW
        {CATALOG}.{SCHEMA}.{silver_current_view}
        AS
        SELECT *
        FROM {silver_history_table}
        WHERE is_current = true
    """)

    history_count = spark.table(
        silver_history_table
    ).count()

    current_count = spark.table(
        silver_history_table
    ).filter(
        F.col("is_current") == True
    ).count()

    print(f"History table: {silver_history_table}")
    print(f"History records: {history_count}")
    print(f"Current records: {current_count}")
    print(f"Current view: {silver_current_view}")

print("\nSilver transformations finished.")

# ============================================================
# 5. VERIFY CREATED TABLES
# ============================================================
display(
    spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
)
