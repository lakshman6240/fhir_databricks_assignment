# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 — Gold reporting views
# MAGIC Creates stable views for analytics and a small daily ingestion summary.

# COMMAND ----------


from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "default"

print("Creating Gold analytics views...")

# ============================================================
# 1. RESOURCE SUMMARY
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_resource_summary AS

    SELECT 'Patient' AS resource_type, COUNT(*) AS current_record_count
    FROM {CATALOG}.{SCHEMA}.silver_patient_current

    UNION ALL

    SELECT 'Encounter', COUNT(*)
    FROM {CATALOG}.{SCHEMA}.silver_encounter_current

    UNION ALL

    SELECT 'Observation', COUNT(*)
    FROM {CATALOG}.{SCHEMA}.silver_observation_current

    UNION ALL

    SELECT 'Condition', COUNT(*)
    FROM {CATALOG}.{SCHEMA}.silver_condition_current
""")

# ============================================================
# 2. ENCOUNTERS PER PATIENT
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_encounters_per_patient AS
    SELECT
        get_json_object(raw_json, '$.subject.reference')
            AS patient_reference,
        COUNT(*) AS encounter_count
    FROM {CATALOG}.{SCHEMA}.silver_encounter_current
    WHERE get_json_object(raw_json, '$.subject.reference') IS NOT NULL
    GROUP BY get_json_object(raw_json, '$.subject.reference')
""")

# ============================================================
# 3. OBSERVATION CATEGORY SUMMARY
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_observation_summary AS
    SELECT
        COALESCE(
            get_json_object(raw_json, '$.category[0].coding[0].display'),
            get_json_object(raw_json, '$.category[0].coding[0].code'),
            'Unknown'
        ) AS observation_category,
        COUNT(*) AS observation_count
    FROM {CATALOG}.{SCHEMA}.silver_observation_current
    GROUP BY
        COALESCE(
            get_json_object(raw_json, '$.category[0].coding[0].display'),
            get_json_object(raw_json, '$.category[0].coding[0].code'),
            'Unknown'
        )
""")

# ============================================================
# 4. CONDITIONS PER PATIENT
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_conditions_per_patient AS
    SELECT
        get_json_object(raw_json, '$.subject.reference')
            AS patient_reference,
        COUNT(*) AS condition_count
    FROM {CATALOG}.{SCHEMA}.silver_condition_current
    WHERE get_json_object(raw_json, '$.subject.reference') IS NOT NULL
    GROUP BY get_json_object(raw_json, '$.subject.reference')
""")

print("Gold analytics views created successfully.")

# ============================================================
# 5. VERIFY GOLD RESULTS
# ============================================================
display(
    spark.sql(f"""
        SELECT *
        FROM {CATALOG}.{SCHEMA}.gold_resource_summary
        ORDER BY resource_type
    """)
)

print("Gold views available:")
display(
    spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
    .filter(F.col("tableName").like("gold_%"))
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'Bronze Patient' AS table_name, COUNT(*) AS records
# MAGIC FROM workspace.default.bronze_patient
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze Encounter', COUNT(*)
# MAGIC FROM workspace.default.bronze_encounter
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze Observation', COUNT(*)
# MAGIC FROM workspace.default.bronze_observation
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze Condition', COUNT(*)
# MAGIC FROM workspace.default.bronze_condition
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Silver Patient Current', COUNT(*)
# MAGIC FROM workspace.default.silver_patient_current
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Silver Encounter Current', COUNT(*)
# MAGIC FROM workspace.default.silver_encounter_current
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Silver Observation Current', COUNT(*)
# MAGIC FROM workspace.default.silver_observation_current
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'Silver Condition Current', COUNT(*)
# MAGIC FROM workspace.default.silver_condition_current;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.gold_resource_summary
# MAGIC ORDER BY resource_type;