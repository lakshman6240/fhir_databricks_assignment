
# ============================================================
# FHIR DATABRICKS PIPELINE
# Compatible with Databricks Serverless
# File: src/fhir_pipeline.py
# ============================================================

import os
import json
import time
import hashlib
import uuid
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import requests

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)


# ============================================================
# 1. HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_run_id():
    return str(uuid.uuid4())


def quote_identifier(name):
    """Quote a SQL identifier."""
    return "`" + name.replace("`", "``") + "`"


def table_name(catalog, schema, table):
    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(table)}"
    )


def resource_table_name(resource_type):
    return "bronze_" + resource_type.lower()


def record_hash(resource):
    content = json.dumps(
        resource,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def get_last_updated(resource):
    return (
        resource.get("meta", {}).get("lastUpdated")
        if isinstance(resource, dict)
        else None
    )


# ============================================================
# 2. BRONZE TABLE SCHEMA
# ============================================================

BRONZE_SCHEMA = StructType([
    StructField("resource_type", StringType(), False),
    StructField("resource_id", StringType(), False),
    StructField("source_last_updated", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("run_id", StringType(), False),
    StructField("record_hash", StringType(), False),
    StructField("raw_json", StringType(), False)
])


AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("resource_type", StringType(), False),
    StructField("page_number", LongType(), False),
    StructField("http_status", LongType(), True),
    StructField("records_fetched", LongType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), False)
])


# ============================================================
# 3. CREATE TABLES
# ============================================================

def ensure_tables(spark, catalog, schema):
    """
    Create the database schema, Bronze tables, and API audit table.
    """

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}"
    )

    resource_types = [
        "Patient",
        "Encounter",
        "Observation",
        "Condition"
    ]

    for resource_type in resource_types:
        full_table = table_name(
            catalog,
            schema,
            resource_table_name(resource_type)
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                resource_type STRING,
                resource_id STRING,
                source_last_updated STRING,
                ingestion_timestamp TIMESTAMP,
                run_id STRING,
                record_hash STRING,
                raw_json STRING
            )
            USING DELTA
        """)

    audit_table = table_name(
        catalog, schema, "fhir_api_log"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            run_id STRING,
            resource_type STRING,
            page_number BIGINT,
            http_status BIGINT,
            records_fetched BIGINT,
            status STRING,
            error_message STRING,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        )
        USING DELTA
    """)

    print("Bronze and audit tables are ready.")


# ============================================================
# 4. SERVERLESS-COMPATIBLE RAW FILE WRITER
# ============================================================

def write_raw_json(
    raw_root,
    resource_type,
    run_id,
    page_number,
    data
):
    """
    Save one FHIR Bundle to a Unity Catalog Volume.
    Does not access spark._jvm.
    """

    output_dir = os.path.join(
        raw_root,
        resource_type.lower(),
        run_id
    )

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(
        output_dir,
        f"page_{page_number:05d}.json"
    )

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False
        )

    return output_path


# ============================================================
# 5. AUDIT LOGGING
# ============================================================

def append_audit(
    spark,
    catalog,
    schema,
    run_id,
    resource_type,
    page_number,
    http_status,
    records_fetched,
    status,
    error_message,
    started_at,
    finished_at
):
    audit_table = table_name(
        catalog, schema, "fhir_api_log"
    )

    row = [(
        run_id,
        resource_type,
        int(page_number),
        int(http_status) if http_status is not None else None,
        int(records_fetched),
        status,
        error_message,
        started_at,
        finished_at
    )]

    df = spark.createDataFrame(row, schema=AUDIT_SCHEMA)

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            f"{catalog}.{schema}.fhir_api_log"
        )
    )


# ============================================================
# 6. FETCH FHIR BUNDLES
# ============================================================

def fetch_bundle(
    session,
    url,
    params,
    timeout_seconds
):
    """
    Fetch a FHIR Bundle. Follow pagination links returned by
    the FHIR server. Return the response and parsed JSON.
    """

    response = session.get(
        url,
        params=params,
        timeout=timeout_seconds
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("FHIR response is not a JSON object.")

    if payload.get("resourceType") != "Bundle":
        raise ValueError(
            "Expected a FHIR Bundle; received resourceType="
            + str(payload.get("resourceType"))
        )

    return response, payload


def next_page_url(bundle):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def extract_resources(bundle, expected_type):
    resources = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource")

        if not isinstance(resource, dict):
            continue

        if resource.get("resourceType") != expected_type:
            continue

        if not resource.get("id"):
            continue

        resources.append(resource)

    return resources


# ============================================================
# 7. WRITE RESOURCE RECORDS TO BRONZE
# ============================================================

def append_bronze(
    spark,
    catalog,
    schema,
    resource_type,
    resources,
    run_id
):
    if not resources:
        return 0

    ingestion_time = utc_now()
    rows = []

    for resource in resources:
        rows.append((
            resource_type,
            str(resource["id"]),
            get_last_updated(resource),
            ingestion_time,
            run_id,
            record_hash(resource),
            json.dumps(
                resource,
                ensure_ascii=False,
                separators=(",", ":")
            )
        ))

    df = spark.createDataFrame(
        rows,
        schema=BRONZE_SCHEMA
    )

    target = (
        f"{catalog}.{schema}."
        f"{resource_table_name(resource_type)}"
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target)
    )

    return len(rows)


# ============================================================
# 8. INGEST ONE RESOURCE TYPE
# ============================================================

def ingest_resource(
    spark,
    config,
    session,
    resource_type,
    run_id
):
    catalog = config["catalog"]
    schema = config["schema"]
    raw_root = config["raw_root"]

    base_url = config.get(
        "base_url",
        "https://hapi.fhir.org/baseR4"
    ).rstrip("/")

    page_size = int(config.get("page_size", 100))
    timeout_seconds = int(
        config.get("timeout_seconds", 60)
    )
    lookback_days = int(config.get("lookback_days", 3))

    # FHIR search date is expressed in UTC.
    since = (
        datetime.now(timezone.utc)
        - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"{base_url}/{resource_type}"
    params = {
        "_lastUpdated": f"ge{since}",
        "_count": page_size
    }

    page_number = 0
    total_records = 0
    seen_urls = set()

    print(f"Starting {resource_type} for run_id={run_id}")

    while url:
        page_number += 1
        started_at = utc_now()
        response = None

        # Prevent accidental pagination loops.
        if url in seen_urls:
            raise RuntimeError(
                f"Pagination loop detected for {resource_type}"
            )

        seen_urls.add(url)

        try:
            response, bundle = fetch_bundle(
                session,
                url,
                params,
                timeout_seconds
            )

            # Save the original Bundle to the Volume.
            raw_path = write_raw_json(
                raw_root,
                resource_type,
                run_id,
                page_number,
                bundle
            )

            resources = extract_resources(
                bundle,
                resource_type
            )

            inserted = append_bronze(
                spark,
                catalog,
                schema,
                resource_type,
                resources,
                run_id
            )

            total_records += inserted

            append_audit(
                spark=spark,
                catalog=catalog,
                schema=schema,
                run_id=run_id,
                resource_type=resource_type,
                page_number=page_number,
                http_status=response.status_code,
                records_fetched=inserted,
                status="SUCCESS",
                error_message=None,
                started_at=started_at,
                finished_at=utc_now()
            )

            print(
                f"{resource_type} page {page_number}: "
                f"{inserted} resources; raw file: {raw_path}"
            )

            url = next_page_url(bundle)

            # The next link is an absolute URL supplied by the API.
            # Clear params so the search parameters aren't duplicated.
            params = None

        except Exception as exc:
            append_audit(
                spark=spark,
                catalog=catalog,
                schema=schema,
                run_id=run_id,
                resource_type=resource_type,
                page_number=page_number,
                http_status=(
                    response.status_code
                    if response is not None
                    else getattr(
                        getattr(exc, "response", None),
                        "status_code",
                        None
                    )
                ),
                records_fetched=0,
                status="FAILED",
                error_message=str(exc)[:4000],
                started_at=started_at,
                finished_at=utc_now()
            )

            raise

        # Be polite to the public test server.
        time.sleep(float(config.get("request_pause_seconds", 0.2)))

    print(
        f"Completed {resource_type}: "
        f"{total_records} resources ingested."
    )

    return total_records


# ============================================================
# 9. RUN THE COMPLETE PIPELINE
# ============================================================

def run_pipeline(spark, config):
    required = [
        "catalog",
        "schema",
        "raw_root"
    ]

    missing = [
        key for key in required
        if not config.get(key)
    ]

    if missing:
        raise ValueError(
            "Missing required configuration keys: "
            + ", ".join(missing)
        )

    raw_root = config["raw_root"]

    # Ensure the Volume is accessible before calling the API.
    os.makedirs(raw_root, exist_ok=True)

    run_id = get_run_id()

    resource_types = [
        "Patient",
        "Encounter",
        "Observation",
        "Condition"
    ]

    session = requests.Session()
    session.headers.update({
        "Accept": "application/fhir+json",
        "User-Agent": "Databricks-FHIR-Assignment/1.0"
    })

    totals = {}

    try:
        for resource_type in resource_types:
            totals[resource_type] = ingest_resource(
                spark=spark,
                config=config,
                session=session,
                resource_type=resource_type,
                run_id=run_id
            )
    finally:
        session.close()

    print("\n========== INGESTION SUMMARY ==========")
    print("Run ID:", run_id)

    for resource_type, count in totals.items():
        print(f"{resource_type}: {count}")

    print("=======================================")

    return {
        "run_id": run_id,
        "counts": totals
    }
