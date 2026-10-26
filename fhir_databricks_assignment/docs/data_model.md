# Data model and pipeline notes

## Flow

```text
HAPI FHIR R4
  └── Bundle pages (FHIR next links)
       ├── Raw files: raw/<Resource>/extract_date=.../run_id=.../page-....json
       ├── fhir_api_log (Delta audit)
       └── Bronze Delta tables
            └── Silver SCD2 history tables
                 ├── silver_patient_current
                 ├── silver_encounter_current
                 ├── silver_observation_current
                 └── silver_condition_current
                      └── Gold reporting views
```

## Table inventory

| Table / view | Purpose |
|---|---|
| `fhir_api_log` | One audit row per API page attempt; API call, response, save timestamps, status, count, URL, raw path and error |
| `bronze_patient` | Append-only ingested Patient resource versions |
| `bronze_encounter` | Append-only ingested Encounter resource versions |
| `bronze_observation` | Append-only ingested Observation resource versions |
| `bronze_condition` | Append-only ingested Condition resource versions |
| `silver_<resource>_history` | One row per observed resource content version, with `valid_from`, `valid_to`, `is_current`, `record_hash` |
| `silver_<resource>_current` | View of current SCD2 rows |
| `gold_<resource>_overview` | Current resource counts grouped by status |
| `gold_ingestion_daily` | Daily API page success/failure and entry count summary |
| `gold_patient_summary` | High-level current patient count and latest update |

## SCD Type 2 rules

- Business key: `(resource_type, resource_id)`.
- Change detector: SHA-256 of canonicalized resource JSON.
- New ID: insert first current row.
- Changed JSON for existing ID: close prior row (`valid_to`, `is_current=false`) and append a new current version.
- Identical JSON: no new history version.
- `valid_from` uses FHIR `meta.lastUpdated` when available, otherwise `data_saved_at`.
- `valid_to` is the pipeline processing time at which the previous version was superseded.

The implementation retains the complete JSON payload for flexible downstream flattening. Common fields (`status`, references, display text, effective datetime) are extracted for basic analytics. A production implementation should use explicit, resource-specific schemas and test FHIR choice types and arrays.

## API and pagination

The pipeline uses `_lastUpdated=ge<UTC timestamp>` and `_count`, then follows each Bundle's `link[relation=next].url`. Raw payloads are written before parsing so the response body can be replayed. API calls are sequential in the required order.

## Production hardening ideas

Add a durable watermark, bounded exponential backoff, rate-limit handling, dead-letter table, retry policy, API response schema validation, source deletion/tombstone reconciliation, alerting, secrets management, and PHI governance. The public HAPI endpoint is a test endpoint and does not provide production reliability guarantees.
