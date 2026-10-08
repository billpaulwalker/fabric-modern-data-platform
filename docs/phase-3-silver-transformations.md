# Phase 3: Silver Transformations

## Objective

Convert raw Bronze records into reliable, typed, deduplicated Silver datasets while retaining source lineage and isolating invalid records.

## Processing Contract

Each table follows the same order:

1. Read the Bronze dataset.
2. Normalize source column names to snake_case.
3. Convert blank strings to nulls.
4. Cast configured columns to business data types.
5. Validate required fields, numeric ranges, and date order.
6. Route invalid records to quarantine with one or more reasons.
7. Deduplicate valid records by business key using latest-record-wins logic.
8. Preserve Bronze audit columns and add Silver processing metadata.
9. Write valid and rejected outputs plus row-count metrics.

## Outputs

| Runtime | Valid output | Rejected output |
|---|---|---|
| Local/CI | `data/silver/silver_<table>.csv` | `data/rejected/silver_<table>_rejected.csv` |
| Fabric | `lh_cre_silver.<domain>.<table>` Delta table | `lh_cre_silver.quarantine.rejected_records` Delta table |

Local CSV outputs are development analogs. The Fabric PySpark notebook is the production-shaped implementation and writes managed Delta tables.

## Lineage Columns Preserved

- `source_system`
- `source_object`
- `source_file_name`
- `ingestion_timestamp`
- `pipeline_run_id`
- `load_type`
- `raw_record_hash`

Silver adds `data_quality_status` and `silver_processed_timestamp`. Rejected records also receive `rejection_reason`.

## Local Execution

Run Phase 2 first so `data/bronze/` exists, then execute:

```powershell
python notebooks/04_silver_transformations.py
python -m pytest
```

Review:

```text
data/silver/silver_run_metrics.json
data/silver/
data/rejected/
```

The run should satisfy this reconciliation formula for each table:

```text
rows_read = rows_valid + rows_rejected + duplicate_rows_removed
```

## Fabric Execution

1. Ensure Phase 2 data exists in `lh_cre_bronze`.
2. Upload `config/fabric_layout.json` and `config/silver_table_config.json` to `lh_cre_bronze` at `Files/config/`, where every notebook reads config. The notebook reads the same contract, including `column_mappings` and `derived_columns`, as the local pipeline, and fails before writing if a configured column is absent from Bronze.
3. Create a Fabric notebook from `notebooks/fabric/nb_cre_silver_transform.py` with `lh_cre_bronze` as its default Lakehouse. It writes to `lh_cre_silver` by full table name.
4. Run all cells. The notebook reads Bronze tables from `lh_cre_bronze` by their full names.
5. Confirm Delta tables in the `property`, `leasing`, `finance`, `operations`, and `environment` schemas.
6. Review `quarantine.rejected_records`.
7. Run `sql/silver_acceptance_queries.sql` in the `lh_cre_silver` SQL analytics endpoint.

The full layout and setup steps are in `docs/fabric-getting-started.md`.

## Design Decisions

**Fail the pipeline for missing required columns.** This indicates schema drift or a broken data contract, not a bad individual record.

**Quarantine record-level errors.** Invalid types, missing values, negative business amounts, and impossible date ranges should not block valid records.

**Keep the latest duplicate.** The business key identifies the entity; `ingestion_timestamp`, followed by `updated_at`, determines the surviving record.

**Retain raw lineage.** Silver remains traceable to the Bronze source record and its pipeline run.

## Completion Checkpoint

- All Phase 2 Bronze inputs transform successfully.
- Local tests pass.
- Every valid output has Silver status and processing timestamp columns.
- Rejected records contain actionable reasons.
- No duplicate configured business keys remain in valid output.
- Metrics reconcile for every table.
- The GitHub Actions validation workflow is green after the Phase 3 commit.
