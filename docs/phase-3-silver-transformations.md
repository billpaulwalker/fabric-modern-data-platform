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
| Fabric | `silver.<table>` Delta table | `silver_quarantine.rejected_records` Delta table |

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

1. Create or attach a schema-enabled Lakehouse.
2. Ensure Phase 2 data exists as `bronze.<table>` Delta tables.
3. Create a Fabric notebook from `notebooks/fabric/04_silver_transformations_pyspark.py`.
4. Attach the Lakehouse to the notebook.
5. Run all cells.
6. Confirm Delta tables in the `silver` schema.
7. Review `silver_quarantine.rejected_records`.
8. Run `sql/silver_acceptance_queries.sql` through the SQL analytics endpoint.

If the Lakehouse does not support schemas, use table prefixes such as `silver_leases` and `silver_quarantine_rejected_records` instead.

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
