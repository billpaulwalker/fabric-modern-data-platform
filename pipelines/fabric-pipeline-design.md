# Fabric Pipeline Design

## Purpose

`pl_cre_end_to_end` orchestrates the notebook-based medallion workflow in each environment's workspace. `pipelines/fabric-pipeline-manifest.json` is the build specification, `docs/fabric-data-pipeline.md` is the step-by-step build guide, and this page explains the design.

## Activities

| Activity | Notebook | Writes to | Runs when |
|---|---|---|---|
| `nb_cre_bronze_ingest` | `nb_cre_bronze_ingest` | `lh_cre_bronze` | Pipeline starts |
| `nb_cre_silver_transform` | `nb_cre_silver_transform` | `lh_cre_silver` | Bronze succeeded |
| `nb_cre_gold_build_model` | `nb_cre_gold_build_model` | `lh_cre_gold` | Silver succeeded |
| `nb_cre_gold_validate_model` | `nb_cre_gold_validate_model` | (reads `lh_cre_gold`) | Gold succeeded |
| `refresh_semantic_model` | Semantic model refresh: **CRE Portfolio Analytics** | (the model) | Validation succeeded |
| `log_run_succeeded` | `nb_cre_gold_log_pipeline_run` | `lh_cre_gold` | Refresh succeeded |
| `log_run_failed` | `nb_cre_gold_log_pipeline_run` | `lh_cre_gold` | Refresh failed **or** was skipped |

Medallion activities are named after the notebook they run.

## Retries and Spark Sessions

Every activity retries once after 120 seconds. The failures a retry fixes are transient: Spark session start-up errors, and a capacity with no free compute rejecting the job (HTTP 430, `TooManyRequestsForCapacity`), which can happen whenever the capacity is busy. A retry is safe because the medallion notebooks replace their tables, so rerunning never duplicates business data; at worst, a log activity that failed after writing its row logs the run twice. A genuine data or contract failure fails the retry the same way, about two minutes later.

## One Spark Session per Run

Every notebook uses `lh_cre_bronze` as its default Lakehouse and every activity carries the session tag `cre_pipeline`. With high concurrency for pipelines enabled, Fabric reuses a Spark session only between notebooks with the same default Lakehouse and session tag, so the whole run shares one session.

This was learned in the first Fabric runs. With each notebook defaulting to the Lakehouse it writes, every layer boundary (Bronze to Silver, Silver to Gold) needed a new session while the previous one still held compute, and the trial capacity rejected the new session with HTTP 430. Each first attempt failed and the retry two minutes later succeeded; activities that kept the same default Lakehouse never failed.

The default Lakehouse now only supplies config and landing files. Every table is addressed by its full `lakehouse.schema.table` name from `config/fabric_layout.json`, so the shared default changes nothing about where data is written. The workspace Starter Pool is also capped at two nodes; see step 1 of `docs/fabric-data-pipeline.md`.

## Run Logging

Every run writes exactly one row to `lh_cre_gold.audit.pipeline_runs`, with the run ID, pipeline name, environment, status, trigger time, and log time.

- `log_run_succeeded` depends on the refresh **succeeding**.
- `log_run_failed` depends on the refresh **failing or being skipped**. Two conditions on the same dependency are combined with OR, and any failure earlier in the chain skips the refresh, so this one activity catches every failed run without a dependency on each step.

## Validation Gates the Report

The semantic model's automatic Direct Lake updates are turned off, so it changes only when `refresh_semantic_model` runs, and that runs only after `nb_cre_gold_validate_model` succeeds. Gold tables that fail validation are never shown in the report; it keeps the last validated data until a run passes.

This is the generic error-handling pattern Fabric pipelines inherit from Azure Data Factory. The run log lives in the Gold Lakehouse so the Direct Lake model can report pipeline health beside the business data.

## Parameters

| Name | Source | Used by |
|---|---|---|
| `environment` | Pipeline parameter; `dev`, `test`, or `prod`, set by deployment rules | Run log |
| `pipeline_run_id` | `@pipeline().RunId` | Bronze audit columns, carried forward through Silver and Gold, and the run log |

Because Bronze stamps every row with the pipeline's own run ID, any Bronze, Silver, or Gold row can be traced to a run in the Monitor hub and to its `audit.pipeline_runs` row.

## Design Notes

- Each notebook fails loudly before writing bad data (missing landing files, absent configured columns, grain violations, semantic contract failures), so the pipeline's success condition is the data-quality gate.
- `pipelines/adf-style-orchestration-pattern.md` describes the metadata-driven Lookup/ForEach pattern planned for watermark-based incremental loads.
