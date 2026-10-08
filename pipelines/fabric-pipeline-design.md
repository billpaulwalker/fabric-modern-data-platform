# Fabric Pipeline Design

## Purpose

`pl_cre_end_to_end` orchestrates the notebook-based medallion workflow in each environment's workspace. `pipelines/fabric-pipeline-manifest.json` is the build specification, `docs/fabric-data-pipeline.md` is the step-by-step build guide, and this page explains the design.

## Activities

| Activity | Notebook | Default Lakehouse | Runs when | Retry |
|---|---|---|---|---|
| `nb_cre_bronze_ingest` | `nb_cre_bronze_ingest` | `lh_cre_bronze` | Pipeline starts | 1 |
| `nb_cre_silver_transform` | `nb_cre_silver_transform` | `lh_cre_silver` | Bronze succeeded | 0 |
| `nb_cre_gold_build_model` | `nb_cre_gold_build_model` | `lh_cre_gold` | Silver succeeded | 0 |
| `nb_cre_gold_validate_model` | `nb_cre_gold_validate_model` | `lh_cre_gold` | Gold succeeded | 0 |
| `log_run_succeeded` | `nb_cre_gold_log_pipeline_run` | `lh_cre_gold` | Validation succeeded | 1 |
| `log_run_failed` | `nb_cre_gold_log_pipeline_run` | `lh_cre_gold` | Validation failed **or** was skipped | 1 |

Medallion activities are named after the notebook they run. Bronze gets one retry because it reads external sources; the later layers read only Lakehouse data, so a failure there points to a data or contract problem that a retry won't fix.

## Run Logging

Every run writes exactly one row to `lh_cre_gold.audit.pipeline_runs`, with the run ID, pipeline name, environment, status, trigger time, and log time.

- `log_run_succeeded` depends on validation **succeeding**.
- `log_run_failed` depends on validation **failing or being skipped**. Two conditions on the same dependency are combined with OR, and any failure earlier in the chain skips validation, so this one activity catches every failed run without a dependency on each step.

This is the generic error-handling pattern Fabric pipelines inherit from Azure Data Factory. The run log lives in the Gold Lakehouse so the Direct Lake model can report pipeline health beside the business data.

## Parameters

| Name | Source | Used by |
|---|---|---|
| `environment` | Pipeline parameter; `dev`, `test`, or `prod`, set by deployment rules | Run log |
| `pipeline_run_id` | `@pipeline().RunId` | Bronze audit columns, carried forward through Silver and Gold, and the run log |

Because Bronze stamps every row with the pipeline's own run ID, any Bronze, Silver, or Gold row can be traced to a run in the Monitor hub and to its `audit.pipeline_runs` row.

## Design Notes

- Each notebook fails loudly before writing bad data (missing landing files, absent configured columns, grain violations, semantic contract failures), so the pipeline's success condition is the data-quality gate.
- The semantic model should refresh only after `nb_cre_gold_validate_model` succeeds.
- `pipelines/adf-style-orchestration-pattern.md` describes the metadata-driven Lookup/ForEach pattern planned for watermark-based incremental loads.
