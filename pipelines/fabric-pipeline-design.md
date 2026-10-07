# Fabric Pipeline Design

## Purpose

`pl_cre_end_to_end` orchestrates the notebook-based medallion workflow in each environment's workspace. `pipelines/fabric-pipeline-manifest.json` is the build specification; this page explains it.

## Activities

| Order | Activity | Default Lakehouse | Retry | Runs when |
|---|---|---|---|---|
| 1 | `nb_cre_bronze_ingest` | `lh_cre_bronze` | 1 | Pipeline starts |
| 2 | `nb_cre_silver_transform` | `lh_cre_silver` | 0 | Bronze succeeded |
| 3 | `nb_cre_gold_build_model` | `lh_cre_gold` | 0 | Silver succeeded |
| 4 | `nb_cre_gold_validate_model` | `lh_cre_gold` | 0 | Gold succeeded |
| — | `log_pipeline_failure` | — | — | Any activity failed or was skipped |

Each activity is named after the notebook it runs. Bronze gets one retry because it calls external sources; the later layers read only Lakehouse data, so a failure there indicates a data or contract problem that a retry won't fix.

## Parameters

- `environment`: `dev`, `test`, or `prod`, set by deployment rules.
- `pipeline_run_id`: to be passed to the notebooks so every layer's audit columns share one run ID. Until the pipeline is built, `nb_cre_bronze_ingest` generates its own run ID, which Silver and Gold carry forward from the Bronze rows.

## Design Notes

- Each notebook fails loudly before writing bad data (missing landing files, absent configured columns, grain violations, semantic contract failures), so the pipeline's success condition is the data-quality gate.
- The semantic model should refresh only after `nb_cre_gold_validate_model` succeeds.
- `pipelines/adf-style-orchestration-pattern.md` describes the metadata-driven Lookup/ForEach pattern planned for watermark-based incremental loads.
