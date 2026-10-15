# Project Case Study

## Problem

A Commercial Real Estate company needs one trusted source for rent collections, leasing, maintenance, and budget reporting. The data sits in an operational SQL system, business-managed spreadsheets, and an external weather API, and reports are rebuilt by hand with no audit trail.

## Solution

A Microsoft Fabric lakehouse with a Bronze, Silver, and Gold Lakehouse per environment, orchestrated by one Data Pipeline and published through a Direct Lake semantic model and a six-page Power BI report.

| Layer | What it does |
|---|---|
| Bronze (`lh_cre_bronze`) | Lands SQL extracts, business files, and API JSON as Delta tables with audit columns and the pipeline's run ID |
| Silver (`lh_cre_silver`) | Config-driven typing, validation, deduplication, and quarantine of rejected records |
| Gold (`lh_cre_gold`) | Star schema: three conformed dimensions, four facts, Unknown members, and a pipeline run log |
| Semantic model | **CRE Portfolio Analytics**: Direct Lake on OneLake, twelve relationships, governed measures |
| Report | Executive Overview, Rent Collections, Lease Portfolio, Maintenance Operations, Budget Versus Actual, and Data Quality |

## What Is Running

- `pl_cre_end_to_end` runs the four medallion notebooks, validates Gold against the model contract, refreshes the model only if validation passes, and logs every run, successful or failed.
- The Development workspace is versioned in Azure DevOps and promoted to Test with the deployment pipeline `dp-cre-modernization`; per-stage values come from the Variable library `vl_cre_settings`.
- This repository holds the tested reference implementation: unit and contract tests, including local PySpark runs of every Fabric notebook on Fabric Runtime 2.0 with ANSI mode on and off, and CI on every push.

## Engineering Decisions

- **Validation gates the report.** Automatic Direct Lake updates are off, so the report changes only after a run passes validation; a bad load never reaches users.
- **One Spark session per run.** Every notebook shares one default Lakehouse and session tag, after the first pipeline runs hit trial-capacity rejections (HTTP 430) at each layer boundary.
- **Run ID lineage.** Bronze stamps every row with the pipeline's run ID, so any Gold row traces back to a run in the Monitor hub and in `audit.pipeline_runs`.
- **Stage values outside the pipeline.** Deployments copy fixed IDs unchanged and Data Pipelines have no deployment rules, so the refresh target and environment come from a Variable library.
- **Same names in every stage.** Item names and folders never include the environment; the workspace carries it, which lets the deployment pipeline pair items automatically.

## Business Outcomes

- Rent collection rate, outstanding rent, and year-over-year collections by property, region, and tenant
- Lease portfolio and expiry visibility
- Maintenance cost, variance, and resolution time by property and priority
- Budget versus actual rent and NOI
- Pipeline health and data-quality checks beside the business data

## Talking Points

- **Pitch:** a Fabric lakehouse that turns SQL, file, and API sources into a validated star schema and a governed Direct Lake model, promoted from Development to Test with Git versioning and a deployment pipeline.
- **Architecture:** Bronze preserves raw data for replay and audit, Silver enforces quality rules and quarantines failures, Gold provides the star schema; each layer is its own Lakehouse so access can be granted by layer.
- **Production readiness:** validation gates the refresh, every run is logged, failures route to one handler, Test and Production never get manual edits, and Production uses a service principal, alerts, and a rollback runbook.
- **Next steps:** watermark-based incremental loads and row-level security, both designed in [`architecture.md`](architecture.md).
