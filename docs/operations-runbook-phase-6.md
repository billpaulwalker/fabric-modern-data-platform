# Operations Runbook: End-to-End Pipeline

## Success Criteria

- Pipeline status is `SUCCEEDED`.
- No steps are failed or skipped.
- Silver, Gold, and semantic validation metrics reconcile.
- Semantic validation returns `passed: true`.
- Generated Gold tables contain expected row counts.

## Failure Triage

1. Run `python notebooks/08_pipeline_health_report.py`.
2. Identify the failed step and pipeline run ID.
3. Review `data/operations/pipeline_step_runs.jsonl` for its final attempt.
4. Read the captured error and reproduce that step directly.
5. Correct code, configuration, credentials, schema, or source data as appropriate.
6. Rerun the complete pipeline unless the recovery procedure explicitly supports a safe partial rerun.
7. Confirm downstream validation and record the incident outcome.

## Common Failure Classes

| Failure | Response |
|---|---|
| API timeout or transient connection | Allow bounded retry; confirm source availability |
| Missing required column | Stop and reconcile schema contract |
| Invalid source records | Review quarantine output and quality thresholds |
| Duplicate Gold grain | Correct Silver key or Gold grain logic |
| Orphan semantic relationship | Correct date coverage or dimension lookup |
| Unit-test failure | Do not promote; fix regression and rerun |

## Rerun Safety

Local demonstration outputs are overwritten deterministically. Fabric implementations should use transactional Delta writes, idempotent MERGE logic where incremental processing applies, and watermark updates only after successful target commits.

## Escalation Evidence

Provide the pipeline run ID, failed step, attempt count, timestamps, error text, affected source object, row-count metrics, and last known successful run.
