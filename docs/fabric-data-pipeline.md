# Building the Fabric Data Pipeline

This guide builds `pl_cre_end_to_end` in the Development workspace. It assumes the four notebooks from `docs/fabric-getting-started.md` already run successfully by hand. The design is explained in `pipelines/fabric-pipeline-design.md`, and `pipelines/fabric-pipeline-manifest.json` is the exact specification.

Fabric's interface changes often, so menu names below may differ slightly from what you see.

## What You Will Build

```text
nb_cre_bronze_ingest ──► nb_cre_silver_transform ──► nb_cre_gold_build_model ──► nb_cre_gold_validate_model
   (on success)              (on success)                 (on success)                 │
                                                                    on success ────────┤──── on fail or on skip
                                                                         ▼                         ▼
                                                                 log_run_succeeded          log_run_failed
```

Each run writes one row to `lh_cre_gold.audit.pipeline_runs`, and every Bronze, Silver, and Gold row carries the pipeline's run ID.

## 1. Update the Notebooks

1. **Upload the updated layout.** `config/fabric_layout.json` now declares `audit.pipeline_runs`. Upload it again to `Files/config/` in all three Lakehouses so every copy matches.
2. **Re-create `nb_cre_bronze_ingest` with two cells.** The file now has a `# PARAMETERS CELL` marker:
   - Cell 1: the lines between `# PARAMETERS CELL` and `# CELL`, ending with `pipeline_run_id = ""`. Mark it as the parameter cell from the cell's **...** menu (**Toggle parameter cell**).
   - Cell 2: everything from `# CELL` to the end.

   The pipeline's value is injected in a new cell right after the parameter cell, which is why the parameters must be in their own cell above the code.
3. **Create `nb_cre_gold_log_pipeline_run`** from `notebooks/fabric/nb_cre_gold_log_pipeline_run.py`, the same way: parameter cell first (six parameters), then the code. Set `lh_cre_gold` as its default Lakehouse.

Run `nb_cre_bronze_ingest` by hand once to confirm the split works. With `pipeline_run_id` empty it generates its own run ID, as before.

## 2. Create the Pipeline

1. In the workspace, create a **Data pipeline** named `pl_cre_end_to_end`.
2. On the pipeline canvas background, open **Parameters** and add `environment`, type **String**, default `dev`.

## 3. Add the Notebook Activities

Add six **Notebook** activities and set each one's name, notebook, and settings:

| Activity name | Notebook | Retry | Timeout |
|---|---|---|---|
| `nb_cre_bronze_ingest` | `nb_cre_bronze_ingest` | 1 | 0.00:30:00 |
| `nb_cre_silver_transform` | `nb_cre_silver_transform` | 0 | 0.00:45:00 |
| `nb_cre_gold_build_model` | `nb_cre_gold_build_model` | 0 | 0.00:45:00 |
| `nb_cre_gold_validate_model` | `nb_cre_gold_validate_model` | 0 | 0.00:15:00 |
| `log_run_succeeded` | `nb_cre_gold_log_pipeline_run` | 1 | 0.00:10:00 |
| `log_run_failed` | `nb_cre_gold_log_pipeline_run` | 1 | 0.00:10:00 |

Then add **Base parameters**, all of type **String**. Enter values starting with `@` as dynamic content (expressions):

**`nb_cre_bronze_ingest`**

| Name | Value |
|---|---|
| `pipeline_run_id` | `@pipeline().RunId` |

**`log_run_succeeded`**

| Name | Value |
|---|---|
| `pipeline_run_id` | `@pipeline().RunId` |
| `pipeline_name` | `@pipeline().Pipeline` |
| `environment` | `@pipeline().parameters.environment` |
| `status` | `Succeeded` |
| `triggered_at` | `@string(pipeline().TriggerTime)` |

**`log_run_failed`**: the same five, with `status` set to `Failed`, plus:

| Name | Value |
|---|---|
| `message` | `One or more activities failed or were skipped; see this run in the Monitor hub.` |

Base parameter names must match the parameter-cell variables exactly; `tests/test_fabric_artifacts.py` checks this against the manifest.

## 4. Connect the Activities

Drag from each activity's outcome handle to the next activity:

| From | Outcome | To |
|---|---|---|
| `nb_cre_bronze_ingest` | On success | `nb_cre_silver_transform` |
| `nb_cre_silver_transform` | On success | `nb_cre_gold_build_model` |
| `nb_cre_gold_build_model` | On success | `nb_cre_gold_validate_model` |
| `nb_cre_gold_validate_model` | On success | `log_run_succeeded` |
| `nb_cre_gold_validate_model` | On fail | `log_run_failed` |
| `nb_cre_gold_validate_model` | On skip | `log_run_failed` |

The last two connections are the important ones. Two outcomes from the same activity combine with OR, and a failure anywhere earlier skips validation, so `log_run_failed` runs whenever any step fails. Connecting `log_run_failed` to every activity instead would require all of them to fail at once, because dependencies on different activities combine with AND.

## 5. Run and Verify

1. **Save**, then **Run**, and follow the run in the pipeline's output pane or the **Monitor** hub. Copy the run ID.
2. In the `lh_cre_gold` SQL analytics endpoint:

   ```sql
   SELECT pipeline_run_id, pipeline_name, environment, status, triggered_at, logged_at
   FROM audit.pipeline_runs
   ORDER BY logged_at DESC;
   ```

   The newest row should show your run ID with status `Succeeded`.
3. Confirm the run ID reached the data. In the `lh_cre_gold` endpoint:

   ```sql
   SELECT DISTINCT pipeline_run_id FROM leasing.fact_lease;
   ```

   It should return the same run ID. The row counts and totals from step 6 of the getting-started guide should be unchanged.

## 6. Test the Failure Path

1. In `lh_cre_bronze`, rename `Files/landing/sample/leases.csv` to `leases.csv.bak`.
2. Run the pipeline. `nb_cre_bronze_ingest` fails before writing anything, the next three activities are skipped, and `log_run_failed` runs.
3. Check `audit.pipeline_runs`: a new row with status `Failed` and the message.
4. Rename the file back and run once more to return to a clean state.

Capture both runs in the Monitor hub; a deliberate failure handled cleanly is strong portfolio evidence.

## 7. Schedule

Leave Development unscheduled; `config/environments/dev.json` sets `schedule_enabled` to `false`. Production gets a daily schedule after it passes validation, as described in `deployment/dev-test-prod-guide.md`.

## Evidence to Capture

- The pipeline canvas with all six activities and their connections
- A successful run and a deliberately failed run in the Monitor hub
- `audit.pipeline_runs` showing both runs
- A Bronze, Silver, or Gold query showing the pipeline's run ID on the data

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Bronze rows show a generated `bronze_...` run ID, not the pipeline's | The parameter cell isn't marked, or the code is in the same cell. Split the notebook as in step 1 and toggle the parameter cell. |
| `log_run_succeeded` fails with `pipeline_run_id is required` | Its base parameters are missing or misnamed. Compare them with step 3. |
| `log_run_failed` doesn't run after a failure | It is connected only with **On fail**. Add the **On skip** connection from `nb_cre_gold_validate_model`. |
| `audit.pipeline_runs ... is not declared in fabric_layout.json` | Upload the updated `config/fabric_layout.json` to `lh_cre_gold` (step 1). |
| `triggered_at` is empty | The expression wasn't entered as dynamic content. Re-enter `@string(pipeline().TriggerTime)` through the expression editor. |
