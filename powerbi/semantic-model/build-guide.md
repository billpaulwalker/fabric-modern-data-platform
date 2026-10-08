# Semantic Model Build Guide

This guide builds the **CRE Portfolio Analytics** Direct Lake semantic model and report in the Development workspace. It assumes `pl_cre_end_to_end` has run successfully, so the Gold tables and `audit.pipeline_runs` exist in `lh_cre_gold`.

Fabric's interface changes often, so menu names below may differ slightly from what you see.

## 1. Validate Gold

The model contract is validated in two places, and both must pass:

- **Repository:** `python notebooks/06_validate_semantic_model.py` checks the local Gold outputs, relationship coverage, and that every measure in `config/semantic_model_config.json` exists in `measures.dax`.
- **Fabric:** the latest pipeline run's `nb_cre_gold_validate_model` checks the real `lh_cre_gold` tables.

## 2. Create the Direct Lake Model

1. From `lh_cre_gold`, create a new semantic model named **CRE Portfolio Analytics**.
2. Select these eight tables:

   | Schema | Tables | Role |
   |---|---|---|
   | `shared` | `dim_property`, `dim_tenant`, `dim_date` | Conformed dimensions |
   | `leasing` | `fact_lease`, `fact_rent_payment` | Facts |
   | `finance` | `fact_property_budget` | Fact |
   | `operations` | `fact_maintenance_request` | Fact |
   | `audit` | `pipeline_runs` | Pipeline health for the Data Quality page; no relationships |

3. Confirm the storage mode is Direct Lake.

Bronze and Silver tables live in other Lakehouses, so they can't be added to this model by mistake.

## 3. Turn Off Automatic Updates

In the semantic model's settings, turn off the option that keeps Direct Lake data up to date automatically.

With it on, the model picks up Gold changes as soon as `nb_cre_gold_build_model` writes them, before `nb_cre_gold_validate_model` has checked them. With it off, the model changes only when the pipeline's `refresh_semantic_model` activity runs, which happens only after validation succeeds (see section 9 of `docs/fabric-data-pipeline.md`). Until that activity exists, refresh the model manually after a successful run.

## 4. Configure Relationships

Create the twelve relationships in `docs/gold-model-relationships.md` (also declared in `config/semantic_model_config.json`):

- One-to-many from dimension to fact, single cross-filter direction.
- Primary reporting dates active; lease end date and maintenance completion date inactive, used by measures through `USERELATIONSHIP`.
- No fact-to-fact or bidirectional relationships.
- `pipeline_runs` stays disconnected.

## 5. Configure the Date Dimension

1. Mark `dim_date` as the date table, using `full_date`.
2. Sort `month_name` by `calendar_month`.
3. Use `year_month` for chronological month axes.
4. Create a hierarchy: `calendar_year`, `calendar_quarter`, `month_name`, `full_date`.

## 6. Add Measures

1. Open DAX query view.
2. Paste `powerbi/semantic-model/measures.dax` from the repository and run it to validate the expressions.
3. Use **Update model with changes** to add the measures.
4. Apply the formats in `powerbi/semantic-model/formatting.md`.
5. Place measures into display folders: Collections, Leasing, Maintenance, Budget, Portfolio, and Data Quality (the unknown-key, processing-time, and pipeline-run measures).

## 7. Curate the Field List

1. Hide the columns listed under `hidden_columns` in `config/semantic_model_config.json`: surrogate keys, technical natural keys, pipeline run IDs, and processing timestamps.
2. In `pipeline_runs`, keep `pipeline_run_id`, `status`, `environment`, `triggered_at`, and `message` visible for the Data Quality page, and hide `pipeline_name` and `logged_at`.
3. Set fact numeric columns to **Do not summarize**, so report authors use the governed measures.
4. Keep business labels, categories, dates, statuses, and measures visible, and give important fields short descriptions.

## 8. Validate Measures

Compare these with `sql/semantic_model_validation.sql` in the `lh_cre_gold` SQL analytics endpoint. With the sample data:

| Measure | Expected |
|---|---|
| Total Rent Due | 3,988,950 |
| Total Rent Collected | 3,988,950 |
| Outstanding Rent | 0 |
| Collection Rate | 100.0% |
| Monthly Contracted Rent | 664,825 |
| Budget Revenue | 2,428,558 |
| Unknown Property Payments | 0 |
| Latest Run Status | Succeeded |

Test filters for date, property, region, tenant, and status, and confirm Leases Ending and Completed Maintenance Requests use the inactive date relationships.

## 9. Build the Report

Import `powerbi/theme/cre-portfolio-theme.json`, then build the pages in `powerbi/report/report-pages.md`. Keep slicers and navigation consistent across pages. Save the report in the Development workspace as **CRE Portfolio Analytics**.

## 10. Add the Refresh to the Pipeline

Follow section 9 of `docs/fabric-data-pipeline.md` to add `refresh_semantic_model` after validation, so each successful run updates the model and failed runs never reach the report.

## 11. Record Evidence

Capture screenshots of:

- The model diagram with its relationships
- The measures in their display folders
- The Executive Overview, Rent Collections, and Data Quality pages
- The workspace lineage view from `lh_cre_bronze` through the model to the report
- A pipeline run including `refresh_semantic_model`
