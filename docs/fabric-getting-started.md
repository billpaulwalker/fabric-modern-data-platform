# Getting Started in Microsoft Fabric

This guide takes the platform from the repository into a Fabric Development workspace and gets data from landing files to a validated Gold star schema. It covers week 1 of the Fabric rollout; the Data Pipeline, Direct Lake model, Git integration, and deployment pipeline follow in later phases.

Fabric's interface changes often, so menu names below may differ slightly from what you see.

## What You Will Build

```text
Files/landing/*.csv, *.json
        │  01_bronze_ingestion
        ▼
bronze.<table>        raw values + audit columns
        │  04_silver_transformations
        ▼
silver.<table>        typed, mapped, validated   ──► silver_quarantine.rejected_records
        │  05_gold_dimensional_model
        ▼
gold.<dim|fact>       star schema for Direct Lake
        │  06_validate_semantic_model
        ▼
pass / fail           blocks the semantic-model refresh on failure
```

## 1. Start the Trial and Create the Workspace

1. Sign in at [app.fabric.microsoft.com](https://app.fabric.microsoft.com) and start the Fabric trial from your account manager (profile icon).
2. Create a workspace named `ws-cre-modernization-dev`, matching `config/environments/dev.json`.
3. In the workspace settings, confirm the license mode is **Trial** so the workspace runs on trial capacity.

The trial lasts 60 days. When it ends, workspaces on trial capacity become inaccessible unless you move them to paid capacity, so capture evidence as you go (see step 7).

## 2. Create the Lakehouse

1. In the workspace, create a **Lakehouse** named `lh_cre_dev`.
2. Enable **Lakehouse schemas** when creating it. The notebooks write to `bronze`, `silver`, `silver_quarantine`, and `gold` schemas, and schema support may not be available to add to an existing Lakehouse later.

## 3. Upload Configuration and Landing Files

In the Lakehouse explorer, create these folders under **Files** and upload from your local clone:

| Upload from the repository | To the Lakehouse |
|---|---|
| `config/bronze_source_config.json` | `Files/config/` |
| `config/silver_table_config.json` | `Files/config/` |
| `config/gold_model_config.json` | `Files/config/` |
| `config/semantic_model_config.json` | `Files/config/` |
| `data/sample/` (folder) | `Files/landing/sample/` |
| `data/api_sample/` (folder) | `Files/landing/api_sample/` |

When you change a config file in the repository, upload it again; the notebooks read the Lakehouse copy.

## 4. Create the Notebooks

Create one notebook per file, attach `lh_cre_dev` as the **default** Lakehouse in each, and paste the file's full contents into the first cell:

| Notebook name | Repository file |
|---|---|
| `01_bronze_ingestion` | `notebooks/fabric/01_bronze_ingestion_pyspark.py` |
| `04_silver_transformations` | `notebooks/fabric/04_silver_transformations_pyspark.py` |
| `05_gold_dimensional_model` | `notebooks/fabric/05_gold_dimensional_model_pyspark.py` |
| `06_validate_semantic_model` | `notebooks/fabric/06_validate_semantic_model_pyspark.py` |

The default Lakehouse matters: the notebooks read config from `/lakehouse/default/Files/config` and landing files from `Files/landing`.

Each file runs its work under `if __name__ == "__main__":`, which is true inside a Fabric notebook. The same files are imported by `tests/test_fabric_notebooks.py`, which runs this whole chain against the sample data on a local Spark session.

## 5. Run in Order

Run each notebook with **Run all**, waiting for each to finish:

1. `01_bronze_ingestion`
2. `04_silver_transformations`
3. `05_gold_dimensional_model`
4. `06_validate_semantic_model`

Each notebook fails loudly instead of writing bad data:

- Bronze reads every landing file before writing, so a missing upload loads nothing.
- Silver stops before writing if a configured column is absent from Bronze.
- Gold checks every model's grain before writing.
- Semantic validation raises an error when the Gold tables break the model contract.

## 6. Confirm the Results

Expected row counts with the sample data:

| Layer | Table | Rows |
|---|---|---|
| Bronze and Silver | properties | 5 |
| Bronze and Silver | tenants | 6 |
| Bronze and Silver | leases | 6 |
| Bronze and Silver | rent_payments | 36 |
| Bronze and Silver | maintenance_requests | 5 |
| Bronze and Silver | property_budget | 30 |
| Bronze and Silver | property_region_mapping | 5 |
| Bronze and Silver | weather_api_raw | 1 |
| Silver quarantine | rejected_records | 0 |
| Gold | dim_property | 6 (5 properties + Unknown) |
| Gold | dim_tenant | 7 (6 tenants + Unknown) |
| Gold | dim_date | 4,018 |
| Gold | fact_lease | 6 |
| Gold | fact_rent_payment | 36 |
| Gold | fact_maintenance_request | 5 |
| Gold | fact_property_budget | 30 |

`06_validate_semantic_model` prints a report with `"passed": true`.

Spot-check totals through the Lakehouse **SQL analytics endpoint**:

```sql
SELECT SUM(monthly_rent) AS monthly_contracted_rent FROM gold.fact_lease;          -- 664,825.00
SELECT SUM(amount_due)   AS total_rent_due          FROM gold.fact_rent_payment;   -- 3,988,950.00
SELECT SUM(budget_revenue) AS budget_revenue        FROM gold.fact_property_budget; -- 2,428,558.00
```

Then run `sql/silver_acceptance_queries.sql` and `sql/gold_acceptance_queries.sql` in the same endpoint.

## 7. Capture Evidence

These outlast the trial and belong in the README and portfolio:

- The workspace showing the Lakehouse and four notebooks
- The Lakehouse explorer with the `bronze`, `silver`, `silver_quarantine`, and `gold` schemas expanded
- Each notebook's final output, especially the semantic-validation report
- The SQL analytics endpoint returning the spot-check totals above
- A deliberate failure: rename a landing file or remove a `column_mappings` entry, run the notebook, and capture the error that stops the load

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `FileNotFoundError: ... /lakehouse/default/Files/config/...` | Config not uploaded to `Files/config/`, or `lh_cre_dev` is not the notebook's default Lakehouse. |
| `FileNotFoundError: Landing file not found ...` | Upload the named file to the `Files/landing/` path in the message. |
| Errors creating or writing to a schema | The Lakehouse was created without schemas. Create a new schema-enabled Lakehouse and upload again. |
| `Configured columns are absent from Bronze` | A source column was renamed or removed. Update `column_mappings` in `config/silver_table_config.json` and upload it again. |
| `Semantic model validation failed` | The listed issues name the table, column, or relationship. Fix the source or mapping, then rerun from the failing layer. |
| A notebook runs old logic | Notebooks are pasted copies until Git integration is set up. Paste the current file from the repository again. |

## Next Steps

1. **Data Pipeline:** build `pl_cre_end_to_end` from `pipelines/fabric-pipeline-manifest.json`, passing `pipeline_run_id` to the notebooks and logging failures.
2. **Semantic model and report:** create the Direct Lake model over the `gold` schema, using `docs/phase-5-powerbi-semantic-model.md` and `powerbi/semantic-model/`.
3. **Git integration and deployment:** connect the workspace to Git, then promote Development → Test → Production with a Fabric deployment pipeline and `deployment/deployment-rules.json`.
