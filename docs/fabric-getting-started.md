# Getting Started in Microsoft Fabric

This guide takes the platform from the repository into a Fabric Development workspace and gets data from landing files to a validated Gold star schema. It covers week 1 of the Fabric rollout; the Data Pipeline, Direct Lake model, Git integration, and deployment pipeline follow in later phases.

Fabric's interface changes often, so menu names below may differ slightly from what you see.

## Workspace Layout

Each environment has one workspace containing three schema-enabled Lakehouses, one per medallion layer. Lakehouse names are the same in every environment; the workspace name carries the environment.

| Lakehouse | Schemas | Purpose |
|---|---|---|
| `lh_cre_bronze` | One per source system: `cre_sql`, `business_files`, `openweather` | Raw landed data with audit columns; landing files and Bronze config |
| `lh_cre_silver` | One per business domain: `property`, `leasing`, `finance`, `operations`, `environment`, plus `quarantine` | Typed, mapped, validated data; rejected rows |
| `lh_cre_gold` | `shared` for conformed dimensions, plus `leasing`, `finance`, `operations` for facts, and `audit` for the pipeline run log | The star schema behind the Direct Lake semantic model |

Every table's location is declared once in `config/fabric_layout.json`. The notebooks resolve all table names from it as `lakehouse.schema.table`, so moving a table means editing that file only.

```text
lh_cre_bronze  Files/landing/*.csv, *.json
                      │  nb_cre_bronze_ingest
                      ▼
lh_cre_bronze  cre_sql.*, business_files.*, openweather.weather_raw
                      │  nb_cre_silver_transform
                      ▼
lh_cre_silver  property.*, leasing.*, finance.*, operations.*, environment.*  ──► quarantine.rejected_records
                      │  nb_cre_gold_build_model
                      ▼
lh_cre_gold    shared.dim_*, leasing.fact_*, finance.fact_*, operations.fact_*
                      │  nb_cre_gold_validate_model
                      ▼
               pass / fail: blocks the semantic-model refresh on failure
```

## 1. Start the Trial and Create the Workspace

1. Sign in at [app.fabric.microsoft.com](https://app.fabric.microsoft.com) and start the Fabric trial from your account manager (profile icon).
2. Create a workspace named `ws-cre-modernization-dev`, matching `config/environments/dev.json`.
3. In the workspace settings, confirm the license mode is **Trial** so the workspace runs on trial capacity.

The trial lasts 60 days. When it ends, workspaces on trial capacity become inaccessible unless you move them to paid capacity, so capture evidence as you go (see step 7).

## 2. Create the Lakehouses

Create three Lakehouses in the workspace: `lh_cre_bronze`, `lh_cre_silver`, and `lh_cre_gold`.

Enable **Lakehouse schemas** when creating each one. The notebooks create their schemas automatically, but schema support may not be available to add to an existing Lakehouse later.

## 3. Upload Configuration and Landing Files

Each Lakehouse holds the config for the notebook that writes into it. Create these folders under **Files** in each Lakehouse explorer and upload from your local clone:

| Lakehouse | Upload from the repository | To |
|---|---|---|
| `lh_cre_bronze` | `config/fabric_layout.json`, `config/bronze_source_config.json` | `Files/config/` |
| `lh_cre_bronze` | `data/sample/` (folder) | `Files/landing/sample/` |
| `lh_cre_bronze` | `data/api_sample/` (folder) | `Files/landing/api_sample/` |
| `lh_cre_silver` | `config/fabric_layout.json`, `config/silver_table_config.json` | `Files/config/` |
| `lh_cre_gold` | `config/fabric_layout.json`, `config/gold_model_config.json`, `config/semantic_model_config.json` | `Files/config/` |

`fabric_layout.json` goes into all three, because every notebook resolves table names from it. When you change a config file in the repository, upload it again to each Lakehouse that holds it.

## 4. Create the Notebooks

Create one notebook per file, named exactly as the file without `.py`, set its **default** Lakehouse as shown, and paste in the file's contents. If the file contains a `# PARAMETERS CELL` marker (only `nb_cre_bronze_ingest` among these four), paste the lines between that marker and `# CELL` into the first cell and mark it as the parameter cell, then paste everything from `# CELL` onward into a second cell; otherwise paste the whole file into one cell. The names follow the project naming standard in `architecture/architecture-overview.md`, so the workspace, the repository, and Git integration later all use the same names.

| Notebook name | Repository file | Default Lakehouse |
|---|---|---|
| `nb_cre_bronze_ingest` | `notebooks/fabric/nb_cre_bronze_ingest.py` | `lh_cre_bronze` |
| `nb_cre_silver_transform` | `notebooks/fabric/nb_cre_silver_transform.py` | `lh_cre_silver` |
| `nb_cre_gold_build_model` | `notebooks/fabric/nb_cre_gold_build_model.py` | `lh_cre_gold` |
| `nb_cre_gold_validate_model` | `notebooks/fabric/nb_cre_gold_validate_model.py` | `lh_cre_gold` |

Each notebook's default Lakehouse is the one it writes to. It reads its config from `/lakehouse/default/Files/config`, and the Bronze notebook reads landing files from `Files/landing`. Tables in other Lakehouses are read by their full `lakehouse.schema.table` name.

Each file runs its work under `if __name__ == "__main__":`, which is true inside a Fabric notebook. The same files are imported by `tests/test_fabric_notebooks.py`, which runs all four notebooks in order against the sample data on a local Spark session and checks that every table lands where `fabric_layout.json` says.

## 5. Run in Order

Run each notebook with **Run all**, waiting for each to finish:

1. `nb_cre_bronze_ingest`
2. `nb_cre_silver_transform`
3. `nb_cre_gold_build_model`
4. `nb_cre_gold_validate_model`

The first run of `nb_cre_bronze_ingest` also confirms that Fabric accepts the `lakehouse.schema.table` names the notebooks use. If it fails on a table or schema name, see Troubleshooting.

Each notebook fails loudly instead of writing bad data:

- Bronze reads every landing file before writing, so a missing upload loads nothing.
- Silver stops before writing if a configured column is absent from Bronze.
- Gold checks every model's grain before writing.
- Semantic validation raises an error when the Gold tables break the model contract.

## 6. Confirm the Results

Expected tables and row counts with the sample data:

| Logical table | Bronze (`lh_cre_bronze`) | Silver (`lh_cre_silver`) | Rows |
|---|---|---|---|
| properties | `cre_sql.properties` | `property.properties` | 5 |
| property_region_mapping | `business_files.property_region_mapping` | `property.property_region_mapping` | 5 |
| tenants | `cre_sql.tenants` | `leasing.tenants` | 6 |
| leases | `cre_sql.leases` | `leasing.leases` | 6 |
| rent_payments | `cre_sql.rent_payments` | `leasing.rent_payments` | 36 |
| property_budget | `business_files.property_budget` | `finance.property_budget` | 30 |
| maintenance_requests | `cre_sql.maintenance_requests` | `operations.maintenance_requests` | 5 |
| weather | `openweather.weather_raw` | `environment.weather_observations` | 1 |
| rejected rows | — | `quarantine.rejected_records` | 0 |

| Gold table (`lh_cre_gold`) | Rows |
|---|---|
| `shared.dim_property` | 6 (5 properties + Unknown) |
| `shared.dim_tenant` | 7 (6 tenants + Unknown) |
| `shared.dim_date` | 4,018 |
| `leasing.fact_lease` | 6 |
| `leasing.fact_rent_payment` | 36 |
| `finance.fact_property_budget` | 30 |
| `operations.fact_maintenance_request` | 5 |

`nb_cre_gold_validate_model` prints a report with `"passed": true`.

Spot-check totals in the `lh_cre_gold` **SQL analytics endpoint**:

```sql
SELECT SUM(monthly_rent)   AS monthly_contracted_rent FROM leasing.fact_lease;           -- 664,825.00
SELECT SUM(amount_due)     AS total_rent_due          FROM leasing.fact_rent_payment;    -- 3,988,950.00
SELECT SUM(budget_revenue) AS budget_revenue          FROM finance.fact_property_budget; -- 2,428,558.00
```

Then run `sql/silver_acceptance_queries.sql` in the `lh_cre_silver` endpoint, and `sql/gold_acceptance_queries.sql` and `sql/semantic_model_validation.sql` in the `lh_cre_gold` endpoint.

## 7. Capture Evidence

These outlast the trial and belong in the README and portfolio:

- The workspace showing the three Lakehouses and four notebooks
- Each Lakehouse explorer with its schemas expanded
- Each notebook's final output, especially the semantic-validation report
- The SQL analytics endpoint returning the spot-check totals above
- The workspace lineage view showing Bronze → Silver → Gold
- A deliberate failure: rename a landing file or remove a `column_mappings` entry, run the notebook, and capture the error that stops the load

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `FileNotFoundError: ... /lakehouse/default/Files/config/...` | The config wasn't uploaded to that notebook's default Lakehouse, or the default Lakehouse is wrong. Check the tables in steps 3 and 4. |
| `FileNotFoundError: Landing file not found ...` | Upload the named file to the `Files/landing/` path in `lh_cre_bronze`. |
| Errors creating or writing to a schema | The Lakehouse was created without schemas. Create a new schema-enabled Lakehouse with the same name and upload again. |
| A table or schema name is rejected, or a table in another Lakehouse isn't found | First check that the Lakehouse names match `config/fabric_layout.json` exactly. If they do, add the other Lakehouse to the notebook's explorer (it can stay non-default) and rerun. If it still fails, the name format needs adjusting: it is built in `resolve_table` and `create_schema_for` in each notebook. Capture the error message. |
| `Configured columns are absent from Bronze` | A source column was renamed or removed. Update `column_mappings` in `config/silver_table_config.json` and upload it again. |
| `... is not declared in fabric_layout.json` | A config names a table the layout doesn't place. Add it to `config/fabric_layout.json` and upload it to all three Lakehouses. |
| `Semantic model validation failed` | The listed issues name the table, column, or relationship. Fix the source or mapping, then rerun from the failing layer. |
| A notebook runs old logic | Notebooks are pasted copies until Git integration is set up. Paste the current file from the repository again. |

## Next Steps

1. **Data Pipeline:** build `pl_cre_end_to_end` by following `docs/fabric-data-pipeline.md`. It runs the four notebooks in order, passes the pipeline run ID into Bronze, and logs every run to `lh_cre_gold.audit.pipeline_runs`.
2. **Semantic model and report:** create the Direct Lake model over `lh_cre_gold`, including the `shared`, `leasing`, `finance`, and `operations` schemas, using `docs/phase-5-powerbi-semantic-model.md` and `powerbi/semantic-model/`.
3. **Git integration and deployment:** connect the workspace to Git, then promote Development → Test → Production with a Fabric deployment pipeline and `deployment/deployment-rules.json`, which binds each notebook to the matching Lakehouse in each stage's workspace.
