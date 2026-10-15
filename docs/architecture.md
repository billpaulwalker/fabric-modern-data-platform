# Architecture

A Microsoft Fabric lakehouse for Commercial Real Estate operations and finance reporting: SQL extracts, business files, and an external API move through Bronze, Silver, and Gold Lakehouses into a Direct Lake semantic model and a Power BI report.

```text
Sources → Bronze → Silver → Gold → Validation → Semantic Model → Power BI
```

- [Workspace and Lakehouse Layout](#workspace-and-lakehouse-layout)
- [Naming Standard](#naming-standard)
- [Bronze](#bronze)
- [Silver](#silver)
- [Gold](#gold)
- [Source-to-Target Mapping](#source-to-target-mapping)
- [Semantic Model](#semantic-model)
- [Incremental Load Design (planned)](#incremental-load-design-planned)

## Workspace and Lakehouse Layout

Each environment (Development, Test, Production) is one workspace containing a schema-enabled Lakehouse per medallion layer. Lakehouse names are identical in every workspace, so cross-layer references never change between environments; the workspace name carries the environment.

| Lakehouse | Schemas organized by | Schemas |
|---|---|---|
| `lh_cre_bronze` | Source system | `cre_sql`, `business_files`, `openweather` |
| `lh_cre_silver` | Business domain | `property`, `leasing`, `finance`, `operations`, `environment`, `quarantine` |
| `lh_cre_gold` | Business domain, with conformed dimensions shared | `shared`, `leasing`, `finance`, `operations`, plus `audit` for the pipeline run log |

`config/fabric_layout.json` is the single declaration of where every table lives. Notebooks address every table by its full `lakehouse.schema.table` name from that file.

Why separate Lakehouses per layer:

- **Access by layer.** Report authors and analysts can be granted Gold only, while raw Bronze data stays restricted to engineering.
- **A clean semantic model.** The Direct Lake model binds to `lh_cre_gold` alone, and its SQL analytics endpoint exposes only curated tables.
- **Ownership and lineage.** Source-aligned Bronze and domain-aligned Silver and Gold appear as separate items in Fabric lineage.
- **Shared dimensions stay shared.** `dim_property`, `dim_tenant`, and `dim_date` serve every domain's facts, so they live in `shared` rather than inside one domain.

## Naming Standard

Technical Fabric items follow `<type>_<project>_<layer>[_<source system | domain>]_<purpose>` in lowercase snake_case, with no environment in the name; the workspace carries the environment. Repository files under `notebooks/` are named exactly as their Fabric notebooks.

| Item | Pattern | Examples |
|---|---|---|
| Workspace | `ws-<project>-<environment>` | `ws-cre-modernization-dev` |
| Lakehouse | `lh_` | `lh_cre_bronze`, `lh_cre_silver`, `lh_cre_gold` |
| Notebook | `nb_` | `nb_cre_bronze_ingest`, `nb_cre_silver_transform`, `nb_cre_gold_build_model`, `nb_cre_gold_validate_model`, `nb_cre_gold_log_pipeline_run` |
| Data Pipeline | `pl_` | `pl_cre_end_to_end` |
| Spark environment | `env_` | `env_cre_spark` |
| Connection | `conn_` | `conn_cre_semantic_model` |
| Variable library | `vl_` | `vl_cre_settings` |
| Deployment pipeline | `dp-<project>` | `dp-cre-modernization` |
| Activator | `act_` | `act_cre_pipeline_alerts` |
| Entra app registration | `app-<project>-<purpose>` | `app-cre-fabric-automation` |
| Entra security group | `sg-<project>-<purpose>` | `sg-cre-fabric-automation`, `sg-cre-portfolio-readers` |
| Semantic model and report | Business-friendly name | **CRE Portfolio Analytics** |

Business-facing items keep readable names because report consumers see them in Power BI and apps. Workspace folders are lowercase and plural (`lakehouses`, `notebooks`, `pipelines`, `powerbi/semantic-models`, `powerbi/reports`) and identical in every stage.

## Bronze

`nb_cre_bronze_ingest` lands every source listed in `config/bronze_source_config.json` from the `lh_cre_bronze` `Files/landing/` folder into Delta tables, one schema per source system. CSV values stay strings and API JSON stays nested; Silver handles typing and flattening. No business transformations happen here, so raw records stay available for replay and audit.

| Source type | Objects |
|---|---|
| Operational SQL extracts (`cre_sql`) | properties, tenants, leases, rent_payments, maintenance_requests |
| Business-managed files (`business_files`) | property_budget, property_region_mapping |
| REST API (`openweather`) | Current weather per property location |

The sample data in `data/sample/` and `data/api_sample/` mirrors `Files/landing/`, so the platform runs without source credentials.

Every Bronze row carries these audit columns, which are preserved through Silver and Gold:

| Column | Purpose |
|---|---|
| `source_system` | Upstream system |
| `source_object` | Source table, file, or endpoint |
| `source_file_name` | File name or API response marker |
| `ingestion_timestamp` | When the record landed |
| `pipeline_run_id` | The Data Pipeline's run ID, linking every row to its run |
| `load_type` | Full or incremental |
| `raw_record_hash` | SHA-256 of the original raw record |

Every source is a full load today, replaced on each run. Watermark-based incremental loading is designed [below](#incremental-load-design-planned).

## Silver

`nb_cre_silver_transform` turns Bronze into typed, deduplicated tables by business domain, driven entirely by `config/silver_table_config.json`. Each table follows the same contract:

1. Read the Bronze table.
2. Normalize column names to snake_case and apply configured renames (`column_mappings`).
3. Convert blank strings to nulls and cast columns to business types.
4. Compute configured derived columns (`derived_columns`).
5. Validate required fields, numeric ranges, and date order.
6. Route invalid records to `quarantine.rejected_records` with one or more `rejection_reason` values.
7. Deduplicate valid records by business key, latest record wins.
8. Preserve the Bronze audit columns and add `data_quality_status` and `silver_processed_timestamp`.

Each table reconciles: `rows_read = rows_valid + rows_rejected + duplicate_rows_removed`.

Design decisions:

- **Fail the run for a missing configured column.** That is schema drift or a broken contract, not a bad record, so the notebook stops before writing anything.
- **Quarantine record-level errors.** Invalid types, missing values, negative amounts, and impossible date ranges never block valid records.
- **Keep the latest duplicate.** The business key identifies the entity; `ingestion_timestamp`, then `updated_at`, picks the survivor.
- **Retain raw lineage.** Every Silver row traces back to its Bronze record and pipeline run.

## Gold

`nb_cre_gold_build_model` publishes a star schema from Silver, configured by `config/gold_model_config.json`, and checks every model's grain before writing.

| Model | Grain | Purpose |
|---|---|---|
| `shared.dim_property` | One row per property | Property, geography, type, size, and status |
| `shared.dim_tenant` | One row per tenant | Tenant identity, industry, and status |
| `shared.dim_date` | One row per calendar date | Conformed calendar and time intelligence |
| `leasing.fact_lease` | One row per lease | Contract dates, rent, and lease status |
| `leasing.fact_rent_payment` | One row per payment | Due, paid, outstanding, and collection performance |
| `operations.fact_maintenance_request` | One row per request | Volume, cost, variance, and resolution time |
| `finance.fact_property_budget` | One row per property and month | Budgeted revenue, expense, and NOI |
| `audit.pipeline_runs` | One row per pipeline run | Run status for the report's Data Quality page |

Design decisions:

- **Deterministic surrogate keys.** Namespaced `xxhash64` of the business key, so keys are stable across reruns without a key-mapping table.
- **Unknown members.** `dim_property` and `dim_tenant` carry an explicit Unknown row with key `0`. Facts with an unresolved natural key point there instead of losing the transaction, and validation caps how many rows may do so.
- **Type 1 dimensions.** The sources represent current operational state. Type 2 history is an extension for when historical attribute analysis becomes a requirement.
- **Calculations live in Gold, not DAX.** Row-level metrics (annualized rent, outstanding amount, collection rate, cost variance, resolution days, budget NOI) are columns, so DAX handles only filter-context aggregation.

## Source-to-Target Mapping

Column renames and calculations live in `config/silver_table_config.json`; Gold-only logic lives in `notebooks/nb_cre_gold_build_model.py`.

| Source Object | Source Type | Bronze (`lh_cre_bronze`) | Silver (`lh_cre_silver`) | Gold (`lh_cre_gold`) |
|---|---|---|---|---|
| properties | SQL | cre_sql.properties | property.properties | shared.dim_property |
| property_region_mapping | CSV | business_files.property_region_mapping | property.property_region_mapping | shared.dim_property (region, market) |
| tenants | SQL | cre_sql.tenants | leasing.tenants | shared.dim_tenant |
| leases | SQL | cre_sql.leases | leasing.leases | leasing.fact_lease |
| rent_payments | SQL | cre_sql.rent_payments | leasing.rent_payments | leasing.fact_rent_payment |
| maintenance_requests | SQL | cre_sql.maintenance_requests | operations.maintenance_requests | operations.fact_maintenance_request |
| property_budget | CSV | business_files.property_budget | finance.property_budget | finance.fact_property_budget |
| weather | REST API | openweather.weather_raw | environment.weather_observations | Not modelled in Gold |
| — | — | — | — | shared.dim_date (generated) |

Columns not listed below keep their source name, normalized to snake_case.

**Leases**

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| leased_square_feet, monthly_rate_per_sqft | monthly_rent | fact_lease.monthly_rent | `leased_square_feet × monthly_rate_per_sqft`; must be non-negative |
| — | — | fact_lease.annualized_rent | `monthly_rent × 12` |
| lease_start_date, lease_end_date | same | lease_start_date_key, lease_end_date_key | `yyyyMMdd`; end date may not precede start date |

**Rent Payments**

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| amount_billed | amount_due | fact_rent_payment.amount_due | Renamed; must be non-negative |
| payment_amount | amount_paid | fact_rent_payment.amount_paid | Renamed; must be non-negative |
| — | — | outstanding_amount, collection_rate | `max(amount_due − amount_paid, 0)`; `amount_paid ÷ amount_due` |
| property_id, tenant_id | same | property_key, tenant_key | Payment values win; the lease supplies them only where the payment has none |
| payment_date | same | payment_date_key | `yyyyMMdd` |

**Maintenance Requests**

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| opened_date | request_date | request_date_key | Renamed |
| closed_date | completed_date | completed_date_key | Renamed; may not precede request_date |
| request_category | category | category | Renamed |
| request_status | status | status | Renamed |
| — | — | resolution_days | Days from request to completion; null while open |
| estimated_cost | same | estimated_cost | Must be non-negative |
| *(no source)* | — | actual_cost, cost_variance | Null until an actual-cost source is added |

**Property Budget**

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| budget_month | same | budget_year, budget_month, budget_date_key | Parsed from the first-of-month date |
| budgeted_rent | same | budget_revenue | Gold alias |
| budgeted_maintenance, budgeted_operating_expense | same | budget_expense | Sum of the components, as a positive magnitude |
| — | — | budget_noi | `budget_revenue − budget_expense` |

**Region Mapping**: `region` and `market` join to `dim_property` on `property_id`; `asset_manager` is not carried into Gold.

**Weather API**: nested JSON fields are flattened to `parent_child` names in Silver, for example `main.temp` → `main_temp`.

## Semantic Model

**CRE Portfolio Analytics** is a Direct Lake model over `lh_cre_gold`. Its measures are in [`powerbi/semantic-model/measures.dax`](../powerbi/semantic-model/measures.dax) and its report design in [`powerbi/report/report-pages.md`](../powerbi/report/report-pages.md).

### Relationships

Declared in `config/semantic_model_config.json`, which `nb_cre_gold_validate_model` checks for orphan and null keys before every refresh.

| Dimension | Column | Fact | Column | Active |
|---|---|---|---|---|
| `shared.dim_property` | `property_key` | `leasing.fact_lease` | `property_key` | Yes |
| `shared.dim_tenant` | `tenant_key` | `leasing.fact_lease` | `tenant_key` | Yes |
| `shared.dim_date` | `date_key` | `leasing.fact_lease` | `lease_start_date_key` | Yes |
| `shared.dim_date` | `date_key` | `leasing.fact_lease` | `lease_end_date_key` | No |
| `shared.dim_property` | `property_key` | `leasing.fact_rent_payment` | `property_key` | Yes |
| `shared.dim_tenant` | `tenant_key` | `leasing.fact_rent_payment` | `tenant_key` | Yes |
| `shared.dim_date` | `date_key` | `leasing.fact_rent_payment` | `payment_date_key` | Yes |
| `shared.dim_property` | `property_key` | `operations.fact_maintenance_request` | `property_key` | Yes |
| `shared.dim_date` | `date_key` | `operations.fact_maintenance_request` | `request_date_key` | Yes |
| `shared.dim_date` | `date_key` | `operations.fact_maintenance_request` | `completed_date_key` | No |
| `shared.dim_property` | `property_key` | `finance.fact_property_budget` | `property_key` | Yes |
| `shared.dim_date` | `date_key` | `finance.fact_property_budget` | `budget_date_key` | Yes |

Every relationship is one-to-many from dimension to fact with single-direction filtering. Active relationships carry each fact's primary reporting date; the two inactive ones, lease end and maintenance completion, are used only by `Leases Ending` and `Completed Maintenance Requests` through `USERELATIONSHIP`. There are no fact-to-fact or bidirectional relationships; the shared dimensions are the path across facts. `audit.pipeline_runs` is in the model for the Data Quality page and has no relationships.

Report authors use the governed measures, with format strings and display folders, rather than implicit column sums; technical columns are hidden.

### Validation Contract

`nb_cre_gold_validate_model` fails the pipeline, before the model is refreshed, when:

- a table or required column is missing, or a table's key is null or not unique;
- a relationship has orphaned foreign keys, or an active relationship has null ones;
- a mapped measure column such as `monthly_rent` or `amount_due` has no non-zero values, so a broken mapping cannot reach Power BI as zeros;
- more than `max_unknown_member_ratio` of a fact's rows resolve to the Unknown member.

### Storage Mode: Direct Lake

Gold is already a star schema of managed Delta tables inside Fabric, so Power BI reads them from OneLake without an imported copy, and lineage runs unbroken from Lakehouse to report. Direct Lake on OneLake has no DirectQuery fallback: a query that exceeds the capacity's Direct Lake limits fails instead, so table sizes are monitored against the capacity's guardrails.

Import would be the choice for a source outside Fabric or where testing shows it performs better; DirectQuery only when freshness or governance rules keep data out of OneLake.

### Row-Level Security (planned)

There is no row-level security yet, because there is no governed user-to-property access data to base it on; hard-coded email addresses and workspace roles are not substitutes. To add it:

1. Ingest an approved access mapping (`user_principal_name`, `property_key`, `region`, `effective_from`, `effective_to`) through the medallion layers.
2. Publish a Gold security bridge with one row per authorized user and property, related to `dim_property`.
3. Create a role that filters the bridge on `USERPRINCIPALNAME()`.
4. Test allowed and denied users with **View as role**.

## Incremental Load Design (planned)

**Status: designed, not implemented.** This records how the high-volume sources move to watermark-based incremental loads.

| Source system | Source object | Bronze table | Load type | Watermark column | Frequency |
|---|---|---|---|---|---|
| CRE_SQL | `properties` | `cre_sql.properties` | Full | | Daily |
| CRE_SQL | `tenants` | `cre_sql.tenants` | Full | | Daily |
| CRE_SQL | `leases` | `cre_sql.leases` | Incremental | `updated_at` | Daily |
| CRE_SQL | `rent_payments` | `cre_sql.rent_payments` | Incremental | `payment_date` | Daily |
| CRE_SQL | `maintenance_requests` | `cre_sql.maintenance_requests` | Incremental | `updated_at` | Daily |
| OpenWeather | `weather` | `openweather.weather_raw` | Incremental | `observation_timestamp` | Daily |
| BusinessFiles | `property_budget.csv` | `business_files.property_budget` | Full | | Monthly |
| BusinessFiles | `property_region_mapping.csv` | `business_files.property_region_mapping` | Full | | Monthly |

Small master and reference data stays a full load; transactional sources with a reliable change column become incremental.

**Control tables** in a `control` schema of `lh_cre_bronze`:

- `control.source_config`: one row per source (the table above plus `source_id`, `source_type`, and `is_active`). The pipeline reads it instead of a config file, so adding a source is a row insert.
- `control.ingestion_watermark`: one row per incremental source, with `watermark_column`, `last_successful_value`, `last_successful_run_id`, `last_successful_time`, and `is_active`.

**Orchestration:**

```text
Lookup active rows in control.source_config
  → ForEach source
      → Set parameters (source, load type, watermark value)
      → Run the ingestion notebook
      → Capture rows read, written, and rejected
      → Log the source run to audit.source_runs
      → Update the watermark, only if the target commit succeeded
```

One parameterized pipeline serves every source. Incremental targets are written with Delta `MERGE` on the business key, so a rerun after a failure reprocesses the same window without duplicating rows.

**Rules:**

- Advance a watermark only after the target commit succeeds; never on a failed or partial load.
- Read with a small overlap (for example, `>= last_successful_value` minus one day) and let `MERGE` absorb the duplicates, to tolerate late-arriving changes.
- Rolling back code doesn't roll back data. Restore a prior watermark only after confirming the target table is consistent with it.
