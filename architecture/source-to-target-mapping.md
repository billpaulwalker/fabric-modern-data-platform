# Source-to-Target Mapping

Column renames and calculations live in `config/silver_table_config.json` (`column_mappings`, `derived_columns`) and are applied identically by the local pandas pipeline and the Fabric PySpark notebook. Gold-only logic lives in `src/gold_utils.py` and `notebooks/fabric/nb_cre_gold_build_model.py`.

Fabric table locations are declared in `config/fabric_layout.json`: Bronze tables in `lh_cre_bronze` by source system, Silver tables in `lh_cre_silver` by business domain, and Gold tables in `lh_cre_gold` with conformed dimensions in `shared` and facts by domain. Local runs write the same data as flat CSV files, for example `data/bronze/bronze_leases.csv` and `data/silver/silver_leases.csv`.

## Table Mapping

| Source Object | Source Type | Bronze (`lh_cre_bronze`) | Silver (`lh_cre_silver`) | Gold (`lh_cre_gold`) |
|---|---|---|---|---|
| properties | SQL / CSV | cre_sql.properties | property.properties | shared.dim_property |
| property_region_mapping | CSV | business_files.property_region_mapping | property.property_region_mapping | shared.dim_property (region, market) |
| tenants | SQL / CSV | cre_sql.tenants | leasing.tenants | shared.dim_tenant |
| leases | SQL / CSV | cre_sql.leases | leasing.leases | leasing.fact_lease |
| rent_payments | SQL / CSV | cre_sql.rent_payments | leasing.rent_payments | leasing.fact_rent_payment |
| maintenance_requests | SQL / CSV | cre_sql.maintenance_requests | operations.maintenance_requests | operations.fact_maintenance_request |
| property_budget | CSV | business_files.property_budget | finance.property_budget | finance.fact_property_budget |
| weather | REST API | openweather.weather_raw | environment.weather_observations | Not modelled in Gold |
| — | — | — | — | shared.dim_date (generated) |

Rejected rows from every Silver table land in `quarantine.rejected_records` in `lh_cre_silver`, with a `rejection_reason`.

## Column Mapping

Columns not listed keep their source name, normalized to snake_case.

### Leases

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| leased_square_feet, monthly_rate_per_sqft | monthly_rent | fact_lease.monthly_rent | `leased_square_feet × monthly_rate_per_sqft`; must be non-negative |
| — | — | fact_lease.annualized_rent | `monthly_rent × 12` |
| lease_start_date, lease_end_date | same | lease_start_date_key, lease_end_date_key | `yyyyMMdd`; end date may not precede start date |

### Rent Payments

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| amount_billed | amount_due | fact_rent_payment.amount_due | Renamed; must be non-negative |
| payment_amount | amount_paid | fact_rent_payment.amount_paid | Renamed; must be non-negative |
| — | — | outstanding_amount, collection_rate | `max(amount_due − amount_paid, 0)`; `amount_paid ÷ amount_due` |
| property_id, tenant_id | same | property_key, tenant_key | Payment values win; the lease supplies them only where the payment has none |
| payment_date | same | payment_date_key | `yyyyMMdd` |

### Maintenance Requests

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| opened_date | request_date | request_date_key | Renamed |
| closed_date | completed_date | completed_date_key | Renamed; may not precede request_date |
| request_category | category | category | Renamed |
| request_status | status | status | Renamed |
| — | — | resolution_days | Days from request to completion; null while open |
| estimated_cost | same | estimated_cost | Must be non-negative |
| *(no source)* | — | actual_cost, cost_variance | Null until an actual-cost source is added |

### Property Budget

| Source Column | Silver Column | Gold Column | Rule |
|---|---|---|---|
| budget_month | same | budget_year, budget_month, budget_date_key | Parsed from the first-of-month date |
| budgeted_rent | same | budget_revenue | Gold alias |
| budgeted_maintenance, budgeted_operating_expense | same | budget_expense | Sum of the components, as a positive magnitude |
| — | — | budget_noi | `budget_revenue − budget_expense` |

### Region Mapping

| Source Column | Gold Column | Rule |
|---|---|---|
| region, market | dim_property.region, dim_property.market | Joined on property_id; asset_manager is not carried into Gold |

### Weather API

Nested JSON fields are flattened to `parent_child` names in Silver, for example `main.temp` → `main_temp` and `wind.speed` → `wind_speed`.

## Validation

- A configured column that is absent from the source is reported in the Silver run metrics. Locally it fails the deployment gate, and in Fabric it stops the Silver notebook before any table is written.
- The semantic-model contract (`config/semantic_model_config.json`) fails when a mapped measure column such as `monthly_rent` or `amount_due` has no non-zero values, so a broken mapping cannot reach Power BI as zeros.
