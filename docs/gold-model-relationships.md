# Gold Model Relationships

All tables are in `lh_cre_gold`. Dimensions are in the `shared` schema; facts are in their domain schemas. The same relationships are declared in `config/semantic_model_config.json`, which the semantic validation checks for orphan and null keys.

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

Every relationship is one-to-many from dimension to fact with a single cross-filter direction.

Active relationships carry each fact's primary reporting date. The two inactive ones, lease end date and maintenance completion date, are used only by the measures that need them (`Leases Ending` and `Completed Maintenance Requests`) through `USERELATIONSHIP`.

Avoid fact-to-fact relationships and bidirectional filtering; the shared dimensions provide the reporting path across facts. `audit.pipeline_runs` is in the model for the Data Quality page and has no relationships.
