# Gold Model Relationships

| From | Column | To | Cardinality | Filter direction |
|---|---|---|---|---|
| `dim_property` | `property_key` | `fact_lease.property_key` | One-to-many | Single |
| `dim_property` | `property_key` | `fact_rent_payment.property_key` | One-to-many | Single |
| `dim_property` | `property_key` | `fact_maintenance_request.property_key` | One-to-many | Single |
| `dim_property` | `property_key` | `fact_property_budget.property_key` | One-to-many | Single |
| `dim_tenant` | `tenant_key` | `fact_lease.tenant_key` | One-to-many | Single |
| `dim_tenant` | `tenant_key` | `fact_rent_payment.tenant_key` | One-to-many | Single |
| `dim_date` | `date_key` | `fact_rent_payment.payment_date_key` | One-to-many | Single |
| `dim_date` | `date_key` | `fact_lease.lease_start_date_key` | One-to-many | Single |
| `dim_date` | `date_key` | `fact_maintenance_request.request_date_key` | One-to-many | Single |
| `dim_date` | `date_key` | `fact_property_budget.budget_date_key` | One-to-many | Single |

Use active relationships for the primary reporting date. Secondary dates such as lease end date and maintenance completion date should be inactive relationships activated by measures when needed.

Avoid fact-to-fact relationships and bidirectional filtering. Shared dimensions provide the reporting path across facts.
