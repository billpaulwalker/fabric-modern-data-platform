# Phase 4: Gold Dimensional Model

## Objective

Publish a stable, business-oriented star schema from Silver data for Power BI Direct Lake or Import models.

## Model Inventory

| Model | Declared grain | Purpose |
|---|---|---|
| `dim_property` | One row per property | Property, geography, type, size, and status attributes |
| `dim_tenant` | One row per tenant | Tenant identity, industry, and status attributes |
| `dim_date` | One row per calendar date | Conformed calendar filters and time intelligence |
| `fact_lease` | One row per lease | Contract dates, rent, and lease status |
| `fact_rent_payment` | One row per payment | Due, paid, outstanding, and collection performance |
| `fact_maintenance_request` | One row per request | Service volume, cost, variance, and resolution time |
| `fact_property_budget` | One row per property/month | Budgeted revenue, expense, and NOI |

## Engineering Decisions

### Deterministic surrogate keys

Local execution uses namespaced SHA-256-derived integer keys. Fabric uses namespaced `xxhash64`. Both are repeatable for the same business key and reserve key `0` for unknown members. Key values do not need to match between the local demonstration and Fabric; consistency within each target environment is the requirement.

### Unknown members

`dim_property` and `dim_tenant` contain explicit unknown rows with surrogate key `0`. Facts with unresolved natural keys use that key rather than losing the transaction. Acceptance checks report unknown-key usage for investigation.

### Current-state dimensions

This phase implements Type 1 dimensions because the source samples represent current operational state. Type 2 history is a future extension when historical attribute analysis is a stated business requirement.

### Fact calculations

- Lease annualized rent = monthly rent multiplied by 12.
- Outstanding rent = maximum of amount due minus amount paid and zero.
- Collection rate = amount paid divided by amount due, safely handling zero due.
- Maintenance cost variance = actual cost minus estimated cost.
- Resolution days = completed date minus request date.
- Budget NOI = budget revenue minus budget expense.

## Local Execution

Run Phases 2 and 3 first, then:

```powershell
python notebooks/05_gold_dimensional_model.py
python -m pytest
```

Review `data/gold/gold_model_metrics.json` and the generated model CSV files. The `data/gold/` directory should remain ignored by Git because it contains generated outputs.

## Fabric Execution

1. Confirm the required Silver tables exist in `lh_cre_silver`.
2. Upload `config/fabric_layout.json` and `config/gold_model_config.json` to `lh_cre_gold` at `Files/config/`.
3. Create a notebook from `notebooks/fabric/05_gold_dimensional_model_pyspark.py` with `lh_cre_gold` as its default Lakehouse.
4. Run all cells. Conformed dimensions land in the `shared` schema and facts in `leasing`, `finance`, and `operations`. The notebook checks every model's grain before writing.
5. Execute `sql/gold_acceptance_queries.sql` in the `lh_cre_gold` SQL analytics endpoint.
6. Compare fact counts to their corresponding Silver sources.
7. Review unknown-member usage and resolve unexpected source-key gaps.

## Completion Checkpoint

- Each model has a documented, enforced grain.
- Dimension surrogate keys are unique.
- Facts contain no duplicate business-grain rows.
- Every fact foreign key resolves to a dimension row, including key `0`.
- Financial calculations reconcile.
- Gold Delta tables are queryable from the SQL analytics endpoint.
- Local and GitHub Actions tests pass.
- The model relationship plan is ready for Phase 5 semantic-model implementation.
