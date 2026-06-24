# Semantic Model Build Guide

## 1. Validate Gold

From the repository root:

```powershell
python notebooks/06_validate_semantic_model.py
python -m pytest
```

The validation must pass before model creation. It checks Gold schemas, unique keys, relationship coverage, and the required DAX inventory.

## 2. Create the Direct Lake Model

1. Open the Fabric workspace containing the project Lakehouse.
2. Create a new semantic model from the Lakehouse.
3. Name it **CRE Portfolio Analytics**.
4. Select only the seven `gold` tables in `config/semantic_model_config.json`.
5. Confirm the model uses Direct Lake storage mode.

Do not expose Bronze or Silver tables to report authors.

## 3. Configure Relationships

Create the relationships from `config/semantic_model_config.json` and `docs/gold-model-relationships.md`.

- Dimension side: one
- Fact side: many
- Cross-filter direction: single
- Primary reporting-date relationships: active
- Lease end date and maintenance completion date: inactive
- Assume referential integrity only after validation confirms it

Do not add fact-to-fact or bidirectional relationships.

## 4. Configure the Date Dimension

1. Mark `dim_date[full_date]` as the date table.
2. Sort `month_name` by `calendar_month`.
3. Use `year_month` for chronological month axes.
4. Hide `date_key` from report view.
5. Create a hierarchy: calendar year, calendar quarter, month name, full date.

## 5. Add Measures

1. Open DAX Query View.
2. Open `powerbi/semantic-model/measures.dax` from the repository.
3. Run the query to validate the expressions.
4. Use **Update model with changes** to add the measures.
5. Apply the formats in `powerbi/semantic-model/formatting.md`.
6. Place measures into display folders: Collections, Leasing, Maintenance, Budget, Portfolio.

## 6. Curate the Field List

Hide surrogate keys, natural technical keys, pipeline identifiers, and processing timestamps listed in the semantic configuration. Set fact numeric columns to **Do not summarize**.

Keep business labels, categories, dates, statuses, and governed measures visible. Give important fields concise descriptions.

## 7. Validate Measures

Compare these measures with `sql/semantic_model_validation.sql`:

- Total Rent Due
- Total Rent Collected
- Outstanding Rent
- Collection Rate

Test filters for date, property, region, tenant, and status. Confirm inactive-date measures use lease-end and completion dates as intended.

## 8. Build the Report

Import `powerbi/theme/cre-portfolio-theme.json`, then implement the pages in `powerbi/report/report-pages.md`. Keep slicers and navigation consistent across pages.

## 9. Publish and Record Evidence

Save the report and semantic model in the Development workspace. Capture screenshots of:

- Model relationships
- DAX measures
- Executive Overview
- Collections page
- Fabric lineage view
- Successful semantic validation output

These images provide concrete portfolio and interview evidence.
