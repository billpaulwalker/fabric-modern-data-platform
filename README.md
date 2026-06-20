# Fabric Modern Data Platform

## Project Summary

This repository demonstrates an end-to-end Microsoft Fabric lakehouse platform for Commercial Real Estate operations and finance analytics.

The project is designed to show senior data engineering capabilities across:

- SQL and file ingestion
- REST API ingestion
- Metadata-driven orchestration concepts
- Bronze, Silver, and Gold medallion architecture
- Delta table design patterns
- Data validation and pipeline logging
- Power BI-ready star schema modeling
- Git-based development workflow
- Dev/Test/Prod deployment strategy

> Phase 1 creates the repository scaffold, documentation framework, starter sample data, starter SQL scripts, starter Python modules, and placeholder notebooks.

## Business Scenario

A commercial real estate company manages properties, tenants, leases, rent payments, maintenance requests, budgets, and external weather data.

The business needs trusted reporting for:

- Rent collections
- Occupancy
- Property performance
- Tenant activity
- Overdue payments
- Maintenance operations
- Portfolio-level analytics
- Data quality and pipeline health

## Architecture

Source systems include simulated SQL operational tables, REST API weather data, CSV/Excel extracts, and business reference files.

```text
Source Systems
  ├── SQL Server / Azure SQL sample database
  ├── REST API source
  ├── CSV / Excel files
  └── Reference data

        ↓

Ingestion Layer
  ├── Metadata-driven source config
  ├── ADF-style orchestration concepts
  ├── Fabric pipeline design
  ├── Python API ingestion
  └── Watermark tracking

        ↓

Bronze Layer
  ├── Raw SQL extracts
  ├── Raw API JSON
  ├── Raw CSV files
  └── Ingestion audit columns

        ↓

Silver Layer
  ├── Cleaned and standardized tables
  ├── Deduplication
  ├── JSON flattening
  ├── Type casting
  ├── Business rule standardization
  └── Data quality checks

        ↓

Gold Layer
  ├── Dimensions
  ├── Facts
  ├── Star schema
  └── Power BI-ready model

        ↓

Power BI / Semantic Model
  ├── Relationships
  ├── DAX measures
  ├── Direct Lake or Import-ready model
  └── Executive dashboard

        ↓

Production Layer
  ├── Logging
  ├── Validation
  ├── Monitoring
  ├── Git
  ├── Pull requests
  ├── Dev/Test/Prod promotion
  └── Deployment checklist
```

## Tools Used

- Microsoft Fabric Lakehouse
- Microsoft Fabric notebooks
- Python
- PySpark
- Delta Lake
- Fabric pipelines
- Azure Data Factory orchestration concepts
- SQL
- Power BI
- Git / GitHub
- GitHub Actions

## Data Sources

| Source | Type | Description |
|---|---|---|
| properties | CSV / simulated SQL | Property master data |
| tenants | CSV / simulated SQL | Tenant master data |
| leases | CSV / simulated SQL | Lease contracts |
| rent_payments | CSV / simulated SQL | Rent payment transactions |
| maintenance_requests | CSV / simulated SQL | Property maintenance operations |
| property_budget | CSV reference | Property-level monthly budget |
| property_region_mapping | CSV reference | Business region enrichment |
| OpenWeather API | REST API | Weather enrichment for property operations |

## Medallion Design

### Bronze

Bronze preserves raw source data with ingestion audit fields.

Example Bronze tables:

- bronze_properties
- bronze_tenants
- bronze_leases
- bronze_rent_payments
- bronze_maintenance_requests
- bronze_weather_api_raw
- bronze_property_budget
- bronze_property_region_mapping

### Silver

Silver cleans and standardizes source data.

Example Silver tables:

- silver_properties
- silver_tenants
- silver_leases
- silver_rent_payments
- silver_maintenance_requests
- silver_weather_observations
- silver_property_budget
- silver_property_region_mapping

### Gold

Gold is modeled for reporting and semantic model consumption.

Example Gold tables:

- dim_property
- dim_tenant
- dim_date
- dim_region
- fact_lease
- fact_rent_payment
- fact_maintenance_request
- fact_property_daily_metric

## Ingestion Patterns

This project will demonstrate:

- Full loads
- Incremental loads
- Watermark-based ingestion
- Raw landing patterns
- Metadata-driven table loading
- Logging and row-count capture
- Validation gates

## API Ingestion

The API pattern will demonstrate:

- API authentication
- Parameters
- Pagination or simulated pagination
- Retry handling
- Rate limit handling
- Status code handling
- Raw JSON landing
- JSON flattening
- Watermark updates
- Pipeline logging

## Incremental Loading

Incremental loading will use:

- `ingestion_watermark`
- `watermark_column`
- `last_successful_value`
- `last_successful_run_id`
- MERGE/upsert concepts into Delta tables

## Validation and Logging

The project includes starter scripts and utilities for:

- Required field validation
- Duplicate detection
- Row count comparison
- Rejected record handling
- Pipeline run logging
- Source-to-target validation

## Gold Model

The Gold layer will use a star schema designed for Power BI reporting.

Primary facts:

- fact_lease
- fact_rent_payment
- fact_maintenance_request
- fact_property_daily_metric

Primary dimensions:

- dim_property
- dim_tenant
- dim_date
- dim_region

## Power BI Semantic Model

Planned report pages:

1. Executive Overview
2. Property Performance
3. Rent Collections
4. Tenant Detail
5. Maintenance Operations
6. Data Quality / Pipeline Health

## Dev/Test/Prod Deployment

Planned Fabric workspaces:

- fabric-modern-data-platform-dev
- fabric-modern-data-platform-test
- fabric-modern-data-platform-prod

## Repository Structure

See the folder structure in this repository for architecture docs, configuration, sample data, notebooks, reusable Python modules, SQL scripts, Power BI notes, pipeline design, deployment documentation, tests, and GitHub Actions validation.

## How to Run

This repository is built phase by phase.

### Phase 1

Review the scaffold, documentation placeholders, sample data, starter SQL, starter Python modules, and GitHub Actions workflow.

### Phase 2: Bronze Ingestion

Run the Bronze ingestion scripts from the repository root:

```powershell
python notebooks/01_bronze_sql_ingestion.py
python notebooks/02_bronze_api_ingestion.py
python notebooks/03_bronze_file_ingestion.py
```

These scripts generate raw Bronze outputs in:

```text
data/bronze/
```

The Bronze layer lands SQL-style, API-style, and file-style source data with standard audit columns:

- source_system
- source_object
- source_file_name
- ingestion_timestamp
- pipeline_run_id
- load_type
- raw_record_hash

The generated `data/bronze/` folder is ignored by Git because it represents local run output.

### Later Phases

Detailed run instructions will be added as the Bronze, Silver, Gold, validation, Power BI, and deployment phases are implemented.

## Screenshots

Screenshots will be added after the Fabric Lakehouse, notebooks, semantic model, and Power BI report are built.

## Interview Talking Points

See `docs/interview-talking-points.md`.

## Lessons Learned

See `docs/lessons-learned.md`.
