# Phase 2: Bronze Ingestion

## Goal

Land SQL-style, file-style, and API-style source data into raw Bronze outputs while adding standard ingestion audit columns.

## Bronze Audit Columns

Each Bronze record includes:

| Column | Purpose |
|---|---|
| source_system | Identifies the upstream source system |
| source_object | Identifies the source table, file, endpoint, or logical object |
| source_file_name | Captures the file name or API response marker |
| ingestion_timestamp | Captures when the record was landed |
| pipeline_run_id | Groups records from the same ingestion run |
| load_type | Identifies full or incremental load behavior |
| raw_record_hash | SHA-256 hash of the original raw source record |

## Implemented Sources

### SQL-style sources

These are simulated by CSV files in `data/sample`:

- properties
- tenants
- leases
- rent_payments
- maintenance_requests

Notebook:

- `notebooks/01_bronze_sql_ingestion.py`

### API-style source

A sample OpenWeather-style JSON response is included so the repo can run without secrets.

Notebook:

- `notebooks/02_bronze_api_ingestion.py`

Sample payload:

- `data/api_sample/openweather_sample_response.json`

### File/reference sources

Business-managed files:

- property_budget.csv
- property_region_mapping.csv

Notebook:

- `notebooks/03_bronze_file_ingestion.py`

## Local Run Commands

From the repo root:

```powershell
python notebooks/01_bronze_sql_ingestion.py
python notebooks/02_bronze_api_ingestion.py
python notebooks/03_bronze_file_ingestion.py
```

Expected output folder:

```text
data/bronze/
```

The `data/bronze/` folder is intentionally ignored by Git because it is generated output.

## Fabric Translation

In Microsoft Fabric, this same pattern maps to:

```text
Read source data
  ↓
Add Bronze audit columns
  ↓
Write to Lakehouse Bronze Delta table
  ↓
Capture logging metadata
  ↓
Update watermark where applicable
```

For the portfolio, this phase proves the raw landing pattern. Later phases will add Silver transformations, validation/quarantine logic, and Gold dimensional modeling.
