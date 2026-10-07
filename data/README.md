# Sample Data

Small Commercial Real Estate datasets used by the local pipeline, the tests, and the first Fabric runs.

## Files

`sample/` simulates operational SQL tables and business-managed reference files:

- properties.csv
- tenants.csv
- leases.csv
- rent_payments.csv
- maintenance_requests.csv
- property_budget.csv
- property_region_mapping.csv

`api_sample/openweather_sample_response.json` is an OpenWeather-style API response, so the API path runs without a key.

## Use in Fabric

Upload `sample/` and `api_sample/` to `lh_cre_bronze` under `Files/landing/`, as described in `docs/fabric-getting-started.md`. `config/bronze_source_config.json` lists each file and the Bronze table it lands in.

## Generated Output

Local runs write `bronze/`, `silver/`, `rejected/`, `gold/`, and `operations/` here. Those folders are ignored by Git.
