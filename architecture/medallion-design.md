# Medallion Design

## Bronze Layer

Bronze stores raw source-aligned data.

Design principles:

- Preserve source structure where practical
- Add ingestion metadata
- Avoid business transformations
- Keep raw records available for replay and audit

Common audit fields:

- source_system
- source_object
- source_file_name
- ingestion_timestamp
- pipeline_run_id
- load_type
- raw_record_hash

## Silver Layer

Silver stores cleaned, standardized, and validated data.

Transformation examples:

- Rename columns
- Cast data types
- Standardize dates
- Standardize status values
- Deduplicate records
- Flatten JSON
- Validate required fields
- Quarantine bad records

## Gold Layer

Gold stores business-ready dimensional models.

Design principles:

- Star schema
- Clear fact grain
- Conformed dimensions
- Power BI-ready naming
- Stable business metrics
