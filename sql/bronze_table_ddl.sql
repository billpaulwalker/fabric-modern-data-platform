-- Bronze table DDL starter.
-- Adjust data types and syntax for Fabric Warehouse SQL endpoint or Lakehouse SQL endpoint as needed.
-- In Fabric Lakehouse, these tables would typically be created as Delta tables by notebooks.

CREATE TABLE bronze_properties (
    property_id             VARCHAR(100),
    property_name           VARCHAR(300),
    property_type           VARCHAR(100),
    city                    VARCHAR(100),
    state                   VARCHAR(50),
    postal_code             VARCHAR(20),
    square_feet             VARCHAR(100),
    property_status         VARCHAR(100),
    acquired_date           VARCHAR(100),
    updated_at              VARCHAR(100),
    source_system           VARCHAR(100),
    source_object           VARCHAR(200),
    source_file_name        VARCHAR(300),
    ingestion_timestamp     VARCHAR(100),
    pipeline_run_id         VARCHAR(100),
    load_type               VARCHAR(50),
    raw_record_hash         VARCHAR(256)
);

CREATE TABLE bronze_tenants (
    tenant_id               VARCHAR(100),
    tenant_name             VARCHAR(300),
    industry                VARCHAR(100),
    tenant_status           VARCHAR(100),
    tenant_start_date       VARCHAR(100),
    updated_at              VARCHAR(100),
    source_system           VARCHAR(100),
    source_object           VARCHAR(200),
    source_file_name        VARCHAR(300),
    ingestion_timestamp     VARCHAR(100),
    pipeline_run_id         VARCHAR(100),
    load_type               VARCHAR(50),
    raw_record_hash         VARCHAR(256)
);

CREATE TABLE bronze_weather_api_raw (
    raw_payload             VARCHAR(MAX),
    source_system           VARCHAR(100),
    source_object           VARCHAR(200),
    source_file_name        VARCHAR(300),
    ingestion_timestamp     VARCHAR(100),
    pipeline_run_id         VARCHAR(100),
    load_type               VARCHAR(50),
    raw_record_hash         VARCHAR(256)
);
