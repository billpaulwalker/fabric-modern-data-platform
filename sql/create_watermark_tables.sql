CREATE TABLE ingestion_watermark (
    source_system              VARCHAR(100),
    source_object              VARCHAR(200),
    target_table               VARCHAR(200),
    watermark_column           VARCHAR(100),
    last_successful_value      VARCHAR(200),
    last_successful_run_id     VARCHAR(100),
    last_successful_time       DATETIME2,
    is_active                  BIT
);
