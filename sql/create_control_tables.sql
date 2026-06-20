CREATE TABLE control_source_config (
    source_id              INT,
    source_system          VARCHAR(100),
    source_type            VARCHAR(50),
    source_object          VARCHAR(200),
    target_bronze_table    VARCHAR(200),
    load_type              VARCHAR(50),
    watermark_column       VARCHAR(100),
    is_active              BIT,
    load_frequency         VARCHAR(50)
);
