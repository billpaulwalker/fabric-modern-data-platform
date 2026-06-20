CREATE TABLE pipeline_run_log (
    pipeline_run_id      VARCHAR(100),
    pipeline_name        VARCHAR(200),
    environment          VARCHAR(50),
    source_system        VARCHAR(100),
    source_object        VARCHAR(200),
    target_table         VARCHAR(200),
    load_type            VARCHAR(50),
    start_time           DATETIME2,
    end_time             DATETIME2,
    status               VARCHAR(50),
    rows_read            BIGINT,
    rows_written         BIGINT,
    rows_rejected        BIGINT,
    error_message        VARCHAR(MAX),
    created_at           DATETIME2
);
