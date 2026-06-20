-- Gold dimensional model starter DDL.
-- Adjust syntax for Fabric Warehouse, SQL endpoint, or local SQL engine as needed.

CREATE TABLE dim_property (
    property_key        INT,
    property_id         INT,
    property_name       VARCHAR(200),
    property_type       VARCHAR(100),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    square_feet         BIGINT,
    region              VARCHAR(100),
    market              VARCHAR(100),
    property_status     VARCHAR(50)
);

CREATE TABLE dim_tenant (
    tenant_key          INT,
    tenant_id           INT,
    tenant_name         VARCHAR(200),
    industry            VARCHAR(100),
    tenant_status       VARCHAR(50)
);

CREATE TABLE dim_date (
    date_key            INT,
    calendar_date       DATE,
    year_number         INT,
    quarter_number      INT,
    month_number        INT,
    month_name          VARCHAR(20),
    day_of_month        INT
);

CREATE TABLE fact_rent_payment (
    payment_id          INT,
    lease_id            INT,
    property_key        INT,
    tenant_key          INT,
    due_date_key        INT,
    payment_date_key    INT,
    amount_billed       DECIMAL(18,2),
    payment_amount      DECIMAL(18,2),
    payment_status      VARCHAR(50)
);
