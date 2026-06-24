-- Gold acceptance checks through the Lakehouse SQL analytics endpoint.

-- 1. Model row counts
SELECT 'dim_property' AS model_name, COUNT(*) AS row_count FROM gold.dim_property
UNION ALL SELECT 'dim_tenant', COUNT(*) FROM gold.dim_tenant
UNION ALL SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL SELECT 'fact_lease', COUNT(*) FROM gold.fact_lease
UNION ALL SELECT 'fact_rent_payment', COUNT(*) FROM gold.fact_rent_payment
UNION ALL SELECT 'fact_maintenance_request', COUNT(*) FROM gold.fact_maintenance_request
UNION ALL SELECT 'fact_property_budget', COUNT(*) FROM gold.fact_property_budget;

-- 2. Dimension key uniqueness: each query should return zero rows
SELECT property_key, COUNT(*) AS duplicate_count
FROM gold.dim_property
GROUP BY property_key
HAVING COUNT(*) > 1;

SELECT tenant_key, COUNT(*) AS duplicate_count
FROM gold.dim_tenant
GROUP BY tenant_key
HAVING COUNT(*) > 1;

-- 3. Fact grain uniqueness: expected to return zero rows
SELECT payment_id, COUNT(*) AS duplicate_count
FROM gold.fact_rent_payment
GROUP BY payment_id
HAVING COUNT(*) > 1;

-- 4. Orphan check: expected to return zero rows
SELECT COUNT(*) AS orphan_property_keys
FROM gold.fact_rent_payment f
LEFT JOIN gold.dim_property d ON f.property_key = d.property_key
WHERE d.property_key IS NULL;

-- 5. Unknown-member usage: review rather than automatically fail
SELECT
    SUM(CASE WHEN property_key = 0 THEN 1 ELSE 0 END) AS unknown_properties,
    SUM(CASE WHEN tenant_key = 0 THEN 1 ELSE 0 END) AS unknown_tenants,
    COUNT(*) AS payment_rows
FROM gold.fact_rent_payment;

-- 6. Financial reconciliation
SELECT
    SUM(amount_due) AS total_due,
    SUM(amount_paid) AS total_paid,
    SUM(outstanding_amount) AS total_outstanding,
    SUM(amount_due) - SUM(amount_paid) AS calculated_difference
FROM gold.fact_rent_payment;
