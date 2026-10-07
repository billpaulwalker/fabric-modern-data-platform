-- Run in the lh_cre_gold SQL analytics endpoint before creating or refreshing the Power BI semantic model.
-- Table names follow config/fabric_layout.json: dimensions in shared, facts in their domain schema.

-- Gold tables required by the semantic model
SELECT 'dim_property' AS model_name, COUNT(*) AS row_count FROM shared.dim_property
UNION ALL SELECT 'dim_tenant', COUNT(*) FROM shared.dim_tenant
UNION ALL SELECT 'dim_date', COUNT(*) FROM shared.dim_date
UNION ALL SELECT 'fact_lease', COUNT(*) FROM leasing.fact_lease
UNION ALL SELECT 'fact_rent_payment', COUNT(*) FROM leasing.fact_rent_payment
UNION ALL SELECT 'fact_maintenance_request', COUNT(*) FROM operations.fact_maintenance_request
UNION ALL SELECT 'fact_property_budget', COUNT(*) FROM finance.fact_property_budget;

-- Unknown-member usage
SELECT 'fact_lease.property_key' AS key_name, COUNT(*) AS unknown_rows
FROM leasing.fact_lease WHERE property_key = 0
UNION ALL
SELECT 'fact_lease.tenant_key', COUNT(*) FROM leasing.fact_lease WHERE tenant_key = 0
UNION ALL
SELECT 'fact_rent_payment.property_key', COUNT(*) FROM leasing.fact_rent_payment WHERE property_key = 0
UNION ALL
SELECT 'fact_rent_payment.tenant_key', COUNT(*) FROM leasing.fact_rent_payment WHERE tenant_key = 0
UNION ALL
SELECT 'fact_maintenance_request.property_key', COUNT(*) FROM operations.fact_maintenance_request WHERE property_key = 0
UNION ALL
SELECT 'fact_property_budget.property_key', COUNT(*) FROM finance.fact_property_budget WHERE property_key = 0;

-- Payment reconciliation used to spot-check DAX results
SELECT
    SUM(amount_due) AS expected_total_rent_due,
    SUM(amount_paid) AS expected_total_rent_collected,
    SUM(outstanding_amount) AS expected_outstanding_rent,
    CASE WHEN SUM(amount_due) = 0 THEN 0 ELSE SUM(amount_paid) / SUM(amount_due) END AS expected_collection_rate
FROM leasing.fact_rent_payment;
