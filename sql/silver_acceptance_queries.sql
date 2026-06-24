-- Run through the Lakehouse SQL analytics endpoint after the Fabric notebook.

-- 1. Row counts by Silver table
SELECT 'properties' AS table_name, COUNT(*) AS row_count FROM silver.properties
UNION ALL SELECT 'tenants', COUNT(*) FROM silver.tenants
UNION ALL SELECT 'leases', COUNT(*) FROM silver.leases
UNION ALL SELECT 'rent_payments', COUNT(*) FROM silver.rent_payments
UNION ALL SELECT 'maintenance_requests', COUNT(*) FROM silver.maintenance_requests;

-- 2. Primary-key uniqueness example: expected to return zero rows
SELECT lease_id, COUNT(*) AS duplicate_count
FROM silver.leases
GROUP BY lease_id
HAVING COUNT(*) > 1;

-- 3. Required-field example: expected to return zero
SELECT COUNT(*) AS invalid_required_rows
FROM silver.leases
WHERE lease_id IS NULL OR property_id IS NULL OR tenant_id IS NULL;

-- 4. Quarantine review
SELECT source_object, rejection_reason, COUNT(*) AS rejected_rows
FROM silver_quarantine.rejected_records
GROUP BY source_object, rejection_reason
ORDER BY rejected_rows DESC;
