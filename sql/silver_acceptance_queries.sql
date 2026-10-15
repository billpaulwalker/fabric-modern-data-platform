-- Run in the lh_cre_silver SQL analytics endpoint after nb_cre_silver_transform.
-- Table names follow config/fabric_layout.json (one schema per business domain).

-- 1. Row counts by Silver table
SELECT 'properties' AS table_name, COUNT(*) AS row_count FROM property.properties
UNION ALL SELECT 'tenants', COUNT(*) FROM leasing.tenants
UNION ALL SELECT 'leases', COUNT(*) FROM leasing.leases
UNION ALL SELECT 'rent_payments', COUNT(*) FROM leasing.rent_payments
UNION ALL SELECT 'maintenance_requests', COUNT(*) FROM operations.maintenance_requests;

-- 2. Primary-key uniqueness example: expected to return zero rows
SELECT lease_id, COUNT(*) AS duplicate_count
FROM leasing.leases
GROUP BY lease_id
HAVING COUNT(*) > 1;

-- 3. Required-field example: expected to return zero
SELECT COUNT(*) AS invalid_required_rows
FROM leasing.leases
WHERE lease_id IS NULL OR property_id IS NULL OR tenant_id IS NULL;

-- 4. Quarantine review
SELECT source_object, rejection_reason, COUNT(*) AS rejected_rows
FROM quarantine.rejected_records
GROUP BY source_object, rejection_reason
ORDER BY rejected_rows DESC;
