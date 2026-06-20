-- Semantic model notes.
-- Keep heavy transformations in SQL/PySpark.
-- Use DAX for business measures and report-time calculations.

-- Example measures:
-- Total Rent Collected = SUM(fact_rent_payment[payment_amount])
-- Total Rent Billed = SUM(fact_rent_payment[amount_billed])
-- Collection Rate = DIVIDE([Total Rent Collected], [Total Rent Billed])
-- Late Payment Count = CALCULATE(COUNTROWS(fact_rent_payment), fact_rent_payment[payment_status] = "Late")
