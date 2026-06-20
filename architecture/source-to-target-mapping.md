# Source-to-Target Mapping

| Source Object | Source Type | Bronze Table | Silver Table | Gold Object |
|---|---|---|---|---|
| properties | SQL / CSV | bronze_properties | silver_properties | dim_property |
| tenants | SQL / CSV | bronze_tenants | silver_tenants | dim_tenant |
| leases | SQL / CSV | bronze_leases | silver_leases | fact_lease |
| rent_payments | SQL / CSV | bronze_rent_payments | silver_rent_payments | fact_rent_payment |
| maintenance_requests | SQL / CSV | bronze_maintenance_requests | silver_maintenance_requests | fact_maintenance_request |
| weather | REST API | bronze_weather_api_raw | silver_weather_observations | fact_property_daily_metric |
| property_budget | CSV | bronze_property_budget | silver_property_budget | fact_property_daily_metric |
| property_region_mapping | CSV | bronze_property_region_mapping | silver_property_region_mapping | dim_region |
