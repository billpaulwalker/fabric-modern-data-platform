# Power BI Semantic Model Design

## Purpose

Define a Power BI-ready semantic model on top of the Gold dimensional layer.

## Model Style

Star schema with dimensions filtering facts.

## Planned Tables

Dimensions:

- dim_date
- dim_property
- dim_tenant
- dim_region

Facts:

- fact_lease
- fact_rent_payment
- fact_maintenance_request
- fact_property_daily_metric

## Relationship Notes

- dim_property filters lease, rent payment, maintenance, and property daily metric facts.
- dim_tenant filters lease and rent payment facts.
- dim_date filters facts by relevant business dates.
- Avoid bi-directional relationships unless there is a clear modeling reason.
