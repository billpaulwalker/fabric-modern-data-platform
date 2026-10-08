# Report Page Design

## Page 1: Executive Overview

**Slicers:** Year/month, region, market, property type, property

**KPI cards:** Total Rent Collected, Collection Rate, Outstanding Rent, Active Lease Count, Open Maintenance Requests

**Visuals:**

- Monthly rent collected versus budget revenue: line and clustered-column chart
- Rent collected by region: horizontal bar chart
- Collection rate by property: matrix with conditional formatting
- Maintenance request trend: line chart

## Page 2: Rent Collections

**Slicers:** Payment date, region, property, tenant, payment status

**Visuals:**

- Total Due, Total Collected, Outstanding Rent, and Collection Rate cards
- Monthly due versus collected trend
- Outstanding rent by property
- Payment-status distribution
- Property and tenant collection-detail matrix

## Page 3: Lease Portfolio

**Slicers:** Region, property type, property, tenant, lease status

**Visuals:**

- Lease Count, Active Lease Count, Monthly Contracted Rent, Annualized Contracted Rent cards
- Lease expirations by month using the inactive end-date relationship measure
- Contracted rent by property
- Tenant and lease detail table

## Page 4: Maintenance Operations

**Slicers:** Request date, region, property, category, priority, status

**Visuals:**

- Request Count, Open Requests, Actual Cost, Cost Variance, Average Resolution Days cards
- Requests by priority and status
- Actual cost by property
- Resolution-time trend
- Open request detail table

## Page 5: Budget Versus Actual

**Slicers:** Budget year/month, region, property

**Visuals:**

- Budget Revenue, Rent Collected, Rent to Budget Variance, and Variance % cards
- Monthly budget versus actual trend
- Variance by property
- Budget revenue, expense, and NOI matrix

## Page 6: Data Quality

This operational page is optional for business consumers but valuable in the portfolio demonstration. It reads `audit.pipeline_runs`, which `pl_cre_end_to_end` appends to on every run, alongside the Gold facts.

**Slicers:** Environment, run date (`pipeline_runs[triggered_at]`)

**KPI cards:** Latest Run Status, Latest Run Logged At, Pipeline Run Success Rate, Latest Silver Processing Time

**Visuals:**

- Runs by status over time: stacked column chart of Pipeline Run Count by `triggered_at` date and `status`
- Recent runs: table of `triggered_at`, `pipeline_run_id`, `status`, `environment`, and `message`, sorted newest first
- Unknown-member check: cards for Unknown Property Payments and Unknown Tenant Payments, which should read 0
- Model volumes: cards for Payment Count, Lease Count, Maintenance Request Count, Property Count, and Tenant Count

Rejected Silver rows live in `lh_cre_silver.quarantine.rejected_records`, outside this model; review them with `sql/silver_acceptance_queries.sql`.
