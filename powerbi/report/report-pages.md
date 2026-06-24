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

This operational page is optional for business consumers but valuable in the portfolio demonstration.

**Visuals:**

- Gold row counts by model
- Unknown property-key and tenant-key counts
- Latest pipeline run identifier
- Latest Silver processing timestamp
- Validation status from the deployment checklist
