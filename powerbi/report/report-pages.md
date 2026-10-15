# Report Page Design

The **CRE Portfolio Analytics** report has six pages over the semantic model of the same name, with the theme in `powerbi/theme/cre-portfolio-theme.json`.

## Report-Wide Conventions

**Fields**

- Values are always measures. Never drag a numeric column into a value well; "Sum of ..." in a visual title means a column was used instead of a measure.
- Dates come from `dim_date`: `year_month` for month axes and the Year/month slicer. `dim_date` filters each fact by its active date: payment, lease start, request, and budget date.
- Property and tenant labels come from `dim_property` (`property_name`, `region`, `market`, `property_type`) and `dim_tenant` (`tenant_name`).
- Statuses and categories come from the facts: `fact_rent_payment[payment_status]`, `fact_lease[lease_status]`, and `fact_maintenance_request[category]`, `[priority]`, `[status]`.
- Rename fields for display by double-clicking them in the field well: `year_month` to "Month", `property_type` to "Property Type", `property_name` to "Property", `tenant_name` to "Tenant", `payment_status` to "Payment Status". Retitle charts whose default title repeats raw field names.

**Style**

- The theme `powerbi/theme/cre-portfolio-theme.json` sets a light grey page, white visuals with rounded corners and no visible border, and the title, label, and callout fonts. Re-import it after every change (**View** > **Themes** > **Browse for themes**); formatting set by hand on a visual overrides it.
- Header bar: **Insert** > **Shapes** > **Rectangle**, full page width and the height of the header row, fill `#1B5E4B`, no border, sent to the back (**Format** > **Send backward**). Make the page title white, and paste the bar onto every page.
- One insight sentence per page, in a text box under the header, stating what the page shows, for example "All rent collected; 44% of payments arrived late."
- Give KPI numbers context with the card's **Reference labels**, for example Total Rent Collected against Budget Revenue, or Collection Rate with Late Payment Rate.
- Reduce chart clutter: turn off gridlines and the value axis on bar charts and show data labels instead. Use conditional formatting only on values that vary; for a column that is the same on every row, use **Data bars** on a column that differs instead.
- Colors carry meaning: `#237A57` for good or Succeeded, `#B84A4A` for bad or Failed, `#D6A84B` for in progress or warning.

**Layout**

- Top band: page title, page navigator, and slicers in one row.
- Second band: one KPI card row.
- Remainder: a 2×2 grid of visuals, or one wide trend above two visuals. Size visuals to at least half the page width and a quarter of its height, or they render as placeholder icons.
- Build the Executive Overview first, then use **Duplicate page** for pages 2 to 5 and swap the visuals.

**KPI cards**

- Use the new **Card** visual, which holds a row of measures in one visual.
- Set **Callout value** > **Display units** to **None**, so cards show the measure's format string (`$3,988,950`, not `$4M`).
- Size the cards in a row equally across the page width, with their tops aligned (**Format** > **Align** and **Distribute horizontally**).

**Slicers**

- Set **Slicer settings** > **Style** to **Dropdown**.
- In **Filters on this visual**, set each slicer's field to **is not blank**. Blank items come from the Unknown member (key 0) and from facts with an empty date, such as an open maintenance request with no completion date.
- Limit the Year/month slicer to months with data by also filtering it on **Total Rent Collected** is not blank. `dim_date` covers 2020 to 2030.
- In **View** > **Sync slicers**, sync the Year/month, `region`, and `property_name` slicers across pages 1 to 5. Leave the Data Quality page out.

**Navigation**

- Add **Insert** > **Buttons** > **Navigator** > **Page navigator** to the header row, wide enough that no page name is truncated, and paste it in the same position on every page.
- Keep the title, navigator, and slicers in one header row no taller than a dropdown.

## Page 1: Executive Overview

**Slicers:** Year/month, `region`, `market`, `property_type`, `property_name`

**KPI cards:** Total Rent Collected, Collection Rate, Outstanding Rent, Active Lease Count, Open Maintenance Requests

| Visual | Fields |
|---|---|
| Line and clustered column chart, titled "Rent Collected vs Budget by Month" | X-axis `year_month`; column Total Rent Collected; line Budget Revenue |
| Clustered bar chart | Y-axis `market`; X-axis Total Rent Collected. `market` rather than `region`, which has a single value in the sample data |
| Matrix | Rows `property_name`; values Total Rent Due, Total Rent Collected, Collection Rate, with a background-color rule on Collection Rate |
| Stacked bar chart | Y-axis `priority`; legend `status`; X-axis Maintenance Request Count |

## Page 2: Rent Collections

**Slicers:** Year/month, `region`, `property_name`, `tenant_name`, `payment_status`

**KPI cards:** Total Rent Due, Total Rent Collected, Outstanding Rent, Collection Rate, Late Payment Rate

The page pairs collection with timeliness: in the sample data every payment is collected in full (Collection Rate 100.0%), but 16 of 36 arrive late (Late Payment Rate 44.4%).

| Visual | Fields |
|---|---|
| Clustered column chart, titled "Rent Due vs Collected by Month" | X-axis `year_month`; Y-axis Total Rent Due, Total Rent Collected. Columns keep both series visible when they are equal, where lines would overlap |
| Stacked bar chart | Y-axis `property_name`; legend `payment_status`; X-axis Payment Count |
| Donut chart | Legend `payment_status`; values Payment Count; detail labels show category and percent of total, with 0 decimals |
| Matrix | Rows `property_name`, then `tenant_name`; values Total Rent Due, Total Rent Collected, Outstanding Rent, Collection Rate |

## Page 3: Lease Portfolio

**Slicers:** `region`, `property_type`, `property_name`, `tenant_name`, `lease_status`

**KPI cards:** Lease Count, Active Lease Count, Monthly Contracted Rent, Annualized Contracted Rent

| Visual | Fields |
|---|---|
| Clustered column chart | X-axis `year_month`; Y-axis Leases Ending, which uses the inactive lease-end-date relationship so the months are expiry months |
| Clustered bar chart | Y-axis `property_name`; X-axis Monthly Contracted Rent |
| Table | `tenant_name`, `property_name`, `lease_status`, Monthly Contracted Rent, Annualized Contracted Rent |

## Page 4: Maintenance Operations

**Slicers:** Year/month, `region`, `property_name`, `category`, `priority`, `status`

**KPI cards:** Maintenance Request Count, Open Maintenance Requests, Actual Maintenance Cost, Maintenance Cost Variance, Average Resolution Days

| Visual | Fields |
|---|---|
| Stacked bar chart | Y-axis `priority`; legend `status`; X-axis Maintenance Request Count |
| Clustered bar chart | Y-axis `property_name`; X-axis Actual Maintenance Cost |
| Line chart | X-axis `year_month`; Y-axis Average Resolution Days |
| Table | `property_name`, `category`, `priority`, `status`, Estimated Maintenance Cost, with a visual filter of `status` is Open |

## Page 5: Budget Versus Actual

**Slicers:** Year/month, `region`, `property_name`. Use the `dim_date` slicer, not the budget's own year and month columns.

**KPI cards:** Budget Revenue, Total Rent Collected, Rent to Budget Variance, Rent to Budget Variance %

| Visual | Fields |
|---|---|
| Line chart | X-axis `year_month`; Y-axis Budget Revenue, Total Rent Collected |
| Clustered bar chart | Y-axis `property_name`; X-axis Rent to Budget Variance |
| Matrix | Rows `property_name`; values Budget Revenue, Budget Expense, Budget NOI |

## Page 6: Data Quality

This operational page is optional for business consumers but valuable in the portfolio demonstration. It answers one question, "Can I trust today's numbers?", in three rows: did the last run work, is the pipeline reliable over time, and is the data complete. It reads `audit.pipeline_runs`, which `pl_cre_end_to_end` appends to on every run, alongside the Gold facts.

Build it from a blank page, not a duplicate: `pipeline_runs` has no relationships, so `dim_date` and property slicers don't filter it. Copy the header bar, title, and navigator from another page; pasted visuals keep their position.

**Slicers:** `pipeline_runs[environment]` (Dropdown, "Environment") and `pipeline_runs[triggered_at]` (Between, "Run Date"), not synced with other pages

| Row | Visual | Fields and formatting |
|---|---|---|
| 1 | Card | Latest Run Status, Latest Run Logged At, Pipeline Run Success Rate, Latest Silver Processing Time; Latest Run Status colored by **Callout value** > **Color** > **fx** > **Field value** > Status Color |
| 2 | Stacked column chart, "Runs by Day" | X-axis `run_date`; legend `status`; Y-axis Pipeline Run Count; Succeeded `#237A57`, Failed `#B84A4A`; subtitle off. Use `run_date`, not `triggered_at`: a timestamp axis draws each run as a hairline, and Direct Lake models have no automatic date hierarchy |
| 2 | Table, "Recent Runs" | `triggered_at`, `pipeline_run_id`, `status`, `environment`, `message`, renamed Triggered, Run ID, Status, Environment, Message; sorted by Triggered descending; Status font colored by **Cell elements** > **Font color** > **fx** > **Field value** > Status Color |
| 3 | Card, "Unmatched Keys (should be 0)" | Unknown Property Payments, Unknown Tenant Payments |
| 3 | Card, "Model Volumes" | Payment Count, Lease Count, Maintenance Request Count, Property Count, Tenant Count |

`Status Color` returns each row's status color in the table and the latest run's in the card, so one measure serves both.

The pipeline-run slicers don't affect the unknown-member and volume cards, which come from the facts.

Rejected Silver rows live in `lh_cre_silver.quarantine.rejected_records`, outside this model; review them in the `lh_cre_silver` SQL analytics endpoint.
