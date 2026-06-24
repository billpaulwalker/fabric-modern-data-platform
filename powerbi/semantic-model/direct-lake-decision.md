# Direct Lake Decision

## Decision

Use Direct Lake for the primary semantic model because the reporting tables are managed Delta tables in the Fabric Lakehouse and the model is intended to remain within Fabric.

## Why It Fits

- Gold data is already shaped as a star schema.
- Direct Lake avoids maintaining a separate imported copy for normal operation.
- Power BI can query OneLake-backed tables with low-latency access.
- Fabric lineage remains clear from Lakehouse through semantic model to report.
- The design supports centralized measures and governed relationships.

## Preconditions

- Gold Delta tables have stable names and schemas.
- Dimension keys are unique.
- Fact foreign keys resolve to dimension members.
- Workspace identity and user permissions are configured.
- Capacity and model behavior are monitored for fallback or performance issues.

## When Import May Be Better

Choose Import when the source is outside Fabric, a compact cached model is operationally simpler, or model features and performance testing show that Import better meets the requirement.

## When DirectQuery May Be Better

Choose DirectQuery only when source-level freshness or governance requirements prevent data from being represented in OneLake and the source can sustain interactive query workloads.

Storage mode is an architectural decision, not a blanket rule. Validate it against data volume, freshness, security, capacity, and query performance.
