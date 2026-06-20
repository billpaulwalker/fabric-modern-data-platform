# Interview Talking Points

## Project Pitch

I built a Microsoft Fabric lakehouse modernization project for a Commercial Real Estate scenario. The project ingests operational SQL-style data, API data, and business reference files into a Bronze/Silver/Gold architecture, then publishes a Power BI-ready Gold model.

## Senior Data Engineering Signals

- Metadata-driven ingestion
- ADF-style orchestration design
- Incremental loading with watermarks
- Logging and validation
- Delta Lake medallion modeling
- Power BI semantic model readiness
- Dev/Test/Prod promotion strategy
- Git and pull request workflow

## How to Explain the Architecture

The design separates raw preservation, standardized transformation, and reporting-ready modeling. Bronze keeps raw data, Silver applies quality and standardization rules, and Gold provides a star schema for Power BI.

## How to Explain Production Readiness

The project includes control tables, logging tables, watermark tracking, validation gates, rollback planning, and deployment documentation.
