# Architecture Overview

## Purpose

This project demonstrates a production-style Microsoft Fabric lakehouse architecture for Commercial Real Estate operations and finance reporting.

## Architecture Goals

- Ingest data from multiple source types
- Preserve raw data in Bronze
- Standardize and validate data in Silver
- Publish reporting-ready Gold tables
- Support Power BI semantic modeling
- Include operational logging and validation
- Demonstrate Dev/Test/Prod promotion strategy

## High-Level Flow

```text
Sources → Bronze → Silver → Gold → Semantic Model → Power BI
```

## Source Systems

- Simulated SQL operational tables
- REST API weather enrichment
- CSV and Excel-style business files
- Manual reference data

## Fabric Components

- Lakehouse
- Notebooks
- Pipelines
- Delta tables
- Semantic model
- Power BI report
- Deployment pipelines
