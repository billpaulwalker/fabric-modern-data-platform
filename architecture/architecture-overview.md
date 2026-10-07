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

## Workspace and Lakehouse Layout

Each environment (Development, Test, Production) is one workspace containing a schema-enabled Lakehouse per medallion layer. Lakehouse names are identical in every workspace, so cross-layer references never change between environments; the workspace name carries the environment.

| Lakehouse | Schemas organized by | Schemas |
|---|---|---|
| `lh_cre_bronze` | Source system | `cre_sql`, `business_files`, `openweather` |
| `lh_cre_silver` | Business domain | `property`, `leasing`, `finance`, `operations`, `environment`, `quarantine` |
| `lh_cre_gold` | Business domain, with conformed dimensions shared | `shared`, `leasing`, `finance`, `operations` |

`config/fabric_layout.json` is the single declaration of where every table lives; see `architecture/source-to-target-mapping.md` for the full list.

Why separate Lakehouses per layer:

- **Access by layer.** Report authors and analysts can be granted Gold only, while raw Bronze data stays restricted to engineering.
- **A clean semantic model.** The Direct Lake model binds to `lh_cre_gold` alone, and its SQL analytics endpoint exposes only curated tables.
- **Ownership and lineage.** Source-aligned Bronze and domain-aligned Silver and Gold appear as separate items in Fabric lineage.
- **Shared dimensions stay shared.** `dim_property`, `dim_tenant`, and `dim_date` serve every domain's facts, so they live in `shared` rather than inside one domain.

## Naming Standard

Technical Fabric items follow `<type>_<project>_<layer>[_<source system | domain>]_<purpose>` in lowercase snake_case, with no environment in the name; the workspace carries the environment. Source system or domain is added only when an item is scoped to one, for example `nb_cre_bronze_openweather_ingest` or `nb_cre_silver_leasing_transform`. Repository files under `notebooks/fabric/` are named exactly as their Fabric notebooks.

| Item | Pattern | Examples |
|---|---|---|
| Workspace | `ws-<project>-<environment>` | `ws-cre-modernization-dev` |
| Lakehouse | `lh_` | `lh_cre_bronze`, `lh_cre_silver`, `lh_cre_gold` |
| Notebook | `nb_` | `nb_cre_bronze_ingest`, `nb_cre_silver_transform`, `nb_cre_gold_build_model`, `nb_cre_gold_validate_model` |
| Data Pipeline | `pl_` | `pl_cre_end_to_end` |
| Spark environment | `env_` | `env_cre_spark` |
| Semantic model and report | Business-friendly name | **CRE Portfolio Analytics** |

Business-facing items keep readable names because report consumers see them in Power BI and apps.

## Fabric Components

- Lakehouses: `lh_cre_bronze`, `lh_cre_silver`, `lh_cre_gold`
- Notebooks
- Pipelines
- Delta tables
- Semantic model
- Power BI report
- Deployment pipelines
