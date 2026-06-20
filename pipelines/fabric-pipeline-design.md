# Fabric Pipeline Design

## Purpose

Show how Fabric pipelines orchestrate the notebook-based medallion workflow.

## Planned Activities

1. Load source configuration
2. Run Bronze SQL/file ingestion
3. Run Bronze API ingestion
4. Run Silver transformations
5. Run Gold dimensional model
6. Run validation checks
7. Update pipeline run log
8. Trigger semantic model refresh or downstream checks

## Design Notes

This project uses Fabric pipeline concepts while also documenting Azure Data Factory-style patterns such as Lookup, ForEach, parameters, dynamic table loading, logging, and watermarks.
