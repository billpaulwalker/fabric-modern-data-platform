# Production Readiness Checklist

## Architecture

- Clear source-to-target mapping
- Medallion layers documented
- Gold model grain documented
- Semantic model design documented

## Ingestion

- Metadata-driven source config
- Full and incremental load support
- API retry handling
- Watermark tracking

## Quality

- Required field checks
- Duplicate checks
- Row count checks
- Rejected record handling

## Operations

- Pipeline run logging
- Error capture
- Monitoring notes
- Rollback plan

## DevOps

- Git workflow
- Pull request checklist
- Validation workflow
- Dev/Test/Prod deployment strategy
