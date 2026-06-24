# Release Checklist

## Before Merge

- Pull-request workflows pass
- Code and configuration reviewed
- No secrets or environment-specific credentials committed
- Tests cover changed behavior
- Documentation and release rules updated

## Before Test

- Development pipeline succeeds
- Semantic validation passes
- Release package and checksum manifest created
- Test deployment rules reviewed
- Test approval recorded

## Before Production

- Test validation completed
- Business acceptance recorded
- Rollback version identified
- Production deployment rules reviewed
- Production approval recorded
- Maintenance window and support owner confirmed

## After Production

- Pipeline and row counts validated
- Semantic measures reconciled
- Report pages smoke-tested
- Schedule enabled
- Power BI app updated
- Release evidence retained
