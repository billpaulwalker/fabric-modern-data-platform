# Rollback Plan

## Rollback Triggers

- Failed validation checks
- Incorrect row counts
- Broken semantic model
- Broken report visuals
- Pipeline failure after deployment

## Rollback Options

- Revert Git commit
- Redeploy prior version from main
- Restore previous notebook version
- Restore previous semantic model version
- Disable new pipeline schedule
- Reprocess from prior successful watermark when appropriate
