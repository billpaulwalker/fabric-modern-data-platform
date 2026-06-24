# Rollback Runbook

## Rollback Triggers

- Failed Production pipeline or semantic validation
- Material row-count or financial reconciliation difference
- Broken semantic-model relationship or DAX result
- Report visuals fail or expose incorrect data
- Security or connection binding is incorrect
- Performance exceeds the accepted threshold

## Immediate Containment

1. Disable the Production schedule.
2. Stop the Power BI app update or restore the prior audience version.
3. Record the release version, commit SHA, deployment operation, run ID, and symptoms.
4. Preserve operational logs and validation evidence.

## Artifact Rollback

1. Identify the last known good Git tag and release package.
2. Revert the faulty change through a pull request when possible.
3. Sync the corrected `main` state into Development.
4. Revalidate in Development and Test.
5. Promote the last known good artifact set through the Fabric deployment pipeline.
6. Reapply the environment-specific deployment rules.

## Data Rollback

Do not automatically roll back data merely because code is rolled back. Determine whether the failed release changed Delta data.

- For overwrite-based demo tables, rerun the last known good pipeline.
- For incremental tables, restore the prior watermark only after confirming target consistency.
- Use Delta history or a versioned recovery table when production recovery requires it.
- Never advance a watermark after an unsuccessful target commit.

## Recovery Validation

Repeat all Production post-deployment checks. Re-enable schedules and republish the app only after technical and business validation succeeds.
