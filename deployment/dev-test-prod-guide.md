# Dev/Test/Prod Deployment Guide

## Workspace Topology

| Stage | Workspace | Lakehouse | Schedule |
|---|---|---|---|
| Development | `ws-cre-modernization-dev` | `lh_cre_dev` | Disabled |
| Test | `ws-cre-modernization-test` | `lh_cre_test` | Disabled |
| Production | `ws-cre-modernization-prod` | `lh_cre_prod` | Enabled after validation |

Create one Fabric deployment pipeline with Development, Test, and Production stages mapped to these workspaces. Connect only the Development workspace to the Git repository.

## Git Workflow

1. Create a short-lived feature branch.
2. Make and test changes locally.
3. Push the branch and open a pull request.
4. Require repository validation and end-to-end workflows.
5. Review code, schema, DAX, deployment rules, and evidence.
6. Merge through the pull request; do not push directly to `main`.
7. Sync the Development Fabric workspace from the approved `main` commit.

## Development Validation

```powershell
python notebooks/07_run_end_to_end_pipeline.py
python scripts/validate_release.py --environment dev
python scripts/check_deployment_gates.py --environment dev --write-evidence
```

Confirm pipeline success, Gold reconciliation, semantic validation, model relationships, DAX spot checks, and report visuals.

## Promote Development to Test

1. Trigger `.github/workflows/release.yml` for `test` and a semantic version.
2. Review the checksummed release and deployment-gate artifact.
3. Approve the `fabric-test` GitHub Environment gate.
4. In the Fabric deployment pipeline, compare Development and Test.
5. Apply the Test deployment rules in `deployment/deployment-rules.json`.
6. Promote the selected artifacts to Test.
7. Run the Test pipeline against Test connections and data.
8. Validate row counts, relationships, measures, filters, and report pages.
9. Record the commit SHA, release version, Fabric deployment operation, and validation result.

## Promote Test to Production

1. Confirm Test acceptance and business-owner approval.
2. Trigger the release workflow for `prod` using the same tested version.
3. Approve the protected `fabric-prod` environment.
4. Compare Test and Production in the Fabric deployment pipeline.
5. Apply Production connection and Lakehouse binding rules.
6. Promote only the tested artifact set from Test to Production.
7. Keep the Production schedule disabled until smoke tests pass.
8. Run post-deployment validation.
9. Enable the approved Production schedule.
10. Publish or update the Power BI app after report validation.

## Post-Deployment Validation

- Fabric pipeline completes successfully.
- Bronze, Silver, and Gold row counts reconcile.
- Quarantine volume is within the accepted threshold.
- Gold business grains remain unique.
- Semantic-model relationships contain no orphans.
- DAX totals agree with SQL acceptance queries.
- Direct Lake connectivity and report filters work.
- Workspace lineage shows the expected path.
- Alerts and ownership are configured.

## Required GitHub Environments

Create protected environments named:

```text
fabric-dev
fabric-test
fabric-prod
```

Require reviewers for Test and Production. Store any future deployment credentials as environment secrets, never in repository JSON.
