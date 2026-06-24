# Apply Phase 6

Extract this overlay into the repository root and allow folders to merge. It intentionally replaces `src/bronze_utils.py` to propagate the orchestrator run ID.

Add this generated-output rule to `.gitignore`:

```gitignore
data/operations/
```

Run:

```powershell
python notebooks/07_run_end_to_end_pipeline.py
python notebooks/08_pipeline_health_report.py
```

Then commit:

```powershell
git status
git add .
git commit -m "Add end-to-end orchestration and observability"
git push
```
