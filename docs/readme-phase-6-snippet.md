## Phase 6: Orchestration and Observability

Run the complete platform under one audited pipeline execution:

```powershell
python notebooks/07_run_end_to_end_pipeline.py
python notebooks/08_pipeline_health_report.py
```

The pipeline applies ADF/Fabric-style activity dependencies, bounded API retries, failure propagation, a shared run ID, step-level audit logging, and automated semantic and unit-test gates.
