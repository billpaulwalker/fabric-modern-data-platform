## Phase 3: Silver Transformations

After running the Phase 2 Bronze scripts, execute:

```powershell
python notebooks/04_silver_transformations.py
python -m pytest
```

Silver processing standardizes column names and data types, validates required fields and business rules, removes duplicate business keys using latest-record-wins logic, preserves Bronze lineage columns, and writes rejected records with explicit reasons.

Local generated outputs are written to `data/silver/` and `data/rejected/`. The Fabric version in `notebooks/fabric/04_silver_transformations_pyspark.py` writes managed Delta tables to `silver` and `silver_quarantine` schemas.
