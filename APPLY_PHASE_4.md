# Apply Phase 4

Extract this overlay into the repository root and allow folders to merge. It adds new Phase 4 files and does not replace the Phase 2 or Phase 3 implementation.

Run the complete local path:

```powershell
python notebooks/01_bronze_sql_ingestion.py
python notebooks/02_bronze_api_ingestion.py
python notebooks/03_bronze_file_ingestion.py
python notebooks/04_silver_transformations.py
python notebooks/05_gold_dimensional_model.py
python -m pytest
```

Inspect `data/gold/gold_model_metrics.json`, then commit:

```powershell
git status
git add .
git commit -m "Add Gold dimensional model and validation framework"
git push
```
