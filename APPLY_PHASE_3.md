# Apply Phase 3

Extract this overlay into the repository root and allow folders to merge. No Phase 2 source files are replaced.

Add these generated-output rules to `.gitignore` if they are not already present:

```gitignore
data/silver/
data/rejected/
```

Run:

```powershell
python notebooks/01_bronze_sql_ingestion.py
python notebooks/02_bronze_api_ingestion.py
python notebooks/03_bronze_file_ingestion.py
python notebooks/04_silver_transformations.py
python -m pytest
```

Then commit:

```powershell
git status
git add .
git commit -m "Add Silver transformation and data quality framework"
git push
```
