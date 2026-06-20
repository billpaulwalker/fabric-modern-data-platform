"""
01 Bronze SQL Ingestion

Purpose:
    Simulate SQL operational table ingestion into raw Bronze tables.

Portfolio note:
    In a real Microsoft Fabric implementation, these source tables would often
    come from SQL Server, Azure SQL, a Fabric Warehouse, or a mirrored database.
    For this portfolio project, the CSV files in data/sample represent the
    operational source extracts so the repo can run locally and in CI.

Bronze contract:
    Bronze preserves raw source-aligned records and adds audit metadata:
        - source_system
        - source_object
        - source_file_name
        - ingestion_timestamp
        - pipeline_run_id
        - load_type
        - raw_record_hash
"""

from pathlib import Path
import sys

# Allows running this script directly from the notebooks folder or repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.bronze_utils import generate_pipeline_run_id, ingest_csv_to_bronze


SOURCE_TABLES = [
    {
        "source_system": "CRE_SQL",
        "source_object": "properties",
        "input_path": REPO_ROOT / "data/sample/properties.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_properties.csv",
        "load_type": "full",
    },
    {
        "source_system": "CRE_SQL",
        "source_object": "tenants",
        "input_path": REPO_ROOT / "data/sample/tenants.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_tenants.csv",
        "load_type": "full",
    },
    {
        "source_system": "CRE_SQL",
        "source_object": "leases",
        "input_path": REPO_ROOT / "data/sample/leases.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_leases.csv",
        "load_type": "incremental",
    },
    {
        "source_system": "CRE_SQL",
        "source_object": "rent_payments",
        "input_path": REPO_ROOT / "data/sample/rent_payments.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_rent_payments.csv",
        "load_type": "incremental",
    },
    {
        "source_system": "CRE_SQL",
        "source_object": "maintenance_requests",
        "input_path": REPO_ROOT / "data/sample/maintenance_requests.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_maintenance_requests.csv",
        "load_type": "incremental",
    },
]


def main() -> None:
    pipeline_run_id = generate_pipeline_run_id("bronze_sql")
    run_results = []

    for source in SOURCE_TABLES:
        result = ingest_csv_to_bronze(
            input_path=source["input_path"],
            output_path=source["output_path"],
            source_system=source["source_system"],
            source_object=source["source_object"],
            pipeline_run_id=pipeline_run_id,
            load_type=source["load_type"],
        )
        run_results.append(result)

    print("Bronze SQL-style ingestion complete")
    for result in run_results:
        print(result)


if __name__ == "__main__":
    main()
