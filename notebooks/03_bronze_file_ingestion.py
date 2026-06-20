"""
03 Bronze File Ingestion

Purpose:
    Land business-managed CSV/reference files into raw Bronze tables.

Portfolio note:
    These sources represent analyst- or business-managed files such as property
    budget files, chart of accounts files, or region mappings.
"""

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.bronze_utils import generate_pipeline_run_id, ingest_csv_to_bronze


BUSINESS_FILES = [
    {
        "source_system": "BusinessFiles",
        "source_object": "property_budget.csv",
        "input_path": REPO_ROOT / "data/sample/property_budget.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_property_budget.csv",
        "load_type": "full",
    },
    {
        "source_system": "BusinessFiles",
        "source_object": "property_region_mapping.csv",
        "input_path": REPO_ROOT / "data/sample/property_region_mapping.csv",
        "output_path": REPO_ROOT / "data/bronze/bronze_property_region_mapping.csv",
        "load_type": "full",
    },
]


def main() -> None:
    pipeline_run_id = generate_pipeline_run_id("bronze_files")
    run_results = []

    for source in BUSINESS_FILES:
        result = ingest_csv_to_bronze(
            input_path=source["input_path"],
            output_path=source["output_path"],
            source_system=source["source_system"],
            source_object=source["source_object"],
            pipeline_run_id=pipeline_run_id,
            load_type=source["load_type"],
        )
        run_results.append(result)

    print("Bronze file ingestion complete")
    for result in run_results:
        print(result)


if __name__ == "__main__":
    main()
