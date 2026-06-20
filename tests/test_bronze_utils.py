from pathlib import Path

from src.bronze_utils import (
    AUDIT_COLUMNS,
    add_bronze_audit_columns,
    calculate_raw_record_hash,
    ingest_csv_to_bronze,
)


def test_calculate_raw_record_hash_is_stable():
    record_a = {"property_id": 101, "property_name": "Carmel Valley Plaza"}
    record_b = {"property_name": "Carmel Valley Plaza", "property_id": 101}

    assert calculate_raw_record_hash(record_a) == calculate_raw_record_hash(record_b)


def test_add_bronze_audit_columns_adds_expected_columns():
    records = [{"property_id": 101, "property_name": "Carmel Valley Plaza"}]

    bronze_records = add_bronze_audit_columns(
        records,
        source_system="CRE_SQL",
        source_object="properties",
        source_file_name="properties.csv",
        pipeline_run_id="test_run",
        load_type="full",
        ingestion_timestamp="2026-06-20T00:00:00Z",
    )

    assert len(bronze_records) == 1
    for column in AUDIT_COLUMNS:
        assert column in bronze_records[0]

    assert bronze_records[0]["source_system"] == "CRE_SQL"
    assert bronze_records[0]["source_object"] == "properties"
    assert bronze_records[0]["pipeline_run_id"] == "test_run"
    assert len(bronze_records[0]["raw_record_hash"]) == 64


def test_ingest_csv_to_bronze_writes_output(tmp_path):
    input_path = tmp_path / "properties.csv"
    output_path = tmp_path / "bronze_properties.csv"

    input_path.write_text(
        "property_id,property_name\n101,Carmel Valley Plaza\n",
        encoding="utf-8",
    )

    result = ingest_csv_to_bronze(
        input_path=input_path,
        output_path=output_path,
        source_system="CRE_SQL",
        source_object="properties",
        pipeline_run_id="test_run",
        load_type="full",
    )

    assert result["rows_read"] == 1
    assert result["rows_written"] == 1
    assert output_path.exists()

    output_text = output_path.read_text(encoding="utf-8")
    assert "source_system" in output_text
    assert "raw_record_hash" in output_text
