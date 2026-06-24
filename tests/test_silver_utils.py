import pandas as pd
import pytest

from src.silver_utils import SilverSchemaError, normalize_column_name, transform_to_silver


CONFIG = {
    "name": "leases",
    "primary_key": ["lease_id"],
    "required_columns": ["lease_id", "property_id"],
    "column_types": {
        "lease_id": "integer",
        "property_id": "integer",
        "monthly_rent": "decimal",
        "lease_start_date": "date",
        "lease_end_date": "date",
        "ingestion_timestamp": "datetime",
    },
    "non_negative_columns": ["monthly_rent"],
    "date_order_rules": [["lease_start_date", "lease_end_date"]],
}


def test_normalize_column_name():
    assert normalize_column_name("Main.Temp") == "main_temp"
    assert normalize_column_name("Lease Start Date") == "lease_start_date"


def test_transform_casts_and_preserves_audit_columns():
    source = pd.DataFrame([{
        "lease_id": "10", "property_id": "101", "monthly_rent": "2500.50",
        "lease_start_date": "2026-01-01", "lease_end_date": "2026-12-31",
        "source_system": "CRE_SQL", "raw_record_hash": "abc",
        "ingestion_timestamp": "2026-06-20T00:00:00Z",
    }])
    result = transform_to_silver(source, CONFIG)
    assert result.metrics["rows_valid"] == 1
    assert result.metrics["rows_rejected"] == 0
    assert result.valid.loc[0, "lease_id"] == 10
    assert result.valid.loc[0, "monthly_rent"] == 2500.50
    assert result.valid.loc[0, "raw_record_hash"] == "abc"


def test_invalid_rows_are_quarantined_with_reasons():
    source = pd.DataFrame([{
        "lease_id": "bad-id", "property_id": "101", "monthly_rent": "-5",
        "lease_start_date": "2026-12-31", "lease_end_date": "2026-01-01",
        "ingestion_timestamp": "2026-06-20T00:00:00Z",
    }])
    result = transform_to_silver(source, CONFIG)
    reasons = result.rejected.loc[0, "rejection_reason"]
    assert result.metrics["rows_rejected"] == 1
    assert "invalid_integer:lease_id" in reasons
    assert "negative_value:monthly_rent" in reasons
    assert "date_order:lease_start_date>lease_end_date" in reasons


def test_latest_duplicate_wins():
    source = pd.DataFrame([
        {"lease_id": "10", "property_id": "101", "monthly_rent": "2000", "ingestion_timestamp": "2026-06-19T00:00:00Z"},
        {"lease_id": "10", "property_id": "101", "monthly_rent": "2500", "ingestion_timestamp": "2026-06-20T00:00:00Z"},
    ])
    result = transform_to_silver(source, CONFIG)
    assert result.metrics["duplicate_rows_removed"] == 1
    assert result.valid.loc[0, "monthly_rent"] == 2500


def test_missing_required_source_column_fails_fast():
    with pytest.raises(SilverSchemaError, match="property_id"):
        transform_to_silver(pd.DataFrame([{"lease_id": "10"}]), CONFIG)
