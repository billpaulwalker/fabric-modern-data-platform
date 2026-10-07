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


def test_monthly_budget_grain_preserves_each_period():
    config = {
        "name": "property_budget",
        "primary_key": ["property_id", "budget_month"],
        "required_columns": ["property_id", "budget_month"],
        "column_types": {
            "property_id": "integer",
            "budget_month": "string",
            "budgeted_rent": "decimal",
        },
    }
    source = pd.DataFrame([
        {"property_id": "101", "budget_month": "2026-01-01", "budgeted_rent": "1000"},
        {"property_id": "101", "budget_month": "2026-02-01", "budgeted_rent": "1100"},
    ])
    result = transform_to_silver(source, config)
    assert result.metrics["rows_valid"] == 2
    assert result.metrics["duplicate_rows_removed"] == 0


def test_column_mappings_rename_source_columns_to_model_names():
    config = {
        "name": "rent_payments",
        "primary_key": ["payment_id"],
        "required_columns": ["payment_id", "amount_due"],
        "column_mappings": {"amount_billed": "amount_due", "Payment Amount": "amount_paid"},
        "column_types": {"payment_id": "integer", "amount_due": "decimal", "amount_paid": "decimal"},
    }
    source = pd.DataFrame([{"payment_id": "1", "amount_billed": "100.0", "Payment Amount": "80.0"}])
    result = transform_to_silver(source, config)
    assert result.valid.loc[0, "amount_due"] == 100.0
    assert result.valid.loc[0, "amount_paid"] == 80.0
    assert "amount_billed" not in result.valid.columns


def test_column_mapping_onto_existing_column_fails_fast():
    config = {"name": "t", "column_mappings": {"amount_billed": "amount_due"}}
    source = pd.DataFrame([{"amount_billed": "1", "amount_due": "2"}])
    with pytest.raises(SilverSchemaError, match="amount_due"):
        transform_to_silver(source, config)


def test_product_derived_column_is_calculated_after_casting():
    config = {
        "name": "leases",
        "primary_key": ["lease_id"],
        "column_types": {"lease_id": "integer", "leased_square_feet": "decimal", "monthly_rate_per_sqft": "decimal"},
        "derived_columns": {"monthly_rent": {"product": ["leased_square_feet", "monthly_rate_per_sqft"]}},
        "non_negative_columns": ["monthly_rent"],
    }
    source = pd.DataFrame([
        {"lease_id": "1", "leased_square_feet": "25000", "monthly_rate_per_sqft": "4.15"},
        {"lease_id": "2", "leased_square_feet": "100", "monthly_rate_per_sqft": "-1"},
    ])
    result = transform_to_silver(source, config)
    assert result.valid.loc[0, "monthly_rent"] == pytest.approx(103750.0)
    assert "negative_value:monthly_rent" in result.rejected.loc[0, "rejection_reason"]


def test_derived_column_with_missing_input_fails_fast():
    config = {"name": "leases", "derived_columns": {"monthly_rent": {"product": ["leased_square_feet", "rate"]}}}
    with pytest.raises(SilverSchemaError, match="rate"):
        transform_to_silver(pd.DataFrame([{"leased_square_feet": "1"}]), config)


def test_configured_columns_absent_from_source_are_reported_in_metrics():
    source = pd.DataFrame([{"lease_id": "10", "property_id": "101", "ingestion_timestamp": "2026-06-20T00:00:00Z"}])
    result = transform_to_silver(source, CONFIG)
    assert result.metrics["missing_configured_columns"] == [
        "lease_end_date", "lease_start_date", "monthly_rent",
    ]
