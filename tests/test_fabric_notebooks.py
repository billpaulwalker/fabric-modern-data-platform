"""Exercise the Fabric PySpark notebooks against a local Spark session.

Input frames are read from CSV (all strings, blanks as null) the way Bronze lands
them, which also keeps every operation on the JVM so no Python worker is needed.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]
UNKNOWN_KEY = 0


def _load_notebook(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, REPO_ROOT / "notebooks/fabric" / file_name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


silver_nb = _load_notebook("04_silver_transformations_pyspark.py", "silver_notebook")
gold_nb = _load_notebook("05_gold_dimensional_model_pyspark.py", "gold_notebook")


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fabric-notebook-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def csv_frame(spark, tmp_path):
    counter = iter(range(1000))

    def build(rows):
        path = tmp_path / f"frame_{next(counter)}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return spark.read.option("header", True).csv(str(path))

    return build


def _rows(frame, key):
    return {row[key]: row.asDict() for row in frame.collect()}


# Silver -----------------------------------------------------------------------

def test_silver_applies_column_mappings(csv_frame):
    config = {
        "name": "rent_payments",
        "primary_key": ["payment_id"],
        "required_columns": ["payment_id", "amount_due"],
        "column_mappings": {"amount_billed": "amount_due", "Payment Amount": "amount_paid"},
        "column_types": {"payment_id": "integer", "amount_due": "decimal", "amount_paid": "decimal"},
    }
    frame = csv_frame([{"payment_id": "1", "amount_billed": "100.0", "Payment Amount": "80.0"}])
    valid, _, _ = silver_nb.transform_table(frame, config)
    row = valid.collect()[0]
    assert float(row["amount_due"]) == 100.0
    assert float(row["amount_paid"]) == 80.0
    assert "amount_billed" not in valid.columns


def test_silver_mapping_onto_existing_column_fails_fast(csv_frame):
    config = {"name": "t", "column_mappings": {"amount_billed": "amount_due"}}
    with pytest.raises(ValueError, match="amount_due"):
        silver_nb.transform_table(csv_frame([{"amount_billed": "1", "amount_due": "2"}]), config)


def test_silver_derives_product_column_and_validates_it(csv_frame):
    config = {
        "name": "leases",
        "primary_key": ["lease_id"],
        "column_types": {"lease_id": "integer", "leased_square_feet": "decimal", "monthly_rate_per_sqft": "decimal"},
        "derived_columns": {"monthly_rent": {"product": ["leased_square_feet", "monthly_rate_per_sqft"]}},
        "non_negative_columns": ["monthly_rent"],
    }
    frame = csv_frame([
        {"lease_id": "1", "leased_square_feet": "25000", "monthly_rate_per_sqft": "4.15"},
        {"lease_id": "2", "leased_square_feet": "100", "monthly_rate_per_sqft": "-1"},
    ])
    valid, rejected, _ = silver_nb.transform_table(frame, config)
    assert float(valid.collect()[0]["monthly_rent"]) == pytest.approx(103750.0)
    assert "negative_value:monthly_rent" in rejected.collect()[0]["rejection_reason"]


def test_silver_derived_column_with_missing_input_fails_fast(csv_frame):
    config = {"name": "leases", "derived_columns": {"monthly_rent": {"product": ["leased_square_feet", "rate"]}}}
    with pytest.raises(ValueError, match="rate"):
        silver_nb.transform_table(csv_frame([{"leased_square_feet": "1"}]), config)


def test_silver_quarantines_invalid_rows_with_reasons(csv_frame):
    config = {
        "name": "leases",
        "primary_key": ["lease_id"],
        "required_columns": ["lease_id", "property_id"],
        "column_types": {"lease_id": "integer", "lease_start_date": "date", "lease_end_date": "date"},
        "allowed_values": {"lease_status": ["Active", "Expired"]},
        "date_order_rules": [["lease_start_date", "lease_end_date"]],
    }
    frame = csv_frame([{
        "lease_id": "bad-id", "property_id": "  ", "lease_status": "Bogus",
        "lease_start_date": "2026-12-31", "lease_end_date": "2026-01-01",
    }])
    valid, rejected, metrics = silver_nb.transform_table(frame, config)
    reasons = rejected.collect()[0]["rejection_reason"]
    assert valid.count() == 0
    assert metrics["rows_rejected"] == 1
    for reason in ["invalid_integer:lease_id", "required:property_id", "invalid_value:lease_status",
                   "date_order:lease_start_date>lease_end_date"]:
        assert reason in reasons


def test_silver_keeps_latest_duplicate(csv_frame):
    config = {"name": "leases", "primary_key": ["lease_id"], "column_types": {"lease_id": "integer"}}
    frame = csv_frame([
        {"lease_id": "10", "monthly_rent": "2500", "ingestion_timestamp": "2026-06-20T00:00:00Z"},
        {"lease_id": "10", "monthly_rent": "2000", "ingestion_timestamp": "2026-06-19T00:00:00Z"},
    ])
    valid, _, metrics = silver_nb.transform_table(frame, config)
    assert metrics["duplicate_rows_removed"] == 1
    assert valid.collect()[0]["monthly_rent"] == "2500"


def test_silver_flattens_nested_api_payload(spark, tmp_path):
    path = tmp_path / "weather.json"
    path.write_text(json.dumps({"property_id": 101, "main": {"temp": 72.4}, "wind": {"speed": 5.8}}), encoding="utf-8")
    config = {
        "name": "weather_api_raw",
        "primary_key": ["property_id"],
        "column_types": {"property_id": "integer", "main_temp": "decimal", "wind_speed": "decimal"},
    }
    valid, _, metrics = silver_nb.transform_table(spark.read.json(str(path)), config)
    row = valid.collect()[0]
    assert float(row["main_temp"]) == 72.4
    assert float(row["wind_speed"]) == 5.8
    assert metrics["missing_configured_columns"] == []


def test_silver_reports_configured_columns_absent_from_source(csv_frame):
    config = {
        "name": "leases",
        "primary_key": ["lease_id"],
        "column_types": {"lease_id": "integer", "monthly_rent": "decimal"},
        "date_order_rules": [["lease_start_date", "lease_end_date"]],
    }
    _, _, metrics = silver_nb.transform_table(csv_frame([{"lease_id": "1"}]), config)
    assert metrics["missing_configured_columns"] == ["lease_end_date", "lease_start_date", "monthly_rent"]


# Gold -------------------------------------------------------------------------

def test_gold_rent_payment_resolves_keys_when_payment_carries_ids(csv_frame):
    dim_property = gold_nb.build_dim_property(csv_frame([{"property_id": "101"}]), None)
    dim_tenant = gold_nb.build_dim_tenant(csv_frame([{"tenant_id": "201"}]))
    leases = csv_frame([{"lease_id": "301", "property_id": "101", "tenant_id": "201"}])
    payments = csv_frame([
        {"payment_id": "401", "lease_id": "301", "property_id": "101", "tenant_id": "201",
         "amount_due": "100", "amount_paid": "100"},
        {"payment_id": "402", "lease_id": "301", "property_id": "", "tenant_id": "201",
         "amount_due": "100", "amount_paid": "50"},
    ])
    fact = gold_nb.build_fact_rent_payment(payments, leases, dim_property, dim_tenant)
    rows = _rows(fact, "payment_id")
    assert all(row["property_key"] != UNKNOWN_KEY for row in rows.values())
    assert all(row["tenant_key"] != UNKNOWN_KEY for row in rows.values())
    assert float(rows["402"]["outstanding_amount"]) == 50


def test_gold_maintenance_without_actual_cost_is_null_not_an_error(csv_frame):
    dim_property = gold_nb.build_dim_property(csv_frame([{"property_id": "101"}]), None)
    requests = csv_frame([{
        "request_id": "501", "property_id": "101", "request_date": "2026-01-12",
        "completed_date": "2026-01-15", "estimated_cost": "1250", "status": "Closed",
    }])
    row = gold_nb.build_fact_maintenance(requests, dim_property).collect()[0]
    assert row["actual_cost"] is None
    assert row["cost_variance"] is None
    assert row["resolution_days"] == 3
    assert row["request_date_key"] == 20260112


def test_gold_budget_date_key_uses_defaulted_month(csv_frame):
    dim_property = gold_nb.build_dim_property(csv_frame([{"property_id": "101"}]), None)
    budget = csv_frame([
        {"property_id": "101", "budget_month": "2026-02-01", "budgeted_rent": "3000", "budgeted_maintenance": "1000"},
        {"property_id": "101", "budget_year": "2027", "budgeted_rent": "3000"},
    ])
    rows = _rows(gold_nb.build_fact_property_budget(budget, dim_property), "budget_year")
    assert rows[2026]["budget_date_key"] == 20260201
    assert float(rows[2026]["budget_noi"]) == 2000
    assert rows[2027]["budget_month"] == 1
    assert rows[2027]["budget_date_key"] == 20270101


def test_gold_duplicate_grain_is_rejected(csv_frame):
    with pytest.raises(ValueError, match="declared grain"):
        gold_nb.assert_unique_grain("fact_test", csv_frame([{"id": "1"}, {"id": "1"}]), ["id"])


# Sample data, end to end --------------------------------------------------------

@pytest.fixture(scope="module")
def sample_gold(spark):
    silver_configs = silver_nb.load_config(REPO_ROOT / "config/silver_table_config.json")["tables"]
    silver, metrics = {}, []
    for config in silver_configs:
        if config["input_format"] != "csv":
            continue
        bronze = (
            spark.read.option("header", True).csv(str(REPO_ROOT / f"data/sample/{config['name']}.csv"))
            .withColumn("ingestion_timestamp", F.lit("2026-06-20T00:00:00Z"))
        )
        valid, _, table_metrics = silver_nb.transform_table(bronze, config)
        silver[config["name"]] = valid.cache()
        metrics.append(table_metrics)
    gold_config = silver_nb.load_config(REPO_ROOT / "config/gold_model_config.json")
    return metrics, gold_nb.build_models(spark, silver, gold_config)


def test_sample_silver_has_no_rejections_or_missing_columns(sample_gold):
    metrics, _ = sample_gold
    assert {m["table"]: m["missing_configured_columns"] for m in metrics if m["missing_configured_columns"]} == {}
    assert all(m["rows_rejected"] == 0 for m in metrics)


def test_sample_gold_matches_local_pandas_results(sample_gold):
    _, models = sample_gold
    lease = models["fact_lease"].agg(F.sum("monthly_rent").alias("rent")).collect()[0]
    payments = models["fact_rent_payment"].agg(
        F.sum("amount_due").alias("due"),
        F.sum(F.when(F.col("property_key") == UNKNOWN_KEY, 1).otherwise(0)).alias("unknown_property"),
        F.sum(F.when(F.col("tenant_key") == UNKNOWN_KEY, 1).otherwise(0)).alias("unknown_tenant"),
    ).collect()[0]
    maintenance = _rows(models["fact_maintenance_request"], "request_id")
    assert float(lease["rent"]) == pytest.approx(664825.0)
    assert float(payments["due"]) == pytest.approx(3988950.0)
    assert payments["unknown_property"] == 0 and payments["unknown_tenant"] == 0
    assert all(row["request_date_key"] is not None for row in maintenance.values())
    assert maintenance[5001]["resolution_days"] == 3
    assert maintenance[5001]["category"] == "HVAC"
    assert models["fact_property_budget"].filter(F.col("budget_date_key").isNull()).count() == 0
    for name, grain in gold_nb.GRAINS.items():
        gold_nb.assert_unique_grain(name, models[name], grain)
