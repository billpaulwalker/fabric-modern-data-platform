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


bronze_nb = _load_notebook("nb_cre_bronze_ingest.py", "nb_cre_bronze_ingest")
silver_nb = _load_notebook("nb_cre_silver_transform.py", "nb_cre_silver_transform")
gold_nb = _load_notebook("nb_cre_gold_build_model.py", "nb_cre_gold_build_model")
semantic_nb = _load_notebook("nb_cre_gold_validate_model.py", "nb_cre_gold_validate_model")


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


# Bronze -----------------------------------------------------------------------

BRONZE_CONFIG_PATH = REPO_ROOT / "config/bronze_source_config.json"
# The repository's data/ folder mirrors the Lakehouse Files/landing/ folder.
LANDING_ROOT = str(REPO_ROOT / "data")


def _source(name):
    sources = bronze_nb.load_config(BRONZE_CONFIG_PATH)["sources"]
    return next(source for source in sources if source["name"] == name)


def test_bronze_sources_match_silver_tables():
    bronze_names = {source["name"] for source in bronze_nb.load_config(BRONZE_CONFIG_PATH)["sources"]}
    silver_names = {table["name"] for table in silver_nb.load_config(REPO_ROOT / "config/silver_table_config.json")["tables"]}
    assert bronze_names == silver_names


def test_bronze_csv_lands_raw_strings_with_audit_columns(spark):
    frame = bronze_nb.ingest_source(spark, _source("leases"), LANDING_ROOT, "run-1", "2026-06-20T00:00:00Z")
    rows = _rows(frame, "lease_id")
    assert len(rows) == 6
    assert rows["301"]["monthly_rate_per_sqft"] == "4.15"
    assert {column: rows["301"][column] for column in ["source_system", "source_object", "source_file_name",
                                                      "pipeline_run_id", "load_type"]} == {
        "source_system": "CRE_SQL", "source_object": "leases", "source_file_name": "leases.csv",
        "pipeline_run_id": "run-1", "load_type": "full",
    }
    # Format in Spark: collect() converts timestamps to the local machine's time zone.
    landed = frame.select(F.date_format("ingestion_timestamp", "yyyy-MM-dd HH:mm").alias("at")).distinct().collect()
    assert [row["at"] for row in landed] == ["2026-06-20 00:00"]
    assert len(rows["301"]["raw_record_hash"]) == 64
    assert len({row["raw_record_hash"] for row in rows.values()}) == 6


def test_bronze_hash_ignores_audit_columns_and_is_stable(spark):
    first = bronze_nb.ingest_source(spark, _source("tenants"), LANDING_ROOT, "run-1", "2026-06-20T00:00:00Z")
    second = bronze_nb.ingest_source(spark, _source("tenants"), LANDING_ROOT, "run-2", "2026-06-21T00:00:00Z")
    assert _rows(first, "tenant_id")["201"]["raw_record_hash"] == _rows(second, "tenant_id")["201"]["raw_record_hash"]


def test_bronze_api_payload_stays_nested_for_silver_to_flatten(spark):
    frame = bronze_nb.ingest_source(spark, _source("weather_api_raw"), LANDING_ROOT, "run-1", "2026-06-20T00:00:00Z")
    assert frame.count() == 1
    assert "main" in frame.columns and "main_temp" not in frame.columns
    silver_config = next(
        table for table in silver_nb.load_config(REPO_ROOT / "config/silver_table_config.json")["tables"]
        if table["name"] == "weather_api_raw"
    )
    valid, _, metrics = silver_nb.transform_table(frame, silver_config)
    assert float(valid.collect()[0]["main_temp"]) == 72.4
    assert metrics["missing_configured_columns"] == []


def test_bronze_missing_landing_file_explains_the_upload(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="Files/landing"):
        bronze_nb.ingest_source(spark, _source("leases"), str(tmp_path), "run-1", "2026-06-20T00:00:00Z")


def test_bronze_rejects_source_columns_that_collide_with_audit_columns(spark, tmp_path):
    (tmp_path / "sample").mkdir()
    (tmp_path / "sample/leases.csv").write_text("lease_id,load_type\n1,manual\n", encoding="utf-8")
    with pytest.raises(ValueError, match="load_type"):
        bronze_nb.ingest_source(spark, _source("leases"), str(tmp_path), "run-1", "2026-06-20T00:00:00Z")


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


def test_silver_metrics_frame_builds_when_no_columns_are_missing(spark, fabric_run):
    # A healthy run reports [] for every table, from which Spark cannot infer a type.
    # Only the schema is checked: building the frame is where inference failed in Fabric.
    for metrics in ([{"table": "leases", "rows_read": 6, "rows_valid": 6, "rows_rejected": 0,
                      "duplicate_rows_removed": 0, "missing_configured_columns": []}],
                    fabric_run["silver_metrics"]):
        frame = silver_nb.metrics_frame(spark, metrics)
        assert frame.schema.simpleString() == (
            "struct<table:string,rows_read:bigint,rows_valid:bigint,rows_rejected:bigint,"
            "duplicate_rows_removed:bigint,missing_configured_columns:array<string>>"
        )


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
def fabric_run(spark):
    """Run every notebook's main() in order against an in-memory catalog.

    Each notebook reads only what the previous one wrote, by the qualified names it
    resolves from fabric_layout.json, so a naming mismatch between layers fails here.
    """
    catalog = FakeCatalogSpark(spark)
    notebooks = [bronze_nb, silver_nb, gold_nb]  # semantic validation only reads
    originals = [notebook.write_table for notebook in notebooks]
    for notebook in notebooks:
        notebook.write_table = catalog.write_table
    try:
        config_directory = str(REPO_ROOT / "config")
        bronze_nb.main(catalog, pipeline_run_id="test_run", config_directory=config_directory, landing_root=LANDING_ROOT)
        silver_metrics = silver_nb.main(catalog, config_directory=config_directory)
        gold_nb.main(catalog, config_directory=config_directory)
        semantic_report = semantic_nb.main(catalog, config_directory=config_directory)
    finally:
        for notebook, original in zip(notebooks, originals):
            notebook.write_table = original
    return {
        "tables": catalog.tables,
        "schemas": catalog.created_schemas,
        "silver_metrics": silver_metrics,
        "semantic_report": semantic_report,
    }


class FakeCatalogSpark:
    """Stands in for Fabric's lakehouse catalog; everything else goes to the real session."""

    def __init__(self, spark):
        self._spark = spark
        self.tables = {}
        self.created_schemas = set()
        self.catalog = self

    def write_table(self, frame, table_name):
        lakehouse_schema = table_name.rsplit(".", 1)[0]
        assert lakehouse_schema in self.created_schemas, f"{table_name} written before its schema was created"
        self.tables[table_name] = frame.cache()

    def table(self, table_name):
        if table_name not in self.tables:
            raise AssertionError(f"Read of {table_name}, which no earlier notebook wrote")
        return self.tables[table_name]

    def tableExists(self, table_name):  # noqa: N802 - mirrors spark.catalog.tableExists
        return table_name in self.tables

    def sql(self, statement):
        prefix = "CREATE SCHEMA IF NOT EXISTS "
        if statement.startswith(prefix):
            self.created_schemas.add(statement[len(prefix):].strip())
            return None
        return self._spark.sql(statement)

    def __getattr__(self, name):
        return getattr(self._spark, name)


LAYOUT_PATH = REPO_ROOT / "config/fabric_layout.json"
EXPECTED_TABLES = {
    "lh_cre_bronze.cre_sql.properties", "lh_cre_bronze.cre_sql.tenants", "lh_cre_bronze.cre_sql.leases",
    "lh_cre_bronze.cre_sql.rent_payments", "lh_cre_bronze.cre_sql.maintenance_requests",
    "lh_cre_bronze.business_files.property_budget", "lh_cre_bronze.business_files.property_region_mapping",
    "lh_cre_bronze.openweather.weather_raw",
    "lh_cre_silver.property.properties", "lh_cre_silver.property.property_region_mapping",
    "lh_cre_silver.leasing.tenants", "lh_cre_silver.leasing.leases", "lh_cre_silver.leasing.rent_payments",
    "lh_cre_silver.finance.property_budget", "lh_cre_silver.operations.maintenance_requests",
    "lh_cre_silver.environment.weather_observations", "lh_cre_silver.quarantine.rejected_records",
    "lh_cre_gold.shared.dim_property", "lh_cre_gold.shared.dim_tenant", "lh_cre_gold.shared.dim_date",
    "lh_cre_gold.leasing.fact_lease", "lh_cre_gold.leasing.fact_rent_payment",
    "lh_cre_gold.finance.fact_property_budget", "lh_cre_gold.operations.fact_maintenance_request",
}


def test_notebooks_write_the_agreed_lakehouse_layout(fabric_run):
    assert set(fabric_run["tables"]) == EXPECTED_TABLES


def test_each_notebook_writes_only_to_its_own_lakehouse(fabric_run):
    for table_name in fabric_run["tables"]:
        lakehouse = table_name.split(".")[0]
        assert lakehouse in {"lh_cre_bronze", "lh_cre_silver", "lh_cre_gold"}
    assert fabric_run["semantic_report"]["passed"]


@pytest.mark.parametrize("notebook", [bronze_nb, silver_nb, gold_nb, semantic_nb], ids=lambda n: n.__name__)
def test_every_notebook_resolves_names_identically(notebook):
    layout = bronze_nb.load_config(LAYOUT_PATH)
    assert notebook.resolve_table(layout, "silver", "rent_payments") == "lh_cre_silver.leasing.rent_payments"
    assert notebook.resolve_table(layout, "bronze", "weather_api_raw") == "lh_cre_bronze.openweather.weather_raw"
    assert notebook.resolve_table(layout, "gold", "dim_tenant") == "lh_cre_gold.shared.dim_tenant"
    with pytest.raises(ValueError, match="fabric_layout.json"):
        notebook.resolve_table(layout, "gold", "fact_unknown")


def test_layout_covers_every_configured_table():
    layout = bronze_nb.load_config(LAYOUT_PATH)["tables"]
    bronze = {source["name"] for source in bronze_nb.load_config(BRONZE_CONFIG_PATH)["sources"]}
    silver = {table["name"] for table in silver_nb.load_config(REPO_ROOT / "config/silver_table_config.json")["tables"]}
    semantic = set(semantic_nb.load_config(REPO_ROOT / "config/semantic_model_config.json")["tables"])
    assert set(layout["bronze"]) == bronze
    assert set(layout["silver"]) == silver | {"rejected_records"}
    assert set(layout["gold"]) == set(gold_nb.GRAINS) == semantic


@pytest.fixture(scope="module")
def sample_gold(fabric_run):
    layout = gold_nb.load_config(LAYOUT_PATH)
    models = {name: fabric_run["tables"][gold_nb.resolve_table(layout, "gold", name)] for name in gold_nb.GRAINS}
    return fabric_run["silver_metrics"], models


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


# Semantic validation ------------------------------------------------------------

SEMANTIC_CONFIG = {
    "model_name": "Test Model",
    "unknown_member_key": 0,
    "max_unknown_member_ratio": 0.1,
    "tables": {
        "dim_property": {"key": "property_key", "required_columns": ["property_key"]},
        "fact_payment": {
            "key": "payment_key",
            "required_columns": ["payment_key", "property_key", "amount"],
            "non_empty_columns": ["amount"],
        },
    },
    "relationships": [{
        "name": "Property to Payment",
        "from_table": "dim_property", "from_column": "property_key",
        "to_table": "fact_payment", "to_column": "property_key", "active": True,
    }],
}


def _validate(csv_frame, payments, properties=({"property_key": "0"}, {"property_key": "101"})):
    tables = {"dim_property": csv_frame(list(properties)), "fact_payment": csv_frame(payments)}
    return semantic_nb.validate_gold_tables(tables, SEMANTIC_CONFIG)


def test_semantic_validation_passes_clean_star(csv_frame):
    report = _validate(csv_frame, [{"payment_key": "1", "property_key": "101", "amount": "50"}])
    assert report["passed"], report["issues"]
    assert report["table_rows"] == {"dim_property": 2, "fact_payment": 1}


def test_semantic_validation_flags_unknown_member_share(csv_frame):
    report = _validate(csv_frame, [
        {"payment_key": "1", "property_key": "0", "amount": "50"},
        {"payment_key": "2", "property_key": "101", "amount": "50"},
    ])
    assert any("Unknown member" in issue and "50.0%" in issue for issue in report["issues"])


def test_semantic_validation_flags_null_foreign_keys_and_orphans(csv_frame):
    report = _validate(csv_frame, [
        {"payment_key": "1", "property_key": "", "amount": "50"},
        {"payment_key": "2", "property_key": "999", "amount": "50"},
    ])
    assert any("1 null foreign keys" in issue for issue in report["issues"])
    assert any("orphan keys" in issue and "999" in issue for issue in report["issues"])


def test_semantic_validation_flags_all_zero_measures_and_key_problems(csv_frame):
    report = _validate(csv_frame, [
        {"payment_key": "1", "property_key": "101", "amount": "0"},
        {"payment_key": "1", "property_key": "101", "amount": ""},
    ])
    assert any("fact_payment.amount has no non-zero values" in issue for issue in report["issues"])
    assert any("fact_payment.payment_key is not unique" in issue for issue in report["issues"])


def test_semantic_validation_flags_missing_tables_and_columns(csv_frame):
    tables = {"dim_property": csv_frame([{"property_key": "0"}])}
    report = semantic_nb.validate_gold_tables(tables, SEMANTIC_CONFIG)
    assert any("Missing Gold table: fact_payment" in issue for issue in report["issues"])


def test_sample_gold_passes_real_semantic_contract(sample_gold):
    _, models = sample_gold
    config = semantic_nb.load_config(REPO_ROOT / "config/semantic_model_config.json")
    report = semantic_nb.validate_gold_tables(models, config)
    assert report["issues"] == []
