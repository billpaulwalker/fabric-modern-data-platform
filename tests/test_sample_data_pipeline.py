"""Run the committed sample data through the real Silver, Gold, and semantic contracts."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.bronze_utils import ingest_csv_to_bronze
from src.gold_utils import (
    UNKNOWN_KEY,
    build_dim_date,
    build_dim_property,
    build_dim_tenant,
    build_fact_lease,
    build_fact_maintenance,
    build_fact_property_budget,
    build_fact_rent_payment,
    write_models,
)
from src.semantic_model_utils import validate_semantic_model
from src.silver_utils import load_silver_config, transform_to_silver


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def sample_run(tmp_path_factory):
    root = tmp_path_factory.mktemp("sample_run")
    silver, metrics = {}, []
    for config in load_silver_config(REPO_ROOT / "config/silver_table_config.json"):
        if config["input_format"] != "csv":
            continue
        bronze_path = root / f"bronze_{config['name']}.csv"
        ingest_csv_to_bronze(
            input_path=REPO_ROOT / f"data/sample/{config['name']}.csv",
            output_path=bronze_path,
            source_system="test",
            source_object=config["name"],
            pipeline_run_id="test_run",
            load_type="full",
        )
        result = transform_to_silver(pd.read_csv(bronze_path, dtype=object), config)
        # Gold reads Silver back from CSV, so round-trip to match the notebook.
        silver_path = root / f"silver_{config['name']}.csv"
        result.valid.to_csv(silver_path, index=False)
        silver[config["name"]] = pd.read_csv(silver_path)
        metrics.append(result.metrics)

    gold_config = json.loads((REPO_ROOT / "config/gold_model_config.json").read_text(encoding="utf-8"))
    dim_property = build_dim_property(silver["properties"], silver["property_region_mapping"])
    dim_tenant = build_dim_tenant(silver["tenants"])
    models = [
        dim_property,
        dim_tenant,
        build_dim_date(**gold_config["date_dimension"]),
        build_fact_lease(silver["leases"], dim_property.frame, dim_tenant.frame),
        build_fact_rent_payment(silver["rent_payments"], silver["leases"], dim_property.frame, dim_tenant.frame),
        build_fact_maintenance(silver["maintenance_requests"], dim_property.frame),
        build_fact_property_budget(silver["property_budget"], dim_property.frame),
    ]
    gold_directory = root / "gold"
    write_models(models, gold_directory)
    return {"silver_metrics": metrics, "gold": {model.name: model.frame for model in models}, "gold_dir": gold_directory}


def test_silver_config_columns_all_exist_in_sample_sources(sample_run):
    gaps = {m["table"]: m["missing_configured_columns"] for m in sample_run["silver_metrics"] if m["missing_configured_columns"]}
    assert gaps == {}


def test_sample_rows_are_not_rejected(sample_run):
    assert all(metric["rows_rejected"] == 0 for metric in sample_run["silver_metrics"])


def test_lease_rent_is_populated_from_area_and_rate(sample_run):
    lease = sample_run["gold"]["fact_lease"].set_index("lease_id")
    assert lease.loc[301, "monthly_rent"] == pytest.approx(25000 * 4.15)
    assert (lease["monthly_rent"] > 0).all()


def test_rent_payment_amounts_and_keys_are_populated(sample_run):
    payments = sample_run["gold"]["fact_rent_payment"]
    assert payments["amount_due"].sum() > 0
    assert payments["amount_paid"].sum() > 0
    assert (payments["property_key"] != UNKNOWN_KEY).all()
    assert (payments["tenant_key"] != UNKNOWN_KEY).all()


def test_maintenance_dates_and_category_are_populated(sample_run):
    maintenance = sample_run["gold"]["fact_maintenance_request"].set_index("request_id")
    assert maintenance["request_date_key"].notna().all()
    assert maintenance.loc[5001, "resolution_days"] == 3
    assert maintenance.loc[5001, "category"] == "HVAC"


def test_sample_gold_satisfies_semantic_contract(sample_run):
    config = json.loads((REPO_ROOT / "config/semantic_model_config.json").read_text(encoding="utf-8"))
    report = validate_semantic_model(config, sample_run["gold_dir"], REPO_ROOT / "powerbi/semantic-model/measures.dax")
    assert report.issues == []
