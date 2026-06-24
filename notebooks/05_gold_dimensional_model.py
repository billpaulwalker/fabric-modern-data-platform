"""Build the local Power BI-ready Gold star schema from Silver CSV outputs."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.gold_utils import (
    build_dim_date,
    build_dim_property,
    build_dim_tenant,
    build_fact_lease,
    build_fact_maintenance,
    build_fact_property_budget,
    build_fact_rent_payment,
    write_models,
)


def read_silver(name: str) -> pd.DataFrame:
    path = REPO_ROOT / f"data/silver/silver_{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Run Phase 3 first; Silver input not found: {path}")
    return pd.read_csv(path)


def main() -> None:
    config = json.loads((REPO_ROOT / "config/gold_model_config.json").read_text(encoding="utf-8"))
    properties = read_silver("properties")
    tenants = read_silver("tenants")
    leases = read_silver("leases")
    payments = read_silver("rent_payments")
    maintenance = read_silver("maintenance_requests")
    budget = read_silver("property_budget")
    regions = read_silver("property_region_mapping")

    dim_property = build_dim_property(properties, regions)
    dim_tenant = build_dim_tenant(tenants)
    dim_date = build_dim_date(**config["date_dimension"])
    models = [
        dim_property,
        dim_tenant,
        dim_date,
        build_fact_lease(leases, dim_property.frame, dim_tenant.frame),
        build_fact_rent_payment(payments, leases, dim_property.frame, dim_tenant.frame),
        build_fact_maintenance(maintenance, dim_property.frame),
        build_fact_property_budget(budget, dim_property.frame),
    ]
    metrics = write_models(models, REPO_ROOT / "data/gold")
    metrics_path = REPO_ROOT / "data/gold/gold_model_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("Gold dimensional model complete")
    for metric in metrics:
        print(metric)


if __name__ == "__main__":
    main()
