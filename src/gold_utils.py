"""Power BI-ready Gold star-schema builders for local execution and CI."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


UNKNOWN_KEY = 0


@dataclass
class GoldModel:
    name: str
    frame: pd.DataFrame
    grain_columns: list[str]


def deterministic_key(namespace: str, *values: Any) -> int:
    """Return a stable positive 63-bit key; zero remains reserved for unknown."""
    payload = "|".join([namespace, *("<NULL>" if pd.isna(value) else str(value) for value in values)])
    value = int(hashlib.sha256(payload.encode("utf-8")).hexdigest()[:15], 16)
    return value or 1


def add_surrogate_key(
    frame: pd.DataFrame,
    target_column: str,
    natural_key_columns: list[str],
    namespace: str,
) -> pd.DataFrame:
    missing = sorted(set(natural_key_columns) - set(frame.columns))
    if missing:
        raise ValueError(f"Cannot build {target_column}; missing natural keys: {missing}")
    result = frame.copy()
    result[target_column] = result[natural_key_columns].apply(
        lambda row: deterministic_key(namespace, *row.tolist()), axis=1
    ).astype("int64")
    return result


def date_key(series: pd.Series) -> pd.Series:
    converted = pd.to_datetime(series, errors="coerce", utc=True)
    return converted.dt.strftime("%Y%m%d").astype("Int64")


def numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype("Float64")


def datetime_series(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return a timezone-aware datetime Series, including for absent optional columns."""
    if column not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    return pd.to_datetime(frame[column], errors="coerce", utc=True)


def select_existing(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return frame[[column for column in columns if column in frame.columns]].copy()


def build_dim_date(start_date: str, end_date: str) -> GoldModel:
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    frame = pd.DataFrame({"full_date": dates})
    frame["date_key"] = frame["full_date"].dt.strftime("%Y%m%d").astype("int64")
    frame["calendar_year"] = frame["full_date"].dt.year
    frame["calendar_quarter"] = frame["full_date"].dt.quarter
    frame["calendar_month"] = frame["full_date"].dt.month
    frame["month_name"] = frame["full_date"].dt.month_name()
    frame["year_month"] = frame["full_date"].dt.strftime("%Y-%m")
    frame["day_of_month"] = frame["full_date"].dt.day
    frame["day_name"] = frame["full_date"].dt.day_name()
    frame["iso_week"] = frame["full_date"].dt.isocalendar().week.astype("int64")
    frame["is_weekend"] = frame["full_date"].dt.dayofweek >= 5
    return GoldModel("dim_date", frame, ["date_key"])


def build_dim_property(properties: pd.DataFrame, regions: pd.DataFrame | None = None) -> GoldModel:
    columns = [
        "property_id", "property_name", "property_type", "city", "state", "postal_code",
        "square_feet", "property_status", "acquired_date", "updated_at",
    ]
    frame = select_existing(properties, columns)
    if "property_id" not in frame.columns:
        raise ValueError("dim_property requires property_id")
    if regions is not None and "property_id" in regions.columns:
        region_columns = [column for column in ["property_id", "region", "market"] if column in regions.columns]
        frame = frame.merge(regions[region_columns].drop_duplicates("property_id"), on="property_id", how="left")
    frame = frame.drop_duplicates("property_id", keep="last")
    frame = add_surrogate_key(frame, "property_key", ["property_id"], "property")
    frame["is_current"] = True
    unknown = {column: pd.NA for column in frame.columns}
    unknown.update({"property_key": UNKNOWN_KEY, "property_id": -1, "property_name": "Unknown", "is_current": True})
    frame = pd.concat([pd.DataFrame([unknown]), frame], ignore_index=True)
    ordered = ["property_key", *[column for column in frame.columns if column != "property_key"]]
    return GoldModel("dim_property", frame[ordered], ["property_id"])


def build_dim_tenant(tenants: pd.DataFrame) -> GoldModel:
    columns = ["tenant_id", "tenant_name", "industry", "tenant_status", "tenant_start_date", "updated_at"]
    frame = select_existing(tenants, columns)
    if "tenant_id" not in frame.columns:
        raise ValueError("dim_tenant requires tenant_id")
    frame = frame.drop_duplicates("tenant_id", keep="last")
    frame = add_surrogate_key(frame, "tenant_key", ["tenant_id"], "tenant")
    frame["is_current"] = True
    unknown = {column: pd.NA for column in frame.columns}
    unknown.update({"tenant_key": UNKNOWN_KEY, "tenant_id": -1, "tenant_name": "Unknown", "is_current": True})
    frame = pd.concat([pd.DataFrame([unknown]), frame], ignore_index=True)
    ordered = ["tenant_key", *[column for column in frame.columns if column != "tenant_key"]]
    return GoldModel("dim_tenant", frame[ordered], ["tenant_id"])


def _lookup_key(
    fact: pd.DataFrame,
    dimension: pd.DataFrame,
    natural_key: str,
    surrogate_key: str,
) -> pd.DataFrame:
    if natural_key not in fact.columns:
        fact[surrogate_key] = UNKNOWN_KEY
        return fact
    lookup = dimension[[natural_key, surrogate_key]].drop_duplicates(natural_key)
    result = fact.merge(lookup, on=natural_key, how="left")
    result[surrogate_key] = result[surrogate_key].fillna(UNKNOWN_KEY).astype("int64")
    return result


def build_fact_lease(leases: pd.DataFrame, dim_property: pd.DataFrame, dim_tenant: pd.DataFrame) -> GoldModel:
    frame = leases.copy()
    if "lease_id" not in frame.columns:
        raise ValueError("fact_lease requires lease_id")
    frame = _lookup_key(frame, dim_property, "property_id", "property_key")
    frame = _lookup_key(frame, dim_tenant, "tenant_id", "tenant_key")
    frame["lease_start_date_key"] = date_key(frame.get("lease_start_date", pd.Series(index=frame.index, dtype=object)))
    frame["lease_end_date_key"] = date_key(frame.get("lease_end_date", pd.Series(index=frame.index, dtype=object)))
    frame["monthly_rent"] = numeric(frame, "monthly_rent")
    frame["annualized_rent"] = frame["monthly_rent"] * 12
    frame = add_surrogate_key(frame, "lease_key", ["lease_id"], "lease")
    columns = [
        "lease_key", "lease_id", "property_key", "tenant_key", "lease_start_date_key",
        "lease_end_date_key", "monthly_rent", "annualized_rent", "lease_status",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    return GoldModel("fact_lease", select_existing(frame, columns), ["lease_id"])


def build_fact_rent_payment(
    payments: pd.DataFrame,
    leases: pd.DataFrame,
    dim_property: pd.DataFrame,
    dim_tenant: pd.DataFrame,
) -> GoldModel:
    frame = payments.copy()
    if "payment_id" not in frame.columns:
        raise ValueError("fact_rent_payment requires payment_id")
    lease_columns = [column for column in ["lease_id", "property_id", "tenant_id"] if column in leases.columns]
    if "lease_id" in frame.columns and "lease_id" in lease_columns:
        frame = frame.merge(leases[lease_columns].drop_duplicates("lease_id"), on="lease_id", how="left")
    frame = _lookup_key(frame, dim_property, "property_id", "property_key")
    frame = _lookup_key(frame, dim_tenant, "tenant_id", "tenant_key")
    frame["payment_date_key"] = date_key(frame.get("payment_date", pd.Series(index=frame.index, dtype=object)))
    frame["amount_due"] = numeric(frame, "amount_due")
    frame["amount_paid"] = numeric(frame, "amount_paid")
    frame["outstanding_amount"] = (frame["amount_due"] - frame["amount_paid"]).clip(lower=0)
    frame["collection_rate"] = (frame["amount_paid"] / frame["amount_due"].replace(0, pd.NA)).fillna(0)
    frame = add_surrogate_key(frame, "payment_key", ["payment_id"], "rent_payment")
    columns = [
        "payment_key", "payment_id", "lease_id", "property_key", "tenant_key", "payment_date_key",
        "amount_due", "amount_paid", "outstanding_amount", "collection_rate", "payment_status",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    return GoldModel("fact_rent_payment", select_existing(frame, columns), ["payment_id"])


def build_fact_maintenance(requests: pd.DataFrame, dim_property: pd.DataFrame) -> GoldModel:
    frame = requests.copy()
    if "request_id" not in frame.columns:
        raise ValueError("fact_maintenance_request requires request_id")
    frame = _lookup_key(frame, dim_property, "property_id", "property_key")
    frame["request_date_key"] = date_key(frame.get("request_date", pd.Series(index=frame.index, dtype=object)))
    frame["completed_date_key"] = date_key(frame.get("completed_date", pd.Series(index=frame.index, dtype=object)))
    frame["estimated_cost"] = numeric(frame, "estimated_cost")
    frame["actual_cost"] = numeric(frame, "actual_cost")
    frame["cost_variance"] = frame["actual_cost"] - frame["estimated_cost"]
    opened = datetime_series(frame, "request_date")
    closed = datetime_series(frame, "completed_date")
    frame["resolution_days"] = (closed - opened).dt.days.astype("Int64")
    frame = add_surrogate_key(frame, "maintenance_key", ["request_id"], "maintenance")
    columns = [
        "maintenance_key", "request_id", "property_key", "request_date_key", "completed_date_key",
        "category", "priority", "status", "estimated_cost", "actual_cost", "cost_variance",
        "resolution_days", "pipeline_run_id", "silver_processed_timestamp",
    ]
    return GoldModel("fact_maintenance_request", select_existing(frame, columns), ["request_id"])


def build_fact_property_budget(budget: pd.DataFrame, dim_property: pd.DataFrame) -> GoldModel:
    frame = budget.copy()
    if "property_id" not in frame.columns:
        raise ValueError("fact_property_budget requires property_id")

    aliases = {
        "budget_year": ["year", "fiscal_year"],
        "budget_month": ["month", "fiscal_month"],
        "budget_revenue": ["budgeted_revenue", "revenue_budget"],
        "budget_expense": ["budgeted_expense", "budgeted_expenses", "expense_budget"],
        "budget_amount": ["budgeted_amount"],
    }
    for canonical, candidates in aliases.items():
        if canonical not in frame.columns:
            source = next((column for column in candidates if column in frame.columns), None)
            if source:
                frame = frame.rename(columns={source: canonical})

    frame = _lookup_key(frame, dim_property, "property_id", "property_key")

    period_column = next(
        (column for column in ["budget_period", "budget_date", "period_start", "month_start"] if column in frame.columns),
        None,
    )
    period = (
        pd.to_datetime(frame[period_column], errors="coerce", utc=True)
        if period_column
        else pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    )

    if "budget_year" in frame.columns:
        year = pd.to_numeric(frame["budget_year"], errors="coerce").astype("Int64")
    else:
        year = period.dt.year.astype("Int64")

    if "budget_month" in frame.columns:
        raw_month = frame["budget_month"]
        numeric_month = pd.to_numeric(raw_month, errors="coerce")
        parsed_month = pd.to_datetime(raw_month, errors="coerce", utc=True)
        month = numeric_month.where(numeric_month.between(1, 12), parsed_month.dt.month).astype("Int64")
        year = year.fillna(parsed_month.dt.year.astype("Int64"))
    else:
        month = period.dt.month.astype("Int64")

    if year.isna().any():
        raise ValueError(
            "Unable to derive budget year. Expected budget_year, year, fiscal_year, "
            "budget_period, budget_date, period_start, or a date-formatted budget_month. "
            f"Available columns: {sorted(frame.columns.tolist())}"
        )
    month = month.fillna(1).astype("Int64")
    frame["budget_year"] = year
    frame["budget_month"] = month
    frame["budget_date_key"] = (year * 10000 + month * 100 + 1).astype("Int64")
    for column in ["budget_revenue", "budget_expense", "budget_amount"]:
        if column in frame.columns:
            frame[column] = numeric(frame, column)
    if "budget_revenue" in frame.columns and "budget_expense" in frame.columns:
        frame["budget_noi"] = frame["budget_revenue"] - frame["budget_expense"]
    source_grain = [column for column in ["property_id", "budget_year", "budget_month"] if column in frame.columns]
    frame = add_surrogate_key(frame, "budget_key", source_grain, "property_budget")
    columns = [
        "budget_key", "property_key", "budget_date_key", "budget_year", "budget_month",
        "budget_revenue", "budget_expense", "budget_noi", "budget_amount",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    output = select_existing(frame, columns)
    output_grain = [column for column in ["property_key", "budget_year", "budget_month"] if column in output.columns]
    return GoldModel("fact_property_budget", output, output_grain)


def validate_model(model: GoldModel) -> dict[str, Any]:
    duplicate_rows = int(model.frame.duplicated(model.grain_columns).sum())
    if duplicate_rows:
        raise ValueError(f"{model.name} violates its declared grain: {duplicate_rows} duplicate rows")
    return {"model": model.name, "rows_written": len(model.frame), "duplicate_grain_rows": duplicate_rows}


def write_models(models: list[GoldModel], output_directory: str | Path) -> list[dict[str, Any]]:
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)
    metrics = []
    for model in models:
        metrics.append(validate_model(model))
        model.frame.to_csv(output / f"{model.name}.csv", index=False)
    return metrics
