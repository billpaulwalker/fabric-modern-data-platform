"""Reusable, locally testable Silver transformation utilities."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SUPPORTED_TYPES = {"string", "integer", "decimal", "date", "datetime", "boolean"}


class SilverSchemaError(ValueError):
    """Raised when source structure cannot satisfy the configured data contract."""


@dataclass
class SilverResult:
    valid: pd.DataFrame
    rejected: pd.DataFrame
    metrics: dict[str, Any]


def normalize_column_name(name: str) -> str:
    """Convert source column names and flattened JSON paths to snake_case."""
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {column: normalize_column_name(column) for column in frame.columns}
    normalized_names = list(renamed.values())
    if len(normalized_names) != len(set(normalized_names)):
        raise SilverSchemaError("Column normalization produced duplicate column names")
    return frame.rename(columns=renamed).copy()


def load_silver_config(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as file:
        document = json.load(file)
    tables = document.get("tables")
    if not isinstance(tables, list) or not tables:
        raise SilverSchemaError("Silver configuration must contain a non-empty tables list")
    return tables


def read_bronze(path: str | Path, input_format: str) -> pd.DataFrame:
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"Bronze input not found: {source}")
    if input_format == "csv":
        return pd.read_csv(source, dtype=object)
    if input_format == "jsonl":
        records = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
        return pd.json_normalize(records)
    raise ValueError(f"Unsupported Bronze input format: {input_format}")


def _present(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in frame.columns]


def _append_reason(reasons: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    current = reasons.fillna("").astype(str)
    separator = current.where(current.eq(""), " | ")
    return current.where(~mask, current + separator + reason)


def _cast_column(series: pd.Series, target_type: str) -> pd.Series:
    if target_type not in SUPPORTED_TYPES:
        raise SilverSchemaError(f"Unsupported Silver data type: {target_type}")
    if target_type == "string":
        return series.astype("string").str.strip()
    if target_type == "integer":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if target_type == "decimal":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    if target_type in {"date", "datetime"}:
        converted = pd.to_datetime(series, errors="coerce", utc=True)
        return converted.dt.date if target_type == "date" else converted
    if target_type == "boolean":
        mapping = {
            "true": True, "1": True, "yes": True, "y": True,
            "false": False, "0": False, "no": False, "n": False,
        }
        return series.astype("string").str.strip().str.lower().map(mapping).astype("boolean")
    raise AssertionError("Unreachable type branch")


def transform_to_silver(frame: pd.DataFrame, config: dict[str, Any]) -> SilverResult:
    """Standardize, validate, quarantine, and deduplicate one Bronze dataset."""
    silver = normalize_columns(frame)
    silver = silver.replace(r"^\s*$", pd.NA, regex=True)
    silver["_source_row_number"] = range(1, len(silver) + 1)
    reasons = pd.Series("", index=silver.index, dtype="string")

    required = [normalize_column_name(value) for value in config.get("required_columns", [])]
    missing_required = [column for column in required if column not in silver.columns]
    if missing_required:
        raise SilverSchemaError(f"Missing required source columns: {missing_required}")

    for column, target_type in config.get("column_types", {}).items():
        column = normalize_column_name(column)
        if column not in silver.columns:
            continue
        original = silver[column].copy()
        converted = _cast_column(original, target_type)
        invalid = original.notna() & converted.isna()
        reasons = _append_reason(reasons, invalid, f"invalid_{target_type}:{column}")
        silver[column] = converted

    for column in required:
        reasons = _append_reason(reasons, silver[column].isna(), f"required:{column}")

    for column, allowed in config.get("allowed_values", {}).items():
        column = normalize_column_name(column)
        if column in silver.columns:
            invalid = silver[column].notna() & ~silver[column].isin(allowed)
            reasons = _append_reason(reasons, invalid, f"invalid_value:{column}")

    for column in _present(silver, config.get("non_negative_columns", [])):
        numeric = pd.to_numeric(silver[column], errors="coerce")
        reasons = _append_reason(reasons, numeric.notna() & numeric.lt(0), f"negative_value:{column}")

    for start, end in config.get("date_order_rules", []):
        start, end = normalize_column_name(start), normalize_column_name(end)
        if start in silver.columns and end in silver.columns:
            invalid = silver[start].notna() & silver[end].notna() & (silver[end] < silver[start])
            reasons = _append_reason(reasons, invalid, f"date_order:{start}>{end}")

    rejected_mask = reasons.ne("")
    rejected = silver.loc[rejected_mask].copy()
    rejected["data_quality_status"] = "REJECTED"
    rejected["rejection_reason"] = reasons.loc[rejected_mask]

    valid = silver.loc[~rejected_mask].copy()
    primary_key = [normalize_column_name(value) for value in config.get("primary_key", [])]
    available_key = _present(valid, primary_key)
    duplicate_rows_removed = 0
    if available_key:
        sort_columns = _present(valid, ["ingestion_timestamp", "updated_at", "_source_row_number"])
        if sort_columns:
            valid = valid.sort_values(sort_columns, kind="stable")
        before = len(valid)
        valid = valid.drop_duplicates(subset=available_key, keep="last")
        duplicate_rows_removed = before - len(valid)

    processed_at = datetime.now(timezone.utc).isoformat()
    valid["data_quality_status"] = "VALID"
    valid["silver_processed_timestamp"] = processed_at
    rejected["silver_processed_timestamp"] = processed_at
    valid = valid.drop(columns=["_source_row_number"], errors="ignore").reset_index(drop=True)
    rejected = rejected.drop(columns=["_source_row_number"], errors="ignore").reset_index(drop=True)

    metrics = {
        "table": config.get("name", "unknown"),
        "rows_read": len(frame),
        "rows_valid": len(valid),
        "rows_rejected": len(rejected),
        "duplicate_rows_removed": duplicate_rows_removed,
    }
    return SilverResult(valid=valid, rejected=rejected, metrics=metrics)


def write_silver_result(result: SilverResult, valid_path: str | Path, rejected_path: str | Path) -> None:
    valid_target, rejected_target = Path(valid_path), Path(rejected_path)
    valid_target.parent.mkdir(parents=True, exist_ok=True)
    rejected_target.parent.mkdir(parents=True, exist_ok=True)
    result.valid.to_csv(valid_target, index=False)
    result.rejected.to_csv(rejected_target, index=False)
