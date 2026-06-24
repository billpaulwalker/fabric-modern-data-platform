"""Bronze ingestion utilities with orchestrated run-ID propagation."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


AUDIT_COLUMNS = [
    "source_system",
    "source_object",
    "source_file_name",
    "ingestion_timestamp",
    "pipeline_run_id",
    "load_type",
    "raw_record_hash",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_pipeline_run_id(prefix: str = "bronze") -> str:
    """Use the orchestrator run ID when present, otherwise create a standalone ID."""
    orchestrated_run_id = os.getenv("PIPELINE_RUN_ID")
    if orchestrated_run_id:
        return orchestrated_run_id
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{timestamp}"


def normalize_record_for_hash(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, default=str, separators=(",", ":"))


def calculate_raw_record_hash(record: dict[str, Any]) -> str:
    normalized = normalize_record_for_hash(record)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def add_bronze_audit_columns(
    records: Iterable[dict[str, Any]],
    *,
    source_system: str,
    source_object: str,
    source_file_name: str | None,
    pipeline_run_id: str,
    load_type: str,
    ingestion_timestamp: str | None = None,
) -> list[dict[str, Any]]:
    timestamp = ingestion_timestamp or utc_now_iso()
    bronze_records = []
    for record in records:
        raw_record = dict(record)
        bronze_records.append({
            **raw_record,
            "source_system": source_system,
            "source_object": source_object,
            "source_file_name": source_file_name or "",
            "ingestion_timestamp": timestamp,
            "pipeline_run_id": pipeline_run_id,
            "load_type": load_type,
            "raw_record_hash": calculate_raw_record_hash(raw_record),
        })
    return bronze_records


def read_csv_records(file_path: str | Path) -> list[dict[str, str]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def write_csv_records(records: list[dict[str, Any]], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = []
    for record in records:
        for key in record:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def write_jsonl_records(records: list[dict[str, Any]], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, default=str) + "\n")


def ingest_csv_to_bronze(
    *, input_path: str | Path, output_path: str | Path, source_system: str,
    source_object: str, pipeline_run_id: str, load_type: str,
) -> dict[str, Any]:
    source_records = read_csv_records(input_path)
    bronze_records = add_bronze_audit_columns(
        source_records,
        source_system=source_system,
        source_object=source_object,
        source_file_name=Path(input_path).name,
        pipeline_run_id=pipeline_run_id,
        load_type=load_type,
    )
    write_csv_records(bronze_records, output_path)
    return {
        "source_system": source_system,
        "source_object": source_object,
        "target_path": str(output_path),
        "rows_read": len(source_records),
        "rows_written": len(bronze_records),
        "pipeline_run_id": pipeline_run_id,
    }


def ingest_api_payload_to_bronze(
    *, payload: dict[str, Any], output_path: str | Path, source_system: str,
    source_object: str, pipeline_run_id: str, load_type: str,
) -> dict[str, Any]:
    bronze_records = add_bronze_audit_columns(
        [payload],
        source_system=source_system,
        source_object=source_object,
        source_file_name="api_response",
        pipeline_run_id=pipeline_run_id,
        load_type=load_type,
    )
    write_jsonl_records(bronze_records, output_path)
    return {
        "source_system": source_system,
        "source_object": source_object,
        "target_path": str(output_path),
        "rows_read": 1,
        "rows_written": len(bronze_records),
        "pipeline_run_id": pipeline_run_id,
    }
