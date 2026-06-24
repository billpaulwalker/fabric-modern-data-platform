"""Print recent pipeline health and step reliability from local audit logs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DIRECTORY = REPO_ROOT / "data/operations"


def read_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return pd.DataFrame(records)


def main() -> None:
    runs = read_jsonl(AUDIT_DIRECTORY / "pipeline_runs.jsonl")
    steps = read_jsonl(AUDIT_DIRECTORY / "pipeline_step_runs.jsonl")
    if runs.empty:
        print("No pipeline audit records found. Run notebooks/07_run_end_to_end_pipeline.py first.")
        return

    recent_columns = [
        "pipeline_run_id", "status", "started_at", "duration_seconds",
        "steps_succeeded", "steps_failed", "steps_skipped",
    ]
    print("\nRecent pipeline runs")
    print(runs[recent_columns].tail(10).to_string(index=False))

    if not steps.empty:
        attempts = steps[steps["status"].isin(["SUCCEEDED", "FAILED"])].copy()
        health = attempts.groupby("step_name").agg(
            attempts=("status", "size"),
            failures=("status", lambda values: int((values == "FAILED").sum())),
            average_duration_seconds=("duration_seconds", "mean"),
        )
        health["success_rate"] = (health["attempts"] - health["failures"]) / health["attempts"]
        print("\nStep health")
        print(health.round({"average_duration_seconds": 3, "success_rate": 3}).to_string())


if __name__ == "__main__":
    main()
