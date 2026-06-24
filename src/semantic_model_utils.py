"""Validate the Power BI semantic-model contract against local Gold outputs."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class SemanticValidationReport:
    model_name: str
    checks_run: int = 0
    issues: list[str] = field(default_factory=list)
    table_rows: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.issues

    def check(self, condition: bool, message: str) -> None:
        self.checks_run += 1
        if not condition:
            self.issues.append(message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "passed": self.passed,
            "checks_run": self.checks_run,
            "issues": self.issues,
            "table_rows": self.table_rows,
        }


def load_semantic_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def parse_measure_names(path: str | Path) -> set[str]:
    text = Path(path).read_text(encoding="utf-8")
    return set(re.findall(r"MEASURE\s+'[^']+'\[([^]]+)]\s*=", text, flags=re.IGNORECASE))


def validate_semantic_model(
    config: dict[str, Any],
    gold_directory: str | Path,
    measures_path: str | Path,
) -> SemanticValidationReport:
    report = SemanticValidationReport(config["model_name"])
    gold_path = Path(gold_directory)
    frames: dict[str, pd.DataFrame] = {}

    for table_name, table_config in config["tables"].items():
        path = gold_path / f"{table_name}.csv"
        report.check(path.exists(), f"Missing Gold model file: {path.name}")
        if not path.exists():
            continue
        frame = pd.read_csv(path)
        frames[table_name] = frame
        report.table_rows[table_name] = len(frame)
        required = set(table_config["required_columns"])
        missing = sorted(required - set(frame.columns))
        report.check(not missing, f"{table_name} missing required columns: {missing}")
        key = table_config["key"]
        if key in frame.columns:
            report.check(frame[key].notna().all(), f"{table_name}.{key} contains null values")
            report.check(frame[key].is_unique, f"{table_name}.{key} is not unique")

    for relationship in config["relationships"]:
        one_table = relationship["from_table"]
        many_table = relationship["to_table"]
        if one_table not in frames or many_table not in frames:
            continue
        one_column = relationship["from_column"]
        many_column = relationship["to_column"]
        if one_column not in frames[one_table].columns or many_column not in frames[many_table].columns:
            report.check(False, f"Relationship {relationship['name']} references a missing column")
            continue
        one_values = set(frames[one_table][one_column].dropna().tolist())
        many_values = set(frames[many_table][many_column].dropna().tolist())
        orphans = sorted(many_values - one_values)
        report.check(not orphans, f"Relationship {relationship['name']} has orphan keys: {orphans[:10]}")

    measures_file = Path(measures_path)
    report.check(measures_file.exists(), f"Missing DAX measures file: {measures_file.name}")
    if measures_file.exists():
        actual_measures = parse_measure_names(measures_file)
        missing_measures = sorted(set(config["required_measures"]) - actual_measures)
        report.check(not missing_measures, f"Missing required DAX measures: {missing_measures}")

    return report
