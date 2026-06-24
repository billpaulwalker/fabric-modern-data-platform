"""Validate Gold outputs against the version-controlled Power BI model contract."""

from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.semantic_model_utils import load_semantic_config, validate_semantic_model


def main() -> None:
    config = load_semantic_config(REPO_ROOT / "config/semantic_model_config.json")
    report = validate_semantic_model(
        config,
        REPO_ROOT / "data/gold",
        REPO_ROOT / "powerbi/semantic-model/measures.dax",
    )
    output = REPO_ROOT / "data/gold/semantic_model_validation.json"
    output.write_text(json.dumps(report.as_dict(), indent=2), encoding="utf-8")
    print(json.dumps(report.as_dict(), indent=2))
    if not report.passed:
        raise SystemExit("Semantic model validation failed")


if __name__ == "__main__":
    main()
