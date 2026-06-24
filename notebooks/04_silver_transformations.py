"""Run all local Bronze-to-Silver transformations from the repository root."""

from __future__ import annotations

from pathlib import Path
import json
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.silver_utils import load_silver_config, read_bronze, transform_to_silver, write_silver_result


def main() -> None:
    configs = load_silver_config(REPO_ROOT / "config/silver_table_config.json")
    run_metrics = []

    for config in configs:
        name = config["name"]
        extension = "jsonl" if config["input_format"] == "jsonl" else "csv"
        input_path = REPO_ROOT / f"data/bronze/bronze_{name}.{extension}"
        result = transform_to_silver(read_bronze(input_path, config["input_format"]), config)
        write_silver_result(
            result,
            REPO_ROOT / f"data/silver/silver_{name}.csv",
            REPO_ROOT / f"data/rejected/silver_{name}_rejected.csv",
        )
        run_metrics.append(result.metrics)

    metrics_path = REPO_ROOT / "data/silver/silver_run_metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(run_metrics, indent=2), encoding="utf-8")

    print("Silver transformation complete")
    for metric in run_metrics:
        print(metric)


if __name__ == "__main__":
    main()
