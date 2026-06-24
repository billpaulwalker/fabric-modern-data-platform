"""Execute the complete local data platform through one audited pipeline run."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.orchestration_utils import load_pipeline_config, run_pipeline


def main() -> None:
    config = load_pipeline_config(REPO_ROOT / "config/pipeline_config.json")
    result = run_pipeline(config, REPO_ROOT)
    summary = asdict(result)
    summary.pop("step_results")
    print(json.dumps(summary, indent=2))
    for step in result.step_results:
        print(f"{step.status:9} {step.step_name} ({step.duration_seconds:.3f}s, attempt {step.attempt})")
        if step.error_message:
            print(step.error_message)
    if result.status != "SUCCEEDED":
        raise SystemExit("End-to-end pipeline failed")


if __name__ == "__main__":
    main()
