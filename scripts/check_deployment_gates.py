"""Require current operational, Gold, and semantic evidence before promotion."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.deployment_utils import evaluate_deployment_gates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", choices=["dev", "test", "prod"], required=True)
    parser.add_argument("--write-evidence", action="store_true")
    args = parser.parse_args()
    report = evaluate_deployment_gates(REPO_ROOT, args.environment)
    print(json.dumps(report.as_dict(), indent=2))
    if args.write_evidence:
        output = REPO_ROOT / f"dist/deployment-gate-{args.environment}.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report.as_dict(), indent=2), encoding="utf-8")
    if not report.passed:
        raise SystemExit("Deployment gates failed")


if __name__ == "__main__":
    main()
