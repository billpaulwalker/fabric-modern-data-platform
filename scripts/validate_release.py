"""Validate repository structure and environment configuration for promotion."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.deployment_utils import load_json, validate_release_structure


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", choices=["dev", "test", "prod"], required=True)
    args = parser.parse_args()
    artifact_config = load_json(REPO_ROOT / "deployment/release-artifacts.json")
    issues = validate_release_structure(REPO_ROOT, artifact_config, args.environment)
    if issues:
        for issue in issues:
            print(f"FAILED: {issue}")
        raise SystemExit("Release validation failed")
    print(f"Release structure validated for {args.environment}")


if __name__ == "__main__":
    main()
