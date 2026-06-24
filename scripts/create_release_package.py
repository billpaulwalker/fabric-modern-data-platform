"""Build a versioned, checksummed release archive from approved repository paths."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.deployment_utils import create_release_package, load_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True, help="Release version such as 1.0.0")
    args = parser.parse_args()
    config = load_json(REPO_ROOT / "deployment/release-artifacts.json")
    output = REPO_ROOT / f"dist/{config['release_name']}-{args.version}.zip"
    manifest = create_release_package(REPO_ROOT, config, args.version, output)
    print(f"Created {output}")
    print(f"Packaged {len(manifest['files'])} files")


if __name__ == "__main__":
    main()
