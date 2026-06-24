"""Release validation, evidence gates, and deterministic package utilities."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zipfile import ZIP_DEFLATED, ZipFile


VALID_ENVIRONMENTS = {"dev", "test", "prod"}
SECRET_FILE_NAMES = {".env", "secrets.json", "credentials.json"}


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_environment_config(config: dict[str, Any], expected_environment: str) -> list[str]:
    required = {
        "environment", "workspace_name", "lakehouse_name", "semantic_model_name",
        "deployment_stage", "schedule_enabled", "data_validation_required",
    }
    issues = []
    missing = sorted(required - set(config))
    if missing:
        issues.append(f"Environment configuration missing keys: {missing}")
    if config.get("environment") != expected_environment:
        issues.append(
            f"Environment file expected {expected_environment}, found {config.get('environment')}"
        )
    if expected_environment not in VALID_ENVIRONMENTS:
        issues.append(f"Unsupported environment: {expected_environment}")
    if expected_environment != "prod" and config.get("schedule_enabled") is True:
        issues.append("Schedules must remain disabled outside Production")
    if config.get("data_validation_required") is not True:
        issues.append("Data validation must be required in every environment")
    return issues


def find_secret_files(repo_root: str | Path) -> list[str]:
    root = Path(repo_root)
    ignored = {".git", ".venv", "__pycache__", "data", "dist"}
    findings = []
    for path in root.rglob("*"):
        if any(part in ignored for part in path.parts) or not path.is_file():
            continue
        if path.name.lower() in SECRET_FILE_NAMES or path.suffix.lower() in {".key", ".pem", ".pfx"}:
            findings.append(str(path.relative_to(root)))
    return sorted(findings)


def validate_release_structure(
    repo_root: str | Path,
    artifact_config: dict[str, Any],
    environment: str,
) -> list[str]:
    root = Path(repo_root)
    issues = []
    for required_path in artifact_config["required_paths"]:
        if not (root / required_path).exists():
            issues.append(f"Missing required release path: {required_path}")
    environment_path = root / f"config/environments/{environment}.json"
    if not environment_path.exists():
        issues.append(f"Missing environment configuration: {environment_path.relative_to(root)}")
    else:
        issues.extend(validate_environment_config(load_json(environment_path), environment))
    secret_files = find_secret_files(root)
    if secret_files:
        issues.append(f"Potential secret files must not be released: {secret_files}")
    return issues


def _read_last_jsonl(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else None


@dataclass
class DeploymentGateReport:
    environment: str
    passed: bool = True
    checks: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    def add(self, condition: bool, check: str, issue: str) -> None:
        self.checks.append(check)
        if not condition:
            self.passed = False
            self.issues.append(issue)

    def as_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "passed": self.passed,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "checks": self.checks,
            "issues": self.issues,
        }


def evaluate_deployment_gates(repo_root: str | Path, environment: str) -> DeploymentGateReport:
    root = Path(repo_root)
    report = DeploymentGateReport(environment)
    pipeline = _read_last_jsonl(root / "data/operations/pipeline_runs.jsonl")
    report.add(
        pipeline is not None and pipeline.get("status") == "SUCCEEDED",
        "latest_pipeline_run_succeeded",
        "Latest end-to-end pipeline run is missing or did not succeed",
    )

    semantic_path = root / "data/gold/semantic_model_validation.json"
    semantic = load_json(semantic_path) if semantic_path.exists() else None
    report.add(
        semantic is not None and semantic.get("passed") is True,
        "semantic_model_validation_passed",
        "Semantic-model validation evidence is missing or failed",
    )

    gold_path = root / "data/gold/gold_model_metrics.json"
    gold_metrics = json.loads(gold_path.read_text(encoding="utf-8")) if gold_path.exists() else None
    gold_valid = bool(gold_metrics) and all(
        item.get("duplicate_grain_rows") == 0 and item.get("rows_written", 0) > 0
        for item in gold_metrics
    )
    report.add(
        gold_valid,
        "gold_models_nonempty_and_unique",
        "Gold metrics are missing, empty, or contain duplicate-grain rows",
    )
    return report


def iter_release_files(
    repo_root: str | Path,
    artifact_config: dict[str, Any],
) -> list[Path]:
    root = Path(repo_root)
    excluded_directories = set(artifact_config["excluded_directories"])
    excluded_suffixes = set(artifact_config["excluded_suffixes"])
    files: set[Path] = set()
    for required_path in artifact_config["required_paths"]:
        path = root / required_path
        candidates: Iterable[Path] = path.rglob("*") if path.is_dir() else [path]
        for candidate in candidates:
            if not candidate.is_file():
                continue
            relative = candidate.relative_to(root)
            if any(part in excluded_directories for part in relative.parts):
                continue
            if candidate.suffix.lower() in excluded_suffixes:
                continue
            files.add(relative)
    return sorted(files, key=lambda value: value.as_posix())


def create_release_package(
    repo_root: str | Path,
    artifact_config: dict[str, Any],
    version: str,
    output_path: str | Path,
) -> dict[str, Any]:
    root = Path(repo_root)
    files = iter_release_files(root, artifact_config)
    manifest = {
        "release_name": artifact_config["release_name"],
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {"path": path.as_posix(), "sha256": sha256_file(root / path)}
            for path in files
        ],
    }
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(root / path, path.as_posix())
        archive.writestr("release-manifest.json", json.dumps(manifest, indent=2))
    return manifest
