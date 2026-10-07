import json
import os
import time
from pathlib import Path
from zipfile import ZipFile

import pytest

from src.deployment_utils import (
    create_release_package,
    evaluate_deployment_gates,
    find_secret_files,
    sha256_file,
    validate_environment_config,
    validate_release_structure,
)


def test_nonproduction_schedule_must_be_disabled():
    config = {
        "environment": "test",
        "workspace_name": "ws-test",
        "lakehouse_name": "lh_test",
        "semantic_model_name": "Model - Test",
        "deployment_stage": "Test",
        "schedule_enabled": True,
        "data_validation_required": True,
    }
    issues = validate_environment_config(config, "test")
    assert any("Schedules must remain disabled" in issue for issue in issues)


def test_release_structure_and_package(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src/app.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "config/environments").mkdir(parents=True)
    environment = {
        "environment": "dev",
        "workspace_name": "ws-dev",
        "lakehouse_name": "lh_dev",
        "semantic_model_name": "Model - Dev",
        "deployment_stage": "Development",
        "schedule_enabled": False,
        "data_validation_required": True,
    }
    (tmp_path / "config/environments/dev.json").write_text(json.dumps(environment), encoding="utf-8")
    artifact_config = {
        "release_name": "test-release",
        "required_paths": ["src", "config"],
        "excluded_directories": [".git", ".venv", "__pycache__", "data", "dist"],
        "excluded_suffixes": [".pyc"],
    }
    assert validate_release_structure(tmp_path, artifact_config, "dev") == []
    output = tmp_path / "dist/release.zip"
    manifest = create_release_package(tmp_path, artifact_config, "1.0.0", output)
    assert output.exists()
    assert manifest["version"] == "1.0.0"
    with ZipFile(output) as archive:
        assert "src/app.py" in archive.namelist()
        assert "release-manifest.json" in archive.namelist()


def _write_silver_metrics(root, missing_columns):
    (root / "data/silver").mkdir(parents=True, exist_ok=True)
    (root / "data/silver/silver_run_metrics.json").write_text(
        json.dumps([{"table": "leases", "rows_valid": 6, "missing_configured_columns": missing_columns}]),
        encoding="utf-8",
    )


def test_deployment_gates_require_successful_current_evidence(tmp_path):
    (tmp_path / "data/operations").mkdir(parents=True)
    (tmp_path / "data/gold").mkdir(parents=True)
    (tmp_path / "data/operations/pipeline_runs.jsonl").write_text(
        json.dumps({"status": "SUCCEEDED", "pipeline_run_id": "run-1"}) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "data/gold/semantic_model_validation.json").write_text(
        json.dumps({"passed": True}), encoding="utf-8"
    )
    (tmp_path / "data/gold/gold_model_metrics.json").write_text(
        json.dumps([{"model": "fact_test", "rows_written": 10, "duplicate_grain_rows": 0}]),
        encoding="utf-8",
    )
    _write_silver_metrics(tmp_path, [])
    report = evaluate_deployment_gates(tmp_path, "test")
    assert report.passed
    assert len(report.checks) == 4


def test_deployment_gate_fails_when_semantic_validation_failed(tmp_path):
    (tmp_path / "data/operations").mkdir(parents=True)
    (tmp_path / "data/gold").mkdir(parents=True)
    (tmp_path / "data/operations/pipeline_runs.jsonl").write_text(
        json.dumps({"status": "SUCCEEDED"}) + "\n", encoding="utf-8"
    )
    (tmp_path / "data/gold/semantic_model_validation.json").write_text(
        json.dumps({"passed": False}), encoding="utf-8"
    )
    (tmp_path / "data/gold/gold_model_metrics.json").write_text(
        json.dumps([{"rows_written": 10, "duplicate_grain_rows": 0}]), encoding="utf-8"
    )
    report = evaluate_deployment_gates(tmp_path, "prod")
    assert not report.passed
    assert "Semantic-model validation evidence is missing or failed" in report.issues


def test_deployment_gate_fails_when_silver_config_references_absent_source_columns(tmp_path):
    _write_silver_metrics(tmp_path, ["monthly_rent"])
    report = evaluate_deployment_gates(tmp_path, "dev")
    assert not report.passed
    assert any("monthly_rent" in issue for issue in report.issues)


def test_deployment_gate_fails_when_silver_evidence_is_missing(tmp_path):
    report = evaluate_deployment_gates(tmp_path, "dev")
    assert "silver_configured_columns_present" in report.checks
    assert any("Silver run metrics" in issue for issue in report.issues)


def _package_fixture(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src/app.py").write_text("VALUE = 1\n", encoding="utf-8")
    return {
        "release_name": "test-release",
        "required_paths": ["src"],
        "excluded_directories": [],
        "excluded_suffixes": [],
    }


@pytest.mark.parametrize("version", ["1.0", "v1.0.0", "1.0.0; rm -rf /", "../../1.0.0", "1.0.0/evil", ""])
def test_release_package_rejects_unsafe_or_non_semver_versions(tmp_path, version):
    config = _package_fixture(tmp_path)
    with pytest.raises(ValueError, match="version"):
        create_release_package(tmp_path, config, version, tmp_path / "dist/release.zip")


def test_release_package_accepts_semver_with_prerelease(tmp_path):
    config = _package_fixture(tmp_path)
    manifest = create_release_package(tmp_path, config, "1.2.3-rc.1", tmp_path / "dist/release.zip")
    assert manifest["version"] == "1.2.3-rc.1"


def test_release_package_is_byte_for_byte_reproducible(tmp_path):
    config = _package_fixture(tmp_path)
    first = tmp_path / "dist/first.zip"
    second = tmp_path / "dist/second.zip"
    create_release_package(tmp_path, config, "1.0.0", first)
    os.utime(tmp_path / "src/app.py", (1_000_000_000, 1_000_000_000))
    time.sleep(1.1)
    create_release_package(tmp_path, config, "1.0.0", second)
    assert sha256_file(first) == sha256_file(second)


def test_secret_scan_covers_data_folder_and_key_formats(tmp_path):
    for relative in ["data/api_sample/credentials.json", "config/cert.pfx", "keys/client.p12", "keys/server.pem"]:
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / relative).write_text("secret", encoding="utf-8")
    (tmp_path / ".venv/lib").mkdir(parents=True)
    (tmp_path / ".venv/lib/vendor.pem").write_text("ignored", encoding="utf-8")
    assert find_secret_files(tmp_path) == [
        "config/cert.pfx", "data/api_sample/credentials.json", "keys/client.p12", "keys/server.pem",
    ]


def test_environment_config_does_not_require_lakehouse_names():
    # Lakehouse names are environment-independent and live in config/fabric_layout.json.
    config = {
        "environment": "dev",
        "workspace_name": "ws-dev",
        "semantic_model_name": "Model - Dev",
        "deployment_stage": "Development",
        "schedule_enabled": False,
        "data_validation_required": True,
    }
    assert validate_environment_config(config, "dev") == []


def test_repository_environment_configs_are_valid():
    root = Path(__file__).resolve().parents[1]
    for environment in ["dev", "test", "prod"]:
        config = json.loads((root / f"config/environments/{environment}.json").read_text(encoding="utf-8"))
        assert validate_environment_config(config, environment) == []
        assert "lakehouse_name" not in config
