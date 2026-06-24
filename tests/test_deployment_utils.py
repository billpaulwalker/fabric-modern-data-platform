import json
from zipfile import ZipFile

from src.deployment_utils import (
    create_release_package,
    evaluate_deployment_gates,
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
    report = evaluate_deployment_gates(tmp_path, "test")
    assert report.passed
    assert len(report.checks) == 3


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
