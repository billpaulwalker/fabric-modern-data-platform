import subprocess

from src.orchestration_utils import run_pipeline, validate_pipeline_config


def test_pipeline_retries_and_propagates_run_id(tmp_path):
    calls = []

    def runner(command, cwd, environment, timeout):
        calls.append((command, environment["PIPELINE_RUN_ID"]))
        return_code = 1 if len(calls) == 1 else 0
        return subprocess.CompletedProcess(command, return_code, stdout="ok", stderr="temporary failure")

    config = {
        "pipeline_name": "test_pipeline",
        "audit_directory": "audit",
        "steps": [{
            "name": "ingest", "command": ["ingest.py"], "depends_on": [],
            "max_attempts": 2, "retry_delay_seconds": 0, "timeout_seconds": 10,
        }],
    }
    result = run_pipeline(config, tmp_path, runner)
    assert result.status == "SUCCEEDED"
    assert len(calls) == 2
    assert calls[0][1] == result.pipeline_run_id == calls[1][1]
    assert (tmp_path / "audit/pipeline_runs.jsonl").exists()
    assert (tmp_path / "audit/pipeline_step_runs.jsonl").exists()


def test_failed_step_skips_dependent_step(tmp_path):
    def runner(command, cwd, environment, timeout):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

    config = {
        "pipeline_name": "test_pipeline",
        "steps": [
            {"name": "bronze", "command": ["bronze.py"], "depends_on": []},
            {"name": "silver", "command": ["silver.py"], "depends_on": ["bronze"]},
        ],
    }
    result = run_pipeline(config, tmp_path, runner)
    assert result.status == "FAILED"
    assert result.steps_failed == 1
    assert result.steps_skipped == 1
    assert [step.status for step in result.step_results] == ["FAILED", "SKIPPED"]


def test_config_rejects_unknown_dependency():
    config = {
        "pipeline_name": "test_pipeline",
        "steps": [{"name": "silver", "command": ["silver.py"], "depends_on": ["missing"]}],
    }
    try:
        validate_pipeline_config(config)
    except ValueError as error:
        assert "unknown dependencies" in str(error)
    else:
        raise AssertionError("Expected invalid pipeline configuration to fail")
