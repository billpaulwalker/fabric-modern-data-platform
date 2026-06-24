"""Dependency-aware local orchestration with production-shaped audit logging."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id(pipeline_name: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{pipeline_name}_{timestamp}_{uuid.uuid4().hex[:8]}"


def append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, default=str) + "\n")


def load_pipeline_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        config = json.load(file)
    validate_pipeline_config(config)
    return config


def validate_pipeline_config(config: dict[str, Any]) -> None:
    steps = config.get("steps", [])
    if not config.get("pipeline_name") or not steps:
        raise ValueError("Pipeline configuration requires pipeline_name and steps")
    names = [step.get("name") for step in steps]
    if any(not name for name in names) or len(names) != len(set(names)):
        raise ValueError("Pipeline step names must be present and unique")
    known = set(names)
    previously_declared: set[str] = set()
    for step in steps:
        unknown = sorted(set(step.get("depends_on", [])) - known)
        if unknown:
            raise ValueError(f"Step {step['name']} has unknown dependencies: {unknown}")
        if step["name"] in step.get("depends_on", []):
            raise ValueError(f"Step {step['name']} cannot depend on itself")
        out_of_order = sorted(set(step.get("depends_on", [])) - previously_declared)
        if out_of_order:
            raise ValueError(
                f"Step {step['name']} dependencies must appear earlier in configuration: {out_of_order}"
            )
        if not step.get("command"):
            raise ValueError(f"Step {step['name']} requires a command")
        previously_declared.add(step["name"])


@dataclass
class StepResult:
    pipeline_run_id: str
    step_name: str
    status: str
    attempt: int
    started_at: str
    completed_at: str
    duration_seconds: float
    return_code: int | None = None
    stdout_tail: str = ""
    error_message: str = ""


@dataclass
class PipelineResult:
    pipeline_name: str
    pipeline_run_id: str
    status: str
    started_at: str
    completed_at: str
    duration_seconds: float
    steps_succeeded: int
    steps_failed: int
    steps_skipped: int
    step_results: list[StepResult] = field(default_factory=list)


CommandRunner = Callable[[list[str], Path, dict[str, str], int], subprocess.CompletedProcess[str]]


def default_command_runner(
    command: list[str], cwd: Path, environment: dict[str, str], timeout_seconds: int
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *command],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )


def _tail(value: str, limit: int = 4000) -> str:
    return value[-limit:].strip()


def _execute_step(
    step: dict[str, Any],
    repo_root: Path,
    pipeline_run_id: str,
    step_log: Path,
    runner: CommandRunner,
) -> StepResult:
    attempts = int(step.get("max_attempts", 1))
    delay = float(step.get("retry_delay_seconds", 0))
    timeout = int(step.get("timeout_seconds", 600))
    environment = dict(os.environ)
    environment["PIPELINE_RUN_ID"] = pipeline_run_id
    last_result: StepResult | None = None

    for attempt in range(1, attempts + 1):
        started_at, started_clock = utc_now(), time.monotonic()
        try:
            completed = runner(step["command"], repo_root, environment, timeout)
            status = "SUCCEEDED" if completed.returncode == 0 else "FAILED"
            error = _tail(completed.stderr) if completed.returncode else ""
            return_code = completed.returncode
            stdout = _tail(completed.stdout)
        except subprocess.TimeoutExpired as exc:
            status, return_code = "FAILED", None
            stdout = _tail(exc.stdout or "")
            error = f"Step timed out after {timeout} seconds"
        except Exception as exc:  # noqa: BLE001 - audit unexpected runner failures
            status, return_code, stdout, error = "FAILED", None, "", f"{type(exc).__name__}: {exc}"

        last_result = StepResult(
            pipeline_run_id=pipeline_run_id,
            step_name=step["name"],
            status=status,
            attempt=attempt,
            started_at=started_at,
            completed_at=utc_now(),
            duration_seconds=round(time.monotonic() - started_clock, 3),
            return_code=return_code,
            stdout_tail=stdout,
            error_message=error,
        )
        append_jsonl(step_log, asdict(last_result))
        if status == "SUCCEEDED":
            return last_result
        if attempt < attempts:
            time.sleep(delay)

    assert last_result is not None
    return last_result


def run_pipeline(
    config: dict[str, Any],
    repo_root: str | Path,
    runner: CommandRunner = default_command_runner,
) -> PipelineResult:
    root = Path(repo_root).resolve()
    pipeline_name = config["pipeline_name"]
    pipeline_run_id = new_run_id(pipeline_name)
    audit_directory = root / config.get("audit_directory", "data/operations")
    run_log = audit_directory / "pipeline_runs.jsonl"
    step_log = audit_directory / "pipeline_step_runs.jsonl"
    started_at, started_clock = utc_now(), time.monotonic()
    statuses: dict[str, str] = {}
    results: list[StepResult] = []

    for step in config["steps"]:
        dependencies = step.get("depends_on", [])
        if any(statuses.get(dependency) != "SUCCEEDED" for dependency in dependencies):
            now = utc_now()
            result = StepResult(
                pipeline_run_id=pipeline_run_id,
                step_name=step["name"],
                status="SKIPPED",
                attempt=0,
                started_at=now,
                completed_at=now,
                duration_seconds=0.0,
                error_message="Upstream dependency did not succeed",
            )
            append_jsonl(step_log, asdict(result))
        else:
            result = _execute_step(step, root, pipeline_run_id, step_log, runner)
        statuses[step["name"]] = result.status
        results.append(result)

    failed = sum(result.status == "FAILED" for result in results)
    skipped = sum(result.status == "SKIPPED" for result in results)
    succeeded = sum(result.status == "SUCCEEDED" for result in results)
    final_status = "SUCCEEDED" if failed == 0 and skipped == 0 else "FAILED"
    pipeline_result = PipelineResult(
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        status=final_status,
        started_at=started_at,
        completed_at=utc_now(),
        duration_seconds=round(time.monotonic() - started_clock, 3),
        steps_succeeded=succeeded,
        steps_failed=failed,
        steps_skipped=skipped,
        step_results=results,
    )
    run_record = asdict(pipeline_result)
    run_record.pop("step_results")
    append_jsonl(run_log, run_record)
    return pipeline_result
