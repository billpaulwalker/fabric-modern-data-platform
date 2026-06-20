from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional


@dataclass
class PipelineRunLog:
    pipeline_run_id: str
    pipeline_name: str
    environment: str
    source_system: str
    source_object: str
    target_table: str
    load_type: str
    start_time: str
    end_time: Optional[str] = None
    status: str = "Started"
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_pipeline_run(
    pipeline_run_id: str,
    pipeline_name: str,
    environment: str,
    source_system: str,
    source_object: str,
    target_table: str,
    load_type: str,
) -> PipelineRunLog:
    return PipelineRunLog(
        pipeline_run_id=pipeline_run_id,
        pipeline_name=pipeline_name,
        environment=environment,
        source_system=source_system,
        source_object=source_object,
        target_table=target_table,
        load_type=load_type,
        start_time=utc_now_iso(),
    )


def complete_pipeline_run(log: PipelineRunLog, rows_read: int, rows_written: int, rows_rejected: int = 0) -> PipelineRunLog:
    log.end_time = utc_now_iso()
    log.status = "Succeeded"
    log.rows_read = rows_read
    log.rows_written = rows_written
    log.rows_rejected = rows_rejected
    return log


def fail_pipeline_run(log: PipelineRunLog, error_message: str) -> PipelineRunLog:
    log.end_time = utc_now_iso()
    log.status = "Failed"
    log.error_message = error_message
    return log
