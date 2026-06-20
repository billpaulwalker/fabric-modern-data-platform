from dataclasses import dataclass
from typing import Optional


@dataclass
class Watermark:
    source_system: str
    source_object: str
    target_table: str
    watermark_column: str
    last_successful_value: Optional[str]
    last_successful_run_id: Optional[str]


def get_watermark_placeholder(source_system: str, source_object: str, target_table: str, watermark_column: str) -> Watermark:
    return Watermark(
        source_system=source_system,
        source_object=source_object,
        target_table=target_table,
        watermark_column=watermark_column,
        last_successful_value=None,
        last_successful_run_id=None,
    )


def update_watermark_placeholder(watermark: Watermark, new_value: str, pipeline_run_id: str) -> Watermark:
    watermark.last_successful_value = new_value
    watermark.last_successful_run_id = pipeline_run_id
    return watermark
