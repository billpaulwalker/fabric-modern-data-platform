# Fabric notebook source
# Default Lakehouse: lh_cre_bronze, shared by every pipeline notebook so a run reuses one Spark session;
# its Files/config/ holds all config. Tables are read and written by full lakehouse.schema.table name.
# Appends one row per pipeline run to lh_cre_gold.audit.pipeline_runs, so pipeline health is available
# to the Direct Lake model alongside the Gold tables. Needs config/fabric_layout.json.
# pl_cre_end_to_end calls this twice: with status Succeeded after validation succeeds, and with status
# Failed when validation fails or is skipped, which happens whenever any earlier activity fails.

# PARAMETERS CELL ********************
# In Fabric, put these lines in their own first cell and mark it as the parameter cell;
# the pipeline's base parameters override them at run time.

pipeline_run_id = ""
pipeline_name = "pl_cre_end_to_end"
environment = "dev"
status = ""
triggered_at = ""
message = ""

# CELL ********************

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CONFIG_DIRECTORY = "/lakehouse/default/Files/config"
STATUSES = {"Succeeded", "Failed"}


def load_config(path) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def resolve_table(layout: dict, layer: str, name: str) -> str:
    """Return lakehouse.schema.table for a logical table, as declared in fabric_layout.json."""
    location = layout["tables"].get(layer, {}).get(name)
    if location is None:
        raise ValueError(f"{layer} table {name!r} is not declared in fabric_layout.json")
    return f"{layout['lakehouses'][layer]}.{location}"


def create_schema_for(spark, table_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_name.rsplit('.', 1)[0]}")


def append_table(frame: DataFrame, table_name: str) -> None:
    frame.write.format("delta").mode("append").saveAsTable(table_name)


def build_run_record(
    spark, pipeline_run_id: str, pipeline_name: str, environment: str, status: str, triggered_at: str, message: str
) -> DataFrame:
    if not pipeline_run_id:
        raise ValueError("pipeline_run_id is required; pass @pipeline().RunId from the pipeline")
    if status not in STATUSES:
        raise ValueError(f"status must be one of {sorted(STATUSES)}, got {status!r}")
    # Built from literals so no Python data has to be shipped to the executors.
    return spark.range(1).select(
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        F.lit(pipeline_name).alias("pipeline_name"),
        F.lit(environment).alias("environment"),
        F.lit(status).alias("status"),
        F.lit(message or None).cast("string").alias("message"),
        F.lit(triggered_at or None).cast("string").try_cast("timestamp").alias("triggered_at"),
        F.current_timestamp().alias("logged_at"),
    )


def main(
    spark, pipeline_run_id: str, pipeline_name: str, environment: str, status: str,
    triggered_at: str = "", message: str = "", config_directory: str = CONFIG_DIRECTORY,
) -> dict:
    layout = load_config(f"{config_directory}/fabric_layout.json")
    table = resolve_table(layout, "gold", "pipeline_runs")
    record = build_run_record(spark, pipeline_run_id, pipeline_name, environment, status, triggered_at, message)
    create_schema_for(spark, table)
    append_table(record, table)
    return {"table": table, "pipeline_run_id": pipeline_run_id, "status": status}


if __name__ == "__main__":
    print(main(spark, pipeline_run_id, pipeline_name, environment, status, triggered_at, message))  # noqa: F821 - spark is a Fabric notebook global
