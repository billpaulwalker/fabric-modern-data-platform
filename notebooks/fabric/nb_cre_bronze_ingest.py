# Fabric notebook source
# Default Lakehouse: lh_cre_bronze, shared by every pipeline notebook so a run reuses one Spark session;
# its Files/config/ holds all config. Tables are read and written by full lakehouse.schema.table name.
# Writes lh_cre_bronze, one schema per source system. Needs config/fabric_layout.json and
# config/bronze_source_config.json in Files/config/, and data/sample and data/api_sample in Files/landing/.
# Bronze keeps source values raw: CSV columns stay strings and API JSON stays nested.

# PARAMETERS CELL ********************
# In Fabric, put this line in its own first cell and mark it as the parameter cell.
# pl_cre_end_to_end passes @pipeline().RunId; Silver and Gold carry it forward from the Bronze rows.
# Left empty, as in a manual run, the notebook generates its own run ID.

pipeline_run_id = ""

# CELL ********************

from datetime import datetime, timezone
import json

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CONFIG_DIRECTORY = "/lakehouse/default/Files/config"
AUDIT_COLUMNS = [
    "source_system", "source_object", "source_file_name", "ingestion_timestamp",
    "pipeline_run_id", "load_type", "raw_record_hash",
]


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


def write_table(frame: DataFrame, table_name: str) -> None:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def new_pipeline_run_id(started_at: datetime) -> str:
    return f"bronze_{started_at.strftime('%Y%m%dT%H%M%SZ')}"


def read_landing_file(spark, source: dict, landing_root: str) -> DataFrame:
    path = f"{landing_root}/{source['path']}"
    try:
        if source["format"] == "csv":
            frame = spark.read.option("header", True).option("inferSchema", False).csv(path)
        elif source["format"] == "json":
            frame = spark.read.option("multiLine", True).json(path)
        else:
            raise ValueError(f"Unsupported Bronze source format for {source['name']}: {source['format']}")
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" not in str(exc):
            raise
        raise FileNotFoundError(
            f"Landing file not found: {path}. Upload data/{source['path']} from the repository "
            f"to the Lakehouse at Files/landing/{source['path']}."
        ) from exc
    return frame.select("*", F.col("_metadata.file_name").alias("_source_file_name"))


def add_audit_columns(
    frame: DataFrame, source: dict, pipeline_run_id: str, ingestion_timestamp: str
) -> DataFrame:
    source_columns = [column for column in frame.columns if column != "_source_file_name"]
    collisions = sorted(set(source_columns) & set(AUDIT_COLUMNS))
    if collisions:
        raise ValueError(f"{source['name']} has source columns that collide with audit columns: {collisions}")
    # Hash only the raw source values, so the same record hashes the same in every run.
    raw_record = F.to_json(F.struct(*[F.col(f"`{column}`") for column in sorted(source_columns)]))
    return frame.select(
        *[F.col(f"`{column}`") for column in source_columns],
        F.lit(source["source_system"]).alias("source_system"),
        F.lit(source["source_object"]).alias("source_object"),
        F.col("_source_file_name").alias("source_file_name"),
        F.lit(ingestion_timestamp).cast("timestamp").alias("ingestion_timestamp"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        F.lit(source["load_type"]).alias("load_type"),
        F.sha2(raw_record, 256).alias("raw_record_hash"),
    )


def ingest_source(spark, source: dict, landing_root: str, pipeline_run_id: str, ingestion_timestamp: str) -> DataFrame:
    frame = read_landing_file(spark, source, landing_root)
    return add_audit_columns(frame, source, pipeline_run_id, ingestion_timestamp)


def main(spark, pipeline_run_id=None, config_directory: str = CONFIG_DIRECTORY, landing_root=None) -> list:
    config = load_config(f"{config_directory}/bronze_source_config.json")
    layout = load_config(f"{config_directory}/fabric_layout.json")
    started_at = datetime.now(timezone.utc)
    run_id = pipeline_run_id or new_pipeline_run_id(started_at)
    root = landing_root or config["landing_root"]

    # Read every source before writing, so a missing upload fails the run without partial loads.
    frames = {
        source["name"]: ingest_source(spark, source, root, run_id, started_at.isoformat())
        for source in config["sources"]
    }

    metrics = []
    for source in config["sources"]:
        table = resolve_table(layout, "bronze", source["name"])
        create_schema_for(spark, table)
        # Every source is a full load: the table is replaced on each run.
        write_table(frames[source["name"]], table)
        metrics.append({"table": table, "rows_written": spark.table(table).count(), "pipeline_run_id": run_id})
    return metrics


if __name__ == "__main__":
    display(spark.createDataFrame(main(spark, pipeline_run_id=pipeline_run_id or None)))  # noqa: F821 - spark and display are Fabric notebook globals
