# Fabric notebook source
# Attach the schema-enabled project Lakehouse before running this notebook.
# Upload config/bronze_source_config.json to Files/config/ and the repository's
# data/sample and data/api_sample folders to Files/landing/ first.
# Bronze keeps source values raw: CSV columns stay strings and API JSON stays nested.

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
    started_at = datetime.now(timezone.utc)
    run_id = pipeline_run_id or new_pipeline_run_id(started_at)
    root = landing_root or config["landing_root"]

    # Read every source before writing, so a missing upload fails the run without partial loads.
    frames = {
        source["name"]: ingest_source(spark, source, root, run_id, started_at.isoformat())
        for source in config["sources"]
    }

    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    metrics = []
    for source in config["sources"]:
        table = f"bronze.{source['name']}"
        # Every source is a full load: the table is replaced on each run.
        frames[source["name"]].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        metrics.append({"table": table, "rows_written": spark.table(table).count(), "pipeline_run_id": run_id})
    return metrics


if __name__ == "__main__":
    display(spark.createDataFrame(main(spark)))  # noqa: F821 - spark and display are Fabric notebook globals
