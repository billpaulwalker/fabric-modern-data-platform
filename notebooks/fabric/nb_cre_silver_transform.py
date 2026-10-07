# Fabric notebook source
# Default Lakehouse: lh_cre_silver (schema-enabled). Reads lh_cre_bronze; writes one schema per domain.
# Upload config/fabric_layout.json and config/silver_table_config.json to its Files/config/;
# the table config is the same contract the local pandas pipeline uses.

from functools import reduce
import json
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


CONFIG_DIRECTORY = "/lakehouse/default/Files/config"

SPARK_TYPES = {
    "string": "string",
    "integer": "long",
    "decimal": "decimal(18,2)",
    "date": "date",
    "datetime": "timestamp",
    "boolean": "boolean",
}


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


def snake_case(name: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9]+", "_", value)).strip("_").lower()


def spark_type(type_name: str) -> str:
    if type_name not in SPARK_TYPES:
        raise ValueError(f"Unsupported Silver data type: {type_name}")
    return SPARK_TYPES[type_name]


def flatten_structs(frame: DataFrame) -> DataFrame:
    """Expand nested API structs to parent_child columns, matching pandas json_normalize."""
    while any(isinstance(field.dataType, StructType) for field in frame.schema.fields):
        columns = []
        for field in frame.schema.fields:
            if isinstance(field.dataType, StructType):
                columns.extend(
                    F.col(f"`{field.name}`.`{child.name}`").alias(f"{field.name}_{child.name}")
                    for child in field.dataType.fields
                )
            else:
                columns.append(F.col(f"`{field.name}`"))
        frame = frame.select(*columns)
    return frame


def normalize_columns(frame: DataFrame) -> DataFrame:
    names = [snake_case(column) for column in frame.columns]
    if len(names) != len(set(names)):
        raise ValueError("Column normalization produced duplicate column names")
    return frame.select([F.col(f"`{column}`").alias(name) for column, name in zip(frame.columns, names)])


def blank_strings_to_null(frame: DataFrame) -> DataFrame:
    for field in frame.schema.fields:
        if isinstance(field.dataType, StringType):
            frame = frame.withColumn(
                field.name, F.when(F.trim(F.col(field.name)) == "", F.lit(None)).otherwise(F.col(field.name))
            )
    return frame


def apply_column_mappings(frame: DataFrame, mappings: dict) -> DataFrame:
    for source, target in mappings.items():
        source, target = snake_case(source), snake_case(target)
        if source not in frame.columns:
            continue
        if target in frame.columns:
            raise ValueError(f"Column mapping {source} -> {target} collides with an existing column")
        frame = frame.withColumnRenamed(source, target)
    return frame


def apply_derived_columns(frame: DataFrame, derivations: dict) -> DataFrame:
    """Calculate configured columns from typed inputs. Supported operation: product."""
    for target, rule in derivations.items():
        target = snake_case(target)
        if set(rule) != {"product"}:
            raise ValueError(f"Unsupported derivation for {target}: {sorted(rule)}")
        inputs = [snake_case(column) for column in rule["product"]]
        missing = [column for column in inputs if column not in frame.columns]
        if missing:
            raise ValueError(f"Cannot derive {target}; missing input columns: {missing}")
        product = reduce(lambda left, right: left * right, [F.col(column).cast("decimal(18,6)") for column in inputs])
        frame = frame.withColumn(target, product.cast("decimal(18,2)"))
    return frame


def configured_columns(config: dict) -> set:
    columns = set(config.get("column_types", {})) | set(config.get("allowed_values", {}))
    columns |= set(config.get("non_negative_columns", []))
    columns |= {column for rule in config.get("date_order_rules", []) for column in rule}
    return {snake_case(column) for column in columns}


class RejectionChecks:
    """Record each rule as its own flag column, then build the reasons array once.

    Re-wrapping a single reasons column per rule doubles the plan at every step and
    exhausts driver memory during optimization once a table has a dozen rules.
    """

    def __init__(self):
        self.reasons = []

    def add(self, frame: DataFrame, condition, reason: str) -> DataFrame:
        flag = f"_check_{len(self.reasons)}"
        self.reasons.append((flag, reason))
        return frame.withColumn(flag, F.coalesce(condition, F.lit(False)))

    def finish(self, frame: DataFrame) -> DataFrame:
        if not self.reasons:
            return frame.withColumn("_rejection_reasons", F.array().cast("array<string>"))
        reasons = F.array_compact(F.array(*[F.when(F.col(flag), F.lit(reason)) for flag, reason in self.reasons]))
        return frame.withColumn("_rejection_reasons", reasons).drop(*[flag for flag, _ in self.reasons])


def transform_table(frame: DataFrame, config: dict) -> tuple:
    """Standardize, validate, quarantine, and deduplicate one Bronze table."""
    name = config["name"]
    frame = normalize_columns(flatten_structs(frame))
    frame = apply_column_mappings(frame, config.get("column_mappings", {}))
    frame = blank_strings_to_null(frame).withColumn("_source_row_number", F.monotonically_increasing_id())

    required = [snake_case(column) for column in config.get("required_columns", [])]
    missing_required = sorted(set(required) - set(frame.columns))
    if missing_required:
        raise ValueError(f"{name} is missing required source columns: {missing_required}")

    checks = RejectionChecks()
    for column, type_name in config.get("column_types", {}).items():
        column = snake_case(column)
        if column not in frame.columns:
            continue
        original = F.col(column)
        converted = F.trim(original) if type_name == "string" else original.cast(spark_type(type_name))
        frame = checks.add(
            frame, original.isNotNull() & converted.isNull(), f"invalid_{type_name}:{column}"
        ).withColumn(column, converted)

    frame = apply_derived_columns(frame, config.get("derived_columns", {}))
    missing_configured = sorted(configured_columns(config) - set(frame.columns))

    for column in required:
        frame = checks.add(frame, F.col(column).isNull(), f"required:{column}")
    for column, allowed in config.get("allowed_values", {}).items():
        column = snake_case(column)
        if column in frame.columns:
            invalid = F.col(column).isNotNull() & ~F.col(column).isin(allowed)
            frame = checks.add(frame, invalid, f"invalid_value:{column}")
    for column in [snake_case(column) for column in config.get("non_negative_columns", [])]:
        if column in frame.columns:
            frame = checks.add(frame, F.col(column).cast("double") < 0, f"negative_value:{column}")
    for start, end in config.get("date_order_rules", []):
        start, end = snake_case(start), snake_case(end)
        if start in frame.columns and end in frame.columns:
            invalid = F.col(start).isNotNull() & F.col(end).isNotNull() & (F.col(end) < F.col(start))
            frame = checks.add(frame, invalid, f"date_order:{start}>{end}")

    frame = checks.finish(frame)
    rejected = (
        frame.filter(F.size("_rejection_reasons") > 0)
        .withColumn("data_quality_status", F.lit("REJECTED"))
        .withColumn("rejection_reason", F.concat_ws(" | ", "_rejection_reasons"))
        .withColumn("silver_processed_timestamp", F.current_timestamp())
        .withColumn("rejected_source_object", F.lit(name))
        .drop("_rejection_reasons", "_source_row_number")
    )
    valid = frame.filter(F.size("_rejection_reasons") == 0).drop("_rejection_reasons")

    primary_key = [snake_case(column) for column in config.get("primary_key", [])]
    missing_key = sorted(set(primary_key) - set(valid.columns))
    if missing_key:
        raise ValueError(f"{name} is missing configured primary-key columns: {missing_key}")
    valid_before_dedup = valid.count()
    if primary_key:
        order_columns = [
            F.col(column).desc_nulls_last()
            for column in ["ingestion_timestamp", "updated_at", "_source_row_number"]
            if column in valid.columns
        ]
        window = Window.partitionBy(*primary_key).orderBy(*order_columns)
        valid = valid.withColumn("_row_number", F.row_number().over(window)).filter(F.col("_row_number") == 1)
    valid = (
        valid.drop("_row_number", "_source_row_number")
        .withColumn("data_quality_status", F.lit("VALID"))
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )

    rows_valid = valid.count()
    metrics = {
        "table": name,
        "rows_read": frame.count(),
        "rows_valid": rows_valid,
        "rows_rejected": rejected.count(),
        "duplicate_rows_removed": valid_before_dedup - rows_valid,
        "missing_configured_columns": missing_configured,
    }
    return valid, rejected, metrics


def main(spark, config_directory: str = CONFIG_DIRECTORY) -> list:
    configs = load_config(f"{config_directory}/silver_table_config.json")["tables"]
    layout = load_config(f"{config_directory}/fabric_layout.json")
    results = [
        transform_table(spark.table(resolve_table(layout, "bronze", config["name"])), config) for config in configs
    ]

    # Fail before writing anything if the source no longer matches the contract.
    gaps = {metrics["table"]: metrics["missing_configured_columns"] for _, _, metrics in results
            if metrics["missing_configured_columns"]}
    if gaps:
        raise ValueError(f"Configured columns are absent from Bronze; update column_mappings: {gaps}")

    for valid, _, metrics in results:
        table = resolve_table(layout, "silver", metrics["table"])
        create_schema_for(spark, table)
        write_table(valid, table)
    rejected_union = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True), [rejected for _, rejected, _ in results]
    )
    quarantine = resolve_table(layout, "silver", "rejected_records")
    create_schema_for(spark, quarantine)
    write_table(rejected_union, quarantine)
    return [metrics for _, _, metrics in results]


METRICS_SCHEMA = StructType([
    StructField("table", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_valid", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("duplicate_rows_removed", LongType()),
    StructField("missing_configured_columns", ArrayType(StringType())),
])


def metrics_frame(spark, metrics: list) -> DataFrame:
    # An explicit schema: a healthy run reports [] for every table, which Spark cannot type by inference.
    return spark.createDataFrame(metrics, METRICS_SCHEMA)


if __name__ == "__main__":
    display(metrics_frame(spark, main(spark)))  # noqa: F821 - spark and display are Fabric notebook globals
