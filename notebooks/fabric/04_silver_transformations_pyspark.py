# Fabric notebook source
# Attach the project Lakehouse before running this notebook.

from functools import reduce
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, TimestampType


TABLES = [
    {
        "name": "properties",
        "primary_key": ["property_id"],
        "required": ["property_id"],
        "types": {"property_id": "long", "square_feet": "decimal", "acquired_date": "date"},
        "non_negative": ["square_feet"],
    },
    {
        "name": "tenants",
        "primary_key": ["tenant_id"],
        "required": ["tenant_id"],
        "types": {"tenant_id": "long", "tenant_start_date": "date"},
    },
    {
        "name": "leases",
        "primary_key": ["lease_id"],
        "required": ["lease_id", "property_id", "tenant_id"],
        "types": {
            "lease_id": "long", "property_id": "long", "tenant_id": "long",
            "lease_start_date": "date", "lease_end_date": "date", "monthly_rent": "decimal",
        },
        "non_negative": ["monthly_rent"],
        "date_order": [("lease_start_date", "lease_end_date")],
    },
    {
        "name": "rent_payments",
        "primary_key": ["payment_id"],
        "required": ["payment_id", "lease_id"],
        "types": {
            "payment_id": "long", "lease_id": "long", "payment_date": "date",
            "amount_due": "decimal", "amount_paid": "decimal",
        },
        "non_negative": ["amount_due", "amount_paid"],
    },
    {
        "name": "maintenance_requests",
        "primary_key": ["request_id"],
        "required": ["request_id", "property_id"],
        "types": {
            "request_id": "long", "property_id": "long", "request_date": "date",
            "completed_date": "date", "estimated_cost": "decimal", "actual_cost": "decimal",
        },
        "non_negative": ["estimated_cost", "actual_cost"],
        "date_order": [("request_date", "completed_date")],
    },
    {
        "name": "property_budget",
        "primary_key": ["property_id", "budget_year"],
        "required": ["property_id"],
        "types": {
            "property_id": "long", "budget_year": "long", "budget_month": "long",
            "budget_revenue": "decimal", "budget_expense": "decimal", "budget_amount": "decimal",
        },
        "non_negative": ["budget_revenue", "budget_expense", "budget_amount"],
    },
    {
        "name": "property_region_mapping",
        "primary_key": ["property_id"],
        "required": ["property_id"],
        "types": {"property_id": "long"},
    },
    {
        "name": "weather_api_raw",
        "primary_key": ["property_id", "observation_timestamp"],
        "required": ["property_id", "observation_timestamp"],
        "types": {
            "property_id": "long", "observation_timestamp": "timestamp",
            "main_temp": "decimal", "main_feels_like": "decimal",
            "main_humidity": "decimal", "wind_speed": "decimal",
        },
        "non_negative": ["main_humidity", "wind_speed"],
    },
]


def snake_case(name: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9]+", "_", value)).strip("_").lower()


def spark_type(type_name: str):
    return {
        "string": StringType(),
        "long": LongType(),
        "decimal": DecimalType(18, 2),
        "date": "date",
        "timestamp": TimestampType(),
    }[type_name]


def add_reason(frame: DataFrame, condition, reason: str) -> DataFrame:
    return frame.withColumn(
        "_rejection_reasons",
        F.when(condition, F.array_union("_rejection_reasons", F.array(F.lit(reason))))
        .otherwise(F.col("_rejection_reasons")),
    )


def transform_table(config: dict) -> tuple[DataFrame, DataFrame, dict]:
    name = config["name"]
    frame = spark.table(f"bronze.{name}")
    frame = frame.select([F.col(column).alias(snake_case(column)) for column in frame.columns])

    missing = sorted(set(config.get("required", [])) - set(frame.columns))
    if missing:
        raise ValueError(f"bronze.{name} is missing required columns: {missing}")

    frame = frame.withColumn("_rejection_reasons", F.array().cast("array<string>"))
    for column, type_name in config.get("types", {}).items():
        if column not in frame.columns:
            continue
        original = F.col(column)
        converted = original.cast(spark_type(type_name))
        frame = add_reason(
            frame,
            original.isNotNull() & converted.isNull(),
            f"invalid_{type_name}:{column}",
        ).withColumn(column, converted)

    for column in config.get("required", []):
        frame = add_reason(frame, F.col(column).isNull(), f"required:{column}")
    for column in config.get("non_negative", []):
        if column in frame.columns:
            frame = add_reason(frame, F.col(column) < 0, f"negative_value:{column}")
    for start, end in config.get("date_order", []):
        if start in frame.columns and end in frame.columns:
            frame = add_reason(
                frame,
                F.col(start).isNotNull() & F.col(end).isNotNull() & (F.col(end) < F.col(start)),
                f"date_order:{start}>{end}",
            )

    rejected = (
        frame.filter(F.size("_rejection_reasons") > 0)
        .withColumn("data_quality_status", F.lit("REJECTED"))
        .withColumn("rejection_reason", F.concat_ws(" | ", "_rejection_reasons"))
        .withColumn("silver_processed_timestamp", F.current_timestamp())
        .withColumn("rejected_source_object", F.lit(name))
        .drop("_rejection_reasons")
    )
    valid = frame.filter(F.size("_rejection_reasons") == 0).drop("_rejection_reasons")

    order_columns = []
    if "ingestion_timestamp" in valid.columns:
        order_columns.append(F.col("ingestion_timestamp").desc_nulls_last())
    if "updated_at" in valid.columns:
        order_columns.append(F.col("updated_at").desc_nulls_last())
    order_columns.append(F.monotonically_increasing_id().desc())
    window = Window.partitionBy(*config["primary_key"]).orderBy(*order_columns)
    valid = (
        valid.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .withColumn("data_quality_status", F.lit("VALID"))
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )

    metrics = {
        "table": name,
        "rows_read": frame.count(),
        "rows_valid": valid.count(),
        "rows_rejected": rejected.count(),
    }
    return valid, rejected, metrics


spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_quarantine")

all_rejected = []
run_metrics = []
for table_config in TABLES:
    silver_frame, rejected_frame, table_metrics = transform_table(table_config)
    silver_frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"silver.{table_config['name']}"
    )
    all_rejected.append(rejected_frame)
    run_metrics.append(table_metrics)

if all_rejected:
    rejected_union = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), all_rejected)
    rejected_union.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "silver_quarantine.rejected_records"
    )

display(spark.createDataFrame(run_metrics))
