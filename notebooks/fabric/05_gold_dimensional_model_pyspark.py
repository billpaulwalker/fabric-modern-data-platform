# Fabric notebook source
# Default Lakehouse: lh_cre_gold (schema-enabled). Reads lh_cre_silver; writes conformed
# dimensions to the shared schema and facts to their domain schemas.
# Upload config/fabric_layout.json and config/gold_model_config.json to its Files/config/.

from functools import reduce
import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CONFIG_DIRECTORY = "/lakehouse/default/Files/config"
UNKNOWN_KEY = 0
GRAINS = {
    "dim_property": ["property_id"],
    "dim_tenant": ["tenant_id"],
    "dim_date": ["date_key"],
    "fact_lease": ["lease_id"],
    "fact_rent_payment": ["payment_id"],
    "fact_maintenance_request": ["request_id"],
    "fact_property_budget": ["property_key", "budget_year", "budget_month"],
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


def existing(frame: DataFrame, columns: list) -> list:
    return [column for column in columns if column in frame.columns]


def stable_key(namespace: str, columns: list):
    values = [F.lit(namespace), *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in columns]]
    return F.pmod(F.xxhash64(*values), F.lit(9223372036854775807)).cast("long")


def date_key(frame: DataFrame, column: str):
    if column not in frame.columns:
        return F.lit(None).cast("long")
    return F.date_format(F.to_date(F.col(column)), "yyyyMMdd").cast("long")


def measure(frame: DataFrame, column: str):
    """Missing values read as zero; a column absent from the source stays null rather than reading as zero."""
    if column not in frame.columns:
        return F.lit(None).cast("decimal(18,2)")
    return F.coalesce(F.col(column).cast("decimal(18,2)"), F.lit(0).cast("decimal(18,2)"))


def add_unknown_member(frame: DataFrame, key_column: str, business_key: str, label_column: str) -> DataFrame:
    expressions = []
    for field in frame.schema.fields:
        if field.name == key_column:
            value = F.lit(UNKNOWN_KEY)
        elif field.name == business_key:
            value = F.lit(-1)
        elif field.name == label_column:
            value = F.lit("Unknown")
        elif field.name == "is_current":
            value = F.lit(True)
        else:
            value = F.lit(None)
        expressions.append(value.cast(field.dataType).alias(field.name))
    unknown = frame.sparkSession.range(1).select(*expressions)
    return unknown.unionByName(frame)


def lookup_key(fact: DataFrame, dimension: DataFrame, natural_key: str, surrogate_key: str) -> DataFrame:
    if natural_key not in fact.columns:
        return fact.withColumn(surrogate_key, F.lit(UNKNOWN_KEY).cast("long"))
    lookup = dimension.select(natural_key, surrogate_key).dropDuplicates([natural_key])
    return fact.join(lookup, natural_key, "left").fillna({surrogate_key: UNKNOWN_KEY})


def assert_unique_grain(name: str, frame: DataFrame, grain: list) -> None:
    duplicates = frame.groupBy(*grain).count().filter(F.col("count") > 1).count()
    if duplicates:
        raise ValueError(f"{name} violates its declared grain {grain}: {duplicates} duplicate keys")


# Conformed dimensions: current-state (Type 1) for this portfolio phase.
def build_dim_property(properties: DataFrame, regions) -> DataFrame:
    columns = [
        "property_id", "property_name", "property_type", "city", "state", "postal_code",
        "square_feet", "property_status", "acquired_date", "updated_at",
    ]
    frame = properties.select(*existing(properties, columns))
    if regions is not None and "property_id" in regions.columns:
        region_columns = existing(regions, ["property_id", "region", "market"])
        frame = frame.join(regions.select(*region_columns).dropDuplicates(["property_id"]), "property_id", "left")
    frame = (
        frame.dropDuplicates(["property_id"])
        .withColumn("property_key", stable_key("property", ["property_id"]))
        .withColumn("is_current", F.lit(True))
    )
    return add_unknown_member(frame, "property_key", "property_id", "property_name")


def build_dim_tenant(tenants: DataFrame) -> DataFrame:
    columns = ["tenant_id", "tenant_name", "industry", "tenant_status", "tenant_start_date", "updated_at"]
    frame = (
        tenants.select(*existing(tenants, columns))
        .dropDuplicates(["tenant_id"])
        .withColumn("tenant_key", stable_key("tenant", ["tenant_id"]))
        .withColumn("is_current", F.lit(True))
    )
    return add_unknown_member(frame, "tenant_key", "tenant_id", "tenant_name")


def build_dim_date(spark, start_date: str, end_date: str) -> DataFrame:
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date"
        )
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("long"))
        .withColumn("calendar_year", F.year("full_date"))
        .withColumn("calendar_quarter", F.quarter("full_date"))
        .withColumn("calendar_month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("iso_week", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    )


# Lease fact: one row per lease.
def build_fact_lease(leases: DataFrame, dim_property: DataFrame, dim_tenant: DataFrame) -> DataFrame:
    frame = lookup_key(leases, dim_property, "property_id", "property_key")
    frame = lookup_key(frame, dim_tenant, "tenant_id", "tenant_key")
    frame = (
        frame.withColumn("lease_key", stable_key("lease", ["lease_id"]))
        .withColumn("lease_start_date_key", date_key(frame, "lease_start_date"))
        .withColumn("lease_end_date_key", date_key(frame, "lease_end_date"))
        .withColumn("monthly_rent", measure(frame, "monthly_rent"))
        .withColumn("annualized_rent", F.col("monthly_rent") * F.lit(12))
    )
    columns = [
        "lease_key", "lease_id", "property_key", "tenant_key", "lease_start_date_key",
        "lease_end_date_key", "monthly_rent", "annualized_rent", "lease_status",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    return frame.select(*existing(frame, columns))


# Rent-payment fact: payment-level IDs win; the lease fills them only where the payment has none.
def build_fact_rent_payment(
    payments: DataFrame, leases: DataFrame, dim_property: DataFrame, dim_tenant: DataFrame
) -> DataFrame:
    frame = payments
    lease_columns = existing(leases, ["lease_id", "property_id", "tenant_id"])
    if "lease_id" in frame.columns and "lease_id" in lease_columns:
        bridge = leases.select(
            "lease_id", *[F.col(column).alias(f"{column}_lease") for column in lease_columns[1:]]
        ).dropDuplicates(["lease_id"])
        frame = frame.join(bridge, "lease_id", "left")
        for column in lease_columns[1:]:
            lease_value = F.col(f"{column}_lease")
            value = F.coalesce(F.col(column), lease_value) if column in payments.columns else lease_value
            frame = frame.withColumn(column, value).drop(f"{column}_lease")
    frame = lookup_key(frame, dim_property, "property_id", "property_key")
    frame = lookup_key(frame, dim_tenant, "tenant_id", "tenant_key")
    frame = (
        frame.withColumn("payment_key", stable_key("rent_payment", ["payment_id"]))
        .withColumn("payment_date_key", date_key(frame, "payment_date"))
        .withColumn("amount_due", measure(frame, "amount_due"))
        .withColumn("amount_paid", measure(frame, "amount_paid"))
        .withColumn("outstanding_amount", F.greatest(F.col("amount_due") - F.col("amount_paid"), F.lit(0)))
        .withColumn(
            "collection_rate",
            F.when(F.col("amount_due") == 0, F.lit(0.0)).otherwise(F.col("amount_paid") / F.col("amount_due")),
        )
    )
    columns = [
        "payment_key", "payment_id", "lease_id", "property_key", "tenant_key", "payment_date_key",
        "amount_due", "amount_paid", "outstanding_amount", "collection_rate", "payment_status",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    return frame.select(*existing(frame, columns))


# Maintenance fact: one row per service request.
def build_fact_maintenance(maintenance: DataFrame, dim_property: DataFrame) -> DataFrame:
    frame = maintenance
    if "status" not in frame.columns:
        status_source = next(
            (column for column in ["request_status", "maintenance_status"] if column in frame.columns), None
        )
        if status_source:
            frame = frame.withColumnRenamed(status_source, "status")
    frame = lookup_key(frame, dim_property, "property_id", "property_key")
    resolution_days = (
        F.datediff(F.to_date("completed_date"), F.to_date("request_date"))
        if "completed_date" in frame.columns and "request_date" in frame.columns
        else F.lit(None).cast("int")
    )
    frame = (
        frame.withColumn("maintenance_key", stable_key("maintenance", ["request_id"]))
        .withColumn("request_date_key", date_key(frame, "request_date"))
        .withColumn("completed_date_key", date_key(frame, "completed_date"))
        .withColumn("estimated_cost", measure(frame, "estimated_cost"))
        .withColumn("actual_cost", measure(frame, "actual_cost"))
        .withColumn("cost_variance", F.col("actual_cost") - F.col("estimated_cost"))
        .withColumn("resolution_days", resolution_days)
    )
    columns = [
        "maintenance_key", "request_id", "property_key", "request_date_key", "completed_date_key",
        "category", "priority", "status", "estimated_cost", "actual_cost", "cost_variance",
        "resolution_days", "pipeline_run_id", "silver_processed_timestamp",
    ]
    return frame.select(*existing(frame, columns))


# Property-budget fact: one row per property and monthly budget period.
def build_fact_property_budget(budget: DataFrame, dim_property: DataFrame) -> DataFrame:
    aliases = {
        "budget_year": ["year", "fiscal_year"],
        "budget_month": ["month", "fiscal_month"],
        "budget_revenue": ["budgeted_rent", "budgeted_revenue", "revenue_budget"],
        "budget_expense": ["budgeted_expense", "budgeted_expenses", "expense_budget"],
        "budget_maintenance": ["budgeted_maintenance"],
        "budget_operating_expense": ["budgeted_operating_expense"],
        "budget_amount": ["budgeted_amount"],
    }
    frame = budget
    for canonical, candidates in aliases.items():
        if canonical not in frame.columns:
            source = next((column for column in candidates if column in frame.columns), None)
            if source:
                frame = frame.withColumnRenamed(source, canonical)

    frame = lookup_key(frame, dim_property, "property_id", "property_key")
    period_column = next(
        (column for column in ["budget_period", "budget_date", "period_start", "month_start"] if column in frame.columns),
        None,
    )
    period_date = F.to_date(F.col(period_column)) if period_column else F.lit(None).cast("date")
    if "budget_month" in frame.columns:
        parsed_month_date = F.coalesce(
            F.to_date(F.col("budget_month")),
            F.to_date(F.concat(F.col("budget_month").cast("string"), F.lit("-01"))),
        )
        numeric_month = F.col("budget_month").cast("int")
        month = F.when(numeric_month.between(1, 12), numeric_month).otherwise(F.month(parsed_month_date))
    else:
        parsed_month_date = F.lit(None).cast("date")
        month = F.month(period_date)
    year_candidates = [F.year(period_date), F.year(parsed_month_date)]
    if "budget_year" in frame.columns:
        year_candidates.insert(0, F.col("budget_year").cast("int"))

    frame = (
        frame.withColumn("budget_year", F.coalesce(*year_candidates))
        .withColumn("budget_month", F.coalesce(month, F.lit(1)).cast("int"))
    )
    if frame.filter(F.col("budget_year").isNull()).limit(1).count():
        raise ValueError(
            "Unable to derive budget year. Expected budget_year, year, fiscal_year, budget_period, "
            f"budget_date, period_start, or a date-formatted budget_month. Available columns: {sorted(frame.columns)}"
        )
    frame = frame.withColumn(
        "budget_date_key", F.col("budget_year").cast("long") * 10000 + F.col("budget_month") * 100 + 1
    )
    for column in ["budget_revenue", "budget_expense", "budget_maintenance", "budget_operating_expense", "budget_amount"]:
        if column in frame.columns:
            frame = frame.withColumn(column, measure(frame, column))
    if "budget_expense" not in frame.columns:
        components = existing(frame, ["budget_maintenance", "budget_operating_expense"])
        if components:
            frame = frame.withColumn(
                "budget_expense", reduce(lambda left, right: left + right, [F.col(column) for column in components])
            )
    if "budget_expense" in frame.columns:
        frame = frame.withColumn("budget_expense", F.abs(F.col("budget_expense")))
    if "budget_revenue" in frame.columns and "budget_expense" in frame.columns:
        frame = frame.withColumn("budget_noi", F.col("budget_revenue") - F.col("budget_expense"))
    frame = frame.withColumn(
        "budget_key", stable_key("property_budget", existing(frame, ["property_id", "budget_year", "budget_month"]))
    )
    columns = [
        "budget_key", "property_key", "budget_date_key", "budget_year", "budget_month",
        "budget_revenue", "budget_maintenance", "budget_operating_expense",
        "budget_expense", "budget_noi", "budget_amount",
        "pipeline_run_id", "silver_processed_timestamp",
    ]
    return frame.select(*existing(frame, columns))


def build_models(spark, silver: dict, gold_config: dict) -> dict:
    dim_property = build_dim_property(silver["properties"], silver.get("property_region_mapping"))
    dim_tenant = build_dim_tenant(silver["tenants"])
    return {
        "dim_property": dim_property,
        "dim_tenant": dim_tenant,
        "dim_date": build_dim_date(spark, **gold_config["date_dimension"]),
        "fact_lease": build_fact_lease(silver["leases"], dim_property, dim_tenant),
        "fact_rent_payment": build_fact_rent_payment(silver["rent_payments"], silver["leases"], dim_property, dim_tenant),
        "fact_maintenance_request": build_fact_maintenance(silver["maintenance_requests"], dim_property),
        "fact_property_budget": build_fact_property_budget(silver["property_budget"], dim_property),
    }


def main(spark, config_directory: str = CONFIG_DIRECTORY) -> list:
    gold_config = load_config(f"{config_directory}/gold_model_config.json")
    layout = load_config(f"{config_directory}/fabric_layout.json")
    sources = ["properties", "property_region_mapping", "tenants", "leases", "rent_payments",
               "maintenance_requests", "property_budget"]
    silver = {name: spark.table(resolve_table(layout, "silver", name)) for name in sources}
    models = build_models(spark, silver, gold_config)
    for name, frame in models.items():
        assert_unique_grain(name, frame, GRAINS[name])

    metrics = []
    for name, frame in models.items():
        table = resolve_table(layout, "gold", name)
        create_schema_for(spark, table)
        write_table(frame, table)
        metrics.append({"model": name, "table": table, "rows_written": spark.table(table).count()})
    return metrics


if __name__ == "__main__":
    display(spark.createDataFrame(main(spark)))  # noqa: F821 - spark and display are Fabric notebook globals
