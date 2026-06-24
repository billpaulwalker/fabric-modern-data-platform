# Fabric notebook source
# Attach the schema-enabled project Lakehouse before running all cells.

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


DATE_START = "2020-01-01"
DATE_END = "2030-12-31"
UNKNOWN_KEY = 0


def silver(name: str) -> DataFrame:
    return spark.table(f"silver.{name}")


def existing(frame: DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in frame.columns]


def stable_key(namespace: str, columns: list[str]):
    values = [F.lit(namespace), *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in columns]]
    return F.pmod(F.xxhash64(*values), F.lit(9223372036854775807)).cast("long")


def date_key(column: str):
    return F.date_format(F.to_date(F.col(column)), "yyyyMMdd").cast("long")


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
    unknown = spark.range(1).select(*expressions)
    return unknown.unionByName(frame)


def lookup_key(fact: DataFrame, dimension: DataFrame, natural_key: str, surrogate_key: str) -> DataFrame:
    if natural_key not in fact.columns:
        return fact.withColumn(surrogate_key, F.lit(UNKNOWN_KEY).cast("long"))
    lookup = dimension.select(natural_key, surrogate_key).dropDuplicates([natural_key])
    return fact.join(lookup, natural_key, "left").fillna({surrogate_key: UNKNOWN_KEY})


def write_gold(name: str, frame: DataFrame) -> dict:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{name}")
    return {"model": name, "rows_written": frame.count()}


# Conformed dimensions: current-state (Type 1) for this portfolio phase.
property_columns = [
    "property_id", "property_name", "property_type", "city", "state", "postal_code",
    "square_feet", "property_status", "acquired_date", "updated_at",
]
dim_property = silver("properties").select(*existing(silver("properties"), property_columns))
regions = silver("property_region_mapping")
region_columns = existing(regions, ["property_id", "region", "market"])
if "property_id" in region_columns:
    dim_property = dim_property.join(regions.select(*region_columns).dropDuplicates(["property_id"]), "property_id", "left")
dim_property = (
    dim_property.dropDuplicates(["property_id"])
    .withColumn("property_key", stable_key("property", ["property_id"]))
    .withColumn("is_current", F.lit(True))
)
dim_property = add_unknown_member(dim_property, "property_key", "property_id", "property_name")

tenant_columns = ["tenant_id", "tenant_name", "industry", "tenant_status", "tenant_start_date", "updated_at"]
tenant_source = silver("tenants")
dim_tenant = (
    tenant_source.select(*existing(tenant_source, tenant_columns))
    .dropDuplicates(["tenant_id"])
    .withColumn("tenant_key", stable_key("tenant", ["tenant_id"]))
    .withColumn("is_current", F.lit(True))
)
dim_tenant = add_unknown_member(dim_tenant, "tenant_key", "tenant_id", "tenant_name")

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{DATE_START}'), to_date('{DATE_END}'), interval 1 day)) AS full_date")
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
leases = silver("leases")
fact_lease = lookup_key(leases, dim_property, "property_id", "property_key")
fact_lease = lookup_key(fact_lease, dim_tenant, "tenant_id", "tenant_key")
fact_lease = (
    fact_lease.withColumn("lease_key", stable_key("lease", ["lease_id"]))
    .withColumn("lease_start_date_key", date_key("lease_start_date"))
    .withColumn("lease_end_date_key", date_key("lease_end_date"))
    .withColumn("monthly_rent", F.coalesce(F.col("monthly_rent").cast("decimal(18,2)"), F.lit(0)))
    .withColumn("annualized_rent", F.col("monthly_rent") * F.lit(12))
)
fact_lease_columns = [
    "lease_key", "lease_id", "property_key", "tenant_key", "lease_start_date_key",
    "lease_end_date_key", "monthly_rent", "annualized_rent", "lease_status",
    "pipeline_run_id", "silver_processed_timestamp",
]
fact_lease = fact_lease.select(*existing(fact_lease, fact_lease_columns))


# Rent-payment fact: lease lookup supplies conformed property and tenant keys.
payments = silver("rent_payments")
lease_bridge_columns = existing(leases, ["lease_id", "property_id", "tenant_id"])
fact_payment = payments.join(leases.select(*lease_bridge_columns).dropDuplicates(["lease_id"]), "lease_id", "left")
fact_payment = lookup_key(fact_payment, dim_property, "property_id", "property_key")
fact_payment = lookup_key(fact_payment, dim_tenant, "tenant_id", "tenant_key")
fact_payment = (
    fact_payment.withColumn("payment_key", stable_key("rent_payment", ["payment_id"]))
    .withColumn("payment_date_key", date_key("payment_date"))
    .withColumn("amount_due", F.coalesce(F.col("amount_due").cast("decimal(18,2)"), F.lit(0)))
    .withColumn("amount_paid", F.coalesce(F.col("amount_paid").cast("decimal(18,2)"), F.lit(0)))
    .withColumn("outstanding_amount", F.greatest(F.col("amount_due") - F.col("amount_paid"), F.lit(0)))
    .withColumn(
        "collection_rate",
        F.when(F.col("amount_due") == 0, F.lit(0.0)).otherwise(F.col("amount_paid") / F.col("amount_due")),
    )
)
fact_payment_columns = [
    "payment_key", "payment_id", "lease_id", "property_key", "tenant_key", "payment_date_key",
    "amount_due", "amount_paid", "outstanding_amount", "collection_rate", "payment_status",
    "pipeline_run_id", "silver_processed_timestamp",
]
fact_payment = fact_payment.select(*existing(fact_payment, fact_payment_columns))


# Maintenance fact: one row per service request.
maintenance = silver("maintenance_requests")
if "status" not in maintenance.columns:
    maintenance_status = next(
        (column for column in ["request_status", "maintenance_status"] if column in maintenance.columns),
        None,
    )
    if maintenance_status:
        maintenance = maintenance.withColumnRenamed(maintenance_status, "status")
fact_maintenance = lookup_key(maintenance, dim_property, "property_id", "property_key")
completed_date_key = date_key("completed_date") if "completed_date" in fact_maintenance.columns else F.lit(None).cast("long")
resolution_days = (
    F.datediff("completed_date", "request_date")
    if "completed_date" in fact_maintenance.columns and "request_date" in fact_maintenance.columns
    else F.lit(None).cast("int")
)
fact_maintenance = (
    fact_maintenance.withColumn("maintenance_key", stable_key("maintenance", ["request_id"]))
    .withColumn("request_date_key", date_key("request_date"))
    .withColumn("completed_date_key", completed_date_key)
    .withColumn("estimated_cost", F.coalesce(F.col("estimated_cost").cast("decimal(18,2)"), F.lit(0)))
    .withColumn("actual_cost", F.coalesce(F.col("actual_cost").cast("decimal(18,2)"), F.lit(0)))
    .withColumn("cost_variance", F.col("actual_cost") - F.col("estimated_cost"))
    .withColumn("resolution_days", resolution_days)
)
maintenance_columns = [
    "maintenance_key", "request_id", "property_key", "request_date_key", "completed_date_key",
    "category", "priority", "status", "estimated_cost", "actual_cost", "cost_variance",
    "resolution_days", "pipeline_run_id", "silver_processed_timestamp",
]
fact_maintenance = fact_maintenance.select(*existing(fact_maintenance, maintenance_columns))


# Property-budget fact: one row per property and monthly budget period.
budget = silver("property_budget")
budget_aliases = {
    "budget_year": ["year", "fiscal_year"],
    "budget_month": ["month", "fiscal_month"],
    "budget_revenue": ["budgeted_rent", "budgeted_revenue", "revenue_budget"],
    "budget_expense": ["budgeted_expense", "budgeted_expenses", "expense_budget"],
    "budget_maintenance": ["budgeted_maintenance"],
    "budget_operating_expense": ["budgeted_operating_expense"],
    "budget_amount": ["budgeted_amount"],
}
for canonical, candidates in budget_aliases.items():
    if canonical not in budget.columns:
        source = next((column for column in candidates if column in budget.columns), None)
        if source:
            budget = budget.withColumnRenamed(source, canonical)

fact_budget = lookup_key(budget, dim_property, "property_id", "property_key")
period_column = next(
    (column for column in ["budget_period", "budget_date", "period_start", "month_start"] if column in fact_budget.columns),
    None,
)
period_date = F.to_date(F.col(period_column)) if period_column else F.lit(None).cast("date")

if "budget_year" in fact_budget.columns:
    year_expression = F.coalesce(F.col("budget_year").cast("int"), F.year(period_date))
elif period_column:
    year_expression = F.year(period_date)
elif "budget_month" in fact_budget.columns:
    parsed_month_date = F.coalesce(
        F.to_date(F.col("budget_month")),
        F.to_date(F.concat(F.col("budget_month").cast("string"), F.lit("-01"))),
    )
    year_expression = F.year(parsed_month_date)
else:
    raise ValueError(f"Unable to derive budget year. Available columns: {sorted(fact_budget.columns)}")

if "budget_month" in fact_budget.columns:
    parsed_month_date = F.coalesce(
        F.to_date(F.col("budget_month")),
        F.to_date(F.concat(F.col("budget_month").cast("string"), F.lit("-01"))),
    )
    numeric_month = F.col("budget_month").cast("int")
    month_expression = F.when(numeric_month.between(1, 12), numeric_month).otherwise(F.month(parsed_month_date))
else:
    month_expression = F.month(period_date)

fact_budget = (
    fact_budget.withColumn("budget_year", year_expression)
    .withColumn("budget_month", F.coalesce(month_expression, F.lit(1)))
)
budget_grain = existing(fact_budget, ["property_id", "budget_year", "budget_month"])
fact_budget = (
    fact_budget.withColumn("budget_key", stable_key("property_budget", budget_grain))
    .withColumn("budget_date_key", F.col("budget_year").cast("long") * 10000 + month_expression * 100 + 1)
)
for measure in [
    "budget_revenue", "budget_expense", "budget_maintenance",
    "budget_operating_expense", "budget_amount",
]:
    if measure in fact_budget.columns:
        fact_budget = fact_budget.withColumn(measure, F.coalesce(F.col(measure).cast("decimal(18,2)"), F.lit(0)))
if "budget_expense" not in fact_budget.columns:
    expense_components = [
        column for column in ["budget_maintenance", "budget_operating_expense"]
        if column in fact_budget.columns
    ]
    if expense_components:
        fact_budget = fact_budget.withColumn(
            "budget_expense",
            reduce(lambda left, right: left + right, [F.col(column) for column in expense_components]),
        )
if "budget_expense" in fact_budget.columns:
    fact_budget = fact_budget.withColumn("budget_expense", F.abs(F.col("budget_expense")))
if "budget_revenue" in fact_budget.columns and "budget_expense" in fact_budget.columns:
    fact_budget = fact_budget.withColumn("budget_noi", F.col("budget_revenue") - F.col("budget_expense"))
budget_columns = [
    "budget_key", "property_key", "budget_date_key", "budget_year", "budget_month",
    "budget_revenue", "budget_maintenance", "budget_operating_expense",
    "budget_expense", "budget_noi", "budget_amount",
    "pipeline_run_id", "silver_processed_timestamp",
]
fact_budget = fact_budget.select(*existing(fact_budget, budget_columns))


spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
metrics = [
    write_gold("dim_property", dim_property),
    write_gold("dim_tenant", dim_tenant),
    write_gold("dim_date", dim_date),
    write_gold("fact_lease", fact_lease),
    write_gold("fact_rent_payment", fact_payment),
    write_gold("fact_maintenance_request", fact_maintenance),
    write_gold("fact_property_budget", fact_budget),
]
display(spark.createDataFrame(metrics))
