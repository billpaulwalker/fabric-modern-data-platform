# Fabric notebook source
# Default Lakehouse: lh_cre_bronze, shared by every pipeline notebook so a run reuses one Spark session;
# its Files/config/ holds all config. Tables are read and written by full lakehouse.schema.table name.
# Reads the lh_cre_gold tables only. Needs config/fabric_layout.json and config/semantic_model_config.json.
# Run after the Gold notebook; a failure here should stop the semantic-model refresh.
# DAX measure names are validated in repository CI, where the measures file lives.

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CONFIG_DIRECTORY = "/lakehouse/default/Files/config"


def load_config(path) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def resolve_table(layout: dict, layer: str, name: str) -> str:
    """Return lakehouse.schema.table for a logical table, as declared in fabric_layout.json."""
    location = layout["tables"].get(layer, {}).get(name)
    if location is None:
        raise ValueError(f"{layer} table {name!r} is not declared in fabric_layout.json")
    return f"{layout['lakehouses'][layer]}.{location}"


class ValidationReport:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.checks_run = 0
        self.issues = []
        self.table_rows = {}

    def check(self, condition: bool, message: str) -> None:
        self.checks_run += 1
        if not condition:
            self.issues.append(message)

    def as_dict(self) -> dict:
        return {
            "model_name": self.model_name,
            "passed": not self.issues,
            "checks_run": self.checks_run,
            "issues": self.issues,
            "table_rows": self.table_rows,
        }


def _has_rows(frame: DataFrame) -> bool:
    return frame.limit(1).count() > 0


def validate_gold_tables(tables: dict, config: dict) -> dict:
    """Apply the semantic-model contract from semantic_model_config.json to Gold DataFrames."""
    report = ValidationReport(config["model_name"])

    for table_name, table_config in config["tables"].items():
        report.check(table_name in tables, f"Missing Gold table: {table_name}")
        if table_name not in tables:
            continue
        frame = tables[table_name]
        report.table_rows[table_name] = frame.count()
        missing = sorted(set(table_config["required_columns"]) - set(frame.columns))
        report.check(not missing, f"{table_name} missing required columns: {missing}")
        key = table_config["key"]
        if key in frame.columns:
            report.check(not _has_rows(frame.filter(F.col(key).isNull())), f"{table_name}.{key} contains null values")
            report.check(
                frame.select(key).distinct().count() == report.table_rows[table_name],
                f"{table_name}.{key} is not unique",
            )
        for column in table_config.get("non_empty_columns", []):
            non_zero = (
                _has_rows(frame.filter(F.coalesce(F.col(column).try_cast("double"), F.lit(0.0)) != 0))
                if column in frame.columns else False
            )
            report.check(
                non_zero, f"{table_name}.{column} has no non-zero values; check the source-to-model column mapping"
            )

    unknown_key = config.get("unknown_member_key", 0)
    max_unknown_ratio = float(config.get("max_unknown_member_ratio", 0.0))
    for relationship in config["relationships"]:
        one_table, many_table = relationship["from_table"], relationship["to_table"]
        if one_table not in tables or many_table not in tables:
            continue
        one_column, many_column = relationship["from_column"], relationship["to_column"]
        one, many = tables[one_table], tables[many_table]
        if one_column not in one.columns or many_column not in many.columns:
            report.check(False, f"Relationship {relationship['name']} references a missing column")
            continue

        one_keys = one.select(F.col(one_column).alias("_key")).dropna().distinct()
        many_keys = many.select(F.col(many_column).alias("_key"))
        orphans = [row["_key"] for row in many_keys.dropna().distinct().join(one_keys, "_key", "left_anti").limit(10).collect()]
        report.check(not orphans, f"Relationship {relationship['name']} has orphan keys: {sorted(orphans)}")

        if relationship.get("active", True):
            null_count = many_keys.filter(F.col("_key").isNull()).count()
            report.check(
                null_count == 0,
                f"Relationship {relationship['name']} has {null_count} null foreign keys in {many_table}.{many_column}",
            )
        many_rows = report.table_rows.get(many_table, 0)
        if many_rows and _has_rows(one_keys.filter(F.col("_key") == unknown_key)):
            unknown_ratio = many_keys.filter(F.col("_key") == unknown_key).count() / many_rows
            report.check(
                unknown_ratio <= max_unknown_ratio,
                f"Relationship {relationship['name']}: {unknown_ratio:.1%} of {many_table} rows resolve to the "
                f"Unknown member (limit {max_unknown_ratio:.1%})",
            )

    return report.as_dict()


def main(spark, config_directory: str = CONFIG_DIRECTORY) -> dict:
    config = load_config(f"{config_directory}/semantic_model_config.json")
    layout = load_config(f"{config_directory}/fabric_layout.json")
    table_names = {name: resolve_table(layout, "gold", name) for name in config["tables"]}
    tables = {
        name: spark.table(table_name)
        for name, table_name in table_names.items()
        if spark.catalog.tableExists(table_name)
    }
    report = validate_gold_tables(tables, config)
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise ValueError(f"Semantic model validation failed: {report['issues']}")
    return report


if __name__ == "__main__":
    main(spark)  # noqa: F821 - spark is a Fabric notebook global
