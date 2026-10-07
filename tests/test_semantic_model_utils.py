import json

import pandas as pd

from src.semantic_model_utils import parse_measure_names, validate_semantic_model


def test_parse_measure_names(tmp_path):
    dax = tmp_path / "measures.dax"
    dax.write_text(
        "DEFINE\n"
        "MEASURE 'fact_sales'[Total Sales] = SUM('fact_sales'[amount])\n"
        "MEASURE 'fact_sales'[Order Count] = COUNTROWS('fact_sales')\n",
        encoding="utf-8",
    )
    assert parse_measure_names(dax) == {"Total Sales", "Order Count"}


def test_semantic_contract_passes_for_valid_star_schema(tmp_path):
    gold = tmp_path / "gold"
    gold.mkdir()
    pd.DataFrame([{"property_key": 0}, {"property_key": 101}]).to_csv(gold / "dim_property.csv", index=False)
    pd.DataFrame([{"payment_key": 1, "property_key": 101, "amount": 50}]).to_csv(
        gold / "fact_payment.csv", index=False
    )
    dax = tmp_path / "measures.dax"
    dax.write_text("DEFINE\nMEASURE 'fact_payment'[Total Paid] = SUM('fact_payment'[amount])\n", encoding="utf-8")
    config = {
        "model_name": "Test Model",
        "tables": {
            "dim_property": {"key": "property_key", "required_columns": ["property_key"]},
            "fact_payment": {"key": "payment_key", "required_columns": ["payment_key", "property_key", "amount"]},
        },
        "relationships": [{
            "name": "Property to Payment",
            "from_table": "dim_property", "from_column": "property_key",
            "to_table": "fact_payment", "to_column": "property_key", "active": True,
        }],
        "required_measures": ["Total Paid"],
    }
    report = validate_semantic_model(config, gold, dax)
    assert report.passed
    assert report.table_rows["fact_payment"] == 1


def test_semantic_contract_reports_orphan_keys_and_missing_measures(tmp_path):
    gold = tmp_path / "gold"
    gold.mkdir()
    pd.DataFrame([{"property_key": 0}, {"property_key": 101}]).to_csv(gold / "dim_property.csv", index=False)
    pd.DataFrame([{"payment_key": 1, "property_key": 999}]).to_csv(gold / "fact_payment.csv", index=False)
    dax = tmp_path / "measures.dax"
    dax.write_text("DEFINE\n", encoding="utf-8")
    config = {
        "model_name": "Test Model",
        "tables": {
            "dim_property": {"key": "property_key", "required_columns": ["property_key"]},
            "fact_payment": {"key": "payment_key", "required_columns": ["payment_key", "property_key"]},
        },
        "relationships": [{
            "name": "Property to Payment",
            "from_table": "dim_property", "from_column": "property_key",
            "to_table": "fact_payment", "to_column": "property_key", "active": True,
        }],
        "required_measures": ["Total Paid"],
    }
    report = validate_semantic_model(config, gold, dax)
    assert not report.passed
    assert any("orphan keys" in issue for issue in report.issues)
    assert any("Missing required DAX measures" in issue for issue in report.issues)


def _write_star(tmp_path, fact_rows, table_overrides=None, **config_overrides):
    gold = tmp_path / "gold"
    gold.mkdir()
    pd.DataFrame([{"property_key": 0}, {"property_key": 101}]).to_csv(gold / "dim_property.csv", index=False)
    pd.DataFrame(fact_rows).to_csv(gold / "fact_payment.csv", index=False)
    dax = tmp_path / "measures.dax"
    dax.write_text("DEFINE\nMEASURE 'fact_payment'[Total Paid] = SUM('fact_payment'[amount])\n", encoding="utf-8")
    fact_config = {"key": "payment_key", "required_columns": ["payment_key", "property_key", "amount"]}
    fact_config.update(table_overrides or {})
    config = {
        "model_name": "Test Model",
        "unknown_member_key": 0,
        "tables": {
            "dim_property": {"key": "property_key", "required_columns": ["property_key"]},
            "fact_payment": fact_config,
        },
        "relationships": [{
            "name": "Property to Payment",
            "from_table": "dim_property", "from_column": "property_key",
            "to_table": "fact_payment", "to_column": "property_key", "active": True,
        }],
        "required_measures": ["Total Paid"],
        **config_overrides,
    }
    return validate_semantic_model(config, gold, dax)


def test_semantic_contract_flags_facts_resolving_to_unknown_member(tmp_path):
    report = _write_star(
        tmp_path,
        [{"payment_key": 1, "property_key": 0, "amount": 50}, {"payment_key": 2, "property_key": 101, "amount": 50}],
        max_unknown_member_ratio=0.1,
    )
    assert any("Unknown member" in issue and "50.0%" in issue for issue in report.issues)


def test_semantic_contract_tolerates_unknown_members_within_threshold(tmp_path):
    report = _write_star(
        tmp_path,
        [{"payment_key": 1, "property_key": 0, "amount": 50}, {"payment_key": 2, "property_key": 101, "amount": 50}],
        max_unknown_member_ratio=0.5,
    )
    assert report.passed


def test_semantic_contract_flags_null_foreign_keys_on_active_relationship(tmp_path):
    report = _write_star(
        tmp_path,
        [{"payment_key": 1, "property_key": None, "amount": 50}, {"payment_key": 2, "property_key": 101, "amount": 50}],
    )
    assert any("null foreign keys" in issue for issue in report.issues)


def test_semantic_contract_flags_measure_columns_that_are_all_zero(tmp_path):
    report = _write_star(
        tmp_path,
        [{"payment_key": 1, "property_key": 101, "amount": 0}, {"payment_key": 2, "property_key": 101, "amount": None}],
        table_overrides={"non_empty_columns": ["amount", "absent_measure"]},
    )
    assert any("fact_payment.amount has no non-zero values" in issue for issue in report.issues)
    assert any("fact_payment.absent_measure" in issue for issue in report.issues)
