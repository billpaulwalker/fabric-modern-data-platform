import pandas as pd
import pytest

from src.gold_utils import (
    UNKNOWN_KEY,
    GoldModel,
    build_dim_date,
    build_dim_property,
    build_dim_tenant,
    build_fact_lease,
    build_fact_maintenance,
    build_fact_property_budget,
    build_fact_rent_payment,
    deterministic_key,
    validate_model,
)


def test_deterministic_key_is_stable_and_nonzero():
    first = deterministic_key("property", 101)
    assert first == deterministic_key("property", 101)
    assert first != deterministic_key("property", 102)
    assert first > 0


def test_date_dimension_has_expected_grain_and_attributes():
    model = build_dim_date("2026-01-01", "2026-01-03")
    assert model.frame["date_key"].tolist() == [20260101, 20260102, 20260103]
    assert model.frame.loc[0, "calendar_year"] == 2026
    assert model.frame.loc[0, "calendar_quarter"] == 1


def test_dimensions_include_unknown_members():
    properties = pd.DataFrame([{"property_id": 101, "property_name": "Harbor Center"}])
    tenants = pd.DataFrame([{"tenant_id": 201, "tenant_name": "Northwind"}])
    property_model = build_dim_property(properties)
    tenant_model = build_dim_tenant(tenants)
    assert UNKNOWN_KEY in property_model.frame["property_key"].values
    assert UNKNOWN_KEY in tenant_model.frame["tenant_key"].values


def test_lease_fact_resolves_dimension_keys_and_calculates_annual_rent():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101, "property_name": "Harbor Center"}]))
    tenants = build_dim_tenant(pd.DataFrame([{"tenant_id": 201, "tenant_name": "Northwind"}]))
    leases = pd.DataFrame([{
        "lease_id": 301, "property_id": 101, "tenant_id": 201,
        "lease_start_date": "2026-01-01", "lease_end_date": "2026-12-31",
        "monthly_rent": "2500.00", "lease_status": "Active",
    }])
    fact = build_fact_lease(leases, properties.frame, tenants.frame).frame
    assert fact.loc[0, "property_key"] != UNKNOWN_KEY
    assert fact.loc[0, "tenant_key"] != UNKNOWN_KEY
    assert fact.loc[0, "annualized_rent"] == 30000
    assert fact.loc[0, "lease_start_date_key"] == 20260101


def test_unknown_dimension_lookup_uses_zero_key():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    tenants = build_dim_tenant(pd.DataFrame([{"tenant_id": 201}]))
    leases = pd.DataFrame([{"lease_id": 301, "property_id": 999, "tenant_id": 888}])
    fact = build_fact_lease(leases, properties.frame, tenants.frame).frame
    assert fact.loc[0, "property_key"] == UNKNOWN_KEY
    assert fact.loc[0, "tenant_key"] == UNKNOWN_KEY


def test_rent_payment_calculations():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    tenants = build_dim_tenant(pd.DataFrame([{"tenant_id": 201}]))
    leases = pd.DataFrame([{"lease_id": 301, "property_id": 101, "tenant_id": 201}])
    payments = pd.DataFrame([{
        "payment_id": 401, "lease_id": 301, "payment_date": "2026-02-01",
        "amount_due": "2500", "amount_paid": "2000",
    }])
    fact = build_fact_rent_payment(payments, leases, properties.frame, tenants.frame).frame
    assert fact.loc[0, "outstanding_amount"] == 500
    assert fact.loc[0, "collection_rate"] == 0.8


def test_open_maintenance_request_without_completed_date_has_null_resolution_days():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    requests = pd.DataFrame([{
        "request_id": 501,
        "property_id": 101,
        "request_date": "2026-06-01",
        "status": "Open",
    }])
    fact = build_fact_maintenance(requests, properties.frame).frame
    assert pd.isna(fact.loc[0, "completed_date_key"])
    assert pd.isna(fact.loc[0, "resolution_days"])


def test_maintenance_request_status_alias_is_standardized():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    requests = pd.DataFrame([{
        "request_id": 501,
        "property_id": 101,
        "request_date": "2026-06-01",
        "request_status": "Open",
    }])
    fact = build_fact_maintenance(requests, properties.frame).frame
    assert fact.loc[0, "status"] == "Open"


def test_budget_fact_derives_year_and_month_from_budget_period():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    budget = pd.DataFrame([{
        "property_id": 101,
        "budget_period": "2026-02-01",
        "budgeted_revenue": "3000",
        "budgeted_expense": "1000",
    }])
    fact = build_fact_property_budget(budget, properties.frame).frame
    assert fact.loc[0, "budget_year"] == 2026
    assert fact.loc[0, "budget_month"] == 2
    assert fact.loc[0, "budget_date_key"] == 20260201
    assert fact.loc[0, "budget_noi"] == 2000


def test_budget_expense_is_normalized_to_positive_magnitude():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    budget = pd.DataFrame([{
        "property_id": 101,
        "budget_period": "2026-02-01",
        "budgeted_revenue": "3000",
        "budgeted_expense": "-1000",
    }])
    fact = build_fact_property_budget(budget, properties.frame).frame
    assert fact.loc[0, "budget_expense"] == 1000
    assert fact.loc[0, "budget_noi"] == 2000


def test_portfolio_budget_columns_map_to_canonical_measures():
    properties = build_dim_property(pd.DataFrame([{"property_id": 101}]))
    budget = pd.DataFrame([{
        "property_id": 101,
        "budget_month": "2026-06-01",
        "budgeted_rent": 34619,
        "budgeted_maintenance": 12045,
        "budgeted_operating_expense": 1342,
    }])
    fact = build_fact_property_budget(budget, properties.frame).frame
    assert fact.loc[0, "budget_revenue"] == 34619
    assert fact.loc[0, "budget_expense"] == 13387
    assert fact.loc[0, "budget_noi"] == 21232


def test_model_validation_rejects_duplicate_grain():
    model = GoldModel("fact_test", pd.DataFrame([{"id": 1}, {"id": 1}]), ["id"])
    with pytest.raises(ValueError, match="declared grain"):
        validate_model(model)
