"""Keep Fabric item names consistent across notebooks, pipeline manifest, and deployment rules."""

import json
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FABRIC_NOTEBOOKS = {path.stem for path in (REPO_ROOT / "notebooks/fabric").glob("*.py")}
MEDALLION_CHAIN = [
    "nb_cre_bronze_ingest", "nb_cre_silver_transform", "nb_cre_gold_build_model", "nb_cre_gold_validate_model",
]
VALID_CONDITIONS = {"Succeeded", "Failed", "Skipped", "Completed"}
SHARED_DEFAULT_LAKEHOUSE = "lh_cre_bronze"


def _load(relative_path):
    return json.loads((REPO_ROOT / relative_path).read_text(encoding="utf-8"))


def _activities():
    return {activity["name"]: activity for activity in _load("pipelines/fabric-pipeline-manifest.json")["activities"]}


def _notebook_activities():
    return {name: activity for name, activity in _activities().items() if activity["type"] == "Notebook"}


def test_fabric_notebooks_follow_the_naming_standard():
    # nb_<project>_<layer>[_<source system | domain>]_<purpose>
    assert FABRIC_NOTEBOOKS == {*MEDALLION_CHAIN, "nb_cre_gold_log_pipeline_run"}


def test_pipeline_runs_the_medallion_chain_in_order_on_success():
    activities = _activities()
    for previous, current in zip(MEDALLION_CHAIN, MEDALLION_CHAIN[1:]):
        assert activities[current]["depends_on"] == [{"activity": previous, "conditions": ["Succeeded"]}]
    assert activities[MEDALLION_CHAIN[0]]["depends_on"] == []
    for name in MEDALLION_CHAIN:
        assert activities[name]["notebook"] == f"notebooks/fabric/{name}.py"


def test_semantic_model_refreshes_only_after_validation_succeeds():
    # The model's automatic Direct Lake updates are off, so reports see new Gold data only after validation.
    refresh = _activities()["refresh_semantic_model"]
    assert refresh["type"] == "SemanticModelRefresh"
    assert refresh["semantic_model"] == "CRE Portfolio Analytics"
    assert refresh["depends_on"] == [{"activity": MEDALLION_CHAIN[-1], "conditions": ["Succeeded"]}]


def test_pipeline_logs_every_run_exactly_once():
    # Any upstream failure skips the refresh, so Failed-or-Skipped on the last step catches every failure.
    activities = _activities()
    last = "refresh_semantic_model"
    succeeded, failed = activities["log_run_succeeded"], activities["log_run_failed"]
    assert succeeded["depends_on"] == [{"activity": last, "conditions": ["Succeeded"]}]
    assert failed["depends_on"] == [{"activity": last, "conditions": ["Failed", "Skipped"]}]
    for activity, status in [(succeeded, "Succeeded"), (failed, "Failed")]:
        assert activity["notebook"] == "notebooks/fabric/nb_cre_gold_log_pipeline_run.py"
        assert activity["base_parameters"]["status"] == status
        assert activity["base_parameters"]["pipeline_run_id"] == "@pipeline().RunId"


def test_every_activity_retries_once_and_notebooks_share_one_session():
    # Capacity throttling (HTTP 430) and refresh contention are transient, and every step is safe to rerun,
    # so one delayed retry is safe. A shared session tag lets high concurrency mode reuse one Spark session.
    for activity in _activities().values():
        assert activity["retry"] == 1, activity["name"]
        assert activity["retry_interval_seconds"] == 120, activity["name"]
    for activity in _notebook_activities().values():
        assert activity["session_tag"] == "cre_pipeline", activity["name"]


def test_every_pipeline_notebook_shares_one_default_lakehouse():
    # High concurrency reuses a Spark session only between notebooks with the same default lakehouse.
    # A change of default lakehouse forces a new session while the previous one still holds compute,
    # which a trial capacity rejects (HTTP 430). Tables are always addressed by their full names.
    assert {activity["default_lakehouse"] for activity in _notebook_activities().values()} == {SHARED_DEFAULT_LAKEHOUSE}


def test_pipeline_dependencies_and_notebooks_exist():
    activities = _activities()
    for activity in activities.values():
        for dependency in activity["depends_on"]:
            assert dependency["activity"] in activities
            assert set(dependency["conditions"]) <= VALID_CONDITIONS
    for activity in _notebook_activities().values():
        assert (REPO_ROOT / activity["notebook"]).exists()


def test_pipeline_parameters_match_notebook_parameter_cells():
    for activity in _activities().values():
        if not activity.get("base_parameters"):
            continue
        source = (REPO_ROOT / activity["notebook"]).read_text(encoding="utf-8")
        cell = re.search(r"# PARAMETERS CELL \*+\n(.*?)# CELL \*+", source, flags=re.DOTALL)
        assert cell, f"{activity['notebook']} has no parameter cell"
        declared = set(re.findall(r"^(\w+) = ", cell.group(1), flags=re.MULTILINE))
        assert set(activity.get("base_parameters", {})) <= declared


def test_deployment_rules_bind_every_fabric_notebook():
    rules = _load("deployment/deployment-rules.json")["rules"]
    lakehouse_rules = [rule for rule in rules if rule["property"] == "default_lakehouse"]
    assert {rule["artifact"] for rule in lakehouse_rules} == FABRIC_NOTEBOOKS
    for rule in lakehouse_rules:
        for stage in ["dev", "test", "prod"]:
            assert rule[stage] == f"ws-cre-modernization-{stage}/{SHARED_DEFAULT_LAKEHOUSE}", rule["artifact"]
