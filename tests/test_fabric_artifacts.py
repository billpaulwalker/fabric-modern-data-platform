"""Keep Fabric item names consistent across notebooks, pipeline manifest, and deployment rules."""

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FABRIC_NOTEBOOKS = {path.stem for path in (REPO_ROOT / "notebooks/fabric").glob("*.py")}


def _load(relative_path):
    return json.loads((REPO_ROOT / relative_path).read_text(encoding="utf-8"))


def test_fabric_notebooks_follow_the_naming_standard():
    # nb_<project>_<layer>[_<source system | domain>]_<purpose>
    assert FABRIC_NOTEBOOKS == {
        "nb_cre_bronze_ingest",
        "nb_cre_silver_transform",
        "nb_cre_gold_build_model",
        "nb_cre_gold_validate_model",
    }


def test_pipeline_manifest_runs_each_fabric_notebook_once_in_layer_order():
    activities = _load("pipelines/fabric-pipeline-manifest.json")["activities"]
    notebooks = [activity for activity in activities if activity["type"] == "Notebook"]
    assert [activity["name"] for activity in notebooks] == [
        "nb_cre_bronze_ingest", "nb_cre_silver_transform", "nb_cre_gold_build_model", "nb_cre_gold_validate_model",
    ]
    for activity in notebooks:
        assert activity["notebook"] == f"notebooks/fabric/{activity['name']}.py"
    names = {activity["name"] for activity in activities}
    for activity in activities:
        assert set(activity.get("depends_on", [])) <= names


def test_deployment_rules_bind_every_fabric_notebook():
    rules = _load("deployment/deployment-rules.json")["rules"]
    bound = {rule["artifact"] for rule in rules if rule["property"] == "default_lakehouse"}
    assert bound == FABRIC_NOTEBOOKS
