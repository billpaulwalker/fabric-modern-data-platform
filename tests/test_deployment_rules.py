"""Keep the per-stage settings record consistent with the Fabric notebooks."""

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FABRIC_NOTEBOOKS = {path.stem for path in (REPO_ROOT / "notebooks").glob("*.py")}
SHARED_DEFAULT_LAKEHOUSE = "lh_cre_bronze"


def _load(relative_path):
    return json.loads((REPO_ROOT / relative_path).read_text(encoding="utf-8"))


def test_deployment_rules_bind_every_fabric_notebook():
    rules = _load("deployment/deployment-rules.json")["rules"]
    lakehouse_rules = [rule for rule in rules if rule["property"] == "default_lakehouse"]
    assert {rule["artifact"] for rule in lakehouse_rules} == FABRIC_NOTEBOOKS
    for rule in lakehouse_rules:
        for stage in ["dev", "test", "prod"]:
            assert rule[stage] == f"ws-cre-modernization-{stage}/{SHARED_DEFAULT_LAKEHOUSE}", rule["artifact"]
