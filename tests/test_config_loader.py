from pathlib import Path

from src.config_loader import load_csv_config, load_json_config


def test_load_csv_config():
    rows = load_csv_config(Path("config/source_config.csv"))
    assert len(rows) > 0
    assert "source_system" in rows[0]


def test_load_json_config():
    config = load_json_config(Path("config/environment_config_template.json"))
    assert config["environment"] == "dev"
