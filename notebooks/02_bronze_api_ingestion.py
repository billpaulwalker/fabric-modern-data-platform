"""
02 Bronze API Ingestion

Purpose:
    Land raw REST API-style weather data into Bronze.

Portfolio note:
    This notebook is designed to run without committing API secrets. By default,
    it uses data/api_sample/openweather_sample_response.json. Later, this can be
    switched to a live OpenWeather API call using src/api_client.py and an
    environment variable for the API key.

Bronze contract:
    API Bronze keeps the raw JSON payload plus audit metadata. Silver will handle
    flattening, typing, and standardization.
"""

from pathlib import Path
import json
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.bronze_utils import generate_pipeline_run_id, ingest_api_payload_to_bronze


def load_sample_api_payload() -> dict:
    sample_path = REPO_ROOT / "data/api_sample/openweather_sample_response.json"
    with sample_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> None:
    pipeline_run_id = generate_pipeline_run_id("bronze_api")
    payload = load_sample_api_payload()

    result = ingest_api_payload_to_bronze(
        payload=payload,
        output_path=REPO_ROOT / "data/bronze/bronze_weather_api_raw.jsonl",
        source_system="OpenWeather",
        source_object="weather",
        pipeline_run_id=pipeline_run_id,
        load_type="incremental",
    )

    print("Bronze API ingestion complete")
    print(result)


if __name__ == "__main__":
    main()
