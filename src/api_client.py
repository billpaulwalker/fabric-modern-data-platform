import time
from typing import Any, Optional

import requests


class ApiClientError(Exception):
    pass


class ApiClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout_seconds: int = 30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def get(self, endpoint: str, params: Optional[dict[str, Any]] = None, max_retries: int = 3) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_params = params.copy() if params else {}

        if self.api_key:
            request_params["appid"] = self.api_key

        for attempt in range(1, max_retries + 1):
            response = requests.get(url, params=request_params, timeout=self.timeout_seconds)

            if response.status_code == 429:
                wait_seconds = min(2 ** attempt, 30)
                time.sleep(wait_seconds)
                continue

            if 500 <= response.status_code < 600:
                wait_seconds = min(2 ** attempt, 30)
                time.sleep(wait_seconds)
                continue

            if response.status_code >= 400:
                raise ApiClientError(f"API request failed with status {response.status_code}: {response.text}")

            return response.json()

        raise ApiClientError(f"API request failed after {max_retries} retries")
