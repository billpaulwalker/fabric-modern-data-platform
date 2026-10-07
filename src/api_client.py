import time
from typing import Any, Optional

import requests


MAX_WAIT_SECONDS = 60


class ApiClientError(Exception):
    pass


def _backoff_seconds(attempt: int) -> int:
    return min(2 ** attempt, 30)


def _retry_after_seconds(response: Any, attempt: int) -> int:
    """Honour a numeric Retry-After header; fall back to exponential backoff otherwise."""
    value = str(response.headers.get("Retry-After", "")).strip()
    if value.isdigit():
        return min(int(value), MAX_WAIT_SECONDS)
    return _backoff_seconds(attempt)


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

        last_error = "no attempts made"
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=request_params, timeout=self.timeout_seconds)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                wait_seconds = _backoff_seconds(attempt)
            else:
                if response.status_code == 429:
                    last_error = "status 429"
                    wait_seconds = _retry_after_seconds(response, attempt)
                elif 500 <= response.status_code < 600:
                    last_error = f"status {response.status_code}"
                    wait_seconds = _backoff_seconds(attempt)
                elif response.status_code >= 400:
                    raise ApiClientError(f"API request failed with status {response.status_code}: {response.text}")
                else:
                    return response.json()

            if attempt < max_retries:
                time.sleep(wait_seconds)

        raise ApiClientError(f"API request failed after {max_retries} attempts; last error: {last_error}")
