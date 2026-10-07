import pytest
import requests

from src import api_client
from src.api_client import ApiClient, ApiClientError


def test_api_client_base_url_is_normalized():
    client = ApiClient("https://example.com/")
    assert client.base_url == "https://example.com"


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


@pytest.fixture
def sleeps(monkeypatch):
    recorded = []
    monkeypatch.setattr(api_client.time, "sleep", recorded.append)
    return recorded


def _serve(monkeypatch, outcomes):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        outcome = outcomes[len(calls) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(api_client.requests, "get", fake_get)
    return calls


def test_connection_errors_and_timeouts_are_retried(monkeypatch, sleeps):
    calls = _serve(monkeypatch, [
        requests.ConnectionError("reset"),
        requests.Timeout("slow"),
        FakeResponse(200, {"ok": True}),
    ])
    assert ApiClient("https://example.com").get("weather", max_retries=3) == {"ok": True}
    assert len(calls) == 3


def test_final_failure_does_not_sleep_and_reports_last_error(monkeypatch, sleeps):
    _serve(monkeypatch, [FakeResponse(503), FakeResponse(503)])
    with pytest.raises(ApiClientError, match="503"):
        ApiClient("https://example.com").get("weather", max_retries=2)
    assert len(sleeps) == 1


def test_network_error_on_final_attempt_raises_api_client_error(monkeypatch, sleeps):
    _serve(monkeypatch, [requests.ConnectionError("reset")])
    with pytest.raises(ApiClientError, match="ConnectionError"):
        ApiClient("https://example.com").get("weather", max_retries=1)


def test_rate_limit_honours_retry_after_seconds(monkeypatch, sleeps):
    _serve(monkeypatch, [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, {"ok": True})])
    ApiClient("https://example.com").get("weather")
    assert sleeps == [7]


def test_client_errors_are_not_retried(monkeypatch, sleeps):
    calls = _serve(monkeypatch, [FakeResponse(401), FakeResponse(200)])
    with pytest.raises(ApiClientError, match="401"):
        ApiClient("https://example.com").get("weather")
    assert len(calls) == 1 and sleeps == []
