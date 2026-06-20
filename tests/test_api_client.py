from src.api_client import ApiClient


def test_api_client_base_url_is_normalized():
    client = ApiClient("https://example.com/")
    assert client.base_url == "https://example.com"
