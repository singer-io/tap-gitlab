import unittest
from unittest.mock import patch
from datetime import datetime
import requests
from tap_gitlab.client import Client
from tap_gitlab.exceptions import BackoffError, Error
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

class MockResponse:
    """Mock response object class."""

    def __init__(self, status_code, json_data, raise_error=False, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self._json_data = json_data
        self.headers = headers or {}

    def raise_for_status(self):
        if self.raise_error:
            raise requests.HTTPError("Mocked error")
        return self.status_code

    def json(self):
        return self._json_data


def get_mock_response(status_code=200, json_data=None, raise_error=False, headers=None):
    return MockResponse(status_code, json_data or {}, raise_error, headers)


class TestClientRequests(unittest.TestCase):

    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", return_value=get_mock_response(200, {"status": "ok"}))
    def test_get_success(self, mock_request, mock_auth, mock_check_creds):
        config = {
            "api_url": "https://gitlab.com/api/v4",
            "auth_header_key": "Authorization",
            "auth_token_key": "private_token",
            "private_token": "dummy_token"
        }

        with Client(config) as client:
            response = client.get(endpoint="https://gitlab.com/api/v4/projects", params={}, headers={})
            self.assertEqual(response, {"status": "ok"})
            mock_request.assert_called_once()

    @patch("time.sleep")
    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", side_effect=ConnectionError)
    def test_get_connection_error(self, mock_request, mock_auth, mock_check_creds, mock_sleep):
        config = {"api_url": "https://gitlab.com/api/v4"}
        with self.assertRaises(ConnectionError):
            with Client(config) as client:
                client.get(endpoint="dummy", params={}, headers={})
        self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", side_effect=Timeout)
    def test_get_timeout_error(self, mock_request, mock_auth, mock_check_creds, mock_sleep):
        config = {"api_url": "https://gitlab.com/api/v4"}
        with self.assertRaises(Timeout):
            with Client(config) as client:
                client.get(endpoint="dummy", params={}, headers={})
        self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", side_effect=ChunkedEncodingError)
    def test_get_chunked_encoding_error(self, mock_request, mock_auth, mock_check_creds, mock_sleep):
        config = {"api_url": "https://gitlab.com/api/v4"}
        with self.assertRaises(ChunkedEncodingError):
            with Client(config) as client:
                client.get(endpoint="dummy", params={}, headers={})
        self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", return_value=get_mock_response(429, {}, raise_error=True))
    def test_rate_limit_error(self, mock_request, mock_auth, mock_check_creds, mock_sleep):
        mock_request.side_effect = [get_mock_response(429, {}, True)] * 5
        config = {"api_url": "https://gitlab.com/api/v4"}
        with self.assertRaises(BackoffError):
            with Client(config) as client:
                client.get(endpoint="dummy", params={}, headers={})
        self.assertEqual(mock_request.call_count, 5)

    @patch("time.sleep")
    @patch("tap_gitlab.client.Client.check_api_credentials")
    @patch("tap_gitlab.client.Client.authenticate", return_value=({}, {}))
    @patch("requests.Session.request", return_value=get_mock_response(500, {"message": "Internal Server Error"}, raise_error=True))
    def test_generic_http_error(self, mock_request, mock_auth, mock_check_creds, mock_sleep):
        config = {"api_url": "https://gitlab.com/api/v4"}
        with self.assertRaises(Error):
            with Client(config) as client:
                client.get(endpoint="dummy", params={}, headers={})


class TestClientBaseUrl(unittest.TestCase):

    def test_default_base_url_uses_gitlab_cloud(self):
        """When api_url is absent the client targets gitlab.com."""
        config = {"private_token": "dummy_token"}
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.com/api/v4")

    def test_custom_api_url_sets_base_url(self):
        """When api_url is set the client targets the on-prem instance."""
        config = {
            "private_token": "dummy_token",
            "api_url": "https://gitlab.mycompany.com",
        }
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.mycompany.com/api/v4")

    def test_trailing_slash_in_api_url_is_stripped(self):
        """Trailing slashes in api_url must not produce a double-slash in base_url."""
        config = {
            "private_token": "dummy_token",
            "api_url": "https://gitlab.mycompany.com/",
        }
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.mycompany.com/api/v4")

    def test_api_url_without_scheme_defaults_to_https(self):
        """An api_url with no scheme (e.g. 'gitlab.mycompany.com') must get https:// prepended."""
        config = {
            "private_token": "dummy_token",
            "api_url": "gitlab.mycompany.com",
        }
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.mycompany.com/api/v4")


class TestCheckApiCredentials(unittest.TestCase):

    @patch("requests.Session.get", side_effect=ConnectionError("Failed to resolve 'test.gitlab.com'"))
    def test_unreachable_host_raises_friendly_connection_error(self, mock_get):
        """A DNS/network failure in check_api_credentials raises a descriptive ConnectionError."""
        config = {
            "private_token": "dummy_token",
            "api_url": "https://test.gitlab.com",
        }
        client = Client(config)
        with self.assertRaises(ConnectionError) as ctx:
            client.check_api_credentials()
        self.assertIn("Unable to reach GitLab", str(ctx.exception))
        self.assertIn("test.gitlab.com", str(ctx.exception))
        self.assertIn("api_url", str(ctx.exception))
