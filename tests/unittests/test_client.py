import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import requests
from tap_gitlab.client import Client, raise_for_error, wait_if_retry_after
from tap_gitlab.exceptions import (
    BackoffError, Error,
    BadRequestError, UnauthorizedError, ForbiddenError, NotFoundError,
    RateLimitError, InternalServerError,
)
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError


def _mock_response(status_code, json_data=None, headers=None, text=""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    resp.text = text
    return resp

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

    def test_empty_api_url_falls_back_to_gitlab_cloud(self):
        """An empty or whitespace-only api_url must fall back to gitlab.com."""
        for value in ["", "   ", None]:
            with self.subTest(api_url=value):
                config = {"private_token": "dummy_token", "api_url": value}
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

    def test_api_url_with_port_and_no_scheme_gets_https(self):
        """host:port without a scheme must not be mis-detected by urlparse and must get https://."""
        config = {
            "private_token": "dummy_token",
            "api_url": "gitlab.mycompany.com:8443",
        }
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.mycompany.com:8443/api/v4")

    def test_api_url_with_existing_api_v4_suffix_is_not_doubled(self):
        """Providing a full API base URL must not produce /api/v4/api/v4."""
        config = {
            "private_token": "dummy_token",
            "api_url": "https://gitlab.mycompany.com/api/v4",
        }
        client = Client(config)
        self.assertEqual(client.base_url, "https://gitlab.mycompany.com/api/v4")


class TestCheckApiCredentials(unittest.TestCase):

    @patch("requests.Session.get", side_effect=ConnectionError("Failed to resolve 'test.gitlab.com'"))
    def test_unreachable_host_raises_friendly_connection_error(self, mock_get):
        """A DNS/network failure in check_api_credentials raises a descriptive ConnectionError
        that does not leak the raw urllib3 message (which may contain the private_token URL)."""
        config = {
            "private_token": "dummy_token",
            "api_url": "https://test.gitlab.com",
        }
        client = Client(config)
        with self.assertRaises(ConnectionError) as ctx:
            client.check_api_credentials()
        msg = str(ctx.exception)
        self.assertIn("Unable to reach GitLab", msg)
        # base_url (no token) is shown, not the raw original error
        self.assertIn("test.gitlab.com", msg)
        self.assertIn("api_url", msg)
        # Raw urllib3 message must not be included to avoid token leakage
        self.assertNotIn("Failed to resolve", msg)
        self.assertNotIn("private_token", msg)


class TestAuthenticate(unittest.TestCase):

    def _make_client(self, config=None):
        """Return a Client without triggering check_api_credentials."""
        return Client(config if config is not None else {"private_token": "secret_token"})

    def test_private_token_set_in_header(self):
        """private_token must be sent in the PRIVATE-TOKEN header, not as a query param."""
        client = self._make_client({"private_token": "secret_token"})
        headers, params = client.authenticate({}, {})
        self.assertEqual(headers.get("PRIVATE-TOKEN"), "secret_token")

    def test_private_token_not_in_params(self):
        """private_token must never appear in the query parameters."""
        client = self._make_client({"private_token": "secret_token"})
        headers, params = client.authenticate({}, {})
        self.assertNotIn("private_token", params)

    def test_no_private_token_in_config_leaves_header_unset(self):
        """When private_token is absent from config the header must not be added."""
        client = self._make_client({})
        headers, params = client.authenticate({}, {})
        self.assertNotIn("PRIVATE-TOKEN", headers)

    def test_user_agent_added_to_headers_when_configured(self):
        """user_agent in config must be forwarded as the User-Agent header."""
        client = self._make_client({"private_token": "t", "user_agent": "my-tap/1.0"})
        headers, params = client.authenticate({}, {})
        self.assertEqual(headers.get("User-Agent"), "my-tap/1.0")

    def test_existing_headers_are_preserved(self):
        """authenticate() must not discard headers that were already set by the caller."""
        client = self._make_client({"private_token": "secret_token"})
        headers, params = client.authenticate({"Accept": "application/json"}, {})
        self.assertEqual(headers.get("Accept"), "application/json")
        self.assertEqual(headers.get("PRIVATE-TOKEN"), "secret_token")

    @patch("requests.Session.get")
    def test_request_uses_header_not_param(self, mock_get):
        """End-to-end: an actual GET must carry PRIVATE-TOKEN in the header, not in the URL."""
        mock_get.return_value = get_mock_response(200, {"id": 1, "username": "tester"})
        client = self._make_client({"private_token": "secret_token"})
        client.check_api_credentials()
        _, call_kwargs = mock_get.call_args
        sent_headers = call_kwargs.get("headers", {})
        sent_params = call_kwargs.get("params", {})
        self.assertEqual(sent_headers.get("PRIVATE-TOKEN"), "secret_token")
        self.assertNotIn("private_token", sent_params)


class TestRaiseForError(unittest.TestCase):

    def test_200_does_not_raise(self):
        resp = _mock_response(200, {"id": 1})
        raise_for_error(resp)  # should not raise

    def test_400_raises_bad_request(self):
        resp = _mock_response(400, {"message": "bad"})
        with self.assertRaises(BadRequestError):
            raise_for_error(resp)

    def test_401_raises_unauthorized(self):
        resp = _mock_response(401, {"message": "unauthorized"})
        with self.assertRaises(UnauthorizedError):
            raise_for_error(resp)

    def test_403_raises_forbidden(self):
        resp = _mock_response(403, {"message": "forbidden"})
        with self.assertRaises(ForbiddenError):
            raise_for_error(resp)

    def test_404_raises_not_found(self):
        resp = _mock_response(404, {"message": "not found"})
        with self.assertRaises(NotFoundError):
            raise_for_error(resp)

    def test_500_raises_internal_server_error(self):
        resp = _mock_response(500, {"message": "server error"})
        with self.assertRaises(InternalServerError):
            raise_for_error(resp)

    def test_error_field_used_over_message(self):
        resp = _mock_response(400, {"error": "invalid_token", "error_description": "Token expired"})
        with self.assertRaises(BadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("invalid_token", str(ctx.exception))
        self.assertIn("Token expired", str(ctx.exception))

    def test_error_field_without_description(self):
        resp = _mock_response(400, {"error": "invalid_request"})
        with self.assertRaises(BadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("invalid_request", str(ctx.exception))

    def test_response_body_included_in_fallback_message(self):
        resp = _mock_response(503, {}, text="Service Unavailable")
        resp.json.side_effect = ValueError("no json")
        with self.assertRaises(Error):
            raise_for_error(resp)

    def test_json_parse_failure_falls_back_gracefully(self):
        resp = _mock_response(400, text="<html>error</html>")
        resp.json.side_effect = ValueError("no JSON")
        with self.assertRaises(BadRequestError):
            raise_for_error(resp)

    def test_unknown_status_code_raises_generic_error(self):
        resp = _mock_response(418, {"message": "teapot"})
        with self.assertRaises(Error):
            raise_for_error(resp)


class TestWaitIfRetryAfter(unittest.TestCase):

    def test_non_rate_limit_exception_returns_60(self):
        exc = Exception("generic")
        result = wait_if_retry_after(exc)
        self.assertEqual(result, 60)

    def test_rate_limit_exception_returns_retry_after(self):
        resp = MagicMock()
        resp.headers = {"Retry-After": "30"}
        exc = RateLimitError(response=resp)
        result = wait_if_retry_after(exc)
        self.assertEqual(result, 30)

    def test_rate_limit_no_retry_after_defaults_to_60(self):
        resp = MagicMock()
        resp.headers = {}
        exc = RateLimitError(response=resp)
        result = wait_if_retry_after(exc)
        self.assertEqual(result, 60)

    def test_dict_style_exception_info(self):
        exc = Exception("generic")
        result = wait_if_retry_after({"exception": exc})
        self.assertEqual(result, 60)

    def test_rate_limit_with_limit_remaining_logs(self):
        resp = MagicMock()
        resp.headers = {
            "ratelimit-limit": "1000",
            "ratelimit-remaining": "0",
            "Retry-After": "45",
        }
        exc = RateLimitError(response=resp)
        result = wait_if_retry_after(exc)
        self.assertEqual(result, 45)


class TestClientPaginate(unittest.TestCase):

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_paginate_single_page_list(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"id": 1}, {"id": 2}]
        mock_resp.headers = {}
        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.paginate("/projects")
        self.assertEqual(result, [{"id": 1}, {"id": 2}])

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_paginate_multi_page(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        page1 = MagicMock(spec=requests.Response)
        page1.status_code = 200
        page1.json.return_value = [{"id": 1}]
        page1.headers = {"X-Next-Page": "2"}
        page2 = MagicMock(spec=requests.Response)
        page2.status_code = 200
        page2.json.return_value = [{"id": 2}]
        page2.headers = {}
        with patch.object(client._session, "get", side_effect=[page1, page2]):
            result = client.paginate("/projects")
        self.assertEqual(result, [{"id": 1}, {"id": 2}])

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_paginate_dict_response_appended(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": 99, "name": "project"}
        mock_resp.headers = {}
        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.paginate("/projects/99")
        self.assertEqual(result, [{"id": 99, "name": "project"}])

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_request_timeout_from_config(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com", "request_timeout": "120"}
        client = Client(config)
        self.assertEqual(client.request_timeout, 120.0)

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_default_request_timeout(self, mock_creds):
        from tap_gitlab.client import REQUEST_TIMEOUT
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)

    def test_check_api_credentials_raises_connection_error_on_unreachable_host(self):
        from requests.exceptions import ConnectionError as ReqConnError
        from requests import session as req_session
        config = {"private_token": "tok", "api_url": "https://gitlab.example.invalid"}
        client = Client.__new__(Client)
        client.config = config
        client.base_url = "https://gitlab.example.invalid/api/v4"
        client.request_timeout = 5
        client._session = req_session()
        with patch.object(client._session, "get", side_effect=ReqConnError("unreachable")):
            with self.assertRaises(ReqConnError):
                client.check_api_credentials()

    def test_check_api_credentials_logs_user_on_success(self):
        from requests import session as req_session
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client.__new__(Client)
        client.config = config
        client.base_url = "https://gitlab.com/api/v4"
        client.request_timeout = 300
        client._session = req_session()
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"username": "testuser"}
        mock_resp.headers = {}
        with patch.object(client._session, "get", return_value=mock_resp):
            client.check_api_credentials()  # should not raise


class TestClientPost(unittest.TestCase):

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_post_returns_json_response(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"id": 99}
        mock_resp.headers = {}
        with patch.object(client._session, "request", return_value=mock_resp):
            result = client.post(
                endpoint="https://gitlab.com/api/v4/projects",
                params={}, headers={}, body={"name": "test"}
            )
        self.assertEqual(result, {"id": 99})

    @patch("tap_gitlab.client.Client.check_api_credentials")
    def test_post_uses_path_when_no_endpoint(self, mock_creds):
        config = {"private_token": "tok", "api_url": "https://gitlab.com"}
        client = Client(config)
        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 201
        mock_resp.json.return_value = {"id": 5}
        mock_resp.headers = {}
        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            client.post(endpoint="", params={}, headers={}, body={"title": "test"}, path="projects")
        call_url = mock_req.call_args[0][1]
        self.assertIn("projects", call_url)
