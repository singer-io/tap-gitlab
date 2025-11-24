from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_gitlab.exceptions import ERROR_CODE_EXCEPTION_MAPPING, Error, BackoffError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception based on status code."""
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get('message', 'Unknown Error'))}"
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get("raise_exception", Error)
        raise exc(message, response) from None


class Client:
    """
    A Wrapper class for API calls.
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://gitlab.com/api/v4"
        LOGGER.info(f"Base URL set to: {self.base_url}")

        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        pass

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request using dynamic header/token keys."""
        params = params or {}
        params['private_token'] = self.config.get("private_token")

        if isinstance(headers, dict) and "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")

        return headers, params

    def get(self, endpoint: str, params: Dict, headers: Dict, path: str = None) -> Any:
        """Performs a GET request."""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request("GET", endpoint, headers=headers, params=params, timeout=self.request_timeout)

    def post(self, endpoint: str, params: Dict, headers: Dict, body: Dict, path: str = None) -> Any:
        """Performs a POST request."""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request("POST", endpoint, headers=headers, params=params, data=body, timeout=self.request_timeout)

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            BackoffError
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        """Performs the actual HTTP request with backoff and error handling."""
        with metrics.http_request_timer(endpoint) as timer:
            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)

            # Store response headers for pagination
            self.last_response_headers = response.headers

        return response.json()

    def paginate(self, path: str, params: Optional[Dict] = None, context: Optional[Dict] = None) -> list:
        """Handles paginated GET requests."""
        results = []
        params = params or {}
        headers = {}
        next_page = 1

        while True:
            params["page"] = next_page
            full_url = f"{self.base_url}{path}"

            # Perform authenticated GET request
            headers, _ = self.authenticate(headers, params)

            response = self._session.get(full_url, headers=headers, params=params, timeout=self.request_timeout)
            raise_for_error(response)
            data = response.json()
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)

            next_page = response.headers.get("X-Next-Page")
            if not next_page:
                break
            next_page = int(next_page)

        return results
