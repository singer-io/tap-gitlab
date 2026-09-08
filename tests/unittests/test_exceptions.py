"""Unit tests for tap_gitlab.exceptions."""
import time
import unittest
from unittest.mock import MagicMock, patch

from tap_gitlab.exceptions import (
    Error,
    BackoffError,
    BadRequestError,
    UnauthorizedError,
    ForbiddenError,
    NotFoundError,
    ConflictError,
    UnprocessableEntityError,
    RateLimitError,
    InternalServerError,
    APINotImplementedError,
    BadGatewayError,
    ServiceUnavailableError,
)


def _mock_response(headers=None):
    resp = MagicMock()
    resp.headers = headers or {}
    return resp


class TestErrorBase(unittest.TestCase):

    def test_error_message(self):
        err = Error("something went wrong")
        self.assertEqual(err.message, "something went wrong")
        self.assertIsNone(err.response)

    def test_error_with_response(self):
        resp = MagicMock()
        err = Error("msg", response=resp)
        self.assertIs(err.response, resp)

    def test_subclasses_are_errors(self):
        for cls in (BackoffError, BadRequestError, UnauthorizedError,
                    ForbiddenError, NotFoundError, ConflictError,
                    UnprocessableEntityError, InternalServerError,
                    APINotImplementedError, BadGatewayError, ServiceUnavailableError):
            instance = cls("test")
            self.assertIsInstance(instance, Error)


class TestRateLimitErrorNoResponse(unittest.TestCase):

    def test_no_response_defaults(self):
        err = RateLimitError("rate limited")
        self.assertIsNone(err.response)
        # Without a response, retry_after is None (no header to read from)
        self.assertIsNone(err.retry_after)
        self.assertIsNone(err.limit)
        self.assertIsNone(err.remaining)
        self.assertIsNone(err.reset)

    def test_default_message_used_when_none(self):
        err = RateLimitError()
        self.assertIn("GitLab API rate limit exhausted", str(err))


class TestRateLimitErrorWithRetryAfterHeader(unittest.TestCase):

    def test_retry_after_header_parsed(self):
        resp = _mock_response({"Retry-After": "30"})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 30)

    def test_retry_after_header_invalid_falls_back_to_60(self):
        resp = _mock_response({"Retry-After": "not-a-number"})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 60)

    def test_lowercase_retry_after_header(self):
        resp = _mock_response({"retry-after": "45"})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 45)


class TestRateLimitErrorWithResetHeader(unittest.TestCase):

    def test_reset_header_calculates_retry_after(self):
        future_time = int(time.time()) + 120
        resp = _mock_response({"ratelimit-reset": str(future_time)})
        err = RateLimitError(response=resp)
        self.assertGreater(err.retry_after, 0)
        self.assertLessEqual(err.retry_after, 120)

    def test_reset_header_in_past_gives_zero(self):
        past_time = int(time.time()) - 10
        resp = _mock_response({"ratelimit-reset": str(past_time)})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 0)

    def test_reset_header_invalid_falls_back_to_60(self):
        resp = _mock_response({"ratelimit-reset": "invalid"})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 60)

    def test_ratelimit_limit_and_remaining_parsed(self):
        resp = _mock_response({
            "ratelimit-limit": "1000",
            "ratelimit-remaining": "0",
            "ratelimit-reset": str(int(time.time()) + 30),
        })
        err = RateLimitError(response=resp)
        self.assertEqual(err.limit, "1000")
        self.assertEqual(err.remaining, "0")

    def test_no_reset_and_no_retry_after_defaults_to_60(self):
        resp = _mock_response({})
        err = RateLimitError(response=resp)
        self.assertEqual(err.retry_after, 60)

    def test_message_contains_retry_info(self):
        resp = _mock_response({"Retry-After": "15"})
        err = RateLimitError("custom msg", response=resp)
        msg = str(err)
        self.assertIn("custom msg", msg)
        self.assertIn("15", msg)
