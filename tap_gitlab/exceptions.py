class Error(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class BackoffError(Error):
    """class representing backoff error handling."""
    pass

class BadRequestError(Error):
    """class representing 400 status code."""
    pass

class UnauthorizedError(Error):
    """class representing 401 status code."""
    pass


class ForbiddenError(Error):
    """class representing 403 status code."""
    pass

class NotFoundError(Error):
    """class representing 404 status code."""
    pass

class ConflictError(Error):
    """class representing 409 status code."""
    pass

class UnprocessableEntityError(BackoffError):
    """class representing 422 status code."""
    pass

class RateLimitError(BackoffError):
    """class representing 429 status code."""
    pass

class InternalServerError(BackoffError):
    """class representing 500 status code."""
    pass

class NotImplementedError(BackoffError):
    """class representing 501 status code."""
    pass

class BadGatewayError(BackoffError):
    """class representing 502 status code."""
    pass

class ServiceUnavailableError(BackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": BadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": UnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": ForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": NotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": UnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": RateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": NotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": BadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": ServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
