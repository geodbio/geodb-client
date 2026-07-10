"""Exceptions raised by the geoDB client."""


class GeodbError(Exception):
    """Base class for all geoDB client errors."""


class AuthError(GeodbError):
    """The access grant / token was rejected (401/403)."""


class NotFoundError(GeodbError):
    """The requested resource does not exist or is out of scope (404)."""


class APIError(GeodbError):
    """The API returned an unexpected error status."""

    def __init__(self, status_code, message, url=None):
        self.status_code = status_code
        self.url = url
        super().__init__(f"HTTP {status_code} at {url}: {message}")


class ExportError(GeodbError):
    """An export job failed or timed out."""
