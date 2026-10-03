"""Exceptions raised by the geoDB client."""


class GeodbError(Exception):
    """Base class for all geoDB client errors.

    When the server refused with the protocol's error envelope, the refusal's
    machine-readable keys are kept: ``reason_code`` (match on this), ``remedy``
    (what to do next), ``detail``, ``offending`` and the whole ``body``.
    """

    reason_code = None
    remedy = None
    detail = None
    offending = None
    body = None

    def _keep(self, body):
        if isinstance(body, dict):
            self.body = body
            self.reason_code = body.get("reason_code")
            self.remedy = body.get("remedy")
            self.detail = body.get("detail")
            self.offending = body.get("offending")
        return self


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


class WriteRefused(APIError):
    """The records endpoint refused the whole request (bad body, a key that may
    not write, a retract without its confirm, an undo of an undone write, …).

    A refusal of ONE row is not an exception: it comes back in the result's
    ``rows`` with its own ``reason_code`` and ``remedy``, and the rest of the
    batch goes on.
    """

    def __init__(self, status_code, body, url=None):
        body = body if isinstance(body, dict) else {}
        message = (f"{body.get('reason_code') or 'refused'}: {body.get('detail') or ''}"
                   + (f" — {body['remedy']}" if body.get("remedy") else ""))
        super().__init__(status_code, message, url)
        self._keep(body)


class RowsRefused(WriteRefused):
    """Raised only by ``WriteResult.raise_for_refusals()``: the request was
    ANSWERED (HTTP 200) but some rows were refused or skipped. ``rows`` holds
    them, each with its ``reason_code`` and ``remedy``; ``reason_code`` /
    ``remedy`` are the first one's."""

    def __init__(self, rows, url=None):
        rows = list(rows or [])
        first = dict(rows[0]) if rows else {}
        first['detail'] = (f"{len(rows)} row(s) refused or skipped; row {first.get('index')}: "
                           f"{first.get('detail') or first.get('reason_code')}")
        super().__init__(200, first, url)
        self.rows = rows
