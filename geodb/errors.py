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


class PermissionDenied(AuthError):
    """The key is valid but may not do this (403): outside its scope, a write
    a read-only key tried, writes open to staff only (``writes_staff_only``) …
    ``remedy`` says what to do instead."""


class APIError(GeodbError):
    """The API returned an error status (the base of the typed ones below)."""

    def __init__(self, status_code, message, url=None):
        self.status_code = status_code
        self.url = url
        super().__init__(f"HTTP {status_code} at {url}: {message}")


class InvalidRequest(APIError):
    """400: a parameter or body the server will not read as sent
    (``invalid_parameter``, ``validation_error`` …). ``offending`` names it."""


class ProjectRequired(InvalidRequest):
    """400 ``project_required``: the read is about one project and the key
    reads several. geoDB keeps no current project — ask the user which, then
    pass ``project=``. ``choices`` lists the projects, grouped by company."""

    @property
    def choices(self):
        return (self.body or {}).get("choices") or []


class CompanyRequired(ProjectRequired):
    """400 ``company_required``: the read is about one company (a
    company-level table, or ``scope="company"``) and the key reads several.
    Pass ``company=``; ``choices`` lists them."""


class SetChoiceRequired(InvalidRequest):
    """400 ``set_choice_required``: the project holds several sets of this
    interval kind. Ask the user which (``set=<id or name>``) or whether they
    want all (``set="all"``). ``sets`` lists them per project with context."""

    @property
    def sets(self):
        return (self.body or {}).get("sets") or []


class Conflict(APIError):
    """409: the request conflicts with the current state (``conflict``,
    ``export_concurrency`` is a 429)."""


class Throttled(APIError):
    """429: slow down. ``retry_after`` is the server's Retry-After (seconds)."""

    retry_after = None


#: reason_code -> exception class; anything else by HTTP status.
_BY_CODE = {
    "project_required": ProjectRequired,
    "company_required": CompanyRequired,
    "set_choice_required": SetChoiceRequired,
}
_BY_STATUS = {400: InvalidRequest, 401: AuthError, 403: PermissionDenied,
              404: NotFoundError, 409: Conflict, 429: Throttled}


def error_for(status_code, body, url=None, *, text="", headers=None):
    """THE exception for a refused response: the class the ``reason_code``
    (else the HTTP status) names, carrying ``reason_code``, ``remedy``,
    ``detail``, ``offending`` and the whole ``body``."""
    body = body if isinstance(body, dict) else None
    code = (body or {}).get("reason_code")
    cls = _BY_CODE.get(code) or _BY_STATUS.get(status_code, APIError)
    if issubclass(cls, APIError):
        detail = (body or {}).get("detail") or text
        message = f"{code}: {detail}" if code else detail
        if (body or {}).get("remedy"):
            message += f" — {body['remedy']}"
        exc = cls(status_code, message, url)
    else:
        what = {401: "Authentication failed", 403: "Access denied",
                404: "Not found"}.get(status_code, f"HTTP {status_code}")
        exc = cls(f"{what} ({status_code}" + (f", {code}" if code else "") + f") at {url}"
                  + (f" — {body['remedy']}" if (body or {}).get("remedy") else ""))
        exc.status_code = status_code
    exc._keep(body)
    if isinstance(exc, Throttled) and headers is not None:
        try:
            exc.retry_after = int(headers.get("Retry-After"))
        except (TypeError, ValueError):
            exc.retry_after = None
    return exc


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
