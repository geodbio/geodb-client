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


class ProtocolVersionMismatch(GeodbError):
    """The server speaks a different protocol minor than this client was built
    for (``X-GeoDB-Protocol-Version``). ``install`` is the line that fetches a
    client that speaks the server's version."""

    def __init__(self, server_version, client_version, install):
        self.server_version = server_version
        self.client_version = client_version
        self.install = install
        self.remedy = install
        super().__init__(
            f"This geodb-client speaks protocol {client_version}; the server speaks "
            f"{server_version}. Install the matching client: {install}")


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


class ExportConcurrency(Throttled):
    """429 ``export_concurrency``: this key already holds as many unfinished
    export jobs as it may (``in_flight`` of ``limit``). Do NOT resend the
    create: poll the jobs already started and create again once one finished
    — :meth:`Client.export` does that for the jobs it started itself
    (``wait_for_slot``)."""

    @property
    def in_flight(self):
        return ((self.body or {}).get("offending") or {}).get("in_flight")

    @property
    def limit(self):
        return ((self.body or {}).get("offending") or {}).get("limit")


#: reason_code -> exception class; anything else by HTTP status.
_BY_CODE = {
    "project_required": ProjectRequired,
    "company_required": CompanyRequired,
    "set_choice_required": SetChoiceRequired,
    "export_concurrency": ExportConcurrency,
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


class ExportInterrupted(ExportError):
    """The server reports the export's build was interrupted (the machine
    building it stopped) and it will not finish: create the export again,
    which starts a fresh build. ``ExportJob.wait`` already did so once
    (``restart_interrupted``) before raising this. ``error_id`` is the id to
    quote to geoDB; ``job_id`` the job that stopped."""

    def __init__(self, job_id, message, error_id=None):
        self.job_id = job_id
        self.error_id = error_id
        self.server_message = message
        super().__init__(
            f"Export {job_id} was interrupted on the server and will not finish: create the "
            f"export again (gx.export(...) starts a fresh build). Server: {message}")


class WriteRefused(APIError):
    """The records endpoint refused the whole request (bad body, a key that may
    not write, a retract without its confirm, an undo of an undone write, …).

    A refusal of ONE row is not an exception: it comes back in the result's
    ``rows`` with its own ``reason_code`` and ``remedy``, and the rest of the
    batch goes on.
    """

    #: A sentence the client adds to the server's message (what to do here).
    hint = None

    def __init__(self, status_code, body, url=None):
        body = body if isinstance(body, dict) else {}
        message = (f"{body.get('reason_code') or 'refused'}: {body.get('detail') or ''}"
                   + (f" — {body['remedy']}" if body.get("remedy") else ""))
        super().__init__(status_code, message, url)
        self._keep(body)
        if self.hint:
            self.args = (f"{self.args[0]} {self.hint}",)


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


class WriteThrottled(WriteRefused, Throttled):
    """429 ``throttled`` on the records endpoint: the key's request rate, or
    (the dataset door) the key already holds as many queued / running dataset
    jobs as it may (``in_flight`` of ``limit``). Nothing was done by it.
    ``retry_after`` is the server's Retry-After (seconds). The request may be
    sent again — the SAME body; a refusal is never stored under its
    Idempotency-Key, so the same key runs for real. The dataset helpers do so
    a bounded number of times (``geodb.datasets``); ``Client.write`` raises.
    Both a :class:`WriteRefused` and a :class:`Throttled`."""

    @property
    def in_flight(self):
        return (self.offending or {}).get("in_flight")

    @property
    def limit(self):
        return (self.offending or {}).get("limit")


# ── The dataset dry run (protocol import efficiency W6; client W7b) ──────────
class DatasetRefused(WriteRefused):
    """A dataset request (``validate_dataset``, ``DatasetPlan.write``,
    ``DatasetWrite.rehearse_rest``) refused WHOLE — nothing was written by
    it. One subclass per dataset reason code (:data:`DATASET_REASON_CODES`);
    act on ``remedy``, read ``offending``. A member that STOPS a write is not
    raised: it is ``DatasetWrite.stopped`` (``raise_for_stop()`` raises it as
    one of these)."""


class DatasetFileUnreadable(DatasetRefused):
    """``dataset_file_unreadable``: a member is not a UTF-8 CSV with one
    header row (``offending.reason``: not_csv · not_utf8 · no_header · header
    · malformed; ``offending.line``)."""


class DatasetColumnForbidden(DatasetRefused):
    """``dataset_column_forbidden``: a member file carries a column only the
    request may carry (project, dry_run, acknowledge, confirm, qc_mapping,
    plan_token, intent, model) — consent never comes from a file."""


class DatasetTooLarge(DatasetRefused):
    """``dataset_too_large``: past a limit (``offending.limit`` names it,
    ``offending.max`` gives it). Split the work into several datasets."""


class DatasetNotGrouped(DatasetRefused):
    """``dataset_not_grouped``: an Assay member's certificates (or another
    long-form member's records) are split; sort the frame and validate again."""


class DatasetPlanInvalid(DatasetRefused):
    """``dataset_plan_invalid``: the write names no plan this key approved
    (send the rehearsal's ``write`` body exactly as returned —
    ``DatasetPlan.write()`` does)."""


class DatasetPlanExpired(DatasetRefused):
    """``dataset_plan_expired``: the approved plan is over 24 hours old."""

    hint = ("Re-validate: run validate_dataset again (the uploaded files can be reused "
            "by their document ids), show the user the new answer, and write that plan "
            "only after their yes.")


class DatasetPlanChanged(DatasetRefused):
    """``dataset_plan_changed``: the data moved since the rehearsal the user
    approved, so a member would no longer land as approved. Nothing of it
    landed; ``offending`` compares the approved summary with today's."""


class DatasetFileChanged(DatasetRefused):
    """``dataset_file_changed``: a member file is no longer the one approved
    (its bytes changed, or it was trashed / undone)."""


class DatasetAlreadyWritten(DatasetRefused):
    """``dataset_already_written``: the plan landed already (or was undone);
    a plan is never landed twice. ``write_id`` names the write."""

    @property
    def write_id(self):
        return (self.offending or {}).get("write_id")


class DatasetBusy(DatasetRefused):
    """A dataset refusal whose remedy is to send the same request again after
    ``retry_after`` seconds (the server's ``Retry-After``, when it sent one).
    The dataset helpers already re-send a bounded number of times."""

    retry_after = None


class DatasetInProgress(DatasetBusy):
    """``dataset_in_progress``: this plan is being written right now by
    another request (the same write sent twice)."""


class DatasetRowsBusy(DatasetBusy):
    """``dataset_rows_busy``: another write held rows the dataset changes; the
    server stopped after a few seconds rather than make that person wait."""


class DatasetWriteFailed(DatasetRefused):
    """``dataset_write_failed``: a member stopped on an unexpected server
    error; nothing of it landed (members before it may have)."""


class DatasetProjectBusy(DatasetBusy):
    """``dataset_project_busy``: dataset jobs on one project run one at a
    time; this background job waited the longest it may for another one on
    the same project (``waited_seconds``), then stopped. Nothing of it was
    written. It arrives at the job's poll, which carries no ``Retry-After``:
    the dataset helpers send the same request again after
    ``geodb.datasets.PROJECT_BUSY_FALLBACK_SECONDS`` (bounded, warns)."""

    @property
    def waited_seconds(self):
        return (self.offending or {}).get("waited_seconds")


class DatasetInterrupted(DatasetRefused):
    """``dataset_interrupted``: a background dataset job (a rehearsal, write
    or undo) ended without finishing — the server running it stopped or hit
    an unexpected error. A write landed nothing of the member it was on;
    members before it stay landed. The remedy is the same request again (a
    rehearsal runs from the start, a write lands only what has not landed,
    an undo takes back what is still there): the dataset helpers send it again
    ONCE (``restart_interrupted``, warns), like an interrupted export, then
    raise this. ``error_id`` is the id to quote to geoDB."""

    @property
    def error_id(self):
        return (self.offending or {}).get("error_id")


class WriteForbidden(WriteRefused, PermissionDenied):
    """403 on the records endpoint: the key (or the person it acts for) may
    not do this write — ``write_access_insufficient``, ``writes_staff_only``,
    ``project_not_in_scope`` (also at a dataset job's poll once the key lost
    the project) … Never re-sent. Both a :class:`WriteRefused` and a
    :class:`PermissionDenied` (so an :class:`AuthError`)."""


class UndoNeedsCompanyReach(WriteForbidden):
    """``undo_needs_company_reach``: the write being undone created or changed
    a record every project of the company shares (a laboratory, method or
    standard), and this key no longer reaches every project of that company
    (``offending.company_shared``). Refused whole; nothing was changed."""

    hint = ("Do not retry with this key: undo it through a key that reaches every project of "
            "the company (the user's own connection, set to cover the whole company), or ask "
            "the user to undo it from the geoDB web app.")


class WritesSwitchedOff(WriteRefused):
    """``writes_switched_off``: writing through AI connections was switched
    off on the server (503, an operator's switch, not something the request
    did) after a background dataset job was accepted, so it did not run (a
    write already running stopped before its next member; the members before
    it stay landed, under its one Undo). Not a busy wait: the dataset
    helpers send it again at most ONCE, and only when the server named a
    ``Retry-After`` (``retry_after``) within ``MAX_RESEND_WAIT_SECONDS``;
    otherwise it is raised at once."""

    retry_after = None
    hint = ("Writing through AI connections is switched off on the server right now: tell the "
            "user that nothing more was written, and send the same request again later (a "
            "write lands only what has not landed).")


#: The records-door reason codes that are not dataset codes but have their own
#: exception (server batch-C review §6.18; the README's table reads it too).
RECORDS_REASON_CODES = {
    "writes_switched_off": WritesSwitchedOff,
    "undo_needs_company_reach": UndoNeedsCompanyReach,
}


#: dataset reason_code -> exception class (the README's table reads it).
DATASET_REASON_CODES = {
    "dataset_file_unreadable": DatasetFileUnreadable,
    "dataset_column_forbidden": DatasetColumnForbidden,
    "dataset_too_large": DatasetTooLarge,
    "dataset_not_grouped": DatasetNotGrouped,
    "dataset_rows_busy": DatasetRowsBusy,
    "dataset_plan_invalid": DatasetPlanInvalid,
    "dataset_plan_expired": DatasetPlanExpired,
    "dataset_plan_changed": DatasetPlanChanged,
    "dataset_file_changed": DatasetFileChanged,
    "dataset_already_written": DatasetAlreadyWritten,
    "dataset_in_progress": DatasetInProgress,
    "dataset_write_failed": DatasetWriteFailed,
    "dataset_project_busy": DatasetProjectBusy,
    "dataset_interrupted": DatasetInterrupted,
}


def write_refused_for(status_code, body, url=None, *, headers=None):
    """THE exception for a refused records request: the class its
    ``reason_code`` names (:data:`DATASET_REASON_CODES`,
    :data:`RECORDS_REASON_CODES`), :class:`WriteThrottled` for a 429,
    :class:`WriteForbidden` for a 403, else :class:`WriteRefused`; a
    :class:`DatasetBusy`, a :class:`WriteThrottled` and a
    :class:`WritesSwitchedOff` carry the server's ``Retry-After``."""
    code = body.get("reason_code") if isinstance(body, dict) else None
    cls = DATASET_REASON_CODES.get(code) or RECORDS_REASON_CODES.get(code)
    if cls is None:
        if status_code == 429 or code == "throttled":
            cls = WriteThrottled
        elif status_code == 403:
            cls = WriteForbidden
        else:
            cls = WriteRefused
    exc = cls(status_code, body, url)
    if isinstance(exc, (DatasetBusy, WriteThrottled, WritesSwitchedOff)):
        exc.retry_after = _retry_after_header(headers)
    return exc


def _retry_after_header(headers):
    try:
        value = int((headers or {}).get("Retry-After"))
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


class DatasetJobFailed(GeodbError):
    """A dataset job the server ran in the background (a large rehearsal,
    write or undo) failed without a reason code, stalled, or ran past the
    wait's hard cap. ``job_id`` names it; nothing is known to have landed
    beyond what the write record says (``gx.writes(model="Dataset")``)."""

    def __init__(self, job_id, message):
        self.job_id = job_id
        super().__init__(message)
