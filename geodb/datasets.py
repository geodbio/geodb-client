"""
The dataset dry run (protocol import efficiency W6; this client's W7b): many
records of several kinds — new laboratories, methods with their detection
limits, certificates, QC samples, and the assays that hang off them —
rehearsed TOGETHER, then written on the user's yes, under ONE Undo.

    plan = gx.validate_dataset(12, {"Method": methods_df, "Certificate": certs_df,
                                    "Assay": assays_df}, intent="create")
    print(plan.summary())          # show the user; read plan.before_writing + warnings
    done = plan.write()            # ONLY after the user's yes
    done.status                    # "done" | "stopped" — what landed, member by member
    done.undo()                    # one request takes back everything it landed

This module holds the CSV writer for a member, the dataset routes (ONE place:
:data:`ROUTES`), and the results: :class:`DatasetPlan`, :class:`DatasetWrite`
and the background :class:`DatasetJob` (which waits on THE poll loop the
exports use).
"""

import copy
import csv
import datetime
import io
import os
import uuid
import warnings
from decimal import Decimal

from .errors import (DatasetInterrupted, DatasetJobFailed, WriteRefused, WritesSwitchedOff,
                     write_refused_for, _retry_after_header)

__all__ = ["DatasetJob", "DatasetPlan", "DatasetWrite", "DatasetsMixin", "member_csv",
           "ROUTES", "MEMBER_CATEGORY"]


# ── THE dataset routes: the ONE place they are named ─────────────────────────
#: The server design's routes (plan §6.1). The rehearsal, the write and the
#: undo are all ``POST records/`` (``"model": "Dataset"``); a large one answers
#: a poll handle whose own ``poll`` link is followed when it is a path on the
#: configured base URL (else the template below). ⚠ Set from the design before
#: the server's door existed (its build slice C2): re-check against the
#: published spec once it ships, and change them HERE only.
ROUTES = {
    "records": "/records/",
    "poll": "/records/datasets/{id}/",
    "detail": "/records/datasets/{id}/detail/",
}
#: The document category a member file is uploaded as (plan §6.1: "OT", other).
MEMBER_CATEGORY = "OT"
DATASET_MODEL = "Dataset"
#: The intents a dataset is landed by (plan §6.9: never update / retract).
INTENTS = ("create", "upsert")
#: The keys a member given as a dict may carry.
_MEMBER_SPEC_KEYS = frozenset({"data", "path", "document", "intent", "set"})
_JOB_INFLIGHT = ("queued", "running")
_JOB_FAILED = ("error", "failed", "cancelled", "canceled")
#: The refusals whose remedy is "send the same request again in a minute"
#: (``dataset_project_busy``: "again later" — a background job that waited its
#: turn on the project as long as it may).
_RETRY_CODES = ("dataset_in_progress", "dataset_rows_busy", "dataset_project_busy",
                "idempotency_in_progress", "throttled")
#: The refusals whose remedy is "send the same request again" ONCE, at once:
#: the background job ended without finishing (the export rule:
#: ``ExportJob.wait(restart_interrupted=1)``). Never a busy wait.
_RESTART_CODES = ("dataset_interrupted",)
#: The stop reasons a write is sent again for (it lands only what has not
#: landed; a real drift — dataset_plan_changed — is never re-sent).
_RETRY_STOPS = ("dataset_rows_busy",)
#: Seconds before a re-send when the server named no Retry-After ("in a minute").
BUSY_FALLBACK_SECONDS = 60
#: ``dataset_project_busy`` arrives at a job's poll, which carries no
#: Retry-After: the server's own number for it (``DATASET_RETRY_AFTER_SECONDS``
#: × 10 on the refusal it raises in the job) — the job had already waited 30 min.
PROJECT_BUSY_FALLBACK_SECONDS = 300
#: A 429 ``throttled`` with no Retry-After (the server's cap answer names 30).
THROTTLED_FALLBACK_SECONDS = 30
_FALLBACK_SECONDS = {"dataset_project_busy": PROJECT_BUSY_FALLBACK_SECONDS,
                     "throttled": THROTTLED_FALLBACK_SECONDS}
#: The longest wait the helpers sleep before a re-send. The per-key rate is
#: per HOUR, so its Retry-After can be most of an hour: past this, the refusal
#: is raised with its ``retry_after`` and the caller decides (tell the user).
MAX_RESEND_WAIT_SECONDS = 600
#: ``writes_switched_off`` (503) is an operator's kill switch, not a busy
#: wait: re-sent at most this many times, and only after a ``Retry-After`` the
#: server named (never a fallback guess); otherwise raised at once.
SWITCHED_OFF_RETRIES = 1


# ── A member file: one model per UTF-8 CSV (server plan §6.2) ────────────────
def _cell(value):
    """One CSV cell. A missing value is EMPTY (the server reads an empty cell
    as "not sent"); an integral number is written as an integer (so an id a
    missing value turned into a float stays ``101``, never ``101.0``); a
    string is written exactly; nothing is ever written in exponent form."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        import pandas as pd
        if pd.isna(value):                     # NaN, None, pd.NA, NaT
            return ""
    except (TypeError, ValueError):            # list-likes: not a missing scalar
        pass
    if isinstance(value, datetime.datetime):   # pandas Timestamps included
        if value.tzinfo is None and value.time() == datetime.time(0):
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        text = str(value)
        return format(value, "f") if "e" in text.lower() else text
    if hasattr(value, "item") and not isinstance(value, (list, dict, bytes)):
        try:
            value = value.item()               # numpy scalars -> Python ones
        except (TypeError, ValueError):
            pass
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        text = repr(value)
        return format(Decimal(text), "f") if "e" in text.lower() else text
    return str(value)


def member_csv(frame):
    """``frame`` (a pandas DataFrame, or a list of dicts) as the bytes of ONE
    dataset member: UTF-8 (no BOM), a header row of exactly the frame's
    columns, one line per row, no index column; missing values are empty
    cells and identifiers keep their characters (see :func:`_cell`)."""
    import pandas as pd
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(list(frame))
    if not len(frame.columns):
        raise ValueError("a dataset member needs at least one column (the header names the fields)")
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\r\n")
    writer.writerow([str(c) for c in frame.columns])
    for row in frame.itertuples(index=False, name=None):
        writer.writerow([_cell(v) for v in row])
    return out.getvalue().encode("utf-8")


# ── The member specs → uploads ───────────────────────────────────────────────
def _member_specs(members):
    """``{model: value}`` → ``[(model, spec)]``, every value checked BEFORE any
    upload. A value is a DataFrame (or a list of dicts), a path to a CSV
    file, CSV bytes, or a dict ``{"data" | "path" | "document", "intent"?,
    "set"?}`` (``"document"``: a member file this connection uploaded
    earlier, by the id its upload returned)."""
    if not isinstance(members, dict) or not members:
        raise ValueError("members maps each record type to its rows: "
                         "{\"Method\": methods_df, \"Assay\": assays_df}")
    out = []
    for model, value in members.items():
        if not isinstance(model, str) or not model:
            raise ValueError(f"a member is named by its record type (a string), not {model!r}")
        spec = dict(value) if isinstance(value, dict) else {"data": value}
        unknown = sorted(set(spec) - _MEMBER_SPEC_KEYS)
        if unknown:
            raise ValueError(f"member {model}: no key(s) {', '.join(unknown)} (a member is "
                             f"{{'data' | 'path' | 'document', 'intent'?, 'set'?}})")
        sources = [k for k in ("data", "path", "document") if spec.get(k) is not None]
        if len(sources) != 1:
            raise ValueError(f"member {model}: give exactly one of data (a DataFrame), path "
                             f"(a CSV file) or document (an id uploaded earlier)")
        if isinstance(spec.get("data"), (str, os.PathLike)):
            spec["path"] = spec.pop("data")
        if spec.get("intent") is not None and spec["intent"] not in INTENTS:
            raise ValueError(f"member {model}: intent is one of {INTENTS}, not {spec['intent']!r}")
        out.append((model, spec))
    return out


def _member_file(model, spec):
    """``(file name, bytes)`` of one member to upload."""
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if spec.get("path") is not None:
        path = os.fspath(spec["path"])
        with open(path, "rb") as fh:
            return os.path.basename(path), fh.read()
    data = spec["data"]
    if isinstance(data, (bytes, bytearray)):
        return f"{model.lower()}_{stamp}.csv", bytes(data)
    return f"{model.lower()}_{stamp}.csv", member_csv(data)


def _uploaded_id(result):
    """The document id an upload returned, or raise its refusal."""
    result.raise_for_refusals()
    row = result.rows[0] if result.rows else {}
    doc_id = (row.get("result") or {}).get("id") or row.get("id")
    if doc_id is None:
        from .errors import WriteRefused
        raise WriteRefused(200, {"reason_code": None,
                                 "detail": "the upload answered no document id"})
    return doc_id


def _queued(data):
    """The poll handle of an answer that is not yet the result, else None."""
    handle = data.get("dataset") if isinstance(data, dict) else None
    if isinstance(handle, dict) and (handle.get("poll") or handle.get("status") in _JOB_INFLIGHT):
        return handle
    return None


def _refusal_status(data):
    """The HTTP status a failed job's refusal would have carried inline: the
    poll body's ``status_code`` (server C-fix 9: 401, 403, 409, 429, 503 …),
    so the client raises what the same refusal raises at the POST. 409 when
    the server named none (an older server) or one that is not a refusal."""
    try:
        status = int(data.get("status_code"))
    except (TypeError, ValueError):
        return 409
    return status if 400 <= status <= 599 else 409


def _is_stop(data):
    return isinstance(data, dict) and data.get("status") == "stopped" and "members" in data


def _counts(summary):
    """``created 4, updated 2 (values: created 9)`` — the non-zero counts."""
    if not isinstance(summary, dict):
        return "no counts"
    parts = [f"{k} {v}" for k, v in summary.items() if isinstance(v, int) and v]
    for key, value in summary.items():
        if isinstance(value, dict) and value:
            inner = ", ".join(f"{k} {v}" for k, v in value.items() if isinstance(v, int) and v)
            if inner:
                parts.append(f"{key.replace('_', ' ')}: {inner}")
    return ", ".join(parts) or "nothing"


def _codes(by_code):
    return ", ".join(f"{code} {n}" for code, n in sorted((by_code or {}).items()))


# ── A dataset job on the server's rail: THE poll loop ────────────────────────
class DatasetJob:
    """A dataset rehearsal, write or undo the server runs in the background
    (it answered a poll handle). :meth:`wait` polls it with THE loop
    :meth:`geodb.client.ExportJob.wait` uses — the server's ``Retry-After``,
    ``on_progress`` after every poll, a stall clock, a hard cap. ``kind``
    (rehearsal · write · undo) is the queued handle's; while it runs,
    ``state``, ``member`` (its position) of ``members``, ``model``,
    ``rows_done`` / ``rows_total`` and ``waiting_for`` (set while it waits its
    turn behind another dataset job on the project, else None) are refreshed
    on the job. Waiting its turn is not a stall: only the hard cap runs."""

    def __init__(self, client, handle):
        self._client = client
        self.id = handle.get("id")
        self.kind = handle.get("kind")
        self.state = handle.get("status") or "queued"
        self.member = self.members = self.model = self.rows_done = self.rows_total = None
        self.waiting_for = None
        self.progress = {}
        self.retry_after = None
        self.headers = {}
        self.result = None
        self.poll_path = self._poll_path(handle.get("poll"))

    def _poll_path(self, link):
        """The server's ``poll`` link, used only as a PATH on the configured
        base URL (the key goes nowhere else); a link to another host, or none,
        is the :data:`ROUTES` template."""
        if isinstance(link, str) and link and "://" not in link and not link.startswith("//"):
            path = link.lstrip("/")
            prefix = self._client.api_prefix.strip("/") + "/"
            return "/" + (path[len(prefix):] if path.startswith(prefix) else path)
        return ROUTES["poll"].format(id=self.id)

    def status(self):
        """One poll; the job's attributes are refreshed from it."""
        from .client import _retry_after
        data, headers = self._client._poll(self.poll_path, records=True)
        self.headers = headers
        self.state = data.get("status", self.state)
        self.kind = data.get("kind", self.kind)
        progress = data.get("progress")
        if isinstance(progress, dict):
            self.progress = progress
            for key in ("member", "members", "model", "rows_done", "rows_total"):
                if key in progress:
                    setattr(self, key, progress[key])
            # The server names it only while the job waits its turn.
            self.waiting_for = progress.get("waiting_for") or None
        self.retry_after = _retry_after(headers) if self.state in _JOB_INFLIGHT else None
        return data

    def wait(self, poll_seconds=None, timeout=3 * 3600, *, stall_timeout=900, on_progress=None):
        """Block until the job is done; return its result (the answer the
        request would have given inline), or raise its refusal."""
        from . import client as core

        def settle(data):
            status = data.get("status")
            result = data.get("result")
            if status == "done" or (status in _JOB_FAILED and _is_stop(result)):
                if not isinstance(result, dict):
                    raise DatasetJobFailed(self.id, f"Dataset job {self.id} finished with no "
                                                    f"result.")
                self.result = result
                return result
            if status in _JOB_FAILED:
                source = result if isinstance(result, dict) and result.get("reason_code") \
                    else data
                if source.get("reason_code"):
                    # The refusal's own status (a 401 is the AuthError family,
                    # a 403 WriteForbidden …), never re-sent unless its code says.
                    raise write_refused_for(_refusal_status(data), source,
                                            self._client._url(self.poll_path),
                                            headers=self.headers)
                raise DatasetJobFailed(
                    self.id, f"Dataset job {self.id} ({self.kind or 'dataset'}) failed: "
                             f"{data.get('error_message') or data.get('error') or data.get('detail') or 'no reason given'}")
            return core._PENDING

        return core._wait_job(self, settle, poll_seconds=poll_seconds, timeout=timeout,
                              stall_timeout=stall_timeout, on_progress=on_progress)

    # -- what _wait_job asks of a job -----------------------------------------
    def progress_key(self):
        return (self.state, self.member, self.members, self.model, self.rows_done)

    def timed_out(self, timeout):
        return DatasetJobFailed(self.id, f"Dataset job {self.id} did not finish within "
                                         f"{timeout} s (state {self.state}, {self.model}: "
                                         f"{self.rows_done} of {self.rows_total} rows).")

    def stalled(self, stall_timeout):
        return DatasetJobFailed(self.id, f"Dataset job {self.id} made no progress for "
                                         f"{stall_timeout} s (state {self.state}, "
                                         f"{self.model}: {self.rows_done} of "
                                         f"{self.rows_total} rows).")

    def __repr__(self):
        return f"<DatasetJob {self.id} {self.kind or ''} {self.state}>"


# ── What a rehearsal answered ────────────────────────────────────────────────
class _Answer:
    def __init__(self, client, body, send=None):
        self._client = client
        self.body = body or {}
        self._send = dict(send or {})

    def get(self, key, default=None):
        return self.body.get(key, default)

    def __getitem__(self, key):
        return self.body[key]

    def __contains__(self, key):
        return key in self.body

    @property
    def members(self):
        return list(self.body.get("members") or [])

    def member(self, model):
        """The answer's entry for ``model`` (or None)."""
        return next((m for m in self.members if m.get("model") == model), None)


class DatasetPlan(_Answer):
    """A dataset REHEARSAL's answer: what writing the dataset would do,
    member by member — NOTHING has been written.

    ``members`` (each: ``summary``, ``exceptions`` / ``warnings``
    ``{count, by_code, first}`` with the file ``line`` of each row, and for an
    Assay member ``assay``: links by sample kind, certificates with something
    to decide, unit mismatches, missing detection limits by method / element
    pair), ``warnings`` (by code, every member), ``before_writing`` (what to
    tell the user, and the remedy per code raised), ``detail`` (the full
    detail file: :meth:`download_detail`), ``plan_token`` / ``expires_at``
    (24 h) and ``write_body`` — what :meth:`write` sends. ``summary()`` is a
    text to show the user."""

    @property
    def dataset_id(self):
        return self.body.get("dataset")

    @property
    def intent(self):
        return self.body.get("intent")

    @property
    def project(self):
        return self.body.get("project")

    @property
    def order(self):
        return list(self.body.get("order") or [])

    @property
    def warnings(self):
        return dict(self.body.get("warnings") or {})

    @property
    def exceptions(self):
        """``{model: {reason_code: n}}`` for every member with an exception
        (a row that would be refused or skipped)."""
        return {m.get("model"): dict((m.get("exceptions") or {}).get("by_code") or {})
                for m in self.members if (m.get("exceptions") or {}).get("count")}

    @property
    def ok(self):
        """True when no member has an exception. Warnings are not counted: a
        plan can be ok and still carry warnings the user must hear."""
        return not self.exceptions

    @property
    def before_writing(self):
        return self.body.get("before_writing") or {}

    @property
    def detail(self):
        return self.body.get("detail") or {}

    @property
    def plan_token(self):
        return self.body.get("plan_token")

    @property
    def expires_at(self):
        return self.body.get("expires_at")

    @property
    def expires(self):
        """``expires_at`` as an aware datetime (or None)."""
        if not self.expires_at:
            return None
        return datetime.datetime.fromisoformat(str(self.expires_at).replace("Z", "+00:00"))

    @property
    def write_body(self):
        """The write the server returned for this plan, sent verbatim by
        :meth:`write` (a copy: never edit it)."""
        body = self.body.get("write")
        return copy.deepcopy(body) if isinstance(body, dict) else None

    def download_detail(self, path):
        """Download the full detail file (parquet: one row per exception and
        warning, per certificate and per method / element pair) — through the
        authenticated redirect; the key never goes to the storage link."""
        link = self.detail.get("download")
        if not link:
            raise ValueError("this answer offers no detail file")
        job = DatasetJob(self._client, {"id": self.dataset_id, "poll": link})
        return self._client._download(self._client._url(job.poll_path), path, records=True)

    def write(self, *, on_progress=None, poll_seconds=None, timeout=3 * 3600,
              stall_timeout=900, busy_retries=3, idempotency_key=None, restart_interrupted=1):
        """Write this plan — ONLY after the user said yes to it.

        Sends the rehearsal's ``write`` body verbatim (it names the approved
        rehearsal and its plan token; it can carry nothing else). The server
        lands the members one at a time, in the approved order, each in its
        own commit and each re-checked against what the user approved; a
        member that would land differently STOPS the write. Returns a
        :class:`DatasetWrite`: ``status`` ``"done"`` or ``"stopped"`` (never
        raised: read ``stopped`` and tell the user exactly what landed and
        what did not), the ``write_id`` and ONE ``undo()`` for everything it
        landed.

        A stop on busy rows (``dataset_rows_busy``) is sent again up to
        ``busy_retries`` times, with a warning each time (a re-send lands only
        what has not landed). A background write the server reports
        interrupted (:class:`geodb.DatasetInterrupted`) is sent again once
        (``restart_interrupted``; it lands only the members that have not
        landed), with a warning. Refused before anything landed: a plan over 24 h
        old (:class:`geodb.DatasetPlanExpired`: re-validate), a body that is not
        this key's plan (:class:`geodb.DatasetPlanInvalid`), a member file that
        changed (:class:`geodb.DatasetFileChanged`), a plan already written
        (:class:`geodb.DatasetAlreadyWritten`)."""
        body = self.write_body
        if body is None:
            raise ValueError("this rehearsal answer carries no write body (no plan was "
                             "sealed): there is nothing to write")
        send = dict(self._send, on_progress=on_progress, poll_seconds=poll_seconds,
                    timeout=timeout, stall_timeout=stall_timeout, busy_retries=busy_retries,
                    restart_interrupted=restart_interrupted)
        attempt = 0
        while True:
            # A 200 answer is replayed for the same Idempotency-Key, so a
            # re-send after a stop carries a new one.
            key = idempotency_key if attempt == 0 and idempotency_key else \
                f"dataset-write-{uuid.uuid4()}"
            data = self._client._send_dataset(copy.deepcopy(body), idempotency_key=key,
                                              accept_stop=True, **send)
            result = DatasetWrite(self._client, data, send)
            code = (result.stopped or {}).get("reason_code")
            if code not in _RETRY_STOPS or attempt >= busy_retries:
                return result
            attempt += 1
            warnings.warn(f"geodb-client: dataset write {self.dataset_id} stopped on {code} "
                          f"(another write held rows it changes); sending it again in "
                          f"{BUSY_FALLBACK_SECONDS} s (attempt {attempt} of {busy_retries}). "
                          f"Members that landed are not landed again.",
                          RuntimeWarning, stacklevel=2)
            from .client import _pause
            _pause(BUSY_FALLBACK_SECONDS)

    def summary(self):
        """A plain-text account of the rehearsal to show the user: per member
        its counts, exception and warning codes; the assay extras; every
        warning; what ``before_writing`` says and the remedy per code; where
        the full detail is; and that nothing is written until their yes.
        Counts, codes and the server's own words only: no cell of a file."""
        b = self.body
        lines = [f"Dataset rehearsal {self.dataset_id} ({self.intent} into project "
                 f"{self.project}): {b.get('rows', 0)} rows in {len(self.members)} member "
                 f"file(s). NOTHING HAS BEEN WRITTEN."]
        if self.order:
            lines.append("Load order: " + " → ".join(self.order))
        for m in self.members:
            lines.append(f"- {m.get('model')} (document {m.get('document')}, "
                         f"{m.get('rows', 0)} rows): {_counts(m.get('summary'))}")
            for kind in ("exceptions", "warnings"):
                part = m.get(kind) or {}
                if part.get("count"):
                    lines.append(f"    {kind} {part['count']}: {_codes(part.get('by_code'))}")
            assay = m.get("assay")
            if isinstance(assay, dict):
                lines.extend(_assay_lines(assay))
        if self.warnings:
            lines.append(f"Warnings, all members: {_codes(self.warnings)}")
        bw = self.before_writing
        if bw.get("say"):
            lines.append(f"Before writing: {bw['say']}")
        for code, remedy in sorted((bw.get("remedies") or {}).items()):
            lines.append(f"    {code}: {remedy}")
        if self.detail.get("download"):
            lines.append(f"Full detail ({self.detail.get('rows', 0)} rows, one per exception "
                         f"and warning): plan.download_detail(path)")
        if self.plan_token:
            lines.append(f"Write it with plan.write() ONLY after the user says yes; the plan "
                         f"expires {self.expires_at}.")
        else:
            lines.append("This answer offers no write.")
        return "\n".join(lines)

    def __repr__(self):
        return (f"<DatasetPlan {self.dataset_id} {self.intent} members="
                f"{[m.get('model') for m in self.members]} exceptions={self.exceptions} "
                f"— NOTHING written; .summary(), then .write() after the user's yes>")


def _assay_lines(assay):
    lines = []
    linked = assay.get("linked") or {}
    if linked:
        lines.append("    linked to samples: " + ", ".join(f"{k} {v}" for k, v in linked.items()))
    certs = assay.get("certificates") or {}
    if certs:
        lines.append(f"    certificates {certs.get('count', 0)}: "
                     f"{certs.get('with_something_to_decide', 0)} with something to decide, "
                     f"{certs.get('first_data', 0)} receiving their first data")
    waiting = (assay.get("waiting_for_sample") or {}).get("count")
    if waiting:
        lines.append(f"    values waiting for a sample: {waiting}")
    for u in assay.get("unit_mismatches") or ():
        lines.append(f"    unit mismatch {u.get('method')} / {u.get('element')}: sent "
                     f"{u.get('sent')}, stored {u.get('stored')} "
                     f"({u.get('certificates')} certificate(s))")
    missing = assay.get("missing_detection_limits") or {}
    if missing.get("count"):
        pairs = ", ".join(f"{p.get('method')} / {p.get('element')}"
                          for p in missing.get("pairs") or ())
        lines.append(f"    no detection limit for {missing['count']} method / element "
                     f"pair(s): {pairs}")
    return lines


# ── What a write answered ────────────────────────────────────────────────────
class DatasetWrite(_Answer):
    """A dataset WRITE's answer. ``status`` is ``"done"`` (every member
    landed) or ``"stopped"``: ``stopped`` names the member it stopped at
    (``member``, ``reason_code``, ``detail``, ``remedy``, ``offending``), and
    ``landed`` / ``not_landed`` list the members each way — tell the user
    both. ``undo()`` is ONE request for everything this dataset landed;
    ``rehearse_rest()`` rehearses what is left (it joins the same write, so
    the one Undo still covers it all)."""

    @property
    def status(self):
        return self.body.get("status")

    @property
    def done(self):
        return self.status == "done"

    @property
    def stopped(self):
        return self.body.get("stopped") if self.status == "stopped" else None

    @property
    def landed(self):
        return [m for m in self.members if m.get("status") == "landed"]

    @property
    def not_landed(self):
        return [m for m in self.members if m.get("status") != "landed"]

    @property
    def write_id(self):
        return self.body.get("write_id")

    @property
    def audit_batch_id(self):
        return self.body.get("audit_batch_id")

    @property
    def undo_handle(self):
        """The server's undo handle (``available``: True · ``"partial"`` ·
        False, ``not_reached``)."""
        return self.body.get("undo")

    @property
    def say(self):
        return self.body.get("say")

    def undo(self, dry_run=False, **wait):
        """Undo the WHOLE dataset write — every member it landed, children
        before parents — in one request (``dry_run=True`` first to show the
        user). A large undo runs in the background and is waited for. Returns
        a :class:`geodb.WriteResult`."""
        from .writes import WriteResult
        if not self.write_id:
            raise ValueError("this write landed nothing, so there is nothing to undo")
        body = {"intent": "undo", "write_id": str(self.write_id), "dry_run": bool(dry_run)}
        data = self._client._send_dataset(body, **dict(self._send, **wait))
        return WriteResult(self._client, data)

    def rehearse_rest(self, **send):
        """Rehearse what the stopped write did not land — the answer's
        ``rehearse_rest`` request, sent verbatim (the same files, the data as
        it is now). Show the user its answer; write it only after their yes."""
        rest = self.body.get("rehearse_rest")
        if not self.stopped or not isinstance(rest, dict):
            raise ValueError("only a stopped write has a rest to rehearse")
        options = dict(self._send, **send)
        data = self._client._send_dataset(copy.deepcopy(rest), **options)
        return DatasetPlan(self._client, data, options)

    def raise_for_stop(self):
        """Raise the stop as its typed exception (e.g.
        :class:`geodb.DatasetPlanChanged`); return self when it did not stop."""
        if not self.stopped:
            return self
        raise write_refused_for(409, dict(self.stopped), None)

    def summary(self):
        """A plain-text account to tell the user: what landed, what did not,
        why it stopped, and the one Undo."""
        lines = [f"Dataset write {self.write_id or '(nothing landed)'}: "
                 f"{'STOPPED part-way' if self.stopped else 'done'}."]
        for m in self.members:
            lines.append(f"- {m.get('model')} (document {m.get('document')}): "
                         f"{m.get('status')}"
                         + (f" — {_counts(m.get('summary'))}" if m.get("summary") else ""))
        if self.stopped:
            s = self.stopped
            lines.append(f"Stopped at {(s.get('member') or {}).get('model')}: "
                         f"{s.get('reason_code')} — {s.get('detail')}")
            if s.get("remedy"):
                lines.append(f"    {s['remedy']}")
        if self.say:
            lines.append(self.say)
        if self.write_id:
            lines.append("Undo everything it landed: .undo() (one request).")
        return "\n".join(lines)

    def __repr__(self):
        return (f"<DatasetWrite {self.status} write_id={self.write_id} landed="
                f"{[m.get('model') for m in self.landed]} not_landed="
                f"{[m.get('model') for m in self.not_landed]}>")


# ── The Client's part ────────────────────────────────────────────────────────
class DatasetsMixin:
    """The dataset helpers of :class:`geodb.Client`."""

    def validate_dataset(self, project, members, intent="create", *, acknowledge=None,
                         qc_mapping=None, on_progress=None, poll_seconds=None,
                         timeout=3 * 3600, stall_timeout=900, busy_retries=3,
                         idempotency_key=None, restart_interrupted=1):
        """Rehearse a whole DATASET — several record types that depend on one
        another (new laboratories, methods with their detection limits,
        certificates, QC samples, and the assays, samples or logging rows that
        hang off them) — and return a :class:`DatasetPlan`. Nothing is
        written.

        ``members`` maps each record type to its rows: a pandas DataFrame (its
        columns are the record type's field names, as ``describe(model)`` lists
        them; empty cells are "not sent"), a path to a UTF-8 CSV, or a dict
        ``{"data" | "path" | "document", "intent"?, "set"?}`` (``document``:
        a member file this connection uploaded earlier, by its id). One member
        per record type; the server puts them in load order, so a parent in
        one member EXISTS when its children are checked. Each member is
        uploaded as a document (category OT; an upload no completed write
        uses goes to the Trash after 7 days), then ONE rehearsal names them.
        ``intent`` is ``"create"`` (never overwrites) or ``"upsert"``;
        ``acknowledge`` and ``qc_mapping`` are as for ``write``. A large
        dataset runs in the background: the wait follows the server's
        ``Retry-After`` and calls ``on_progress(job)``.

        **The dry run is the authority.** Its answer is what the write would
        do, row for row, against the data as it is: never hand-match the
        files against data you pulled to predict it. **Read
        ``before_writing`` and every warning, not just the counts** —
        ``plan.summary()`` is the text to show the user — and **get the
        user's yes before ``plan.write()``**. The write lands member by member
        under **one Undo** for the whole dataset (``DatasetWrite.undo()``).

        A refusal of the whole request raises a :class:`geodb.DatasetRefused`
        subclass (``DatasetFileUnreadable``, ``DatasetTooLarge`` …); busy rows
        (``dataset_rows_busy``) and a project busy with another dataset job
        (``dataset_project_busy``) are re-sent up to ``busy_retries`` times after
        the server's ``Retry-After``, with a warning each time; a background
        job the server reports interrupted (``dataset_interrupted``) is sent
        again once (``restart_interrupted``), with a warning."""
        if intent not in INTENTS:
            raise ValueError(f"a dataset is landed by one of {INTENTS}, not {intent!r}")
        specs = _member_specs(members)                    # all checked before any upload
        entries = []
        for model, spec in specs:
            if spec.get("document") is not None:
                doc_id = spec["document"]
            else:
                name, data = _member_file(model, spec)
                doc_id = _uploaded_id(self.upload_document(
                    data, project, MEMBER_CATEGORY, file_name=name,
                    title=f"Dataset member: {model}"))
            entry = {"document": doc_id, "model": model}
            if spec.get("intent") is not None:
                entry["intent"] = spec["intent"]
            if "set" in spec:
                entry["set"] = spec["set"]
            entries.append(entry)
        body = {"model": DATASET_MODEL, "intent": intent, "project": project, "dry_run": True,
                "dataset": {"members": entries}}
        if acknowledge:
            body["acknowledge"] = list(acknowledge)
        if qc_mapping:
            body["qc_mapping"] = qc_mapping
        send = {"on_progress": on_progress, "poll_seconds": poll_seconds, "timeout": timeout,
                "stall_timeout": stall_timeout, "busy_retries": busy_retries,
                "restart_interrupted": restart_interrupted}
        data = self._send_dataset(body, idempotency_key=idempotency_key, **send)
        return DatasetPlan(self, data, send)

    def _send_dataset(self, body, *, idempotency_key=None, accept_stop=False, on_progress=None,
                      poll_seconds=None, timeout=3 * 3600, stall_timeout=900, busy_retries=3,
                      restart_interrupted=1):
        """THE dataset request (rehearsal, write, undo, rehearse_rest): POST
        ``ROUTES["records"]`` → the answer body. A poll handle is waited for
        (:class:`DatasetJob`, THE poll loop); ``accept_stop`` returns a write's
        stop answer even when it came with a refusal status; anything else
        raises its typed exception, except:

        * a "send it again" refusal (:data:`_RETRY_CODES`: the busy codes and a
          429 ``throttled`` — the key's rate, or its cap on jobs in flight) is
          re-sent after its Retry-After (else :data:`BUSY_FALLBACK_SECONDS`, or
          the code's own :data:`_FALLBACK_SECONDS`), up to ``busy_retries``
          times, out loud — never after a wait past
          :data:`MAX_RESEND_WAIT_SECONDS` (raised instead, with ``retry_after``);
        * ``dataset_interrupted`` (:data:`_RESTART_CODES`) is re-sent at once,
          up to ``restart_interrupted`` times (default once), out loud;
        * ``writes_switched_off`` (a staff switch) is re-sent at most
          :data:`SWITCHED_OFF_RETRIES` time, only after a ``Retry-After`` the
          server named (within :data:`MAX_RESEND_WAIT_SECONDS`), out loud;
          without one it is raised at once — never a loop.

        A re-send is ALWAYS the same body. Its Idempotency-Key follows the
        server's rule (``protocol/writes/idempotency.py``: only a 2xx is
        stored): a refusal of the POST itself was not stored, so the re-send
        keeps the key; a refusal that arrived at a job's POLL came after a 202
        that WAS stored under the key — sent under it again, the server would
        answer the same finished job — so that re-send carries a new key."""
        from .client import _pause
        key = idempotency_key or f"dataset-{uuid.uuid4()}"
        attempt = restarts = switched = 0
        while True:
            resp = self._records_response(body, key, path=ROUTES["records"])
            data = self._body(resp)
            polled = False
            if 200 <= resp.status_code < 300 and isinstance(data, dict):
                handle = _queued(data)
                if handle is None:
                    return data
                try:
                    return DatasetJob(self, handle).wait(
                        poll_seconds=poll_seconds, timeout=timeout,
                        stall_timeout=stall_timeout, on_progress=on_progress)
                except WriteRefused as refused:        # the job's answer at its poll
                    exc, wait, polled = refused, getattr(refused, "retry_after", None), True
            elif accept_stop and _is_stop(data):
                return data
            else:
                exc = write_refused_for(resp.status_code, data, resp.url,
                                        headers=getattr(resp, "headers", None))
                wait = _retry_after_header(getattr(resp, "headers", None))
            if isinstance(exc, DatasetInterrupted) and restarts < restart_interrupted:
                restarts += 1
                warnings.warn(f"geodb-client: the dataset job was interrupted on the server "
                              f"(error_id={exc.error_id}); sending the same request again "
                              f"(restart {restarts} of {restart_interrupted}).",
                              RuntimeWarning, stacklevel=3)
            elif isinstance(exc, WritesSwitchedOff):
                if switched >= SWITCHED_OFF_RETRIES or wait is None \
                        or wait > MAX_RESEND_WAIT_SECONDS:
                    raise exc
                switched += 1
                warnings.warn(f"geodb-client: the dataset request was refused "
                              f"writes_switched_off (writes through AI connections are switched "
                              f"off on the server); sending it again ONCE in {wait} s, as the "
                              f"server's Retry-After says.", RuntimeWarning, stacklevel=3)
                _pause(wait)
            elif exc.reason_code in _RETRY_CODES and attempt < busy_retries and (
                    wait is None or wait <= MAX_RESEND_WAIT_SECONDS):
                attempt += 1
                if wait is None:
                    wait = _FALLBACK_SECONDS.get(exc.reason_code, BUSY_FALLBACK_SECONDS)
                warnings.warn(f"geodb-client: the dataset request was refused {exc.reason_code}; "
                              f"sending it again in {wait} s (attempt {attempt} of "
                              f"{busy_retries}).", RuntimeWarning, stacklevel=3)
                _pause(wait)
            else:
                raise exc
            if polled:
                key = f"dataset-{uuid.uuid4()}"
