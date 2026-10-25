"""
Writing through the geoDB protocol's ONE write endpoint (``POST /api/v2/records/``).

    gx = geodb.Client(token="gdbg_…")          # a key that may write records
    gx.describe("DrillSample")                 # the fields, the identity, the sets
    check = gx.validate("DrillSample", rows, logging_set="pXRF 2026")   # dry run
    result = gx.write("DrillSample", rows, logging_set="pXRF 2026")
    result.summary            # {'created': 5, 'refused': 0, …}
    result.refused            # rows the server refused, each with reason_code + remedy
    gx.undo(result.write_id)  # reverse it
    gx.write("VectorLayer", features, layer={"name": "Faults", "kind": "geology_fault"},
             project=12, dry_run=True)   # a map layer: one write = one new layer

``describe(model)`` is each record type's live contract (fields, identity,
sets, which intents need the user's yes); ``Client.write``'s docstring lists
every intent and every face (projects, reports, settings, map layers).

Every call answers per ROW: a bad row is refused with its own ``reason_code``
and ``remedy`` while the rest of the batch lands — so a write returns a
:class:`WriteResult` rather than raising for one bad row. Only a refusal of
the whole request raises :class:`geodb.WriteRefused`.
"""

import hashlib
import json

__all__ = ["WriteResult", "rows_from", "idempotency_header"]

#: The row statuses that mean the server CHANGED something.
LANDED = ("created", "updated", "accepted", "retracted", "restored", "undone")

#: The server's own limit on an Idempotency-Key (``protocol/writes/
#: idempotency.py`` MAX_KEY_LENGTH); a longer key is refused there.
MAX_IDEMPOTENCY_KEY_LENGTH = 255


def idempotency_header(key):
    """THE ``Idempotency-Key`` header value for ``key`` (every write and upload
    sends it through here).

    A key of printable ASCII within the server's length limit is sent exactly
    as given (byte-identical to earlier releases, so a retry still replays the
    first answer). Any other key (an en dash or other non-ASCII character, a
    control character, longer than 255) cannot travel in an HTTP header or
    would be refused, so it is sent as ``"sha256:"`` + the hex digest of its
    UTF-8 bytes: deterministic, so the same key always sends the same header
    and a retry with it still dedupes."""
    value = str(key)
    if len(value) <= MAX_IDEMPOTENCY_KEY_LENGTH and all(32 <= ord(ch) < 127 for ch in value):
        return value
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sum_counts(dicts):
    out = {}
    for d in dicts:
        for k, v in (d or {}).items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                out[k] = out.get(k, 0) + v
            else:
                out.setdefault(k, v)
    return out


def _unique(items):
    seen, out = set(), []
    for item in items:
        key = json.dumps(item, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def merge_batch_answers(answers, sizes, limit):
    """ONE dry-run answer from the answers to consecutive batches of one
    request's rows (``Client.validate`` over ``batch_limit``): rows keep the
    index of their position in the rows passed (each batch's indexes offset by
    the rows before it), ``summary`` and ``cascade`` counts are summed, warnings
    and list entries joined once each, ``ignored_fields`` united; the first
    batch's answer gives every other key. ``batches`` says it was split."""
    merged = dict(answers[0])
    rows, offset, offsets = [], 0, []
    for answer, size in zip(answers, sizes):
        offsets.append(offset)
        for row in answer.get("rows") or []:
            row = dict(row)
            if isinstance(row.get("index"), int):
                row["index"] += offset
            rows.append(row)
        offset += size
    merged["rows"] = rows
    merged["summary"] = _sum_counts(a.get("summary") for a in answers)
    if any(a.get("cascade") for a in answers):
        merged["cascade"] = _sum_counts(a.get("cascade") for a in answers)
    warnings = []
    for answer, base in zip(answers, offsets):
        for warning in answer.get("warnings") or []:
            if isinstance(warning, dict) and isinstance(warning.get("rows"), list):
                warning = dict(warning, rows=[r + base if isinstance(r, int) else r
                                              for r in warning["rows"]])
            warnings.append(warning)
    if warnings:
        merged["warnings"] = _unique(warnings)
    entries = [a.get("catalog_entries") for a in answers if a.get("catalog_entries")]
    if entries and all(isinstance(e, list) for e in entries):
        merged["catalog_entries"] = _unique([x for e in entries for x in e])
    ignored = sorted({f for a in answers for f in (a.get("ignored_fields") or [])})
    if ignored:
        merged["ignored_fields"] = ignored
    sets = {}
    for answer in answers:
        sets.update(answer.get("sets") or {})
    if sets:
        merged["sets"] = sets
    asks = [a.get("before_writing") for a in answers if a.get("before_writing")]
    if asks:
        ask = dict(asks[0])
        counted = [a.get("warnings") for a in asks if isinstance(a.get("warnings"), int)]
        if counted:
            ask["warnings"] = sum(counted)
        remedies = {}
        for a in asks:
            remedies.update(a.get("remedies") or {})
        if remedies:
            ask["remedies"] = remedies
        merged["before_writing"] = ask
    merged["batches"] = {
        "count": len(answers), "limit": limit,
        "note": (f"validated in {len(answers)} requests of at most {limit} rows (the "
                 f"server's batch_limit); row indexes are positions in the rows passed. "
                 f"A write of these rows must also go in batches of at most {limit}, each "
                 f"its own write_id and Undo, or through the dataset door for one Undo."),
    }
    return merged


def _plain(value):
    """A JSON-safe scalar (numpy scalars and NaN from a DataFrame included)."""
    if hasattr(value, "item") and not isinstance(value, (list, dict, str, bytes)):
        try:
            value = value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, float) and value != value:     # NaN -> null
        return None
    return value


def rows_from(rows):
    """``rows`` as a list of plain dicts: a list of dicts, or a pandas
    DataFrame (NaN becomes null; numpy scalars become Python ones)."""
    if hasattr(rows, "to_dict") and hasattr(rows, "columns"):
        rows = rows.to_dict("records")
    if isinstance(rows, dict):
        rows = [rows]
    out = []
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError(f"each record must be a dict of field: value, got {type(row).__name__}")
        out.append({k: _plain(v) for k, v in row.items()})
    return out


class WriteResult:
    """What the records endpoint answered for one write (or dry run, or undo).

    ``body`` is the whole answer; the common parts have attributes. Index it
    like the dict it wraps (``result["cascade"]``)."""

    def __init__(self, client, body):
        self._client = client
        self.body = body or {}

    # -- the answer ------------------------------------------------------
    @property
    def write_id(self):
        """The write's id — what :meth:`geodb.Client.undo` names. None for a
        dry run, and when nothing changed."""
        return self.body.get("write_id")

    @property
    def intent(self):
        return self.body.get("intent")

    @property
    def dry_run(self):
        return bool(self.body.get("dry_run"))

    @property
    def summary(self):
        return self.body.get("summary") or {}

    @property
    def rows(self):
        return self.body.get("rows") or []

    @property
    def refused(self):
        """The rows the server refused or skipped, each with ``reason_code``
        and ``remedy`` (act on the remedy; never parse ``detail``)."""
        return [r for r in self.rows if r.get("status") in ("refused", "skipped")]

    @property
    def ok(self):
        """True when no row was refused or skipped."""
        return not self.refused

    @property
    def complete(self):
        """For an undo: True when every row was reversed."""
        return self.body.get("complete")

    @property
    def before_writing(self):
        """On a dry run of an intent that changes existing data: what to tell
        the user, and that you must wait for their yes."""
        return self.body.get("before_writing")

    def get(self, key, default=None):
        return self.body.get(key, default)

    def __getitem__(self, key):
        return self.body[key]

    def __contains__(self, key):
        return key in self.body

    # -- acting on it ----------------------------------------------------
    def undo(self, dry_run=False):
        """Undo this write (``client.undo(self.write_id)``)."""
        if not self.write_id:
            raise ValueError("this result has no write_id (a dry run, or nothing changed)")
        return self._client.undo(self.write_id, dry_run=dry_run)

    def raise_for_refusals(self):
        """Raise :class:`geodb.RowsRefused` (status 200: the request was
        answered) carrying every refused or skipped row, if any."""
        from .errors import RowsRefused
        if self.refused:
            raise RowsRefused(self.refused)
        return self

    def to_dataframe(self):
        """The per-row outcomes as a pandas DataFrame."""
        import pandas as pd
        return pd.DataFrame(self.rows)

    def __repr__(self):
        kind = "dry run" if self.dry_run else (self.intent or "write")
        refused = len(self.refused)
        return (f"<WriteResult {kind} summary={json.dumps(self.summary)} "
                f"write_id={self.write_id} rows={len(self.rows)}"
                + (f" refused={refused}" if refused else "")
                + " — per-row outcomes: .rows / .refused / .to_dataframe()"
                + ("; reverse it: .undo()" if self.write_id else "") + ">")
