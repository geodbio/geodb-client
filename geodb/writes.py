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

__all__ = ["WriteResult", "rows_from", "idempotency_header", "batch_rows"]

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
    """Counts summed key by key, RECURSIVELY (``summary.skipped_by_reason`` is
    a nested count dict); a non-number kept when every batch agrees, else the
    list of each batch's value."""
    dicts = [d for d in dicts if d]
    out = {}
    for key in dict.fromkeys(k for d in dicts for k in d):
        values = [d[key] for d in dicts if key in d]
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
            out[key] = sum(values)
        elif all(isinstance(v, dict) for v in values):
            out[key] = _sum_counts(values)
        elif all(v == values[0] for v in values):
            out[key] = values[0]
        else:
            out[key] = values
    return out


def _unique(items):
    seen, out = set(), []
    for item in items:
        key = json.dumps(item, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _canon(value):
    return json.dumps(value, sort_keys=True, default=str)


def _group_key(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, str):
        return value.strip().casefold()   # the server's name match folds case
    return _canon(value)


def batch_rows(records, limit, group_field=None):
    """Positions of ``records`` cut into batches of at most ``limit`` rows,
    never parting the rows that share a value of ``group_field`` (compared
    trimmed and case-folded, as the server matches names). Groups are packed
    in the order they first appear; each batch lists its positions in the
    order given. A row without the field is its own group. Returns the list
    of batches, or ``(value, size)`` for a group larger than the limit."""
    groups, order = {}, []
    for i, row in enumerate(records):
        key = _group_key(row.get(group_field)) if group_field else None
        key = ("row", i) if key is None else ("value", key)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(i)
    batches, current = [], []
    for key in order:
        members = groups[key]
        if len(members) > limit:
            return (records[members[0]].get(group_field), len(members))
        if len(current) + len(members) > limit:
            batches.append(sorted(current))
            current = []
        current.extend(members)
    if current:
        batches.append(sorted(current))
    return batches


#: Top-level keys of a records answer merged by rule; every OTHER key is kept
#: at the top only when every batch answered the same value, else per batch.
_MERGED_KEYS = ("rows", "summary", "cascade", "warnings", "catalog_entries",
                "ignored_fields", "sets", "before_writing", "assays_left_unlinked")

BATCHES_NOTE = (
    "These rows were validated in {n} separate requests of at most {limit} rows (the "
    "server's batch_limit), cut so that rows sharing {group} stay together. Checks that "
    "look ACROSS rows (duplicates in the batch, overlapping intervals of one hole) were "
    "made within each batch only, never across the batches. Row indexes are positions in "
    "the rows passed. A write of these rows must be sent in the SAME batches "
    "(result['batches']['per_batch'][i]['rows'] lists each one's row positions), each its "
    "own write_id and Undo, or through the dataset door for one Undo.")


def merge_batch_answers(answers, batch_rows, limit, group_field=None):
    """ONE dry-run answer from the answers to the batches ``Client.validate``
    cut over ``batch_limit``. ``batch_rows[i]`` lists the positions (in the rows
    passed) of batch ``i``'s rows, in the order sent: each row's ``index``, and
    each warning's ``rows``, map back to those positions. ``summary`` /
    ``cascade`` counts (nested ones too) and ``assays_left_unlinked`` are
    summed, warnings and list entries joined once each, ``ignored_fields``
    united. Any other key is kept at the top only when every batch gave the
    same value; otherwise each batch's value stays under
    ``batches.per_batch[i][key]`` (never batch 1's alone)."""
    def position(i, j):
        rows = batch_rows[i]
        return rows[j] if isinstance(j, int) and 0 <= j < len(rows) else j

    merged = {}
    rows = []
    for i, answer in enumerate(answers):
        for row in answer.get("rows") or []:
            row = dict(row)
            if isinstance(row.get("index"), int):
                row["index"] = position(i, row["index"])
            rows.append(row)
    merged["rows"] = sorted(rows, key=lambda r: r["index"] if isinstance(r.get("index"), int)
                            else float("inf"))
    merged["summary"] = _sum_counts(a.get("summary") for a in answers)
    if any(a.get("cascade") for a in answers):
        merged["cascade"] = _sum_counts(a.get("cascade") for a in answers)
    unlinked = [a.get("assays_left_unlinked") for a in answers if "assays_left_unlinked" in a]
    if unlinked:
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in unlinked):
            merged["assays_left_unlinked"] = sum(unlinked)
    warnings = []
    for i, answer in enumerate(answers):
        for warning in answer.get("warnings") or []:
            if isinstance(warning, dict) and isinstance(warning.get("rows"), list):
                warning = dict(warning, rows=[position(i, r) for r in warning["rows"]])
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

    per_batch = [{"rows": list(r)} for r in batch_rows]
    differing = []
    keys = dict.fromkeys(k for a in answers for k in a if k not in _MERGED_KEYS)
    if not all(isinstance(a.get("catalog_entries"), list) or not a.get("catalog_entries")
               for a in answers):
        keys["catalog_entries"] = None
    if unlinked and "assays_left_unlinked" not in merged:
        keys["assays_left_unlinked"] = None
    for key in keys:
        values = [_canon(a.get(key)) if key in a else None for a in answers]
        if all(v == values[0] for v in values) and values[0] is not None:
            merged[key] = answers[0][key]
        else:
            differing.append(key)
            for i, answer in enumerate(answers):
                if key in answer:
                    per_batch[i][key] = answer[key]
    asks = [a.get("before_writing") for a in answers]
    if any(asks):
        present = [a for a in asks if a]
        if len(present) == len(asks) and all(_canon(a) == _canon(present[0]) for a in present):
            merged["before_writing"] = present[0]
        else:
            ask = {"confirm_with_user": True,
                   "say": ("The batches answered different asks: read each in "
                           "result['batches']['per_batch'][i]['before_writing'] and tell the "
                           "user all of them before any batch is written.")}
            counted = [a.get("warnings") for a in present if isinstance(a.get("warnings"), int)]
            if counted:
                ask["warnings"] = sum(counted)
            remedies = {}
            for a in present:
                remedies.update(a.get("remedies") or {})
            if remedies:
                ask["remedies"] = remedies
            merged["before_writing"] = ask
            differing.append("before_writing")
            for i, a in enumerate(asks):
                if a:
                    per_batch[i]["before_writing"] = a
    merged["batches"] = {
        "count": len(answers), "limit": limit, "grouped_by": group_field,
        "per_batch": per_batch, "differing": differing,
        "note": BATCHES_NOTE.format(n=len(answers), limit=limit,
                                    group=f'"{group_field}"' if group_field
                                    else "nothing (no identifying field)"),
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
