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

import json

__all__ = ["WriteResult", "rows_from"]

#: The row statuses that mean the server CHANGED something.
LANDED = ("created", "updated", "accepted", "retracted", "restored", "undone")


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
