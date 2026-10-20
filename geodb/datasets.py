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

import csv
import datetime
import io
from decimal import Decimal

__all__ = ["member_csv"]


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
