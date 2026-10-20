"""
W7b: a dataset member is a UTF-8 CSV whose header is exactly the frame's
columns (server plan §6.2: the header is the records contract's field names;
an empty cell is "not sent"). The writer must never invent a value: NaN /
None / NA / NaT are EMPTY (never "nan", "None", "<NA>", "NaT"), an integer
stays an integer even in a column pandas made float by a missing value, and a
string identifier keeps its exact characters (leading zeros included).
"""

import csv
import datetime
import io
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from geodb.datasets import member_csv


def _rows(data):
    text = data.decode("utf-8")
    return list(csv.reader(io.StringIO(text, newline="")))


def test_missing_values_are_empty_cells_never_words():
    df = pd.DataFrame({
        "name": ["S-1", "S-2", "S-3", "S-4"],
        "f": [1.5, np.nan, 2.5, 3.5],
        "o": ["a", None, pd.NA, "d"],
        "t": pd.to_datetime(["2026-01-02", None, "2026-01-03", "2026-01-04"]),
        "i": pd.array([1, None, 3, 4], dtype="Int64"),
    })
    rows = _rows(member_csv(df))
    assert rows[2] == ["S-2", "", "", "", ""]
    flat = "|".join(c for r in rows for c in r).lower()
    for word in ("nan", "none", "<na>", "nat"):
        assert word not in flat.split("|")


def test_integers_stay_integers_even_when_a_gap_made_the_column_float():
    df = pd.DataFrame({"hole": [101, None, 103], "count": [1, 2, 3],
                       "nullable": pd.array([7, None, 9], dtype="Int64"),
                       "depth": [2.0, 4.5, 6.0]})
    assert df["hole"].dtype == float           # pandas made it float
    rows = _rows(member_csv(df))
    assert rows[1] == ["101", "1", "7", "2"]
    assert rows[2] == ["", "2", "", "4.5"]
    assert rows[3] == ["103", "3", "9", "6"]


def test_string_identifiers_keep_their_exact_characters():
    df = pd.DataFrame({"name": ["007", "0012.50", "1e5", "  padded "], "bhid": ["DH-01"] * 4})
    rows = _rows(member_csv(df))
    assert [r[0] for r in rows[1:]] == ["007", "0012.50", "1e5", "  padded "]


def test_floats_are_written_plainly_never_in_exponent_form():
    df = pd.DataFrame({"value": [0.5, 1e-7, 123456789012.0, 0.1 + 0.2]})
    rows = _rows(member_csv(df))
    assert [r[0] for r in rows[1:]] == ["0.5", "0.0000001", "123456789012",
                                        "0.30000000000000004"]


def test_decimals_keep_their_digits():
    df = pd.DataFrame({"value": [Decimal("0.0050"), Decimal("1E-7"), None]})
    assert [r[0] for r in _rows(member_csv(df))[1:]] == ["0.0050", "0.0000001", ""]


def test_unicode_round_trips_as_utf8_without_a_bom():
    df = pd.DataFrame({"name": ["Säo João", "岩石-1"], "notes": ["µm, \"quoted\"", "line\nbreak"]})
    data = member_csv(df)
    assert not data.startswith(b"\xef\xbb\xbf")
    rows = _rows(data)
    assert rows[1] == ["Säo João", "µm, \"quoted\""]
    assert rows[2] == ["岩石-1", "line\nbreak"]


def test_the_header_is_exactly_the_columns_and_no_index_is_written():
    df = pd.DataFrame({"name": ["A", "B"], "depth_from": [0, 1]}, index=[10, 20])
    df.index.name = "idx"
    rows = _rows(member_csv(df))
    assert rows[0] == ["name", "depth_from"]
    assert rows[1] == ["A", "0"]
    assert len(rows) == 3


def test_booleans_and_dates_are_plain_text():
    df = pd.DataFrame({"flag": [True, False],
                       "day": [datetime.date(2026, 1, 2), pd.Timestamp("2026-03-04")],
                       "at": [pd.Timestamp("2026-03-04 05:06:07"), None]})
    rows = _rows(member_csv(df))
    assert rows[1] == ["true", "2026-01-02", "2026-03-04T05:06:07"]
    assert rows[2] == ["false", "2026-03-04", ""]


def test_a_list_of_dicts_is_written_like_a_frame():
    rows = _rows(member_csv([{"name": "A", "x": 1}, {"name": "B"}]))
    assert rows == [["name", "x"], ["A", "1"], ["B", ""]]


def test_a_frame_with_no_columns_is_refused():
    with pytest.raises(ValueError):
        member_csv(pd.DataFrame())
