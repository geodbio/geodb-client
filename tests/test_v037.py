"""
0.3.7 — the two client defects an outside AI hit loading a claim block
(2026-10-25):

* an Idempotency-Key holding a non-Latin-1 character (an en dash copied from a
  package name) raised UnicodeEncodeError before the request was sent;
* a 1,054-row validate was refused 400 ``batch_too_large`` whole, though the
  client could have known the limit from describe.
"""

import hashlib

import pytest

import geodb
from geodb import BatchTooLarge, WriteRefused
from geodb.writes import idempotency_header, merge_batch_answers

from test_writes import FakeResponse, client


# ── 1. the Idempotency-Key header ─────────────────────────────────────────────

def test_a_printable_ascii_key_is_sent_byte_identical():
    for key in ("k-1", "my-batch-0001", "a b ~!", "x" * 255):
        assert idempotency_header(key) == key


def test_a_non_ascii_key_is_sent_as_its_stable_sha256():
    key = "Hercules – package 2"     # en dash
    sent = idempotency_header(key)
    assert sent == "sha256:" + hashlib.sha256(key.encode("utf-8")).hexdigest()
    assert sent == idempotency_header(key), "the same key always sends the same header"
    sent.encode("latin-1")                     # travels in an HTTP header
    assert len(sent) <= 255


def test_control_characters_and_over_long_keys_are_encoded_too():
    for key in ("tab\there", "x" * 256, "del\x7f"):
        sent = idempotency_header(key)
        assert sent.startswith("sha256:") and len(sent) == 71


def test_write_sends_an_en_dash_key_instead_of_crashing():
    gx, s = client(FakeResponse(200, {"rows": [], "summary": {}}))
    gx.write("DrillCollar", [{"name": "DH-1"}], idempotency_key="claims – block 1")
    header = s.calls[0][2]["headers"]["Idempotency-Key"]
    assert header == idempotency_header("claims – block 1")
    header.encode("latin-1")


def test_an_upload_key_goes_through_the_same_helper():
    import inspect
    from geodb import uploads
    assert "idempotency_header(" in inspect.getsource(uploads.UploadsMixin._upload)


# ── 2. the batch limit ───────────────────────────────────────────────────────

def _describe(limit=1000, long_form=False):
    return FakeResponse(200, {"model": "ClaimStake", "batch_limit": limit,
                              "long_form": long_form},
                        url="http://t/api/v2/records/describe/ClaimStake/")


def _answer(start, n, *, refused_at=None, warnings=None):
    rows = [{"index": i, "status": "refused" if i == refused_at else "created"}
            for i in range(n)]
    body = {"model": "ClaimStake", "intent": "create", "dry_run": True, "write_id": None,
            "summary": {"created": n - (refused_at is not None),
                        "refused": int(refused_at is not None)},
            "rows": rows}
    if warnings:
        body["warnings"] = warnings
    return FakeResponse(200, body)


ROWS = [{"sequence_number": f"S{i}", "claim_package": "P"} for i in range(1054)]


def test_a_write_at_or_under_the_default_limit_never_asks_describe():
    gx, s = client(FakeResponse(200, {"rows": [], "summary": {}}))
    gx.write("ClaimStake", ROWS[:1000], project=250)
    assert [c[0] for c in s.calls] == ["POST"]


def test_validate_over_the_limit_splits_and_answers_once():
    gx, s = client(_describe(), _answer(0, 1000, refused_at=3), _answer(1000, 54, refused_at=10))
    result = gx.validate("ClaimStake", ROWS, project=250, idempotency_key="load-1")
    get, first, second = s.calls
    assert get[0] == "GET" and get[1].endswith("/records/describe/ClaimStake/")
    assert get[2]["params"] == {"project": 250}
    assert len(first[2]["json"]["records"]) == 1000
    assert second[2]["json"]["records"] == ROWS[1000:]
    assert first[2]["json"]["dry_run"] is True and second[2]["json"]["project"] == 250
    keys = [first[2]["headers"]["Idempotency-Key"], second[2]["headers"]["Idempotency-Key"]]
    assert keys == ["load-1:batch-1", "load-1:batch-2"], "one key per distinct body"
    assert [r["index"] for r in result.rows] == list(range(1054))
    assert [r["index"] for r in result.refused] == [3, 1010]
    assert result.summary == {"created": 1052, "refused": 2}
    assert result["batches"]["count"] == 2 and result["batches"]["limit"] == 1000


def test_merge_offsets_warning_rows_and_joins_once():
    w = {"code": "w", "detail": "same"}
    merged = merge_batch_answers(
        [{"rows": [{"index": 0}], "summary": {"created": 1}, "warnings": [w, {"code": "r",
                                                                         "rows": [0]}],
          "ignored_fields": ["b"], "before_writing": {"say": "x", "warnings": 1,
                                                       "remedies": {"w": "a"}}},
         {"rows": [{"index": 0}], "summary": {"created": 1}, "warnings": [w, {"code": "r",
                                                                         "rows": [0]}],
          "ignored_fields": ["a"], "before_writing": {"say": "y", "warnings": 2,
                                                       "remedies": {"v": "b"}}}],
        [3, 1], 3)
    assert [r["index"] for r in merged["rows"]] == [0, 3]
    assert merged["warnings"] == [w, {"code": "r", "rows": [0]}, {"code": "r", "rows": [3]}]
    assert merged["ignored_fields"] == ["a", "b"]
    assert merged["before_writing"] == {"say": "x", "warnings": 3,
                                        "remedies": {"w": "a", "v": "b"}}


def test_write_over_the_limit_raises_before_sending():
    gx, s = client(_describe())
    with pytest.raises(BatchTooLarge) as caught:
        gx.write("ClaimStake", ROWS, project=250)
    assert [c[0] for c in s.calls] == ["GET"], "nothing POSTed"
    exc = caught.value
    assert isinstance(exc, WriteRefused)
    assert (exc.limit, exc.rows, exc.reason_code) == (1000, 1054, "batch_too_large")
    assert "own write_id" in str(exc) and "dataset door" in str(exc)


def test_a_long_form_validate_is_never_split():
    gx, s = client(_describe(long_form=True))
    with pytest.raises(BatchTooLarge) as caught:
        gx.validate("Assay", ROWS, project=250)
    assert [c[0] for c in s.calls] == ["GET"]
    assert "long-form" in str(caught.value)


def test_the_limit_is_asked_once_per_model_and_project():
    gx, s = client(_describe(), _describe())
    for _ in range(2):
        with pytest.raises(BatchTooLarge):
            gx.write("ClaimStake", ROWS, project=250)
    with pytest.raises(BatchTooLarge):
        gx.write("ClaimStake", ROWS, project=251)
    assert [c[0] for c in s.calls] == ["GET", "GET"]


def test_without_describe_the_servers_refusal_is_the_same_typed_error():
    refused = {"reason_code": "batch_too_large", "detail": "1054 records in one request; "
               "the limit is 1000.", "remedy": "Split it.",
               "offending": {"records": 1054, "limit": 1000}}
    gx, s = client(FakeResponse(400, {"reason_code": "invalid_parameter",
                                      "detail": "name one with ?project=<id>."},
                                url="http://t/api/v2/records/describe/ClaimStake/"),
                   FakeResponse(400, refused))
    with pytest.raises(BatchTooLarge) as caught:
        gx.validate("ClaimStake", ROWS)
    assert [c[0] for c in s.calls] == ["GET", "POST"]
    assert (caught.value.limit, caught.value.rows) == (1000, 1054)


def test_batch_too_large_is_exported():
    assert geodb.BatchTooLarge is BatchTooLarge
    assert geodb.__version__ == "0.3.7"
