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
from geodb.writes import batch_rows, idempotency_header, merge_batch_answers

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


def test_merge_maps_rows_and_warning_rows_to_positions_and_joins_once():
    w = {"code": "w", "detail": "same"}
    merged = merge_batch_answers(
        [{"rows": [{"index": 0}, {"index": 1}], "summary": {"created": 2},
          "warnings": [w, {"code": "r", "rows": [1]}], "ignored_fields": ["b"]},
         {"rows": [{"index": 0}], "summary": {"created": 1},
          "warnings": [w, {"code": "r", "rows": [0]}], "ignored_fields": ["a"]}],
        [[0, 2], [1]], 2)
    assert [r["index"] for r in merged["rows"]] == [0, 1, 2]
    assert merged["warnings"] == [w, {"code": "r", "rows": [2]}, {"code": "r", "rows": [1]}]
    assert merged["ignored_fields"] == ["a", "b"]
    assert merged["batches"]["per_batch"] == [{"rows": [0, 2]}, {"rows": [1]}]


def test_merge_sums_nested_counts_and_unlinked_assays():
    merged = merge_batch_answers(
        [{"summary": {"skipped": 2, "skipped_by_reason": {"exists": 2}},
          "assays_left_unlinked": 3, "rows": []},
         {"summary": {"skipped": 3, "skipped_by_reason": {"exists": 1, "duplicate": 2}},
          "assays_left_unlinked": 4, "rows": []}], [[0], [1]], 1)
    assert merged["summary"] == {"skipped": 5,
                                 "skipped_by_reason": {"exists": 3, "duplicate": 2}}
    assert merged["assays_left_unlinked"] == 7


def test_merge_never_keeps_batch_ones_value_alone():
    a = {"rows": [], "model": "DrillSample", "qaqc": {"verdicts": 1}, "layer": {"id": 1},
         "company_reach": {"note": "x"}, "unknown_key": 1}
    b = {"rows": [], "model": "DrillSample", "qaqc": {"verdicts": 2}, "layer": {"id": 1}}
    merged = merge_batch_answers([a, b], [[0], [1]], 1)
    assert merged["model"] == "DrillSample" and merged["layer"] == {"id": 1}
    for key in ("qaqc", "company_reach", "unknown_key"):
        assert key not in merged, key
        assert key in merged["batches"]["differing"]
    per = merged["batches"]["per_batch"]
    assert per[0]["qaqc"] == {"verdicts": 1} and per[1]["qaqc"] == {"verdicts": 2}
    assert per[0]["company_reach"] == {"note": "x"} and "company_reach" not in per[1]


def test_merge_keeps_every_differing_ask():
    merged = merge_batch_answers(
        [{"rows": [], "before_writing": {"say": "x", "warnings": 1, "remedies": {"w": "a"}}},
         {"rows": [], "before_writing": {"say": "y", "warnings": 2, "remedies": {"v": "b"}}}],
        [[0], [1]], 1)
    ask = merged["before_writing"]
    assert ask["confirm_with_user"] is True and ask["warnings"] == 3
    assert ask["remedies"] == {"w": "a", "v": "b"} and "per_batch" in ask["say"]
    assert [p["before_writing"]["say"] for p in merged["batches"]["per_batch"]] == ["x", "y"]


def test_the_note_says_cross_row_checks_ran_per_batch():
    merged = merge_batch_answers([{"rows": []}, {"rows": []}], [[0], [1]], 1, "bhid")
    note = merged["batches"]["note"]
    assert "within each batch only" in note and "SAME batches" in note and '"bhid"' in note


def test_cuts_never_part_rows_sharing_the_first_identifying_field():
    rows = [{"bhid": h, "depth_from": d} for h, d in
            [("A", 0), ("B", 0), ("a ", 1), ("C", 0), ("B", 1), ("D", 0)]]
    batches = batch_rows(rows, 3, "bhid")
    # groups in first-appearance order: A{0,2} B{1,4} C{3} D{5}, packed greedily
    assert batches == [[0, 2], [1, 3, 4], [5]]
    holes = [{rows[i]["bhid"].strip().lower() for i in b} for b in batches]
    assert not set.intersection(*holes)
    assert sorted(i for b in batches for i in b) == list(range(6))
    assert batch_rows([{"bhid": "A"}] * 4, 3, "bhid") == ("A", 4)


def test_validate_cuts_at_holes_and_maps_indexes_back():
    rows = [{"bhid": f"H{i % 3}", "depth_from": i} for i in range(1200)]
    describe = FakeResponse(200, {"batch_limit": 1000, "long_form": False,
                                  "identifying_fields": ["bhid", "depth_from"]})
    def answer(n):
        return FakeResponse(200, {"rows": [{"index": j, "status": "created"} for j in range(n)],
                                  "summary": {"created": n}})
    gx, s = client(describe, answer(800), answer(400))
    result = gx.validate("DrillLithology", rows, project=1)
    sent = [c[2]["json"]["records"] for c in s.calls[1:]]
    assert [len(b) for b in sent] == [800, 400]
    assert {r["bhid"] for r in sent[0]}.isdisjoint({r["bhid"] for r in sent[1]})
    assert [r["index"] for r in result.rows] == list(range(1200))
    assert result.summary == {"created": 1200}


def test_one_hole_over_the_limit_is_refused_naming_it():
    rows = [{"bhid": "DH-9", "depth_from": i} for i in range(1001)]
    gx, s = client(FakeResponse(200, {"batch_limit": 1000, "identifying_fields": ["bhid"]}))
    with pytest.raises(BatchTooLarge) as caught:
        gx.validate("DrillLithology", rows, project=1)
    assert "'DH-9'" in str(caught.value) and [c[0] for c in s.calls] == ["GET"]


@pytest.mark.parametrize("intent", ["retract", "restore", "undo"])
def test_a_consent_dry_run_is_never_split(intent):
    gx, s = client(_describe())
    with pytest.raises(BatchTooLarge) as caught:
        gx.validate("ClaimStake", ROWS, intent=intent, project=250)
    assert [c[0] for c in s.calls] == ["GET"], "nothing POSTed"
    assert f"{intent} dry run is never split" in str(caught.value)


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
