"""
W7b: every dataset reason code (server plan §6.13–§6.15, ``api/errors.py``)
arrives as its own exception — a subclass of :class:`geodb.WriteRefused`, so
code that catches a refused write keeps working — carrying the code, the
remedy, ``offending`` and, for the two "retry after" codes, ``retry_after``.

Mocked transport: nothing connects.
"""

import pytest

import geodb
from geodb import errors

CASES = [
    ("dataset_file_unreadable", 400, geodb.DatasetFileUnreadable),
    ("dataset_column_forbidden", 400, geodb.DatasetColumnForbidden),
    ("dataset_too_large", 400, geodb.DatasetTooLarge),
    ("dataset_not_grouped", 400, geodb.DatasetNotGrouped),
    ("dataset_rows_busy", 409, geodb.DatasetRowsBusy),
    ("dataset_plan_invalid", 409, geodb.DatasetPlanInvalid),
    ("dataset_plan_expired", 409, geodb.DatasetPlanExpired),
    ("dataset_plan_changed", 409, geodb.DatasetPlanChanged),
    ("dataset_file_changed", 409, geodb.DatasetFileChanged),
    ("dataset_already_written", 409, geodb.DatasetAlreadyWritten),
    ("dataset_in_progress", 409, geodb.DatasetInProgress),
    ("dataset_write_failed", 409, geodb.DatasetWriteFailed),
    ("dataset_project_busy", 409, geodb.DatasetProjectBusy),
    ("dataset_interrupted", 409, geodb.DatasetInterrupted),
]


def _body(code, **offending):
    return {"reason_code": code, "detail": f"the server's words for {code}",
            "remedy": f"what to do about {code}", "offending": offending or None}


@pytest.mark.parametrize("code,status,cls", CASES)
def test_each_reason_code_is_its_own_exception(code, status, cls):
    exc = errors.write_refused_for(status, _body(code), "http://t/api/v2/records/")
    assert type(exc) is cls
    assert isinstance(exc, geodb.DatasetRefused)
    assert isinstance(exc, geodb.WriteRefused)          # existing except clauses still catch it
    assert exc.reason_code == code
    assert exc.remedy == f"what to do about {code}"
    assert exc.status_code == status


def test_the_registry_names_every_class_once():
    assert set(errors.DATASET_REASON_CODES) == {c for c, _s, _k in CASES}
    assert len(set(errors.DATASET_REASON_CODES.values())) == len(CASES)


def test_any_other_code_stays_a_plain_write_refused():
    exc = errors.write_refused_for(400, _body("validation_error"), "u")
    assert type(exc) is geodb.WriteRefused
    assert errors.write_refused_for(500, None, "u").reason_code is None


@pytest.mark.parametrize("code,cls", [("dataset_in_progress", geodb.DatasetInProgress),
                                      ("dataset_rows_busy", geodb.DatasetRowsBusy),
                                      ("dataset_project_busy", geodb.DatasetProjectBusy)])
def test_the_retry_after_codes_carry_the_servers_wait(code, cls):
    exc = errors.write_refused_for(409, _body(code), "u", headers={"Retry-After": "45"})
    assert isinstance(exc, geodb.DatasetBusy) and exc.retry_after == 45
    assert errors.write_refused_for(409, _body(code), "u").retry_after is None


def test_an_expired_plan_says_validate_again():
    exc = errors.write_refused_for(409, _body("dataset_plan_expired"), "u")
    assert "validate_dataset" in str(exc)


def test_already_written_names_the_write_and_its_undo():
    body = _body("dataset_already_written", write_id="w-1",
                 undo={"intent": "undo", "write_id": "w-1"})
    exc = errors.write_refused_for(409, body, "u")
    assert exc.write_id == "w-1"


def test_a_refused_records_write_is_typed_by_its_code():
    class Resp:
        status_code = 409
        url = "http://t/api/v2/records/"
        headers = {"Retry-After": "30"}

        def json(self):
            return _body("dataset_in_progress")

    class Session:
        def post(self, url, **kw):
            return Resp()

    gx = geodb.Client(token="gdbg_x", base_url="http://t", session=Session())
    with pytest.raises(geodb.DatasetInProgress) as caught:
        gx.write("DrillCollar", [{"name": "DH-1"}])
    assert caught.value.retry_after == 30


def test_project_busy_is_the_busy_family_and_interrupted_is_not():
    # W7c (server C2): dataset_project_busy = "send the same request again later";
    # dataset_interrupted = the job ended without finishing (re-sent once, like an
    # export's interruption) — not a busy wait.
    assert issubclass(geodb.DatasetProjectBusy, geodb.DatasetBusy)
    assert not issubclass(geodb.DatasetInterrupted, geodb.DatasetBusy)
    exc = errors.write_refused_for(409, _body("dataset_interrupted", error_id="e1f2"), "u")
    assert exc.error_id == "e1f2"
    assert errors.write_refused_for(409, _body("dataset_interrupted"), "u").error_id is None
    busy = errors.write_refused_for(409, _body("dataset_project_busy", waited_seconds=1800), "u")
    assert busy.waited_seconds == 1800


def test_a_429_on_the_records_door_is_write_throttled_with_its_wait():
    # The dataset door's per-key rate (DRF Throttled) and its cap on queued /
    # running jobs both answer 429 throttled + Retry-After (server C2).
    body = {"reason_code": "throttled", "detail": "2 dataset jobs", "remedy": "Poll them.",
            "offending": {"in_flight": 2, "limit": 2}}
    exc = errors.write_refused_for(429, body, "u", headers={"Retry-After": "30"})
    assert type(exc) is geodb.WriteThrottled
    assert isinstance(exc, geodb.Throttled) and isinstance(exc, geodb.WriteRefused)
    assert exc.retry_after == 30 and exc.status_code == 429
    assert (exc.in_flight, exc.limit) == (2, 2)
    rate = errors.write_refused_for(429, {"reason_code": "throttled", "detail": "rate"}, "u")
    assert type(rate) is geodb.WriteThrottled and rate.retry_after is None
    assert rate.in_flight is None


# ── W7d: the two records-door codes of server batch-C review (§6.18) ────────
def test_writes_switched_off_is_its_own_503_never_a_dataset_code():
    # The server's writes switch (an operator's, not the caller's) was thrown
    # after the job was accepted: 503, retry "maybe".
    exc = errors.write_refused_for(503, _body("writes_switched_off"), "u",
                                   headers={"Retry-After": "120"})
    assert type(exc) is geodb.WritesSwitchedOff
    assert isinstance(exc, geodb.WriteRefused) and not isinstance(exc, geodb.DatasetRefused)
    assert exc.status_code == 503 and exc.retry_after == 120
    assert errors.write_refused_for(503, _body("writes_switched_off"), "u").retry_after is None
    text = str(exc)
    assert "switched off" in text and "nothing more was written" in text.lower()


def test_undo_needs_company_reach_is_a_forbidden_write_that_says_where_to_undo():
    exc = errors.write_refused_for(403, _body("undo_needs_company_reach",
                                              company_shared=["Method"]), "u")
    assert type(exc) is geodb.UndoNeedsCompanyReach
    assert isinstance(exc, geodb.WriteRefused)              # existing except clauses
    assert isinstance(exc, geodb.PermissionDenied)          # and the 403 family
    assert exc.status_code == 403 and exc.offending == {"company_shared": ["Method"]}
    text = str(exc)
    assert "every project of the company" in text and "web app" in text
    assert "not retry" in text


def test_any_other_403_on_the_records_door_is_a_forbidden_write():
    exc = errors.write_refused_for(403, _body("write_access_insufficient"), "u")
    assert type(exc) is geodb.WriteForbidden
    assert isinstance(exc, geodb.WriteRefused) and isinstance(exc, geodb.PermissionDenied)
    assert isinstance(exc, geodb.AuthError)
    assert exc.reason_code == "write_access_insufficient" and exc.status_code == 403


def test_the_records_registry_names_the_two_codes():
    assert errors.RECORDS_REASON_CODES == {
        "writes_switched_off": geodb.WritesSwitchedOff,
        "undo_needs_company_reach": geodb.UndoNeedsCompanyReach}
    assert not set(errors.RECORDS_REASON_CODES) & set(errors.DATASET_REASON_CODES)
