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
                                      ("dataset_rows_busy", geodb.DatasetRowsBusy)])
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
