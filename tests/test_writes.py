"""
The write half of the client: describe / validate / write / retract / restore
/ undo / writes.

Two layers:
* unit tests over a recording fake session — exactly what goes on the wire;
* an end-to-end run against the reference write server that ships with
  ``geodb-conformance`` (its mock, the one the write break matrix proves),
  skipped when that package is not installed.
"""

import json

import pytest

import geodb
from geodb import WriteRefused, WriteResult


class FakeResponse:
    def __init__(self, status_code=200, payload=None, url="http://t/api/v2/records/"):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.text = self.content.decode()
        self.url = url
        self.headers = {}

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class RecordingSession:
    """Answers each request with the next queued response, and records it."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def _next(self, method, url, **kw):
        self.calls.append((method, url, kw))
        return self.responses.pop(0)

    def post(self, url, **kw):
        return self._next("POST", url, **kw)

    def get(self, url, **kw):
        return self._next("GET", url, **kw)


def client(*responses):
    session = RecordingSession(*responses)
    return geodb.Client(token="gdbg_x", base_url="http://t", session=session), session


WRITTEN = {"model": "DrillSample", "intent": "create", "dry_run": False,
           "write_id": "w-1", "summary": {"created": 1, "refused": 1},
           "rows": [{"index": 0, "status": "created", "id": 7},
                    {"index": 1, "status": "refused", "reason_code": "missing_crs",
                     "remedy": "Add epsg."}]}


def test_write_sends_one_body_to_the_one_endpoint():
    gx, s = client(FakeResponse(200, WRITTEN))
    result = gx.write("DrillSample", [{"name": "S1"}, {"name": "S2"}],
                      logging_set={"name": "pXRF", "create": True},
                      idempotency_key="k-1")
    method, url, kw = s.calls[0]
    assert (method, url) == ("POST", "http://t/api/v2/records/")
    assert kw["json"] == {"model": "DrillSample", "intent": "create", "dry_run": False,
                          "records": [{"name": "S1"}, {"name": "S2"}],
                          "set": {"name": "pXRF", "create": True}}
    assert kw["headers"]["Idempotency-Key"] == "k-1"
    assert kw["headers"]["Authorization"] == "Grant gdbg_x"
    assert isinstance(result, WriteResult)
    assert result.write_id == "w-1" and not result.ok
    assert [r["reason_code"] for r in result.refused] == ["missing_crs"]
    with pytest.raises(geodb.RowsRefused) as exc:
        result.raise_for_refusals()
    assert exc.value.reason_code == "missing_crs"
    assert exc.value.status_code == 200                 # answered, never an invented 4xx
    assert [r["index"] for r in exc.value.rows] == [1]


def test_validate_is_the_dry_run():
    gx, s = client(FakeResponse(200, dict(WRITTEN, dry_run=True, write_id=None)))
    result = gx.validate("DrillSample", [{"name": "S1"}], logging_set="pXRF")
    assert s.calls[0][2]["json"]["dry_run"] is True
    assert s.calls[0][2]["json"]["set"] == "pXRF"
    assert result.dry_run and result.write_id is None
    with pytest.raises(ValueError):
        result.undo()


def test_a_request_refusal_raises_with_its_code_and_remedy():
    refusal = {"reason_code": "confirm_required", "detail": "A retract needs a confirm.",
               "remedy": "Confirm with the user, then resend.", "offending": {"confirm": None}}
    gx, s = client(FakeResponse(400, refusal))
    with pytest.raises(WriteRefused) as exc:
        gx.retract("DrillCollar", [{"name": "DH-1"}])
    assert "confirm" not in s.calls[0][2]["json"]
    assert exc.value.status_code == 400
    assert exc.value.reason_code == "confirm_required"
    assert exc.value.remedy.startswith("Confirm")
    assert isinstance(exc.value, geodb.APIError)        # existing handlers still catch it


def test_retract_sends_the_confirm_only_when_asked():
    gx, s = client(FakeResponse(200, {"intent": "retract", "rows": []}),
                   FakeResponse(200, {"intent": "retract", "rows": []}))
    gx.retract("DrillCollar", [{"id": 4}], dry_run=True)
    gx.retract("DrillCollar", [{"id": 4}], confirm=True)
    assert s.calls[0][2]["json"]["dry_run"] is True and "confirm" not in s.calls[0][2]["json"]
    assert s.calls[1][2]["json"]["confirm"] == "retract"
    assert s.calls[1][2]["json"]["intent"] == "retract"


def test_undo_restore_and_the_write_log():
    gx, s = client(FakeResponse(200, {"intent": "undo", "complete": True, "write_id": "u-1",
                                      "rows": []}),
                   FakeResponse(200, {"intent": "restore", "write_id": "r-1", "rows": []}),
                   FakeResponse(200, {"count": 1, "next": None,
                                      "results": [{"write_id": "w-1"}]}),
                   FakeResponse(200, {"write_id": "w-1", "rows": []}))
    assert gx.undo("w-1", idempotency_key="u-key").complete is True
    assert s.calls[0][2]["headers"]["Idempotency-Key"] == "u-key"
    assert s.calls[0][2]["json"] == {"intent": "undo", "write_id": "w-1", "dry_run": False}
    gx.restore("w-9", idempotency_key="r-key")
    assert s.calls[1][2]["json"] == {"intent": "restore", "write_id": "w-9", "dry_run": False}
    assert s.calls[1][2]["headers"]["Idempotency-Key"] == "r-key"
    assert [w["write_id"] for w in gx.writes(undone=False)] == ["w-1"]
    assert s.calls[2][1] == "http://t/api/v2/records/writes/"
    assert s.calls[2][2]["params"]["undone"] == "false"
    assert gx.writes("w-1")["write_id"] == "w-1"
    with pytest.raises(ValueError):
        gx.restore()


def test_describe_and_an_auth_refusal_keeps_its_code():
    gx, s = client(FakeResponse(200, {"model": "DrillCollar", "fields": []}),
                   FakeResponse(403, {"reason_code": "write_access_insufficient",
                                      "remedy": "Ask the owner for a key that may write."}))
    assert gx.describe("DrillCollar", project=3)["model"] == "DrillCollar"
    assert s.calls[0][1] == "http://t/api/v2/records/describe/DrillCollar/"
    with pytest.raises(WriteRefused) as exc:
        gx.write("DrillCollar", [{"name": "x"}])
    assert exc.value.reason_code == "write_access_insufficient"


def test_a_dataframe_is_sent_as_plain_records():
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    gx, s = client(FakeResponse(200, WRITTEN))
    frame = pd.DataFrame({"name": ["A", "B"], "depth_from": np.array([0, 2], dtype="int64"),
                          "notes": ["x", float("nan")]})
    gx.write("DrillSample", frame, logging_set="pXRF")
    sent = s.calls[0][2]["json"]["records"]
    assert sent == [{"name": "A", "depth_from": 0, "notes": "x"},
                    {"name": "B", "depth_from": 2, "notes": None}]
    json.dumps(sent)                                    # JSON-safe: no numpy scalars


# ---------------------------------------------------------------------------
# End to end against the reference write server (geodb-conformance's mock)
# ---------------------------------------------------------------------------



@pytest.fixture
def reference_server():
    mock_server = pytest.importorskip("geodb_conformance.mock_server")
    mock_write = pytest.importorskip("geodb_conformance.mock_write")
    server, base = mock_server.serve(0)
    yield geodb.Client(token=mock_write.WRITE_TOKEN, base_url=base)
    server.shutdown()
    server.server_close()


def test_push_fix_delete_then_undo_all_three(reference_server):
    """The cold-agent scenario, done by the client: push intervals, fix one,
    delete one, undo all three — newest first."""
    gx = reference_server
    hole = gx.write("DrillCollar", [{"name": "DH-1", "latitude": 37.9, "longitude": -91.0,
                                     "epsg": 4326}])
    assert hole.summary == {"created": 1}
    rows = [{"bhid": "DH-1", "name": f"DH-1-{i}", "depth_from": 2.0 * i,
             "depth_to": 2.0 * i + 2} for i in range(5)]
    refused = gx.write("DrillSample", rows)
    assert {r["reason_code"] for r in refused.refused} == {"set_required"}
    check = gx.validate("DrillSample", rows, logging_set={"name": "new pass", "create": True})
    assert check.write_id is None and check.ok
    push = gx.write("DrillSample", rows, logging_set={"name": "new pass", "create": True})
    assert push.summary == {"created": 5} and push.write_id
    ids = [r["id"] for r in push.rows]
    fix = gx.write("DrillSample", [{"id": ids[2], "notes": "re-logged"}], intent="update")
    assert fix.summary == {"updated": 1}
    with pytest.raises(WriteRefused) as exc:
        gx.retract("DrillSample", [{"id": ids[4]}])
    assert exc.value.reason_code == "confirm_required"
    delete = gx.retract("DrillSample", [{"id": ids[4]}], confirm=True)
    assert delete.summary == {"retracted": 1}
    log = [w["write_id"] for w in gx.writes()]
    assert log[:3] == [delete.write_id, fix.write_id, push.write_id]
    for result in (delete, fix, push):
        assert gx.undo(result.write_id).complete is True
    left = {w["write_id"]: w for w in gx.writes()}
    assert all(left[w.write_id]["undone_at"] for w in (delete, fix, push))
    with pytest.raises(WriteRefused) as again:
        gx.undo(push.write_id)
    assert again.value.reason_code == "already_undone"
