"""
geodb-client 0.3.5: exports that wait the way the server asks and fail honestly
(server plan W1/W7: the export build moved onto the jobs rail, every poll says
how far it got, and a dead build reads ``error`` "create the export again").

* a 202 poll is followed after its ``Retry-After`` (10 s when absent), never 2 s;
* progress (``progress_pct``, ``rows_done``, ``rows_total``, ``progress_message``)
  is kept on the job and handed to ``on_progress``;
* the wait budget is a STALL timeout: it runs out only when nothing moves, with
  a hard cap on top;
* 429 ``export_concurrency`` on create: wait, poll the jobs this client already
  started, re-POST only once one of them has finished;
* a build the server reports interrupted is created again ONCE, with a warning,
  then raises :class:`geodb.ExportInterrupted` ("create the export again").

Mocked transport and a fake clock: nothing sleeps, nothing connects.
"""

import json

import pytest

import geodb
from geodb import ExportConcurrency, ExportError, ExportInterrupted, Throttled
from geodb import client as client_module

INTERRUPTED = ("The export stopped before it finished: the server building it was "
               "interrupted. Create the export again; that starts a fresh build. "
               "Quote error_id=0f1e2d3c4b5a69788796a5b4c3d2e1f0 if it happens again.")


class Resp:
    def __init__(self, status_code, payload=None, headers=None, url="http://t/api/v2/x"):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.text = self.content.decode()
        self.url = url
        self.headers = dict(headers or {})

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class Session:
    """Answers each request with the next queued response (or callable)."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def _next(self, method, url, **kw):
        self.calls.append((method, url, kw))
        nxt = self.responses.pop(0)
        return nxt(method, url, kw) if callable(nxt) else nxt

    def post(self, url, **kw):
        return self._next("POST", url, **kw)

    def get(self, url, **kw):
        return self._next("GET", url, **kw)


class Clock:
    """``time`` for geodb.client: sleep advances monotonic, nothing waits."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(client_module, "time", c)
    return c


def _client(*responses):
    session = Session(*responses)
    return geodb.Client(token="gdbg_x", base_url="http://t", session=session), session


def created(job_id="job-1", state="queued"):
    return Resp(202, {"id": job_id, "state": state, "model": "assay_results",
                      "format": "geoparquet", "reused": False,
                      "status_url": f"http://t/api/v2/exports/{job_id}/"})


def running(rows_done=0, rows_total=1000, pct=0, message="", retry_after="10", state="running"):
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return Resp(202, {"id": "job-1", "state": state, "progress_pct": pct,
                      "progress_message": message, "rows_done": rows_done,
                      "rows_total": rows_total, "elapsed_seconds": 5}, headers)


def done(job_id="job-1"):
    return Resp(200, {"id": job_id, "state": "done", "rows_total": 1000,
                      "download_url": f"http://t/api/v2/exports/{job_id}/download/"})


def busy(in_flight=2, limit=2, retry_after="30"):
    return Resp(429, {"reason_code": "export_concurrency",
                      "detail": "This API key already has 2 export jobs running; 2 is the limit.",
                      "remedy": "Wait for a running export to finish or poll its status.",
                      "offending": {"in_flight": in_flight, "limit": limit}},
                {"Retry-After": retry_after})


# ── Retry-After ─────────────────────────────────────────────────────────────
def test_a_202_poll_waits_its_retry_after(clock):
    gx, _s = _client(created(), running(retry_after="10"), running(retry_after="25"), done())
    gx.export("assay_results", project=12).wait()
    assert clock.sleeps == [10, 25]


def test_no_retry_after_waits_ten_seconds_never_two(clock):
    gx, _s = _client(created(), running(retry_after=None), done())
    gx.export("assay_results", project=12).wait()
    assert clock.sleeps == [10]


def test_poll_seconds_below_retry_after_is_raised_to_it(clock):
    gx, _s = _client(created(), running(retry_after="10"), done())
    gx.export("assay_results", project=12).wait(poll_seconds=2)
    assert clock.sleeps == [10]


def test_a_throttled_poll_waits_and_polls_again(clock):
    throttled = Resp(429, {"reason_code": "throttled", "detail": "rate"}, {"Retry-After": "40"})
    gx, _s = _client(created(), throttled, done())
    job = gx.export("assay_results", project=12).wait()
    assert job.state == "done"
    assert clock.sleeps == [40]


# ── progress ────────────────────────────────────────────────────────────────
def test_progress_is_on_the_job_and_handed_to_the_callback(clock):
    seen = []
    gx, _s = _client(created(), running(rows_done=200, pct=20, message="Assays 200/1000"),
                     running(rows_done=900, pct=90, message="Assays 900/1000"), done())
    job = gx.export("assay_results", project=12)
    job.wait(on_progress=lambda j: seen.append((j.state, j.rows_done, j.rows_total,
                                                j.progress_pct, j.progress_message)))
    assert seen[:2] == [("running", 200, 1000, 20, "Assays 200/1000"),
                        ("running", 900, 1000, 90, "Assays 900/1000")]
    assert seen[-1][0] == "done"
    assert job.rows_total == 1000


# ── the budget: a stall timeout + a hard cap ─────────────────────────────────
def test_the_wait_goes_on_while_rows_keep_advancing(clock):
    # 200 polls x 10 s = 2,000 s: past the old flat 900 s, every poll advancing.
    polls = [running(rows_done=i * 10) for i in range(1, 201)]
    gx, _s = _client(created(), *polls, done())
    job = gx.export("assay_results", project=12).wait(stall_timeout=60)
    assert job.state == "done"
    assert clock.now == 2000


def test_a_stalled_build_runs_out_the_stall_timeout(clock):
    polls = [running(rows_done=500) for _ in range(30)]
    gx, _s = _client(created(), *polls)
    with pytest.raises(ExportError, match="no progress for 60 s"):
        gx.export("assay_results", project=12).wait(stall_timeout=60)
    assert clock.now == 60


def test_the_hard_cap_holds_even_while_progressing(clock):
    polls = [running(rows_done=i) for i in range(1, 100)]
    gx, _s = _client(created(), *polls)
    with pytest.raises(ExportError, match="within 300 s"):
        gx.export("assay_results", project=12).wait(timeout=300)
    assert clock.now == 300


def test_a_state_change_counts_as_progress(clock):
    polls = [running(state="queued", rows_done=0) for _ in range(5)] + \
            [running(state="running", rows_done=0) for _ in range(5)]
    gx, _s = _client(created(), *polls, done())
    assert gx.export("assay_results", project=12).wait(stall_timeout=50).state == "done"


# ── 429 export_concurrency on create ─────────────────────────────────────────
def test_concurrency_is_a_typed_refusal():
    gx, _s = _client(busy())
    with pytest.raises(ExportConcurrency) as caught:
        gx.export("assay_results", project=12, wait_for_slot=False)
    exc = caught.value
    assert isinstance(exc, Throttled)
    assert exc.reason_code == "export_concurrency"
    assert (exc.in_flight, exc.limit, exc.retry_after) == (2, 2, 30)


def test_concurrency_polls_this_clients_jobs_and_reposts_once_one_finished(clock):
    gx, s = _client(created("job-a", "running"), created("job-b", "running"),
                    busy(),                                    # the third create
                    running(), running(),                      # poll job-a, job-b: running
                    done("job-a"), running(),                  # job-a finished
                    created("job-c"))                          # the re-POST
    gx.export("assay_results", project=1)
    gx.export("drill_samples", project=1)
    job = gx.export("collars", project=1)
    assert job.id == "job-c"
    methods = [(m, u.rsplit("/api/v2", 1)[-1]) for m, u, _kw in s.calls]
    assert methods == [("POST", "/exports/"), ("POST", "/exports/"), ("POST", "/exports/"),
                       ("GET", "/exports/job-a/"), ("GET", "/exports/job-b/"),
                       ("GET", "/exports/job-a/"), ("GET", "/exports/job-b/"),
                       ("POST", "/exports/")]
    assert clock.sleeps == [30, 30]          # the 429's Retry-After before each poll round


def test_concurrency_with_no_jobs_of_ours_raises_never_reposts(clock):
    gx, s = _client(busy())          # the key's jobs were started by another session
    with pytest.raises(ExportConcurrency, match="poll its status"):
        gx.export("assay_results", project=12)
    assert len(s.calls) == 1
    assert clock.sleeps == []


def test_concurrency_gives_up_at_its_slot_timeout(clock):
    gx, s = _client(created("job-a", "running"), busy(),
                    *[running() for _ in range(10)])
    gx.export("assay_results", project=1)
    with pytest.raises(ExportConcurrency):
        gx.export("drill_samples", project=1, slot_timeout=100)
    assert sum(1 for m, _u, _k in s.calls if m == "POST") == 2


# ── an interrupted build ────────────────────────────────────────────────────
def interrupted(job_id="job-1"):
    return Resp(500, {"id": job_id, "state": "error", "error_message": INTERRUPTED,
                      "retry_url": "/"})


def test_an_interrupted_build_is_created_again_once_with_a_warning(clock):
    gx, s = _client(created("job-1"), interrupted("job-1"), created("job-2"), done("job-2"))
    job = gx.export("assay_results", project=12, set="all")
    with pytest.warns(RuntimeWarning, match="interrupted.*creating it again"):
        job.wait()
    assert job.id == "job-2" and job.state == "done"
    assert job.restarts == 1
    assert job.interrupted_error_ids == ["0f1e2d3c4b5a69788796a5b4c3d2e1f0"]
    first, again = s.calls[0][2]["json"], s.calls[2][2]["json"]
    assert again == first                    # the same request, sent again
    assert first["set"] == "all"


def test_a_second_interruption_raises_create_the_export_again(clock):
    gx, s = _client(created("job-1"), interrupted("job-1"), created("job-2"),
                    interrupted("job-2"))
    job = gx.export("assay_results", project=12)
    with pytest.warns(RuntimeWarning):
        with pytest.raises(ExportInterrupted, match="create the export again") as caught:
            job.wait()
    assert caught.value.error_id == "0f1e2d3c4b5a69788796a5b4c3d2e1f0"
    assert caught.value.job_id == "job-2"
    assert isinstance(caught.value, ExportError)
    assert sum(1 for m, _u, _k in s.calls if m == "POST") == 2


def test_restart_interrupted_zero_raises_at_once(clock):
    gx, s = _client(created("job-1"), interrupted("job-1"))
    with pytest.raises(ExportInterrupted):
        gx.export("assay_results", project=12).wait(restart_interrupted=0)
    assert sum(1 for m, _u, _k in s.calls if m == "POST") == 1


def test_any_other_error_raises_export_error_not_interrupted(clock):
    gx, _s = _client(created(), Resp(500, {"state": "error", "error_message": "boom"}))
    with pytest.raises(ExportError, match="boom") as caught:
        gx.export("assay_results", project=12).wait()
    assert not isinstance(caught.value, ExportInterrupted)


# ── what the docstrings teach ────────────────────────────────────────────────
def test_export_is_taught_for_analysis_never_to_check_an_import():
    for doc in (geodb.Client.export.__doc__, client_module.ExportJob.__doc__):
        assert "analysis" in doc.lower()
        assert "never to check an import" in " ".join(doc.split())
