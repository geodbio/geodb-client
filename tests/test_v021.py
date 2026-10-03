"""geodb-client 0.2.1 (protocol v0.3 lane R) — mocked transport.

* a list whose first page states its total fetches the rest IN PARALLEL by
  offset (bounded), and still yields every row once, in order;
* ``skip_count=True`` asks the server not to count and follows ``next``;
* ``assay_results()`` and ``samples()`` page at 2,000 (the server's raised cap);
* ``export("assay_results")`` is a bulk table; ``surveys()`` says GEOPHYSICAL;
* ``WriteResult``'s repr shows the summary and how to read the rows.
"""

import json
import threading
from urllib.parse import parse_qs, urlparse

import pytest

import geodb
from geodb.writes import WriteResult


class Resp:
    def __init__(self, status=200, payload=None, headers=None, url="http://t/"):
        self.status_code, self._payload, self.url = status, payload, url
        self.content = json.dumps(payload).encode() if payload is not None else b""
        self.text = self.content.decode()
        self.headers = headers or {}

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._payload


class Table:
    """A paged list of ``total`` rows that answers like the server (count on
    every page unless skip_count; next while rows remain)."""

    def __init__(self, total, cap=2000):
        self.total, self.cap = total, cap
        self.calls, self.threads = [], set()
        self.lock = threading.Lock()

    def get(self, url, params=None, headers=None, timeout=None, **kw):
        query = {k: v[-1] for k, v in parse_qs(urlparse(url).query).items()}
        query.update({k: str(v) for k, v in (params or {}).items()})
        with self.lock:
            self.calls.append(query)
            self.threads.add(threading.get_ident())
        limit = min(int(query.get("limit", 100)), self.cap)
        offset = int(query.get("offset", 0))
        rows = [{"id": i} for i in range(offset, min(offset + limit, self.total))]
        more = offset + limit < self.total
        following = dict(query, limit=limit, offset=offset + limit)
        body = {"count": None if query.get("skip_count") == "1" else self.total,
                "next": ("http://t/api/v2/x/?" + "&".join(f"{k}={v}" for k, v in following.items())
                         if more else None),
                "previous": None, "results": rows}
        if int(query.get("limit", 100)) > self.cap:
            body["limit_clamped"] = {"asked": int(query["limit"]), "served": self.cap}
        return Resp(200, body)


def client(table, **kw):
    return geodb.Client(token="gdbg_t", base_url="http://t", session=table, **kw)


def test_parallel_pages_yield_every_row_once_in_order():
    table = Table(total=10_500, cap=500)
    gx = client(table)
    rows = list(gx.collars(project=1))
    assert [r["id"] for r in rows] == list(range(10_500))
    assert len(table.calls) == 21
    offsets = sorted(int(c.get("offset", 0)) for c in table.calls)
    assert offsets == list(range(0, 10_500, 500))
    assert all(c["project"] == "1" for c in table.calls)


def test_parallel_is_bounded_and_can_be_turned_off():
    table = Table(total=3000, cap=500)
    list(client(table, parallel=1).collars(project=1))
    assert len(table.threads) == 1
    table = Table(total=3000, cap=500)
    rows = geodb.client.Paginated(client(table), "/drill-collars/", {"project": 1}, parallel=3)
    assert len(list(rows)) == 3000
    assert len(table.threads) <= 4          # the caller + at most 3 workers


def test_a_clamped_page_pages_at_the_served_size():
    table = Table(total=5000, cap=500)
    rows = list(client(table).collars(project=1, limit=5000))
    assert len(rows) == 5000
    assert all(c["limit"] in ("5000", "500") for c in table.calls)


def test_skip_count_follows_next():
    table = Table(total=1200, cap=500)
    paged = client(table).collars(project=1, skip_count=True)
    assert len(list(paged)) == 1200
    assert [c["skip_count"] for c in table.calls] == ["1", "1", "1"]
    assert len(table.threads) == 1


def test_the_first_pages_envelope_is_kept():
    table = Table(total=10, cap=500)
    paged = client(table).collars(project=1, limit=5000)
    list(paged)
    assert paged.envelope["limit_clamped"] == {"asked": 5000, "served": 500}
    assert paged.count() == 10


def test_values_and_lean_samples_page_at_2000():
    table = Table(total=4000)
    gx = client(table)
    list(gx.assay_results(project=1))
    assert table.calls[0]["limit"] == "2000"
    table.calls.clear()
    list(gx.samples(project=1))
    assert table.calls[0]["limit"] == "2000"
    table.calls.clear()
    list(gx.collars(project=1))
    assert table.calls[0]["limit"] == "500"


def test_assay_results_is_an_export_table():
    exports = {m for _n, _p, models, _s, _w in geodb.client.TABLES for m in models}
    assert "assay_results" in exports
    assert "assay_results" in geodb.Client.export.__doc__


def test_surveys_say_geophysical_and_point_at_drill_surveys():
    doc = geodb.Client.surveys.__doc__
    assert "GEOPHYSICAL" in doc and "drill_surveys()" in doc


def test_write_result_repr_shows_the_summary_and_how_to_read_rows():
    result = WriteResult(None, {"intent": "create", "write_id": "w1",
                                "summary": {"created": 2, "refused": 1},
                                "rows": [{"status": "created"}, {"status": "created"},
                                         {"status": "refused", "reason_code": "x"}]})
    text = repr(result)
    assert '"created": 2' in text and "write_id=w1" in text
    assert "rows=3" in text and "refused=1" in text
    assert ".rows" in text and ".to_dataframe()" in text and ".undo()" in text
    dry = repr(WriteResult(None, {"dry_run": True, "summary": {"would_create": 2}, "rows": []}))
    assert dry.startswith("<WriteResult dry run") and ".undo()" not in dry


def test_an_export_job_keeps_its_guide_pointers_and_notes():
    pointer = {"topic": "assay-values", "section": "below-detection",
               "url": "/api/v2/guide/assay-values/below-detection/", "why": "w"}

    class S:
        calls = []

        def get(self, url, params=None, headers=None, timeout=None, **kw):
            self.calls.append(url)
            if url.endswith("/guide/assay-values/below-detection/"):
                return Resp(200, {"id": "assay-values", "results": [{"id": "below-detection"}]})
            return Resp(200, {"state": "done", "see_guide": [pointer], "notes": {"n": "x"}})

        def post(self, url, json=None, headers=None, timeout=None):
            return Resp(202, {"id": "j1", "state": "queued", "model": "assay_results"})

    gx = geodb.Client(token="gdbg_t", base_url="http://t", session=S())
    job = gx.export("assay_results", project=1).wait(poll_seconds=0)
    assert job.see_guide == [pointer] and job.notes == {"n": "x"}
    assert gx.guide("assay-values", "below-detection")["results"][0]["id"] == "below-detection"


# ── review S4: the parallel pager ──────────────────────────────────────────
class Pages:
    """A server that answers by offset/limit from the URL query, with knobs
    for the failure shapes (host of `next`, a short page, a failing page)."""

    def __init__(self, total, size=500, short_first=0, fail_at=None, fail=(500, {}),
                 next_host="http://t", grow_to=None):
        self.total, self.size, self.short_first = total, size, short_first
        self.fail_at, self.fail, self.next_host, self.grow_to = fail_at, fail, next_host, grow_to
        self.urls = []
        self.lock = threading.Lock()

    def get(self, url, params=None, headers=None, timeout=None, **kw):
        query = {k: v for k, v in parse_qs(urlparse(url).query).items()}
        for k, v in (params or {}).items():
            # requests sends a list as the key repeated
            query[k] = [str(x) for x in v] if isinstance(v, (list, tuple)) else [str(v)]
        with self.lock:
            self.urls.append((url, {k: list(v) for k, v in query.items()}))
        offset = int(query.get("offset", ["0"])[-1])
        limit = min(int(query.get("limit", ["100"])[-1]), self.size)
        if self.fail_at is not None and offset == self.fail_at:
            status, body = self.fail
            return Resp(status, body, headers={"Retry-After": "3"} if status == 429 else None)
        total = self.total
        if self.grow_to and offset >= self.total - limit:
            total = self.grow_to            # rows landed during the read
        ids = list(range(offset, min(offset + limit, total)))
        if offset == 0 and self.short_first:
            ids = ids[:-self.short_first]   # rows vanished between id + fetch
        more = offset + limit < total
        nxt = None
        if more:
            pairs = [(k, x) for k, vs in query.items() if k not in ("offset", "limit") for x in vs]
            pairs += [("limit", str(limit)), ("offset", str(offset + limit))]
            nxt = self.next_host + "/api/v2/x/?" + "&".join(f"{k}={v}" for k, v in pairs)
        return Resp(200, {"count": self.total if offset == 0 else None, "next": nxt,
                          "results": [{"id": i} for i in ids]})


def gx_for(server, **kw):
    return geodb.Client(token="gdbg_t", base_url="http://t", session=server, **kw)


def test_the_stride_is_the_servers_page_size_not_page_ones_length():
    server = Pages(total=2000, size=500, short_first=2)
    ids = [r["id"] for r in gx_for(server).collars(project=1)]
    assert len(ids) == len(set(ids)), "rows duplicated"
    assert ids == [i for i in range(2000) if i not in (498, 499)]


def test_page_links_go_only_to_the_configured_base_url():
    server = Pages(total=2000, size=500, next_host="http://elsewhere:9")
    list(gx_for(server).collars(project=1))
    assert all(url.startswith("http://t/api/v2/drill-collars/") for url, _q in server.urls)


def test_repeated_parameters_ride_every_page():
    server = Pages(total=1500, size=500)
    list(gx_for(server).collars(project=1, element=["Au", "Cu"]))
    assert all(q.get("element") == ["Au", "Cu"] for _url, q in server.urls)


@pytest.mark.parametrize("failure, error", [
    ((401, {"reason_code": "grant_expired", "detail": "expired", "remedy": "renew"}),
     geodb.errors.AuthError),
    ((429, {"reason_code": "throttled", "detail": "slow", "remedy": "wait"}),
     geodb.errors.Throttled),
    ((503, {"detail": "unavailable"}), geodb.errors.APIError),
])
def test_a_page_failing_mid_way_raises_after_the_pages_before_it(failure, error):
    server = Pages(total=3000, size=500, fail_at=1500, fail=failure)
    got = []
    with pytest.raises(error):
        for row in gx_for(server).collars(project=1):
            got.append(row["id"])
    assert got == list(range(1500)), "rows before the failure: in order, none dropped or doubled"


def test_rows_added_during_a_parallel_read_are_followed():
    server = Pages(total=1500, size=500, grow_to=1700)
    ids = [r["id"] for r in gx_for(server).collars(project=1)]
    assert ids == list(range(1700))
