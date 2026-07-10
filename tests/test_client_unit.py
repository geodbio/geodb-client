"""
Unit tests for geodb.Client — mocked transport, no network, no server.

Uses a hand-rolled fake session (no external mock dependency) so `pytest` runs
clean right after `pip install -e .`.
"""

import json

import pytest

import geodb
from geodb.errors import AuthError, NotFoundError, ExportError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, body=b"", url="http://t/"):
        self.status_code = status_code
        self._payload = payload
        self.content = body or (json.dumps(payload).encode() if payload is not None else b"")
        self.text = self.content.decode("utf-8", "replace")
        self.url = url

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise AssertionError(f"status {self.status_code}")

    # streaming download context manager
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, chunk_size=1):
        yield self.content


class FakeSession:
    """Records requests; replies from a routing table keyed on (method, path)."""

    def __init__(self, routes):
        self.routes = routes           # {(method, path_substr): FakeResponse or callable}
        self.calls = []

    def _match(self, method, url):
        for (m, sub), resp in self.routes.items():
            if m == method and sub in url:
                return resp(url) if callable(resp) else resp
        return FakeResponse(404, {"detail": "not found"}, url=url)

    def get(self, url, params=None, headers=None, timeout=None, stream=False,
            allow_redirects=True):
        self.calls.append(("GET", url, headers))
        return self._match("GET", url)

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("POST", url, headers, json))
        return self._match("POST", url)


def make_client(routes):
    return geodb.Client(token="gdbg_test", base_url="http://t",
                        session=FakeSession(routes))


def test_sends_grant_auth_header():
    routes = {("GET", "/drill-collars/"): FakeResponse(200, {"count": 0, "next": None, "results": []})}
    gx = make_client(routes)
    list(gx.collars())
    _, _, headers = gx._session.calls[0]
    assert headers["Authorization"] == "Grant gdbg_test"


def test_knox_scheme():
    gx = geodb.Client(token="k", base_url="http://t", auth_scheme="Token",
                      session=FakeSession({}))
    assert gx._headers()["Authorization"] == "Token k"


def test_paginated_follows_next_and_to_dataframe():
    page2 = FakeResponse(200, {"count": 3, "next": None,
                               "results": [{"name": "C"}]})
    page1 = FakeResponse(200, {"count": 3, "next": "http://t/api/v2/drill-collars/?offset=2",
                               "results": [{"name": "A"}, {"name": "B"}]})
    routes = {
        ("GET", "offset=2"): page2,
        ("GET", "/drill-collars/"): page1,
    }
    gx = make_client(routes)
    df = gx.collars().to_dataframe()
    assert list(df["name"]) == ["A", "B", "C"]


def test_auth_error_maps_to_exception():
    routes = {("GET", "/drill-collars/"): FakeResponse(403, {"detail": "no"})}
    gx = make_client(routes)
    with pytest.raises(AuthError):
        list(gx.collars())


def test_not_found_maps_to_exception():
    routes = {("GET", "/geophysical-surveys/"): FakeResponse(404, {"detail": "no"})}
    gx = make_client(routes)
    with pytest.raises(NotFoundError):
        list(gx.surveys())


def test_stac_items_iterates_and_asset_download(tmp_path):
    item = {
        "type": "Feature", "id": "raster-1", "collection": "rasters",
        "geometry": None, "properties": {}, "assets": {
            "cog": {"href": "http://t/api/v2/stac/assets/cog/1/",
                    "roles": ["data", "cloud-optimized"], "type": "image/tiff"}},
        "links": [],
    }
    fc = {"type": "FeatureCollection", "features": [item], "links": []}
    routes = {
        ("GET", "/stac/collections/rasters/items/"): FakeResponse(200, fc),
        ("GET", "/stac/assets/cog/1/"): FakeResponse(200, body=b"TIFFDATA"),
    }
    gx = make_client(routes)
    items = list(gx.stac().items("rasters"))
    assert items[0].id == "raster-1"
    cog = items[0].asset("cog")
    assert "cloud-optimized" in cog.roles
    out = tmp_path / "g.tif"
    cog.download(str(out))
    assert out.read_bytes() == b"TIFFDATA"


def test_export_create_wait_download(tmp_path):
    create = FakeResponse(202, {"id": "job-1", "state": "queued", "model": "drill_samples",
                                "format": "geoparquet",
                                "status_url": "http://t/api/v2/exports/job-1/"})
    running = FakeResponse(202, {"state": "running", "progress_pct": 50})
    done = FakeResponse(200, {"state": "done", "download_url": "http://t/api/v2/exports/job-1/download/"})
    # status endpoint returns running then done across calls
    seq = [running, done]
    routes = {
        ("POST", "/exports/"): create,
        ("GET", "/exports/job-1/download/"): FakeResponse(200, body=b"PARQUET"),
        ("GET", "/exports/job-1/"): lambda url: seq.pop(0),
    }
    gx = make_client(routes)
    job = gx.export("drill_samples", format="geoparquet")
    assert job.id == "job-1"
    job.wait(poll_seconds=0)
    out = tmp_path / "s.parquet"
    job.download(str(out))
    assert out.read_bytes() == b"PARQUET"


def test_export_error_state_raises():
    create = FakeResponse(202, {"id": "job-2", "state": "queued",
                                "status_url": "http://t/api/v2/exports/job-2/"})
    routes = {
        ("POST", "/exports/"): create,
        ("GET", "/exports/job-2/"): FakeResponse(500, {"state": "error",
                                                       "error_message": "boom"}),
    }
    gx = make_client(routes)
    job = gx.export("drill_samples")
    with pytest.raises(ExportError):
        job.wait(poll_seconds=0)


def test_version_exposed():
    assert geodb.__version__ == "0.1.0"
