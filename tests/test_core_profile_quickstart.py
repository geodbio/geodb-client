"""THE A6 ACCEPTANCE TEST: the README quickstart against a CORE-PROFILE-ONLY server.

The claim the protocol makes is that a second implementer can serve
`PROFILE.md`'s core operations and a client written against this protocol will
work against them. That claim was untested — every test in this repo mocks the
exact endpoints the test itself names, so a quickstart quietly reaching a geoDB
extension would pass forever and fail the first time a real vendor tried it.

So this file stands up a **hostile mock**: an in-process HTTP server that serves
the core profile and answers **404 with a loud body to anything else**, and then
runs the README quickstart, line for line, against it. If the quickstart drifts
onto an extension the test names the path and fails.

It found exactly that on the first run: the published quickstart called
`gx.surveys()` — GEOPHYSICAL surveys, an extension — which a conforming server
has no obligation to serve. The quickstart now reads `gx.drill_surveys()`
(downhole stations, core). One keystroke apart, entirely different table.

Rows served by the mock are shaped from the generated schemas in the protocol
repo: a projected collar (so `latitude` holds a NORTHING, which is the trap the
quickstart now prints), a per-sample assay with a nested `elements[]`, and a
downhole survey station.

No network, no credentials, no geoDB checkout — it runs anywhere `pytest` does.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import geodb

# ── The core profile, as PROFILE.md names it. Anything not matched here is a
#    path a conforming server does not owe, and the mock refuses it. ─────────
CORE_LIST_PATHS = {
    "/api/v2/drill-collars/",
    "/api/v2/drill-surveys/",
    "/api/v2/drill-lithologies/",
    "/api/v2/drill-alterations/",
    "/api/v2/drill-structures/",
    "/api/v2/drill-mineralizations/",
    "/api/v2/drill-veins/",
    "/api/v2/drill-rqds/",
    "/api/v2/drill-spectral-intervals/",
    "/api/v2/drill-custom-intervals/",
    "/api/v2/drill-samples/",
    "/api/v2/point-samples/",
    "/api/v2/assays/",
    "/api/v2/laboratories/",
    "/api/v2/certificates/",
    "/api/v2/qc-samples/",
}

#: A projected collar. `longitude` is the EASTING and `latitude` the NORTHING in
#: EPSG 26915 — the zero-data-loss contract, and the single most misread field
#: pair in the protocol.
COLLAR_ROW = {
    "id": 1,
    "name": "MOCK-001",
    "project": 7,
    "latitude": 4184500.0,          # northing, metres
    "longitude": 690250.0,          # easting, metres
    "epsg": 26915,
    "geometry": "SRID=4326;POINT Z (-90.5 37.8 312.0)",
    "source_coordinate": {"x": 690250.0, "y": 4184500.0, "epsg": 26915},
    "elevation": 312.0,
    "total_depth": 220.5,
    "azimuth": 180.0,
    "dip": -60.0,
    "length_units": "m",
    "date_completed": "2026-03-14",
}

ASSAY_ROW = {
    "id": 11,
    "name": "MOCK-001-S1",
    "project": {"id": 7, "name": "Mock Project", "company": "Mock Co"},
    "certificate": {"id": 3, "name": "MOCK-CERT-1", "laboratory": "Mock Lab",
                    "analysis_date": "2026-03-20", "status": "finalized"},
    "elements": [
        {"element": "Au", "value": "1.2400", "units": "ppm",
         "detection_limit": "0.0050", "above_det_limit": True},
        {"element": "Cu", "value": "0.0310", "units": "pct",
         "detection_limit": "0.0010", "above_det_limit": True},
    ],
    "date_created": "2026-03-21",
}

SURVEY_ROW = {
    "id": 21,
    "project": 7,
    "bhid": 1,
    "bhid_name": "MOCK-001",
    "depth_at": 50.0,
    "azimuth": 181.2,
    "dip": -59.4,
    "length_units": "m",
}

CERTIFICATE_ROW = {
    "id": 3,
    "name": "MOCK-CERT-1",
    "project": {"id": 7, "name": "Mock Project", "company": "Mock Co"},
    "laboratory": {"id": 2, "name": "Mock Lab"},
    "analysis_date": "2026-03-20",
    "date_preliminary": None,
    "status": "finalized",
    "batch_number": "B-7",
    "weight_units": "kg",
    "alt_certificate_numbers": [],
    "methods": [{"id": 5, "name": "FA-ICP"}],
    "result_count": 1,
}

ROWS_FOR_PATH = {
    "/api/v2/drill-collars/": [COLLAR_ROW],
    "/api/v2/assays/": [ASSAY_ROW],
    "/api/v2/drill-surveys/": [SURVEY_ROW],
    "/api/v2/certificates/": [CERTIFICATE_ROW],
}

STAC_ITEM = {
    "type": "Feature",
    "stac_version": "1.1.0",
    "id": "mock-raster-1",
    "collection": "rasters",
    "geometry": {"type": "Polygon", "coordinates": [[[-90.6, 37.7], [-90.4, 37.7],
                                                     [-90.4, 37.9], [-90.6, 37.9],
                                                     [-90.6, 37.7]]]},
    "properties": {"datetime": "2026-03-01T00:00:00Z"},
    "links": [],
    # `href` is filled in with the mock's real base URL once the server has a
    # port: a STAC catalog carries ABSOLUTE asset hrefs, and a relative one
    # would be testing something no server sends.
    "assets": {
        "cog": {"href": None, "type": "image/tiff",
                "roles": ["data", "cloud-optimized"]},
    },
}

COG_BYTES = b"II*\x00MOCK-COG-BYTES"
PARQUET_BYTES = b"PAR1MOCK-PARQUET"


class CoreProfileHandler(BaseHTTPRequestHandler):
    """Serves the core profile. Refuses everything else, loudly."""

    #: Paths this mock was asked for and does not serve — read by the test so a
    #: failure names the offending endpoint instead of a bare 404.
    refused = []

    def log_message(self, *args):        # keep pytest output clean
        pass

    # ── helpers ────────────────────────────────────────────────────────────
    def _send(self, code, payload=None, body=None, headers=None):
        raw = body if body is not None else json.dumps(payload or {}).encode()
        self.send_response(code)
        self.send_header("Content-Type",
                         "application/octet-stream" if body is not None
                         else "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-GeoDB-Protocol-Version", "0.1.0")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    def _envelope(self, rows):
        """The ONE list envelope PROFILE.md says a conforming server owes."""
        return {
            "count": len(rows),
            "next": None,
            "previous": None,
            "results": rows,
            "deleted_ids": [],
            "deleted_since_applied": None,
            "sync_timestamp": "2026-03-21T12:00:00Z",
        }

    def _refuse(self, path):
        type(self).refused.append(path)
        self._send(404, {
            "reason_code": "not_in_core_profile",
            "detail": f"{path} is not in the core profile; this server "
                      f"implements PROFILE.md and nothing else.",
            "remedy": "Use a core-profile operation, or check PROFILE.md.",
        })

    # ── routing ────────────────────────────────────────────────────────────
    def do_GET(self):
        path = self.path.split("?", 1)[0]
        # Every API path is authenticated with a grant. The signed asset URL
        # below deliberately is NOT — see the assertion there.
        if path.startswith("/api/"):
            assert self.headers.get("Authorization", "").startswith("Grant "), (
                "a conforming server sees `Authorization: Grant <token>` on "
                "every API request")

        if path in CORE_LIST_PATHS:
            return self._send(200, self._envelope(ROWS_FOR_PATH.get(path, [])))
        if path == "/api/v2/grant-context/":
            return self._send(200, {"project": {"id": 7, "name": "Mock Project"},
                                    "protocol_version": "0.1.0",
                                    "read_only": True})
        if path == "/api/v2/stac/collections/rasters/items/":
            item = json.loads(json.dumps(STAC_ITEM))
            item["assets"]["cog"]["href"] = (
                f"http://{self.headers.get('Host')}/api/v2/stac/assets/cog/1/")
            return self._send(200, {"type": "FeatureCollection",
                                    "features": [item], "links": []})
        if path == "/api/v2/stac/assets/cog/1/":
            # The core contract: a short-lived signed redirect, and the client
            # must NOT forward the grant credential to it.
            return self._send(302, {}, body=b"",
                              headers={"Location": "/signed/mock.tif?sig=abc"})
        if path == "/signed/mock.tif":
            assert "Authorization" not in self.headers, (
                "the grant token must never travel to the signed asset URL")
            return self._send(200, body=COG_BYTES)
        if path == "/api/v2/exports/job-1/":
            return self._send(200, {"state": "done", "id": "job-1"})
        if path == "/api/v2/exports/job-1/download/":
            return self._send(200, body=PARQUET_BYTES)

        return self._refuse(path)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/v2/exports/":
            return self._send(202, {"id": "job-1", "state": "queued",
                                    "model": "drill_samples",
                                    "format": "geoparquet",
                                    "reused": False,
                                    "status_url": "/api/v2/exports/job-1/"})
        return self._refuse(path)


@pytest.fixture
def core_only_server():
    """A real HTTP server on localhost serving ONLY the core profile."""
    CoreProfileHandler.refused = []
    server = HTTPServer(("127.0.0.1", 0), CoreProfileHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_the_readme_quickstart_runs_against_a_core_only_server(
        core_only_server, tmp_path):
    """The README quickstart, line for line, against core-profile-only.

    ⛔ If this fails with a refused path, the quickstart has drifted onto a
    geoDB extension and the protocol's central claim is false for it.
    """
    gx = geodb.Client(token="gdbg_mock", base_url=core_only_server)

    collars = gx.collars().to_dataframe()
    assays = gx.assays().to_dataframe()
    stations = gx.drill_surveys().to_dataframe()

    assert len(collars) == 1 and collars.loc[0, "name"] == "MOCK-001"
    assert len(assays) == 1
    assert len(stations) == 1 and stations.loc[0, "depth_at"] == 50.0

    # The coordinate line: for a projected project these are NOT degrees.
    row = collars.loc[0]
    assert row["epsg"] == 26915
    assert abs(row["latitude"]) > 180, (
        "the fixture must exercise the native-CRS trap: a northing, not a "
        "latitude")
    assert row["geometry"].startswith("SRID=4326;")

    # STAC: walk the catalog, pull the COG through the signed redirect.
    downloaded = None
    for item in gx.stac().items("rasters"):
        cog = item.asset("cog")
        if cog:
            downloaded = cog.download(str(tmp_path / "grid.tif"))
            break
    assert downloaded and open(downloaded, "rb").read() == COG_BYTES

    # Bulk: create, wait, download.
    job = gx.export("drill_samples", format="geoparquet")
    path = job.wait(poll_seconds=0).download(str(tmp_path / "samples.parquet"))
    assert open(path, "rb").read() == PARQUET_BYTES

    assert CoreProfileHandler.refused == [], (
        f"the quickstart reached endpoints outside the core profile: "
        f"{CoreProfileHandler.refused}. A conforming server does not owe "
        f"these — see PROFILE.md.")


def test_the_mock_really_refuses_an_extension(core_only_server):
    """The mock has to be hostile, or the test above proves nothing.

    `geophysical-surveys` is a geoDB extension. A client call that reaches it
    must fail against a core-only server — which is exactly why the quickstart
    no longer makes one.
    """
    gx = geodb.Client(token="gdbg_mock", base_url=core_only_server)
    with pytest.raises(Exception):
        list(gx.surveys())
    assert CoreProfileHandler.refused == ["/api/v2/geophysical-surveys/"]


def test_every_core_record_lane_the_client_exposes_is_served(core_only_server):
    """Each client accessor for a core resource works against a core server.

    The client also exposes extension lanes (`surveys()`), which is correct —
    it is OUR client. This asserts the core ones are core.
    """
    gx = geodb.Client(token="gdbg_mock", base_url=core_only_server)
    for accessor in ("collars", "drill_surveys", "lithology", "alteration",
                     "structures", "mineralization", "veins", "rqd",
                     "spectral", "custom_intervals", "samples",
                     "point_samples", "assays", "qc_samples"):
        list(getattr(gx, accessor)())
    assert CoreProfileHandler.refused == []


def test_the_grant_context_is_the_cheapest_first_call(core_only_server):
    gx = geodb.Client(token="gdbg_mock", base_url=core_only_server)
    context = gx.project()
    assert context["protocol_version"] == "0.1.0"
    assert context["read_only"] is True
    assert CoreProfileHandler.refused == []
