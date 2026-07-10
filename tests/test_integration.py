"""
Integration tests against a live geoDB dev server + a real access grant.

Skipped unless BOTH env vars are set (so CI / `pytest` stay offline by default):

    GEODB_TEST_BASE_URL=http://localhost:8001
    GEODB_TEST_TOKEN=gdbg_<a real grant on the target project>

Provision a sandbox grant with:
    manage.py provision_protocol_sandbox        # prints a demo token once

These exercise the REAL protocol surface end-to-end: records → DataFrames, the
STAC walk, and a GeoParquet export job round-trip.
"""

import os

import pytest

import geodb

BASE_URL = os.environ.get("GEODB_TEST_BASE_URL")
TOKEN = os.environ.get("GEODB_TEST_TOKEN")

pytestmark = pytest.mark.skipif(
    not (BASE_URL and TOKEN),
    reason="set GEODB_TEST_BASE_URL + GEODB_TEST_TOKEN to run integration tests")


@pytest.fixture(scope="module")
def gx():
    # Local dev server serves the v2 API at /api/v2 on the main path.
    return geodb.Client(token=TOKEN, base_url=BASE_URL, api_prefix="/api/v2")


def test_project_context(gx):
    ctx = gx.project()
    assert isinstance(ctx, dict)


def test_collars_to_dataframe(gx):
    df = gx.collars().to_dataframe()
    # A sandbox project may have zero rows; the call must still succeed + shape.
    assert hasattr(df, "columns")


def test_surveys_to_dataframe(gx):
    df = gx.surveys().to_dataframe()
    assert hasattr(df, "columns")


def test_stac_landing_and_collections(gx):
    landing = gx.stac().landing()
    assert landing.get("type") == "Catalog"
    cids = {c["id"] for c in gx.stac().collections()}
    assert {"geophysical-surveys", "rasters", "documents"} & cids


def test_geoparquet_export_roundtrip(gx, tmp_path):
    job = gx.export("drill_collars", format="geoparquet")
    job.wait(poll_seconds=2, timeout=300)
    out = tmp_path / "collars.parquet"
    job.download(str(out))
    assert out.exists() and out.stat().st_size > 0


def test_grant_is_read_only(gx):
    # A grant may create an export (the one sanctioned write) but nothing else.
    import requests
    resp = requests.post(f"{BASE_URL}/api/v2/drill-collars/",
                         headers={"Authorization": f"Grant {TOKEN}"}, json={}, timeout=30)
    assert resp.status_code == 403
