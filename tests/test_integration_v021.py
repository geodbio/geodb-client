"""0.3.0 against a live geoDB server (protocol 0.3): parallel paging returns the
same rows as one-page-at-a-time, the assay_results export round-trips with its
flags, an over-cap limit is reported, a merged export says what it merged.

Skipped unless GEODB_TEST_BASE_URL + GEODB_TEST_TOKEN are set (and, for a key
reading several projects, GEODB_TEST_PROJECT).
"""

import os
import time

import pytest

import geodb

BASE_URL = os.environ.get("GEODB_TEST_BASE_URL")
TOKEN = os.environ.get("GEODB_TEST_TOKEN")
PROJECT = os.environ.get("GEODB_TEST_PROJECT")

pytestmark = pytest.mark.skipif(
    not (BASE_URL and TOKEN),
    reason="set GEODB_TEST_BASE_URL + GEODB_TEST_TOKEN to run integration tests")


def client(parallel=4):
    return geodb.Client(token=TOKEN, base_url=BASE_URL, api_prefix="/api/v2",
                        parallel=parallel, timeout=300)


def test_parallel_and_sequential_pages_agree():
    t0 = time.perf_counter()
    fast = client().assay_results(project=PROJECT)
    rows = list(fast)
    t1 = time.perf_counter()
    slow = list(client(parallel=1).assay_results(project=PROJECT))
    t2 = time.perf_counter()
    assert [r["assay_id"] for r in rows] == [r["assay_id"] for r in slow]
    assert [(r["element"], r["method_id"]) for r in rows] == \
        [(r["element"], r["method_id"]) for r in slow]
    assert len(rows) == fast.count()
    print(f"\n{len(rows)} values: parallel {t1 - t0:.1f}s, sequential {t2 - t1:.1f}s")


def test_an_over_cap_limit_is_reported():
    paged = client().collars(project=PROJECT, limit=5000)
    list(paged)
    assert paged.envelope.get("limit_clamped") == {"asked": 5000, "served": 500}


def test_assay_results_export_round_trip(tmp_path):
    pd = pytest.importorskip("pandas")
    t0 = time.perf_counter()
    path = client().export("assay_results", format="parquet", project=PROJECT) \
        .wait(poll_seconds=1).download(str(tmp_path / "values.parquet"))
    df = pd.read_parquet(path)
    print(f"\n{len(df)} values exported + downloaded in {time.perf_counter() - t0:.1f}s")
    for column in ("value", "value_numeric", "below_detection", "above_det_limit",
                   "detection_limit", "excluded", "method", "certificate"):
        assert column in df.columns, column
    bdl = df[df["below_detection"]]
    assert bdl["value_numeric"].isna().all()
    assert len(df) == client().assay_results(project=PROJECT).count()


def test_a_merged_csv_export_says_what_it_merged():
    """Review S1: through the real server, not a mock — the job status carries
    the merged table's notes whatever the format."""
    job = client().export("drill_samples", format="csv", project=PROJECT).wait(poll_seconds=1)
    assert "merged_values" in job.notes, job.notes
    assert job.see_guide, "the export is read like its list"
