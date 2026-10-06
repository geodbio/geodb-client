"""geodb-client 0.3.1 (protocol v0.4.1 teaching (f)): ``write(..., layer=)`` and
docstrings that name every protocol v0.4 write intent and face."""

import geodb
from geodb.client import Client

from test_writes import WRITTEN, FakeResponse, client

#: Every intent POST /api/v2/records/ takes on protocol 0.3.1 (server
#: protocol/writes/contract.py ALL_INTENTS + the project and report faces).
V04_INTENTS = ('create', 'upsert', 'update', 'retract', 'restore', 'make_default_set',
               'make_export_set', 'qaqc_verdict', 'qc_reconnect', 'undo',
               'set_coordinate_system', 'set_state', 'publish')
#: The record types a v0.4 write face adds beside the data models.
V04_FACES = ('Project', 'Report', 'ReportSection', 'ReportFigure', 'CustomFieldSchema',
             'ColumnConfiguration', 'AssayMergeSettings', 'AssayRangeConfiguration',
             'VectorLayer', 'QAQCProtocol', 'QCConfiguration', 'MetalEquivalentConfig',
             'ODBCSettings')


def test_version_is_0_3_1():
    assert geodb.__version__.startswith("0.3.")  # 0.3.2 = docs-only release


def test_write_sends_layer_for_a_vector_layer():
    gx, s = client(FakeResponse(200, WRITTEN))
    layer = {"name": "Mapped faults", "kind": "geology_fault"}
    gx.write("VectorLayer", [{"geometry": "LINESTRING (0 0, 1 1)", "epsg": 32611}],
             layer=layer, project=12, dry_run=True)
    body = s.calls[0][2]["json"]
    assert body["layer"] == layer
    assert body["model"] == "VectorLayer" and body["project"] == 12 and body["dry_run"] is True


def test_write_omits_layer_when_not_given():
    gx, s = client(FakeResponse(200, WRITTEN))
    gx.write("DrillSample", [{"name": "S1"}], logging_set="pXRF")
    assert "layer" not in s.calls[0][2]["json"]


def test_validate_forwards_layer():
    gx, s = client(FakeResponse(200, WRITTEN))
    gx.validate("VectorLayer", [], layer={"name": "L", "kind": "other"})
    assert s.calls[0][2]["json"]["layer"] == {"name": "L", "kind": "other"}


def test_write_docstring_names_every_v04_intent_and_face():
    doc = Client.write.__doc__
    missing = [i for i in V04_INTENTS if f'"{i}"' not in doc and f"``{i}``" not in doc
               and f"`{i}`" not in doc]
    assert not missing, f"write() docstring never names: {missing}"
    faces = [f for f in V04_FACES if f not in doc]
    assert not faces, f"write() docstring never names the face(s): {faces}"
    assert "layer" in doc


def test_module_docstring_points_at_write_and_describe():
    import geodb.writes as w
    for word in ("layer", "describe", "undo"):
        assert word in w.__doc__


def test_export_sends_merge_settings_id_only_when_given():
    job = {"job_id": "j1", "status": "pending"}
    gx, s = client(FakeResponse(202, job, url="http://t/api/v2/exports/"))
    try:
        gx.export("drill_samples", project=12, merge_settings_id=7)
    except Exception:
        pass
    assert s.calls[0][2]["json"]["merge_settings_id"] == 7
    gx, s = client(FakeResponse(202, job, url="http://t/api/v2/exports/"))
    try:
        gx.export("drill_samples", project=12)
    except Exception:
        pass
    assert "merge_settings_id" not in s.calls[0][2]["json"]


def test_docstrings_teach_the_v041_assay_reads():
    from geodb.client import Client
    assert "include_trashed_samples" in Client.assay_results.__doc__
    assert "merge_settings" in Client.export.__doc__


def test_write_docstring_teaches_below_detection():
    doc = " ".join(Client.write.__doc__.split())
    for word in ("BDL", "ambiguous_nd", "never send 0"):
        assert word in doc, word


def test_export_sends_include_trashed_samples_only_when_given():
    job = {"job_id": "j1", "status": "pending"}
    gx, s = client(FakeResponse(202, job, url="http://t/api/v2/exports/"))
    try:
        gx.export("assay_results", project=12, include_trashed_samples=True)
    except Exception:
        pass
    assert s.calls[0][2]["json"]["include_trashed_samples"] is True
    gx, s = client(FakeResponse(202, job, url="http://t/api/v2/exports/"))
    try:
        gx.export("assay_results", project=12)
    except Exception:
        pass
    assert "include_trashed_samples" not in s.calls[0][2]["json"]


def test_qc_reconnect_is_not_said_to_need_a_person_key_confirm():
    doc = " ".join(Client.write.__doc__.split())
    assert "``qaqc_verdict`` and ``qc_reconnect`` have no methods" not in doc
    assert "qc_samples" in " ".join(Client.export.__doc__.split())
