"""
geodb-client 0.3.5: the lean reads the server added for planning an import
(server plan W2/W4, rulings R-4/R-5):

* ``record_counts(project)`` → ``GET /api/v2/record-counts/?project=`` — how
  many records of each type a project holds, in one small read;
* ``name=`` / ``bhid=`` take several values: a list is sent comma-joined (the
  server's one multi-value form; a repeated parameter would read as its LAST
  value, silently narrowing the read);
* ``include_trashed=True`` on the reference lists is sent as ``true``.

Mocked transport.
"""

import json

import pytest

import geodb


class Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode()
        self.text = self.content.decode()
        self.url = "http://t/api/v2/x"
        self.headers = {}

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._payload


PAGE = {"count": 0, "next": None, "results": []}


class Session:
    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, **kw):
        self.calls.append((url, params))
        return self.reply(url, params)


def _client(payload=PAGE, status=200):
    session = Session(lambda url, params: Resp(status, payload))
    return geodb.Client(token="gdbg_x", base_url="http://t", session=session), session


COUNTS = {"project": {"id": 12, "name": "Alpha"},
          "counts": {"DrillCollar": {"live": 40, "in_trash": 2}, "Method": {"live": 6}},
          "company_wide": ["Method"], "in_trash_counted": True, "not_readable": []}


def test_record_counts_reads_one_project():
    gx, s = _client(COUNTS)
    counts = gx.record_counts(12)
    assert s.calls == [("http://t/api/v2/record-counts/", {"project": 12})]
    assert counts["counts"]["DrillCollar"] == {"live": 40, "in_trash": 2}
    assert counts["company_wide"] == ["Method"]


def test_record_counts_takes_a_project_name():
    gx, s = _client(COUNTS)
    gx.record_counts(project="Alpha")
    assert s.calls[0][1] == {"project": "Alpha"}


def test_record_counts_needs_a_project():
    gx, _s = _client(COUNTS)
    with pytest.raises(TypeError):
        gx.record_counts()


def test_several_names_are_sent_comma_joined():
    gx, s = _client()
    list(gx.samples(project=12, set="all", name=["DH-1-001", "DH-1-002"]))
    list(gx.qc_samples(project=12, name=("STD-1", "BLK-2")))
    list(gx.assay_results(project=12, name=["S1", "S2", "S3"]))
    assert s.calls[0][1]["name"] == "DH-1-001,DH-1-002"
    assert s.calls[1][1]["name"] == "STD-1,BLK-2"
    assert s.calls[2][1]["name"] == "S1,S2,S3"


def test_several_holes_are_sent_comma_joined():
    gx, s = _client()
    list(gx.lithology(project=12, set="all", bhid=["DH-1", "DH-2", 77]))
    assert s.calls[0][1]["bhid"] == "DH-1,DH-2,77"


def test_one_name_or_hole_is_sent_as_it_is():
    gx, s = _client()
    list(gx.collars(project=12, name="DH-1"))
    list(gx.drill_surveys(project=12, bhid=77))
    assert s.calls[0][1]["name"] == "DH-1"
    assert s.calls[1][1]["bhid"] == 77


def test_include_trashed_true_is_sent_as_true():
    gx, s = _client()
    list(gx.methods(company=3, include_trashed=True))
    list(gx.laboratories(company=3, include_trashed=False))
    list(gx.collars(project=12, include_trashed=True))
    assert s.calls[0][1]["include_trashed"] == "true"
    assert s.calls[1][1]["include_trashed"] == "false"
    assert s.calls[2][1]["include_trashed"] == "true"


def test_a_vendor_key_asking_for_the_trash_is_refused_with_the_reason():
    body = {"reason_code": "trash_needs_person_key",
            "detail": "include_trashed is for a key acting as a person.",
            "remedy": "Leave include_trashed out: this key reads live records only."}
    gx, _s = _client(body, status=400)
    with pytest.raises(geodb.InvalidRequest) as caught:
        list(gx.methods(company=3, include_trashed=True))
    assert caught.value.reason_code == "trash_needs_person_key"


def test_the_list_docstrings_teach_several_values_and_the_trash():
    assert "comma" in geodb.Client.samples.__doc__
    assert "include_trashed" in geodb.Client.methods.__doc__
    assert "include_trashed" in geodb.Client.laboratories.__doc__
    assert "include_trashed" in geodb.Client.collars.__doc__
