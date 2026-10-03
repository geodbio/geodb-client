"""geodb-client 0.2.0 — the protocol v0.2 rules, mocked transport.

* reads name their project / company / scope / set (only what was named);
* ``count()`` reuses the list envelope's count;
* every refusal is a typed exception carrying ``reason_code`` + ``remedy``
  (``ProjectRequired.choices``, ``SetChoiceRequired.sets``), compatible with
  the 0.1.0 classes (``AuthError``, ``APIError``);
* ``export(project=…, set=…)`` and ``assay_results()``.
"""

import json

import pytest

import geodb
from geodb.errors import (APIError, AuthError, InvalidRequest, PermissionDenied,
                          ProjectRequired, SetChoiceRequired, Throttled)


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
        if self._payload is None:
            raise ValueError
        return self._payload


class Session:
    def __init__(self, reply):
        self.reply, self.calls = reply, []

    def get(self, url, params=None, headers=None, timeout=None, **kw):
        self.calls.append(("GET", url, dict(params or {})))
        return self.reply(url, params)

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("POST", url, json))
        return self.reply(url, json)


def client(reply):
    return geodb.Client(token="gdbg_t", base_url="http://t", session=Session(reply))


PAGE = {"count": 3, "next": None, "results": [{"id": 1}, {"id": 2}, {"id": 3}]}


def test_reads_send_only_what_they_name():
    gx = client(lambda url, params: Resp(200, PAGE))
    list(gx.collars(project=12))
    list(gx.lithology(project="Alpha", set="all", depth_from=1))
    list(gx.collars(scope="company", company=4))
    list(gx.collars())
    params = [c[2] for c in gx._session.calls]
    assert params[0] == {"project": 12, "limit": 500}
    assert params[1] == {"project": "Alpha", "set": "all", "depth_from": 1, "limit": 500}
    assert params[2] == {"scope": "company", "company": 4, "limit": 500}
    assert params[3] == {"limit": 500}


def test_count_reuses_the_envelope():
    gx = client(lambda url, params: Resp(200, PAGE))
    rows = gx.collars(project=1)
    assert len(list(rows)) == 3
    assert rows.count() == 3
    assert len(gx._session.calls) == 1, "count() after reading makes no extra request"


def test_project_required_carries_the_choices():
    body = {"reason_code": "project_required", "detail": "name the project",
            "remedy": "Add project=<id>", "choices": [{"company": {"id": 1, "name": "A"},
                                                      "projects": [{"id": 9, "name": "P"}]}]}
    gx = client(lambda url, params: Resp(400, body))
    with pytest.raises(ProjectRequired) as caught:
        list(gx.collars())
    err = caught.value
    assert (err.reason_code, err.remedy, err.status_code) == ("project_required",
                                                             "Add project=<id>", 400)
    assert err.choices[0]["projects"][0]["id"] == 9
    assert isinstance(err, (InvalidRequest, APIError))


def test_set_choice_required_carries_the_sets():
    body = {"reason_code": "set_choice_required", "detail": "several sets", "remedy": "ask",
            "sets": [{"project": {"id": 9}, "family": "lithology", "sets": [{"id": 1}]}]}
    gx = client(lambda url, params: Resp(400, body))
    with pytest.raises(SetChoiceRequired) as caught:
        list(gx.lithology(project=9))
    assert caught.value.sets[0]["family"] == "lithology"


def test_a_403_is_permission_denied_and_still_an_auth_error():
    body = {"reason_code": "grant_surface_forbidden", "detail": "no", "remedy": "use the map"}
    gx = client(lambda url, params: Resp(403, body))
    with pytest.raises(PermissionDenied) as caught:
        list(gx.collars(project=1))
    assert isinstance(caught.value, AuthError)
    assert caught.value.remedy == "use the map"


def test_throttled_reads_retry_after():
    gx = client(lambda url, params: Resp(429, {"reason_code": "throttled", "detail": "slow",
                                               "remedy": "wait"}, headers={"Retry-After": "7"}))
    with pytest.raises(Throttled) as caught:
        list(gx.collars(project=1))
    assert caught.value.retry_after == 7


def test_export_names_its_project_and_set():
    gx = client(lambda url, body: Resp(202, {"id": "j1", "state": "queued",
                                             "model": "drill_lithology"}))
    gx.export("drill_lithology", project=9, set="Relog")
    assert gx._session.calls[0][2] == {"model": "drill_lithology", "format": "geoparquet",
                                       "include_assays": True, "project": 9, "set": "Relog"}


def test_assay_results_is_the_flat_table():
    gx = client(lambda url, params: Resp(200, PAGE))
    list(gx.assay_results(project=9))
    assert gx._session.calls[0][1].endswith("/api/v2/assay-results/")
    assert gx._session.calls[0][2]["project"] == 9


# ── client/server version pairing ──────────────────────────────────────────
def _versioned(version):
    return lambda url, params: Resp(200, PAGE, headers={"X-GeoDB-Protocol-Version": version})


def test_the_client_sends_the_protocol_version_it_speaks():
    seen = {}

    class Recording(Session):
        def get(self, url, params=None, headers=None, timeout=None, **kw):
            seen.update(headers or {})
            return super().get(url, params=params, headers=headers, timeout=timeout, **kw)

    gx = geodb.Client(token="gdbg_t", base_url="http://t", session=Recording(_versioned("0.2.0")))
    list(gx.collars(project=1))
    assert seen["X-GeoDB-Protocol-Version"] == geodb.PROTOCOL_VERSION


def test_a_server_on_another_minor_is_a_clear_error_naming_the_install_line():
    gx = client(_versioned("0.3.1"))
    with pytest.raises(geodb.ProtocolVersionMismatch) as caught:
        list(gx.collars(project=1))
    err = caught.value
    assert err.install == 'pip install "geodb-client>=0.3,<0.4"'
    assert "0.3.1" in str(err) and geodb.PROTOCOL_VERSION in str(err)
    with pytest.raises(geodb.ProtocolVersionMismatch):
        list(gx.collars(project=1))      # raised again, never silently allowed


def test_the_same_minor_and_a_missing_header_pass():
    list(client(_versioned("0.2.9")).collars(project=1))
    list(client(lambda url, params: Resp(200, PAGE)).collars(project=1))


def test_the_check_can_be_switched_off():
    gx = geodb.Client(token="gdbg_t", base_url="http://t", session=Session(_versioned("0.3.0")),
                      check_protocol=False)
    list(gx.collars(project=1))


def test_the_package_is_versioned_in_step_with_the_protocol():
    assert geodb.__version__.split(".")[:2] == geodb.PROTOCOL_VERSION.split(".")[:2]
