"""
geodb-client 0.3.5: TLS through the operating system's certificate store
(``truststore``), so the client works behind HTTPS-inspecting antivirus and
corporate proxies whose root certificate the OS trusts but certifi does not.

The transport is mocked: a fake ``truststore`` module stands in, and no
connection is made.
"""

import ssl
import sys
import types
import warnings

import pytest
import requests

import geodb
from geodb import _tls


class _FakeContext:
    def __init__(self, protocol):
        self.protocol = protocol


@pytest.fixture
def fake_truststore(monkeypatch):
    module = types.ModuleType("truststore")
    module.SSLContext = _FakeContext
    monkeypatch.setitem(sys.modules, "truststore", module)
    monkeypatch.setattr(_tls, "_warned", False)
    return module


def _https_context(session):
    adapter = session.get_adapter("https://api.geodb.io/api/v2/")
    return adapter.poolmanager.connection_pool_kw.get("ssl_context")


def test_the_default_session_verifies_against_the_os_store(fake_truststore):
    gx = geodb.Client(token="gdbg_x")
    ctx = _https_context(gx._session)
    assert isinstance(ctx, _FakeContext)
    assert ctx.protocol == ssl.PROTOCOL_TLS_CLIENT
    assert gx.tls_trust == "os"


def test_a_proxy_connection_uses_the_os_store_too(fake_truststore):
    gx = geodb.Client(token="gdbg_x")
    adapter = gx._session.get_adapter("https://api.geodb.io/")
    manager = adapter.proxy_manager_for("http://proxy.corp:3128")
    assert isinstance(manager.connection_pool_kw.get("ssl_context"), _FakeContext)


def test_only_this_session_is_touched_never_the_process(fake_truststore):
    geodb.Client(token="gdbg_x")
    assert _https_context(requests.Session()) is None     # a fresh session: certifi
    assert ssl.SSLContext.__module__ == "ssl"              # no inject_into_ssl()


def test_missing_truststore_falls_back_to_certifi_with_one_warning(monkeypatch):
    monkeypatch.setitem(sys.modules, "truststore", None)   # import raises ImportError
    monkeypatch.setattr(_tls, "_warned", False)
    with pytest.warns(RuntimeWarning, match="certifi") as caught:
        gx = geodb.Client(token="gdbg_x")
        geodb.Client(token="gdbg_x")                       # warned once per process
    assert len([w for w in caught if "certifi" in str(w.message)]) == 1
    assert "\n" not in str(caught[0].message)
    assert _https_context(gx._session) is None
    assert gx.tls_trust == "certifi"


def test_a_failing_truststore_falls_back_to_certifi(monkeypatch):
    module = types.ModuleType("truststore")

    def broken(protocol):
        raise OSError("no platform trust store")
    module.SSLContext = broken
    monkeypatch.setitem(sys.modules, "truststore", module)
    monkeypatch.setattr(_tls, "_warned", False)
    with pytest.warns(RuntimeWarning, match="no platform trust store"):
        gx = geodb.Client(token="gdbg_x")
    assert _https_context(gx._session) is None
    assert gx.tls_trust == "certifi"


def test_opt_out_uses_certifi_silently(fake_truststore):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        gx = geodb.Client(token="gdbg_x", os_trust_store=False)
    assert _https_context(gx._session) is None
    assert gx.tls_trust == "certifi"


def test_a_session_you_pass_is_left_as_it_is(fake_truststore):
    mine = requests.Session()
    gx = geodb.Client(token="gdbg_x", session=mine)
    assert gx._session is mine
    assert _https_context(mine) is None
    assert gx.tls_trust == "caller"


def test_truststore_is_an_install_dependency_with_a_floor():
    import pathlib
    text = (pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
    assert '"truststore>=0.10; python_version >= \'3.10\'"' in text
