"""
TLS through the operating system's certificate store (geodb-client 0.3.5).

HTTPS-inspecting antivirus and corporate proxies re-sign every connection with
their own root certificate. The operating system trusts that root (the IT
department installed it there); certifi, the bundle ``requests`` uses by
default, does not, so every call failed with ``CERTIFICATE_VERIFY_FAILED``.
``truststore`` verifies against the OS store instead (Windows, macOS, Linux).

Only the client's OWN session is changed: an adapter hands urllib3 a
``truststore.SSLContext`` for every https pool (direct and through a proxy).
The process-wide ``truststore.inject_into_ssl()`` is never called, so the
caller's other libraries keep their own TLS settings.

When truststore is missing (Python 3.9, which it does not support, or an
install without it) or cannot build a context, the session uses certifi as
before, after ONE warning per process. ``geodb.Client(..., os_trust_store=False)``
opts out; a ``session=`` you pass is never touched.
"""

import ssl
import warnings

import requests
from requests.adapters import HTTPAdapter

__all__ = ["new_session"]

_warned = False


class _ContextAdapter(HTTPAdapter):
    """An HTTPAdapter whose pools (direct and proxied) verify with ``ssl_context``."""

    def __init__(self, ssl_context, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().proxy_manager_for(*args, **kwargs)


def _os_context():
    """A truststore SSLContext, or ``(None, why)`` when one cannot be had."""
    try:
        import truststore
    except ImportError:
        return None, "truststore is not installed (pip install truststore; Python 3.10+)"
    try:
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT), None
    except Exception as exc:  # noqa: BLE001 — any failure falls back to certifi
        return None, f"truststore failed ({exc})"


def _warn_once(why):
    global _warned
    if _warned:
        return
    _warned = True
    warnings.warn(
        f"geodb-client: {why}; verifying TLS with certifi, not the operating system's "
        f"certificate store, so HTTPS-inspecting antivirus or a corporate proxy may fail "
        f"with CERTIFICATE_VERIFY_FAILED.", RuntimeWarning, stacklevel=3)


def new_session(os_trust_store=True):
    """``(session, trust)``: a new ``requests.Session`` and what it verifies
    with — ``"os"`` (truststore) or ``"certifi"``."""
    session = requests.Session()
    if not os_trust_store:
        return session, "certifi"
    context, why = _os_context()
    if context is None:
        _warn_once(why)
        return session, "certifi"
    session.mount("https://", _ContextAdapter(context))
    return session, "os"
