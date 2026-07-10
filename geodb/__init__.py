"""
geodb-client — the Python client for the geoDB Open Exploration Protocol.

    import geodb
    gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")
    collars = gx.collars().to_dataframe()

See https://github.com/geodb-io/geodb-protocol for the protocol spec.
"""

from .client import Client
from .errors import (
    GeodbError, AuthError, NotFoundError, APIError, ExportError,
)

__version__ = "0.1.0"

__all__ = [
    "Client",
    "GeodbError", "AuthError", "NotFoundError", "APIError", "ExportError",
    "__version__",
]
