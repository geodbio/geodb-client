"""
geodb-client — the Python client for the geoDB Open Exploration Protocol.

    import geodb
    gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")
    collars = gx.collars().to_dataframe()

    # with a key that may write (see describe / write / undo):
    result = gx.write("DrillCollar", [{"name": "DH-1", "latitude": 4189978.5,
                                       "longitude": 712671.5, "epsg": 26915}])
    gx.undo(result.write_id)

See https://github.com/geodbio/geodb-protocol for the protocol spec.
"""

from .client import Client
from .errors import (
    GeodbError, AuthError, NotFoundError, APIError, ExportError, WriteRefused, RowsRefused,
)
from .writes import WriteResult

__version__ = "0.1.0"

__all__ = [
    "Client",
    "GeodbError", "AuthError", "NotFoundError", "APIError", "ExportError",
    "WriteRefused", "RowsRefused", "WriteResult",
    "__version__",
]
