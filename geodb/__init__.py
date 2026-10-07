"""
geodb-client — the Python client for the geoDB Open Exploration Protocol.

    import geodb
    gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")
    gx.project()                                  # the companies and projects it reads
    collars = gx.collars(project=12).to_dataframe()
    table = gx.assay_results(project=12).to_dataframe()

    # with a key that may write (see describe / write / undo):
    result = gx.write("DrillCollar", [{"name": "DH-1", "latitude": 4189978.5,
                                       "longitude": 712671.5, "epsg": 26915}])
    gx.undo(result.write_id)

    # files, with a key that acts as a person (see geodb.uploads):
    gx.upload_document("report.pdf", project=12, category="RP")
    gx.upload_project_file("pit_shell.glb", project=12)

See https://github.com/geodbio/geodb-protocol for the protocol spec.
"""

from .client import Client, PROTOCOL_VERSION
from .errors import (
    GeodbError, AuthError, PermissionDenied, NotFoundError, APIError, InvalidRequest,
    ProjectRequired, CompanyRequired, SetChoiceRequired, Conflict, Throttled, ExportError,
    ProtocolVersionMismatch, WriteRefused, RowsRefused,
)
from .uploads import StorageUnreachable
from .writes import WriteResult

__version__ = "0.3.3"

__all__ = [
    "Client", "PROTOCOL_VERSION", "ProtocolVersionMismatch",
    "GeodbError", "AuthError", "PermissionDenied", "NotFoundError", "APIError",
    "InvalidRequest", "ProjectRequired", "CompanyRequired", "SetChoiceRequired",
    "Conflict", "Throttled", "ExportError", "WriteRefused", "RowsRefused", "WriteResult",
    "StorageUnreachable",
    "__version__",
]
