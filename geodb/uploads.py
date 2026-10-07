"""
Uploading files through the geoDB protocol (protocol 0.3.3) — with a key that
acts as a person (the user's own connected AI) and may write records.

    gx = geodb.Client(token=…)                     # e.g. a session_key from the connector
    r = gx.upload_document("report.pdf", project=12, category="RP", title="Q3 summary")
    r.rows[0]["result"]["read"]                    # 'documents/345/' — read it back
    gx.upload_photo("outcrop.jpg", project=12, category="GN")
    gx.attach_drill_box_image(box_id=88, path="DH-7_box3.jpg", project=12)
    gx.upload_project_file("pit_shell.glb", project=12, category="3D")
    gx.undo(r.write_id)                            # moves the upload to the Trash

* Documents and photos are multipart POSTs to ``documents/`` / ``photos/`` on
  the API host (at most 95 MiB each).
* A project file (raster, DEM, geophysics grid, GeoPackage, archive, 3D mesh;
  up to 5 GB) goes in blocks STRAIGHT TO STORAGE: ``upload_start`` on the
  records endpoint hands out block URLs on the storage host, the bytes are PUT
  there (no key goes with them), and ``upload_commit`` makes the record. A
  cloud sandbox that may reach only the API host cannot reach the storage
  host: :class:`StorageUnreachable` says so — run the upload from a local
  agent (Claude Code / Cowork on your machine), allow the storage host (or
  "All domains") in the sandbox's network settings, or upload the file in the
  geoDB web app.

Every upload answers like a records write (a :class:`geodb.WriteResult`: one
row, ``created`` or ``refused`` with ``reason_code`` + ``remedy``) and returns
a ``write_id`` that ``undo`` reverses.
"""

import json
import os

__all__ = ["StorageUnreachable", "UploadsMixin"]


class StorageUnreachable(Exception):
    """The block PUT could not reach the storage host (a sandbox's network
    allowlist, typically). Nothing was stored; the upload was aborted."""

    def __init__(self, host, original):
        self.host = host
        self.original = original
        super().__init__(
            f"Could not reach the storage host {host} to send the file's blocks ({original}). "
            "Nothing was stored. A project file goes straight to storage, not through the "
            "API host: run this from a local agent (Claude Code or Cowork on your own "
            "machine), or allow that host (or 'All domains') in this sandbox's network "
            "settings, or upload the file in the geoDB web app (Project Files). Documents "
            "and photos upload through the API host and work from here.")


def _read(path_or_bytes, file_name):
    """``(name, bytes)`` from a path or raw bytes (+ a name)."""
    if isinstance(path_or_bytes, (bytes, bytearray)):
        if not file_name:
            raise ValueError("file_name is required when uploading bytes")
        return file_name, bytes(path_or_bytes)
    path = os.fspath(path_or_bytes)
    with open(path, "rb") as fh:
        return file_name or os.path.basename(path), fh.read()


def _form(fields):
    """Multipart form fields: drop Nones, JSON-encode dicts, stringify the rest."""
    out = {}
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, bool):
            value = "true" if value else "false"
        out[key] = str(value)
    return out


class UploadsMixin:
    """The upload helpers of :class:`geodb.Client`."""

    def _upload(self, path, fields, file_field, name, data):
        from .writes import WriteResult
        from .errors import WriteRefused
        headers = self._headers()          # requests sets the multipart Content-Type
        resp = self._session.post(self._url(path), data=_form(fields),
                                  files={file_field: (name, data)}, headers=headers,
                                  timeout=self.timeout)
        self._check_protocol(resp)
        if resp.status_code == 401:
            self._handle(resp)
        if resp.status_code != 200:
            raise WriteRefused(resp.status_code, self._body(resp), resp.url)
        return WriteResult(self, self._body(resp) or {})

    def upload_document(self, path_or_bytes, project, category, *, file_name=None,
                        title=None, author=None, date=None, description=None,
                        dry_run=False):
        """Upload a document (PDF, Word, Excel, CSV, text, images, GPX/KML/KMZ,
        GeoPackage …; at most 95 MiB) to ``project``. ``category`` is the
        document type code (RP report, TR technical report, MM memo, MP map,
        CT certificate, PM permit, PR press release, QC, IM image, OT other …).
        Read it back at ``result.rows[0]["result"]["read"]``."""
        name, data = _read(path_or_bytes, file_name)
        return self._upload("/documents/", {
            "project": project, "category": category, "file_name": name, "title": title,
            "author": author, "date": date, "description": description,
            "dry_run": True if dry_run else None}, "document", name, data)

    def upload_photo(self, path_or_bytes, project, category="GN", *, file_name=None,
                     description=None, latitude=None, longitude=None, elevation=None,
                     direction=None, attach_to=None, dry_run=False):
        """Upload a photo (JPEG, PNG, TIFF, WebP, HEIC; at most 95 MiB) to
        ``project``. ``latitude``/``longitude`` are WGS84 degrees and override
        the file's own GPS. ``attach_to={"model": "DrillPhoto", "id": <box>}``
        links it to a drill-box photo record in the same project (see
        :meth:`attach_drill_box_image`). The file name's extension must match
        the image's real format (a WebP named .jpg is refused)."""
        name, data = _read(path_or_bytes, file_name)
        return self._upload("/photos/", {
            "project": project, "category": category, "description": description,
            "latitude": latitude, "longitude": longitude, "elevation": elevation,
            "direction": direction, "attach_to": attach_to,
            "dry_run": True if dry_run else None}, "image", name, data)

    def attach_drill_box_image(self, box_id, path_or_bytes, project, *, file_name=None,
                               description=None, dry_run=False):
        """Upload an image and link it to the DrillPhoto (drill-box photo)
        record ``box_id`` — write the box record first (``write("DrillPhoto",
        …)``: bhid, depths, box_no), then attach each image."""
        return self.upload_photo(path_or_bytes, project, category="DH", file_name=file_name,
                                 description=description, dry_run=dry_run,
                                 attach_to={"model": "DrillPhoto", "id": int(box_id)})

    def upload_project_file(self, path, project, category=None, *, name=None,
                            description=None, epsg=None, dry_run=False):
        """Upload a project file (raster, DEM, geophysics grid, GeoPackage,
        ZIP, 3D mesh .glb …; up to 5 GB) straight to storage in blocks:
        start → PUT each block → commit. ``category`` is the project file type
        (a .glb / .gltf defaults to 3D, .zip to ZP, .gpkg to GP; a raster needs
        one: DM DEM, MG magnetics, GV gravity, GL geology, ST satellite …).
        Raises :class:`StorageUnreachable` (after aborting) when the storage
        host cannot be reached from here."""
        import requests
        from .writes import WriteResult
        path = os.fspath(path)
        size = os.path.getsize(path)
        record = {"project": project, "file_name": os.path.basename(path), "size": size,
                  "category": category, "name": name, "description": description,
                  "epsg": epsg}
        record = {k: v for k, v in record.items() if v is not None}
        start = self._records({"model": "ProjectFile", "intent": "upload_start",
                               "records": [record], "dry_run": bool(dry_run)})
        row = start.rows[0] if start.rows else {}
        if dry_run or row.get("status") != "ready":
            return start
        plan = row["result"]
        template = plan["put"]["url_template"]
        from urllib.parse import quote
        try:
            with open(path, "rb") as fh:
                for block in plan["blocks"]:
                    fh.seek(block["offset"])
                    chunk = fh.read(block["length"])
                    url = block.get("url") or template.replace("{block_id}", quote(block["id"]))
                    # No key goes to the storage host: the URL carries its own
                    # write-only signature.
                    resp = self._session.put(url, data=chunk, timeout=self.timeout)
                    if resp.status_code not in (200, 201):
                        raise requests.HTTPError(f"block PUT answered {resp.status_code}")
        except (requests.ConnectionError, requests.Timeout, OSError) as exc:
            self._records({"model": "ProjectFile", "intent": "upload_abort",
                           "records": [{"slot": plan["slot"]}]})
            raise StorageUnreachable(plan.get("storage_host"), exc) from exc
        except requests.HTTPError:
            self._records({"model": "ProjectFile", "intent": "upload_abort",
                           "records": [{"slot": plan["slot"]}]})
            raise
        result = self._records({"model": "ProjectFile", "intent": "upload_commit",
                                "records": [{"slot": plan["slot"]}]})
        return result if isinstance(result, WriteResult) else WriteResult(self, result)
