"""
geodb-client 0.3.3: uploads (protocol 0.3.3) — documents and photos as
multipart POSTs to the API host; a project file in blocks straight to storage
(start → PUT → commit), aborted with a plain-English StorageUnreachable when
the storage host cannot be reached (a sandbox's network allowlist).
"""

import json

import pytest
import requests

import geodb
from geodb import StorageUnreachable

from test_writes import FakeResponse


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def _next(self, method, url, **kw):
        self.calls.append((method, url, kw))
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    def post(self, url, **kw):
        return self._next("POST", url, **kw)

    def get(self, url, **kw):
        return self._next("GET", url, **kw)

    def put(self, url, **kw):
        return self._next("PUT", url, **kw)


def _client(*responses):
    session = Session(*responses)
    return geodb.Client(token="gdbg_x", base_url="http://t", session=session), session


def _row(status="created", **result):
    return {"model": "Document", "intent": "upload", "dry_run": False, "write_id": "w1",
            "summary": {status: 1}, "rows": [{"index": 0, "status": status,
                                               "result": result or None}]}


def test_upload_document_posts_multipart_with_the_key(tmp_path):
    pdf = tmp_path / "memo.pdf"
    pdf.write_bytes(b"%PDF-1.4 x")
    gx, s = _client(FakeResponse(200, _row(read="documents/5/")))
    result = gx.upload_document(pdf, project=12, category="MM", title="Memo")
    method, url, kw = s.calls[0]
    assert (method, url) == ("POST", "http://t/api/v2/documents/")
    assert kw["files"] == {"document": ("memo.pdf", b"%PDF-1.4 x")}
    assert kw["data"] == {"project": "12", "category": "MM", "file_name": "memo.pdf",
                          "title": "Memo"}
    assert kw["headers"]["Authorization"] == "Grant gdbg_x"
    assert "Content-Type" not in kw["headers"]
    assert result.write_id == "w1"
    assert result.rows[0]["result"]["read"] == "documents/5/"


def test_upload_photo_from_bytes_needs_a_name():
    gx, _s = _client()
    with pytest.raises(ValueError):
        gx.upload_photo(b"\x89PNG", project=1)


def test_attach_drill_box_image_sends_attach_to_as_json():
    gx, s = _client(FakeResponse(200, _row()))
    gx.attach_drill_box_image(88, b"\xff\xd8\xff", project=3, file_name="box.jpg")
    _m, url, kw = s.calls[0]
    assert url == "http://t/api/v2/photos/"
    assert json.loads(kw["data"]["attach_to"]) == {"model": "DrillPhoto", "id": 88}
    assert kw["data"]["category"] == "DH"
    assert kw["files"] == {"image": ("box.jpg", b"\xff\xd8\xff")}


def test_a_refused_upload_request_raises_write_refused():
    gx, _s = _client(FakeResponse(403, {"reason_code": "person_key_required",
                                        "detail": "x", "remedy": "y"}))
    with pytest.raises(geodb.WriteRefused) as err:
        gx.upload_photo(b"x", project=1, file_name="a.png")
    assert err.value.reason_code == "person_key_required"


def _start(blocks, host="acct.blob.core.windows.net"):
    return {"model": "ProjectFile", "intent": "upload_start", "dry_run": False,
            "write_id": None, "summary": {"ready": 1},
            "rows": [{"index": 0, "status": "ready", "result": {
                "slot": "S", "storage_host": host, "blocks": blocks,
                "put": {"method": "PUT",
                        "url_template": f"https://{host}/c/k?comp=block&blockid={{block_id}}&sig=x"}}}]}


def test_upload_project_file_puts_each_block_without_the_key_then_commits(tmp_path):
    f = tmp_path / "pit.glb"
    f.write_bytes(b"glTF" + b"0123456789")
    blocks = [{"id": "YQ==", "offset": 0, "length": 8},
              {"id": "Yg==", "offset": 8, "length": 6}]
    commit = {"model": "ProjectFile", "intent": "upload_commit", "write_id": "w9",
              "summary": {"created": 1}, "rows": [{"index": 0, "status": "created",
                                                   "id": 4, "result": {"read": "project-files/4/"}}]}
    gx, s = _client(FakeResponse(200, _start(blocks)), FakeResponse(201, {}),
                    FakeResponse(201, {}), FakeResponse(200, commit))
    result = gx.upload_project_file(f, project=12, category="3D")
    start_body = s.calls[0][2]["json"]
    assert start_body["intent"] == "upload_start"
    assert start_body["records"] == [{"project": 12, "file_name": "pit.glb", "size": 14,
                                      "category": "3D"}]
    puts = [c for c in s.calls if c[0] == "PUT"]
    assert [p[2]["data"] for p in puts] == [b"glTF0123", b"456789"]
    assert puts[0][1].endswith("blockid=YQ%3D%3D&sig=x")
    assert all("headers" not in p[2] for p in puts), "no key goes to the storage host"
    assert s.calls[-1][2]["json"] == {"model": "ProjectFile", "intent": "upload_commit",
                                      "records": [{"slot": "S"}]}
    assert result.write_id == "w9"


def test_an_unreachable_storage_host_aborts_and_says_what_to_do(tmp_path):
    f = tmp_path / "grid.tif"
    f.write_bytes(b"II*\x00rest")
    blocks = [{"id": "YQ==", "offset": 0, "length": 8}]
    abort = {"model": "ProjectFile", "intent": "upload_abort", "summary": {"aborted": 1},
             "rows": [{"index": 0, "status": "aborted"}]}
    gx, s = _client(FakeResponse(200, _start(blocks)),
                    requests.ConnectionError("blocked by proxy"), FakeResponse(200, abort))
    with pytest.raises(StorageUnreachable) as err:
        gx.upload_project_file(f, project=12, category="MG")
    assert s.calls[-1][2]["json"]["intent"] == "upload_abort"
    message = str(err.value)
    assert "acct.blob.core.windows.net" in message
    assert "local agent" in message and "web app" in message and "All domains" in message


def test_a_start_the_server_refused_is_returned_not_uploaded(tmp_path):
    f = tmp_path / "x.exe"
    f.write_bytes(b"MZ")
    refused = {"model": "ProjectFile", "intent": "upload_start", "write_id": None,
               "summary": {"refused": 1}, "rows": [{"index": 0, "status": "refused",
                                                    "reason_code": "file_type_not_allowed"}]}
    gx, s = _client(FakeResponse(200, refused))
    result = gx.upload_project_file(f, project=12)
    assert result.refused[0]["reason_code"] == "file_type_not_allowed"
    assert len(s.calls) == 1
