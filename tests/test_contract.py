"""The client against the PUBLISHED contract (plan v0.2 §3.7): every path it
reads, every parameter it sends and every reason code it types must be in the
protocol repository's spec / errors.json — so the client cannot drift from
the protocol silently again.

The contract is read from a checkout of github.com/geodbio/geodb-protocol:
``$GEODB_PROTOCOL_DIR``, else a sibling ``../geodb-protocol``. Skipped (loudly)
when neither exists.
"""

import json
import os
import pathlib

import pytest

yaml = pytest.importorskip("yaml")

from geodb import client as client_module  # noqa: E402
from geodb.errors import _BY_CODE  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROTOCOL = pathlib.Path(os.environ.get("GEODB_PROTOCOL_DIR") or ROOT.parent / "geodb-protocol")
SPEC = PROTOCOL / "spec" / "openapi.yaml"

pytestmark = pytest.mark.skipif(not SPEC.exists(),
                                reason=f"no geodb-protocol checkout at {PROTOCOL}")

#: Reads beyond the table registry that the client offers.
EXTRA_READS = {"/assay-results/": False, "/methods/": False, "/laboratories/": False}


@pytest.fixture(scope="module")
def spec():
    return yaml.safe_load(SPEC.read_text(encoding="utf-8"))


def _list_params(spec, path):
    op = spec["paths"].get(f"/api/v2{path}", {}).get("get")
    assert op is not None, f"the spec has no GET /api/v2{path}"
    return {p.get("name") for p in op.get("parameters", [])}


def test_every_read_is_a_published_list_taking_the_scope_parameters(spec):
    reads = {path: sets for _n, path, _e, sets, _w in client_module.TABLES}
    reads.update(EXTRA_READS)
    for path, sets in reads.items():
        params = _list_params(spec, path)
        assert {"project", "company"} <= params, (path, sorted(params))
        if path not in ("/methods/", "/laboratories/"):
            assert "scope" in params, path
        assert ("set" in params) == sets, (path, "set declared" if "set" in params else "no set")


def test_every_export_model_and_parameter_is_published(spec):
    schemas = spec["components"]["schemas"]
    # drf-spectacular names a request component "<serializer>Request".
    request = (schemas.get("ExportCreateRequestRequest")
               or schemas["ExportCreateRequest"])["properties"]
    model = request["model"]
    ref = model.get("$ref") or next((b["$ref"] for b in model.get("allOf", []) if "$ref" in b),
                                    None)
    if ref:
        model = schemas[ref.rsplit("/", 1)[-1]]
    models = set(model.get("enum") or [])
    for _n, _p, exports, _s, _w in client_module.TABLES:
        assert set(exports) <= models, (exports, sorted(models))
    assert {"project", "company", "scope", "set"} <= set(request)


def test_every_typed_reason_code_is_registered():
    codes = set(json.loads((PROTOCOL / "errors.json").read_text())["reason_codes"])
    assert set(_BY_CODE) <= codes, sorted(set(_BY_CODE) - codes)


def test_the_readme_table_is_generated_from_the_registry():
    import importlib.util
    spec_ = importlib.util.spec_from_file_location("gen_readme", ROOT / "scripts" / "gen_readme.py")
    gen = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(gen)
    assert gen.stale() == "", "README.md is stale: run python scripts/gen_readme.py"
