# geodb-client

The Python client for the **[geoDB Open Exploration Protocol](https://github.com/geodbio/geodb-protocol)** —
pull a mining-exploration project's drill, assay, geophysics, raster, and document
data as pandas DataFrames and files, over one authenticated, project-scoped API.

```bash
pip install geodb-client            # + geopandas extra:  pip install "geodb-client[geo]"
```

> **Integrating for the first time?** Read
> [`AGENTS.md`](https://github.com/geodbio/geodb-protocol/blob/main/AGENTS.md)
> in the protocol repo first. It names the handful of things in this domain
> that give you silently wrong answers — starting with the coordinate rule
> below — and carries the sync loop, the error codes and runnable examples in
> four languages.

## 20 lines to DataFrames

```python
import geodb

gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")

# Records lane — relational data straight to pandas
collars       = gx.collars().to_dataframe()
samples       = gx.samples().to_dataframe()
assays        = gx.assays().to_dataframe()
point_samples = gx.point_samples().to_dataframe()
surveys       = gx.surveys().to_dataframe()          # geophysical surveys + footprints

# Assets lane — walk the STAC catalog, download a Cloud-Optimized GeoTIFF
for item in gx.stac().items("rasters"):
    cog = item.asset("cog")
    if cog:
        cog.download("grid.tif")                     # short-SAS redirect, streamed
        break

# Bulk lane — GeoParquet export (zero-data-loss geometry + native coordinates)
job = gx.export("drill_samples", format="geoparquet")
job.wait().download("samples.parquet")
```

That's it — a vendor integrates a customer's project in an afternoon.

## Auth

A project owner mints a **project-pinned, read-only, revocable access grant** in
geoDB (Project Settings → API Access Grants) and shares the token. The client
sends it as `Authorization: Grant <token>`. Everything is scoped to that one
project; every pull is logged for the owner to see. First-party callers can use a
Knox token instead:

```python
gx = geodb.Client(token="<knox>", base_url="https://api.geodb.io", auth_scheme="Token")
```

Behind the `api.` subdomain rewrite, pass `api_prefix="/v2"`.

## API

| Call | Returns |
|---|---|
| **Records lane** — see the table list below | a `Paginated` iterator — iterate rows, or `.to_dataframe()`, or `.count()`. Accepts filter kwargs (e.g. `gx.surveys(method="magnetics")`, `gx.collars(modified_since="2024-01-01")`). |
| `gx.stac()` | a `StacCatalog`: `.landing()`, `.collections()`, `.items(collection_id)` (yields `StacItem`), `.item(cid, iid)`. `StacItem.asset(key).download(path)`. |
| `gx.export(model, format="geoparquet", include_assays=True)` | an `ExportJob`: `.wait()` then `.download(path)`. Formats: `geoparquet`, `parquet`, `csv`. Models: the `export()` column below. |
| `gx.project()` | the grant/project context. |
| `gx.describe` · `gx.write` · `gx.validate` · `gx.retract` · `gx.restore` · `gx.undo` · `gx.writes` | the write half — see **Writing** below. |

### Tables

The drilling tables are the same ones geoDB serves to Leapfrog and Vulcan over
ODBC — one definition of a project's drilling data, whichever door you come
through.

| Records lane | `export()` model | What it is |
|---|---|---|
| `gx.collars()` | `drill_collars` | Collar location, orientation, total depth |
| `gx.drill_surveys()` | `drill_surveys` | Downhole survey stations (depth, azimuth, dip) |
| `gx.lithology()` | `drill_lithology` | Downhole lithology intervals |
| `gx.alteration()` | `drill_alteration` | Downhole alteration intervals |
| `gx.samples()` | `drill_samples` | Drill samples (+ merged assays — see below) |
| `gx.structures()` | `drill_structure_point`, `drill_structure_zone` | Structural measurements; point (a depth) vs zone (an interval) |
| `gx.mineralization()` | `drill_mineralization` | Mineralization intervals + mineral percentages |
| `gx.veins()` | `drill_veins` | Vein intervals (type, width, mineral contents) |
| `gx.rqd()` | `drill_rqd` | Geotech: core recovery + rock mass |
| — | `drill_rqd_core_recovery`, `drill_rqd_rock_mass` | The two halves, **only on projects that split their geotech tabs** |
| `gx.spectral()` | `drill_spectral` | Spectral intervals (geounit abundances) |
| `gx.custom_intervals()` | `drill_custom_intervals` | User-defined intervals (TYPE IS DATA) |
| `gx.point_samples()` | `point_samples` | Surface / soil / rock-chip samples |
| `gx.qc_samples()` | `qc_samples` | QA/QC (standards, blanks, duplicates) |
| `gx.assays()` | — | Assay results (merged into the sample exports) |
| `gx.surveys()` | `geophysical_surveys` | Geophysical surveys (metadata + WGS84 footprint) |

Asking to `export()` a model your project does not expose returns HTTP 400 with
the list it does — e.g. the two `drill_rqd_*` halves appear only when the
project has the geotech tab split enabled, exactly as over ODBC.

## What an export inherits

An `export()` is not a raw table dump. It comes back the way the project is
configured, the same as the project's own CSV/XLSX exports and its ODBC feed:

- **Assay merge settings** — the sample exports carry merged assay columns per
  the project's merge strategy and unit conversions (`include_assays=True`).
- **Coordinate reference system** — coordinates honour the project's configured
  output CRS (`odbc_output_crs`), not a forced WGS84.
- **Column configuration** — visibility, ordering and display-name overrides.
- **Custom fields** — as `cf_*` columns.
- **Depth units** — metres or feet, per the project.

## Coordinate contract

GeoParquet exports carry a **WGS84 geometry** column plus the **native/original**
coordinate columns (`latitude`/`longitude`/`epsg`) — zero-data-loss travels with
the data, so you never lose the surveyor's original CRS.

⚠️ On a UTM-imported project the `latitude`/`longitude` columns hold
**northing/easting** in that project's CRS, not degrees — that is what "native"
means, and `epsg` tells you how to read them. The `geometry` column is always
WGS84. Read geometry for maps; read the natives when you need exactly what the
surveyor recorded.

The same rule applies to every record the API returns, not only exports. Each
row also carries `source_coordinate` (`{x, y, epsg}` — the same values under
names that cannot be mistaken for degrees) and `geometry_geojson` (WGS84 as a
parsed GeoJSON `Point`). Prefer those two: decide by `epsg`, never by the field
name.

## Writing (a key that may write records)

> geoDB's write half is not yet served by its production servers; these calls
> answer once it is. `gx.project()` says whether your key may write
> (`read_only: false`).

Every write goes through ONE endpoint, `POST /api/v2/records/`, and is
answered **per row**: a bad row comes back with its own `reason_code` and
`remedy` while the rest of the batch lands. Only a refusal of the whole
request raises `geodb.WriteRefused` (with `.reason_code` and `.remedy`).

```python
gx.describe("DrillSample")                    # fields, identity, sets, coordinate rule

rows = [{"bhid": "DH-1", "name": "DH-1-001", "depth_from": 0, "depth_to": 2}]   # or a DataFrame
check = gx.validate("DrillSample", rows, logging_set="pXRF 2026")   # dry run: nothing written
result = gx.write("DrillSample", rows, logging_set={"name": "pXRF 2026", "create": True},
                  idempotency_key="my-batch-0001")                    # a retry replays
result.summary, result.refused                # per-row outcomes; act on each remedy

gx.write("DrillSample", [{"id": 123, "notes": "re-logged"}], intent="update")
gx.retract("DrillSample", [{"id": 124}], dry_run=True)    # show the user what goes
gx.retract("DrillSample", [{"id": 124}], confirm=True)    # only after their yes
gx.undo(result.write_id)                       # reverse any write; .complete says if all of it
gx.restore(retract_write_id)                   # bring a retract back
for w in gx.writes(undone=False): ...          # what this key wrote, newest first
```

| Call | Does |
|---|---|
| `gx.describe(model)` | the live write contract for `model` |
| `gx.write(model, rows, intent="create", logging_set=, idempotency_key=, dry_run=, acknowledge=)` | `create` never overwrites (a differing record is skipped, both values named) · `upsert` · `update` (existing records by identity or `id`; never creates). Returns a `WriteResult` |
| `gx.validate(model, rows, …)` | the same call with `dry_run=True` |
| `gx.retract(model, rows, confirm=False, dry_run=False)` | to the Trash with everything that belongs to them; needs `confirm=True` |
| `gx.restore(write_id)` / `gx.restore(audit_batch_id=…)` | brings a removed batch back |
| `gx.undo(write_id, dry_run=False)` | reverses one write; rows changed since are left and named (`undo_stale`) |
| `gx.writes(write_id=None, model=, intent=, undone=)` | the key's write log (or one write, with its rows) |

`logging_set=` is the body's `"set"`, for every set-aware family (sample sets
too). `undo()` and `restore()` take `idempotency_key=` like `write()`.
`result.raise_for_refusals()` raises `geodb.RowsRefused` (status 200, `.rows`)
when rows were refused. `make_default_set` and `qaqc_verdict` have no methods
on purpose — each is a person's own act on their explicit request (a vendor key
is refused); send them through `gx.write(...)` with `dry_run=True`, then with
`confirm=` after the user's yes (e.g. `gx.write(model, [],
intent="make_default_set", logging_set="<set>", dry_run=True)`). `qc_reconnect`
(model `"QCSample"`) goes through `gx.write(...)` too.

The rules the server enforces (and an AI writing for a person must respect):
coordinates carry their own `epsg` (never pre-convert; a row without it is
refused `missing_crs`) · interval and sample rows name their set — when the
user has not said which, **ask** (`set_required` lists the choices) · a vendor
key writes only into sets it created or was given (`set_not_owned`) · update,
upsert, retract and restore change what exists: dry-run, show the user, send
after their yes. Hard delete never crosses the API.

## Development

```bash
pip install -e ".[dev,geo]"
pytest                     # unit tests (mocked transport)
GEODB_TEST_BASE_URL=http://localhost:8001 GEODB_TEST_TOKEN=gdbg_... pytest tests/test_integration.py
```

Apache-2.0. Version 0.1.0.
