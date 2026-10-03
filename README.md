# geodb-client

The Python client for the **[geoDB Open Exploration Protocol](https://github.com/geodbio/geodb-protocol)** —
pull a mining-exploration project's drill, assay, geophysics, raster, and document
data as pandas DataFrames and files, over one authenticated, project-scoped API.

```bash
pip install "geodb-client>=0.2,<0.3"     # + geopandas extra: pip install "geodb-client[geo]>=0.2,<0.3"
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
gx.project()                                         # the companies + projects it reads
P = 12                                               # name the project on every read

# Records lane — relational data straight to pandas
collars       = gx.collars(project=P).to_dataframe()
samples       = gx.samples(project=P).to_dataframe()
results       = gx.assay_results(project=P).to_dataframe()   # flat: sample × element × method
methods       = gx.methods(project=P).to_dataframe()         # join on method_id
lithology     = gx.lithology(project=P, set="all").to_dataframe()  # or set=<id or name>
surveys       = gx.surveys(project=P).to_dataframe() # geophysical surveys + footprints

# Assets lane — walk the STAC catalog, download a Cloud-Optimized GeoTIFF
for item in gx.stac().items("rasters"):
    cog = item.asset("cog")
    if cog:
        cog.download("grid.tif")                     # short-SAS redirect, streamed
        break

# Bulk lane — a WHOLE table is one file: the fastest way in (seconds, not pages)
gx.export("assay_results", project=P).wait().download("values.parquet")  # every value, every flag
job = gx.export("drill_samples", format="geoparquet", project=P)          # merged per the project
job.wait().download("samples.parquet")
```

Whole tables → `export()`. Filtered or modest reads → the list methods: once a
list's first page states its total, the rest are fetched four pages at a time
(`geodb.Client(..., parallel=1)` reads one at a time; `skip_count=True` asks the
server not to count and follows `next`).

That's it — a vendor integrates a customer's project in an afternoon.

## Auth

A project owner mints a **revocable access grant** in geoDB (Project Settings →
API Access Grants) and shares the token; it reads the project(s) it was given,
and every pull is logged for the owner to see. The client sends it as
`Authorization: Grant <token>`.

**geoDB keeps no current project.** When the key reads several projects, name
one on every read (`project=<id or exact name>`); otherwise the server answers
`geodb.ProjectRequired`, whose `.choices` lists the projects by company.
Company-level tables (`methods`, `laboratories`, standards, QC types) take
`company=`; `scope="company", company=<id>` reads every project of one company.
An interval table over a project with several sets raises
`geodb.SetChoiceRequired` (`.sets` lists them) until you pass `set=<id or name>`
or `set="all"`.

Every refusal is a typed exception carrying the server's `reason_code` and
`remedy`: `ProjectRequired`, `CompanyRequired`, `SetChoiceRequired`
(`InvalidRequest` for any other 400), `AuthError` (401) / `PermissionDenied`
(403), `NotFoundError`, `Conflict`, `Throttled` (`.retry_after`). Match on
`reason_code`; act on `remedy`.

**Client and server versions are paired.** geodb-client is versioned in step
with the protocol: 0.2.x speaks protocol 0.2. It sends the version it speaks
(`X-GeoDB-Protocol-Version`) and checks the server's on the first response; a
different major.minor raises `geodb.ProtocolVersionMismatch`, whose `.install`
is the line that fetches the matching client (e.g.
`pip install "geodb-client>=0.3,<0.4"`). Install with the range the server
gives you — `pip install "geodb-client>=0.2,<0.3"` for protocol 0.2 — never
unpinned. (`check_protocol=False` turns the check off.)

First-party callers can use a Knox token instead:

```python
gx = geodb.Client(token="<knox>", base_url="https://api.geodb.io", auth_scheme="Token")
```

Behind the `api.` subdomain rewrite, pass `api_prefix="/v2"`.

## API

| Call | Returns |
|---|---|
| **Records lane** — see the table list below | a `Paginated` iterator — iterate rows, or `.to_dataframe()`, or `.count()` (the list's own count; no extra request once read). Takes `project=`, `company=`, `scope=` (+ `set=` on interval tables) and filter kwargs (e.g. `gx.surveys(project=P, method="magnetics")`). |
| `gx.assay_results(project=P)` · `gx.methods(company=C)` · `gx.laboratories(company=C)` | the flat assay table and the lookup tables it names by id. |
| `gx.stac()` | a `StacCatalog`: `.landing()`, `.collections()`, `.items(collection_id)` (yields `StacItem`), `.item(cid, iid)`. `StacItem.asset(key).download(path)`. |
| `gx.export(model, format="geoparquet", include_assays=True, project=…, set=…)` | an `ExportJob` of ONE project's table: `.wait()` then `.download(path)`. Formats: `geoparquet`, `parquet`, `csv`. Models: the `export()` column below. |
| `gx.project()` | the credential's context: its companies and projects (with set counts), whether it may write, its limits. |
| `gx.describe` · `gx.write` · `gx.validate` · `gx.retract` · `gx.restore` · `gx.undo` · `gx.writes` | the write half — see **Writing** below. |

### Tables

The drilling tables are the same ones geoDB serves to Leapfrog and Vulcan over
ODBC — one definition of a project's drilling data, whichever door you come
through.

<!-- BEGIN:tables (generated by scripts/gen_readme.py from geodb.client.TABLES) -->
| Records lane | `export()` model | Sets | What it is |
|---|---|---|---|
| `gx.collars(project=…)` | `drill_collars` | — | Collar location, orientation, total depth (`/drill-collars/`) |
| `gx.drill_surveys(project=…)` | `drill_surveys` | — | Downhole survey stations (depth, azimuth, dip) (`/drill-surveys/`) |
| `gx.lithology(project=…)` | `drill_lithology` | `set=` | Downhole lithology intervals (`/drill-lithologies/`) |
| `gx.alteration(project=…)` | `drill_alteration` | `set=` | Downhole alteration intervals (`/drill-alterations/`) |
| `gx.samples(project=…)` | `drill_samples` | `set=` | Drill samples (the assay by id; expand="assay" for the record) (`/drill-samples/`) |
| `gx.structures(project=…)` | `drill_structure_point`, `drill_structure_zone` | — | Structural measurements; point (a depth) vs zone (an interval) (`/drill-structures/`) |
| `gx.mineralization(project=…)` | `drill_mineralization` | `set=` | Mineralization intervals + mineral percentages (`/drill-mineralizations/`) |
| `gx.veins(project=…)` | `drill_veins` | `set=` | Vein intervals (type, width, mineral contents) (`/drill-veins/`) |
| `gx.rqd(project=…)` | `drill_rqd` | `set=` | Geotech: core recovery + rock mass (`/drill-rqds/`) |
| `gx.spectral(project=…)` | `drill_spectral` | `set=` | Spectral intervals (geounit abundances) (`/drill-spectral-intervals/`) |
| `gx.custom_intervals(project=…)` | `drill_custom_intervals` | `set=` | User-defined intervals (TYPE IS DATA) (`/drill-custom-intervals/`) |
| `gx.point_samples(project=…)` | `point_samples` | — | Surface / soil / rock-chip samples (`/point-samples/`) |
| `gx.qc_samples(project=…)` | `qc_samples` | — | QA/QC (standards, blanks, duplicates) (`/qc-samples/`) |
| `gx.assays(project=…)` | `assay_results` | — | Assay results (flat table: assay_results(); every value: export("assay_results")) (`/assays/`) |
| `gx.surveys(project=…)` | `geophysical_surveys` | — | GEOPHYSICAL surveys (metadata + WGS84 footprint) — downhole surveys are drill_surveys() (`/geophysical-surveys/`) |
<!-- END:tables -->

Asking to `export()` a model your project does not expose returns HTTP 400 with
the list it does — e.g. the two `drill_rqd_*` halves appear only when the
project has the geotech tab split enabled, exactly as over ODBC.

## What an export inherits

An `export()` is not a raw table dump. It comes back the way the project is
configured, the same as the project's own CSV/XLSX exports and its ODBC feed:

- **Assay merge settings** — the sample exports carry merged assay columns per
  the project's merge strategy and unit conversions (`include_assays=True`):
  one value per element, below-detection results substituted and over-range
  results at their limit, NOT flagged (the file's `geodb` metadata says so). For
  statistics, detection limits or flags, export `assay_results`: one row per
  sample × element × method with `below_detection`, `above_det_limit`,
  `detection_limit`, `upper_limit`, the withheld flags and the method /
  certificate / laboratory names.
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

Apache-2.0. Version 0.2.1 (protocol 0.2).
