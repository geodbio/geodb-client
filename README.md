# geodb-client

The Python client for the **[geoDB Open Exploration Protocol](https://github.com/geodbio/geodb-protocol)** —
read a mining-exploration project's drill, assay, QA/QC, geophysics, raster, and
document data as pandas DataFrames and files, and write changes back through one gated
endpoint (dry run first, per-row answers, undo), over one authenticated, project-scoped API.

```bash
pip install "geodb-client>=0.3,<0.4"     # + geopandas extra: pip install "geodb-client[geo]>=0.3,<0.4"
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

# Bulk lane — a WHOLE table for analysis is one file (never a way to check an import)
gx.export("assay_results", project=P).wait().download("values.parquet")  # every value, every flag
job = gx.export("drill_samples", format="geoparquet", project=P)          # merged per the project
job.wait(on_progress=lambda j: print(j.rows_done, "/", j.rows_total)).download("samples.parquet")
```

Whole tables for analysis → `export()`; to check what an import will do, dry-run
the write (`validate`), never export. Plan with `gx.record_counts(project=P)`, then read
only the records you need: `name=` / `bhid=` take one value or a list
(`gx.samples(project=P, set="all", name=["DH-1-001", "DH-1-002"])`, sent comma-joined), and
the reference lists (`methods`, `laboratories`, `collars`) take `include_trashed=True` (a key
acting as a person: rows in the Trash come back marked `in_trash: true`; a vendor key is
refused `trash_needs_person_key`). Filtered or modest reads → the list methods: once a
list's first page states its total, the rest are fetched four pages at a time
(`geodb.Client(..., parallel=1)` reads one at a time; `skip_count=True` asks the
server not to count and follows `next`).

That's it — a vendor integrates a customer's project in an afternoon.

## Auth

A project owner mints a **revocable access grant** in geoDB (Project Settings →
API Access Grants) and shares the token; it reads the project(s) it was given
(and writes, if the owner allowed it to), and every call is logged for the owner to see. The client sends it as
`Authorization: Grant <token>`.

An AI connected to geoDB (its connector at `https://api.geodb.io/mcp`) gets a
1-hour **session key** (`gdbs_…`) from its `session_key` tool for code in its
own sandbox; pass it as `token=` the same way. On a 401 `grant_expired`, ask
for a new one.

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
(403), `NotFoundError`, `Conflict`, `Throttled` (`.retry_after`;
`ExportConcurrency` for `export_concurrency`). Match on
`reason_code`; act on `remedy`.

**Client and server versions are paired.** geodb-client is versioned in step
with the protocol: 0.3.x speaks protocol 0.3 (0.2.x spoke 0.2). It sends the version it speaks
(`X-GeoDB-Protocol-Version`) and checks the server's on the first response; a
different major.minor raises `geodb.ProtocolVersionMismatch`, whose `.install`
is the line that fetches the matching client (e.g.
`pip install "geodb-client>=0.4,<0.5"`). Install with the range the server
gives you — `pip install "geodb-client>=0.3,<0.4"` for protocol 0.3 — never
unpinned. (`check_protocol=False` turns the check off.)

First-party callers can use a Knox token instead:

```python
gx = geodb.Client(token="<knox>", base_url="https://api.geodb.io", auth_scheme="Token")
```

Behind the `api.` subdomain rewrite, pass `api_prefix="/v2"`.

**TLS behind antivirus or a corporate proxy (0.3.5).** The client verifies HTTPS
against the operating system's certificate store (`truststore`, Python 3.10+), so
HTTPS-inspecting antivirus and corporate proxies whose root the OS trusts work
without `CERTIFICATE_VERIFY_FAILED`. Only the client's own session is changed.
Without truststore it uses certifi after one warning; `geodb.Client(...,
os_trust_store=False)` opts out, and a `session=` you pass is used as it is
(`gx.tls_trust` says `"os"`, `"certifi"` or `"caller"`).

## API

| Call | Returns |
|---|---|
| **Records lane** — see the table list below | a `Paginated` iterator — iterate rows, or `.to_dataframe()`, or `.count()` (the list's own count; no extra request once read). Takes `project=`, `company=`, `scope=` (+ `set=` on interval tables) and filter kwargs (e.g. `gx.surveys(project=P, method="magnetics")`). |
| `gx.assay_results(project=P)` · `gx.methods(company=C)` · `gx.laboratories(company=C)` | the flat assay table and the lookup tables it names by id. |
| `gx.stac()` | a `StacCatalog`: `.landing()`, `.collections()`, `.items(collection_id)` (yields `StacItem`), `.item(cid, iid)`. `StacItem.asset(key).download(path)`. |
| `gx.export(model, format="geoparquet", include_assays=True, project=…, set=…)` | an `ExportJob` of ONE project's whole table, for analysis: `.wait()` then `.download(path)`. Formats: `geoparquet`, `parquet`, `csv`. Models: the `export()` column below. See **Waiting for an export**. |
| `gx.project()` | the credential's context: its companies and projects (with set counts), whether it may write, its limits. |
| `gx.record_counts(project=P)` | how many records of each type the project holds (`counts: {type: {live, in_trash}}`, `company_wide`, `not_readable`) — one small read to plan with before reading any table (0.3.5). |
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

### Waiting for an export (0.3.5)

```python
job = gx.export("assay_results", project=P)
job.wait(on_progress=lambda j: print(j.state, j.progress_pct, j.rows_done, j.rows_total))
job.download("values.parquet")
```

- **It polls when the server says.** Each "still building" answer (202) carries
  `Retry-After`; the client waits that long (10 s when absent) — every poll counts
  against the key's hourly rate. A throttled poll (429) waits its `Retry-After` too.
- **Progress** — `state`, `progress_pct`, `rows_done`, `rows_total`,
  `progress_message` — is on the job after every poll and handed to `on_progress`.
- **It waits as long as the build moves.** `stall_timeout` (900 s) runs out only
  when nothing changed for that long; `timeout` (3 h) is the hard cap.
- **A full slot is not resent.** A key holds a few unfinished exports at most; on
  429 `export_concurrency` the client waits, polls the exports IT started and creates
  again once one has finished (`wait_for_slot=True`, `slot_timeout=1800`). Exports a
  different session started cannot be polled from here: `geodb.ExportConcurrency`
  (`.in_flight`, `.limit`, `.retry_after`).
- **An interrupted build is created again once, out loud.** When the server says
  the machine building it stopped ("create the export again"), `wait()` sends the
  same request again ONCE, with a `RuntimeWarning` (`job.restarts`), then raises
  `geodb.ExportInterrupted` (`.error_id` to quote). `restart_interrupted=0` raises
  at once.

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

> Writing is live on geoDB's production servers and, for now, open to the AI
> connections of geoDB staff and of members of companies in the geoDB protocol
> beta (to join, contact geoDB: https://geodb.io/contact-us/). Any other key may
> read; a write answers 403 `writes_staff_only`. `gx.project()` says whether
> your key may write (`read_only: false`).

Every write goes through ONE endpoint, `POST /api/v2/records/`, and is
answered **per row**: a bad row comes back with its own `reason_code` and
`remedy` while the rest of the batch lands. Only a refusal of the whole
request raises `geodb.WriteRefused` (with `.reason_code` and `.remedy`); a 403 is
`geodb.WriteForbidden` (also a `geodb.PermissionDenied`), never worth re-sending.

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
| `gx.write(model, rows, intent="create", logging_set=, project=, idempotency_key=, dry_run=, acknowledge=, confirm=, layer=)` | `create` never overwrites (a differing record is skipped, both values named) · `upsert` · `update` (existing records by identity or `id`; never creates) · every other intent (below). Returns a `WriteResult` |
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

**Every intent and face goes through `gx.write`** (its docstring has the
full list): `make_export_set` · `qaqc_verdict` · `qc_reconnect` · projects
(`model="Project"`: `create` — ask the user which company — `update`,
`set_coordinate_system`, `set_state`, `retract`, `restore`, each sent with the
`confirm` its dry run returned) · reports (`Report` create / update / publish,
`ReportSection` create / update / retract, `ReportFigure` create) · settings
(`CustomFieldSchema`, `ColumnConfiguration`, `AssayMergeSettings`,
`AssayRangeConfiguration`, `MetalEquivalentConfig`: create / update / retract /
restore; `QAQCProtocol`: create / update; `QCConfiguration`, `ODBCSettings`:
update — a change that moves QC verdicts sends the `confirm` its dry run
returned) · a map layer:

```python
features = [{"geometry": "LINESTRING (512300 4925100, 512800 4925600)", "epsg": 32611,
             "fault": "F1"}]
gx.write("VectorLayer", features, layer={"name": "Mapped faults", "kind": "geology_fault"},
         project=12, dry_run=True)    # one write lands ONE new (draft) layer
```

The rules the server enforces (and an AI writing for a person must respect):
coordinates carry their own `epsg` (never pre-convert; a row without it is
refused `missing_crs`) · interval and sample rows name their set — when the
user has not said which, **ask** (`set_required` lists the choices) · a vendor
key writes only into sets it created or was given (`set_not_owned`) · update,
upsert, retract and restore change what exists: dry-run, show the user, send
after their yes. Hard delete never crosses the API.

## Uploading files (0.3.3; a key that acts as a person)

The user's own connected AI may put FILES into the project — through the same upload doors the web
and mobile apps use (the file is scanned before it is stored), with the user's own permissions:

```python
gx = geodb.Client(token=key)          # a session key from the connector, or an agent key

r = gx.upload_document("Q3_summary.pdf", project=12, category="RP", title="Q3 summary")
r.rows[0]["result"]["read"]            # 'documents/345/' — read it back with gx._get(...)
gx.upload_document("lab_certificate.pdf", project=12,       # the lab's file on its
                   attach_to={"model": "Certificate", "id": 412})   # certificate (category CT)
gx.upload_photo("outcrop.jpg", project=12, category="GN", description="north face")

box = gx.write("DrillPhoto", [{"bhid": "DH-7", "depth_from": 10, "depth_to": 13, "box_no": 3}])
gx.attach_drill_box_image(box.rows[0]["id"], "DH-7_box3.jpg", project=12)

gx.upload_project_file("pit_shell.glb", project=12, category="3D")   # rasters, DEMs, meshes …
gx.undo(r.write_id)                    # every upload is undoable (it goes to the Trash)
```

- **Documents and photos** (at most 95 MiB each) are multipart POSTs to the API host — they work
  from any sandbox that can reach `api.geodb.io`. The extension must match the bytes (a WebP named
  `.jpg` is refused `file_type_mismatch`).
- **Project files** (up to 5 GB) go in blocks **straight to storage**: `upload_start` returns
  block URLs on the storage host, the client PUTs the bytes there (no key goes with them) and
  `upload_commit` makes the record. A cloud sandbox limited to the API host cannot reach the
  storage host: the client aborts and raises `StorageUnreachable` naming the fixes — run it from a
  local agent (Claude Code / Cowork on your machine), allow the storage host (or "All domains") in
  the sandbox's network settings, or upload in the geoDB web app.
- **A certificate's own file (0.3.5):** `upload_document(..., attach_to={"model": "Certificate",
  "id": <id>})` links the document to that certificate (same project) in the same request; the
  category may be left out (the server gives it CT). Photos attach to a drill-box record the same
  way (`attach_drill_box_image`).
- A vendor or organisation key cannot upload (`person_key_required`).
- **HTTP 200 does not mean it landed:** read `r.rows[0]["status"]` (or call
  `r.raise_for_refusals()`), as for any write.
- **Safe to retry (0.3.4):** each document / photo upload carries an `Idempotency-Key` (fresh per
  call, or pass `idempotency_key=`). A dropped connection or a timeout at the network edge (a large
  file can time out AFTER it landed) is re-sent under the same key, and the server answers with
  the first upload's result instead of storing the file twice.

## Writing a dataset (0.3.5; a key that acts as a person)

> ⚠ The server's dataset endpoints are being built: until geoDB announces them, a dataset request
> is refused. The shapes below are the server design's.

When the data has several record types that depend on one another — a new laboratory, methods
with their detection limits, certificates, QC samples, and the assays that hang off them — send
them as ONE dataset instead of many writes. The server rehearses every member together, in load
order (a parent in one member EXISTS when its children are checked), against the data as it is,
and answers what the write would do, sized for a decision.

```python
plan = gx.validate_dataset(12, {
    "Laboratory": labs_df, "Method": methods_df,          # columns = describe(model)'s fields
    "Certificate": certs_df, "Assay": assays_df,          # one long-form row per value
}, intent="create")                                       # or "upsert"; nothing is written
print(plan.summary())               # show the user: counts, exception + warning codes, remedies
plan.before_writing, plan.warnings  # read these, not just the counts
plan.download_detail("detail.parquet")      # one row per exception / warning, if you need it

done = plan.write()                 # ONLY after the user's yes: the plan, verbatim
done.status                         # "done" | "stopped"
done.landed, done.not_landed, done.stopped  # what landed, what did not, and why
done.undo()                         # ONE request takes back everything it landed
again = done.rehearse_rest()        # after a stop: rehearse what is left (same files)
```

- **The dry run is the authority.** Its answer is the write's own outcome, row for row: never
  hand-match the files against data you pulled to predict it. Read `before_writing` and every
  warning, not just the counts, and get the user's yes before `plan.write()`.
- **Members** are DataFrames (written as UTF-8 CSV: the header is exactly the columns, a missing
  value is an empty cell, an id stays the characters it was), CSV paths, or
  `{"data" | "path" | "document", "intent"?, "set"?}` (`document`: an upload made earlier, by id).
  Each is uploaded as a document (category OT); an upload no completed write uses goes to the
  Trash after 7 days.
- **A large dataset runs in the background**: the rehearsal (and the write, and a large undo) is
  polled the way exports are — the server's `Retry-After`, `on_progress(job)` with `job.kind`
  (rehearsal · write · undo), `job.member` of `job.members`, `job.model`, `job.rows_done`,
  `job.rows_total` and `job.waiting_for` (set while it waits its turn behind another dataset job on
  the project — not a stall; only the hard cap runs), a stall timeout.
- **The write lands member by member**, each in its own commit and each re-checked against what
  the user approved. A member that would land differently STOPS the write: the answer names what
  landed and what did not (a `DatasetWrite`, not an exception; `raise_for_stop()` raises it). The
  plan is good for 24 h (`plan.expires_at`); after that re-validate.
- **One Undo** covers the whole dataset, including what a `rehearse_rest()` write adds to it.

The dataset reason codes, and the two records-door codes a dataset job (or an undo) can answer,
each its own exception (all subclasses of `geodb.WriteRefused`):

<!-- BEGIN:datasets (generated by scripts/gen_readme.py from geodb.errors.DATASET_REASON_CODES) -->
| Reason code | Exception | Means | The client |
|---|---|---|---|
| `dataset_file_unreadable` | `geodb.DatasetFileUnreadable` | a member is not a UTF-8 CSV with one header row (`offending.reason`: not_csv · not_utf8 · no_header · header · malformed; `offending.line`) | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_column_forbidden` | `geodb.DatasetColumnForbidden` | a member file carries a column only the request may carry (project, dry_run, acknowledge, confirm, qc_mapping, plan_token, intent, model) — consent never comes from a file | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_too_large` | `geodb.DatasetTooLarge` | past a limit (`offending.limit` names it, `offending.max` gives it) | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_not_grouped` | `geodb.DatasetNotGrouped` | an Assay member's certificates (or another long-form member's records) are split; sort the frame and validate again | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_rows_busy` | `geodb.DatasetRowsBusy` | another write held rows the dataset changes; the server stopped after a few seconds rather than make that person wait | sends again after `Retry-After` (bounded, warns) — a refused request and a write that stopped on it alike |
| `dataset_plan_invalid` | `geodb.DatasetPlanInvalid` | the write names no plan this key approved (send the rehearsal's `write` body exactly as returned — `DatasetPlan.write()` does) | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_plan_expired` | `geodb.DatasetPlanExpired` | the approved plan is over 24 hours old | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_plan_changed` | `geodb.DatasetPlanChanged` | the data moved since the rehearsal the user approved, so a member would no longer land as approved | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_file_changed` | `geodb.DatasetFileChanged` | a member file is no longer the one approved (its bytes changed, or it was trashed / undone) | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_already_written` | `geodb.DatasetAlreadyWritten` | the plan landed already (or was undone); a plan is never landed twice | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_in_progress` | `geodb.DatasetInProgress` | this plan is being written right now by another request (the same write sent twice) | sends again after `Retry-After` (bounded, warns), then raises |
| `dataset_write_failed` | `geodb.DatasetWriteFailed` | a member stopped on an unexpected server error; nothing of it landed (members before it may have) | raises; in a write's answer it is `DatasetWrite.stopped` |
| `dataset_project_busy` | `geodb.DatasetProjectBusy` | dataset jobs on one project run one at a time; this background job waited the longest it may for another one on the same project (`waited_seconds`), then stopped | sends again 1× by default (`project_busy_retries`: each server wait can be 30 min), warns, then raises |
| `dataset_interrupted` | `geodb.DatasetInterrupted` | a background dataset job (a rehearsal, write or undo) ended without finishing — the server running it stopped or hit an unexpected error | sends the same request again once (`restart_interrupted`, warns), then raises |
| `writes_switched_off` | `geodb.WritesSwitchedOff` | writing through AI connections was switched off on the server (503, an operator's switch, not something the request did) after a background dataset job was accepted, so it did not run (a write already running stopped before its next member; the members before it stay landed, under its one Undo) | sends again at most 1×, only after a `Retry-After` the server named (warns); else raises at once. In a write's answer it is `DatasetWrite.stopped` |
| `undo_needs_company_reach` | `geodb.UndoNeedsCompanyReach` | the write being undone created or changed a record every project of the company shares (a laboratory, method or standard), and this key no longer reaches every project of that company (`offending.company_shared`) | raises; never sent again |
<!-- END:datasets -->

A background dataset job that fails with no reason code, stalls or runs past the hard cap raises
`geodb.DatasetJobFailed`. One that failed WITH a reason code raises what the same refusal raises
when it answers the request itself: the poll names its original `status_code`, so a 401 is the
`geodb.AuthError` family (`grant_revoked`, `grant_holder_removed` — the key no longer works; never
re-sent), a 403 is `geodb.WriteForbidden` (`project_not_in_scope`, `write_access_insufficient`;
also when the poll itself is refused because the key lost the project), and a 409 / 503 is its
code's class above. (From a server that names no `status_code`, a refusal at the poll reads as
409.) A write's stop block names its `status_code` the same way, so `raise_for_stop()` raises a
stop by a revoked key as the `geodb.AuthError` family and one by a lowered key as
`geodb.WriteForbidden` (409 from a server that names none).

A 429 `throttled` on a dataset request (the key's dataset rate, or the dataset jobs it already has
queued or running) raises `geodb.WriteThrottled` (also a `geodb.Throttled`; `.retry_after`,
`.in_flight` / `.limit`). The dataset helpers wait its `Retry-After` and send the same request
again (bounded, warns), but never sleep past 10 minutes on their own: a longer wait is raised.

## Development

```bash
pip install -e ".[dev,geo]"
pytest                     # unit tests (mocked transport)
GEODB_TEST_BASE_URL=http://localhost:8001 GEODB_TEST_TOKEN=gdbg_... pytest tests/test_integration.py
```

Apache-2.0. Version 0.3.5.dev0 (protocol 0.3) — unreleased: 0.3.5 adds OS-certificate-store TLS,
exports that follow `Retry-After` with progress, a stall timeout and one loud restart,
`record_counts`, several `name=` / `bhid=` values, `include_trashed`, `upload_document(attach_to=)`,
and the dataset dry run (`validate_dataset` → `DatasetPlan.write()` → one Undo).
