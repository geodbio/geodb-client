# geodb-client

The Python client for the **[geoDB Open Exploration Protocol](https://github.com/geodbio/geodb-protocol)** —
pull a mining-exploration project's drill, assay, geophysics, raster, and document
data as pandas DataFrames and files, over one authenticated, project-scoped API.

```bash
pip install geodb-client            # + geopandas extra:  pip install "geodb-client[geo]"
```

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

## Development

```bash
pip install -e ".[dev,geo]"
pytest                     # unit tests (mocked transport)
GEODB_TEST_BASE_URL=http://localhost:8001 GEODB_TEST_TOKEN=gdbg_... pytest tests/test_integration.py
```

Apache-2.0. Version 0.1.0.
