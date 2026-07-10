# geodb-client

The Python client for the **[geoDB Open Exploration Protocol](https://github.com/geodb-io/geodb-protocol)** —
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
| `gx.collars() / .samples() / .assays() / .point_samples() / .surveys()` | a `Paginated` iterator — iterate rows, or `.to_dataframe()`, or `.count()`. Accepts filter kwargs (e.g. `gx.surveys(method="magnetics")`, `gx.collars(modified_since="2024-01-01")`). |
| `gx.stac()` | a `StacCatalog`: `.landing()`, `.collections()`, `.items(collection_id)` (yields `StacItem`), `.item(cid, iid)`. `StacItem.asset(key).download(path)`. |
| `gx.export(model, format="geoparquet", include_assays=True)` | an `ExportJob`: `.wait()` then `.download(path)`. Models: `drill_collars`, `drill_samples`, `point_samples`, `qc_samples`, `geophysical_surveys`. Formats: `geoparquet`, `parquet`, `csv`. |
| `gx.project()` | the grant/project context. |

## Coordinate contract

GeoParquet exports carry a **WGS84 geometry** column plus the **native/original**
coordinate columns (`latitude`/`longitude`/`epsg`) — zero-data-loss travels with
the data, so you never lose the surveyor's original CRS.

## Development

```bash
pip install -e ".[dev,geo]"
pytest                     # unit tests (mocked transport)
GEODB_TEST_BASE_URL=http://localhost:8001 GEODB_TEST_TOKEN=gdbg_... pytest tests/test_integration.py
```

Apache-2.0. Version 0.1.0.
