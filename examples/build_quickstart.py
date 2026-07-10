"""Build examples/quickstart.ipynb via nbformat (then executed by the caller)."""
import nbformat as nbf

nb = nbf.v4.new_notebook()
c = []


def md(s): c.append(nbf.v4.new_markdown_cell(s))
def code(s): c.append(nbf.v4.new_code_cell(s))


md("""# geoDB Open Exploration Protocol — Quickstart

Pull an entire exploration project into **pandas / geopandas in ~20 lines**, over a
standard, read-only, project-scoped API — no bespoke SDK dance:

* **records** — drill collars, samples, assays, point samples, geophysical surveys
  as paginated iterators that materialise straight to a `DataFrame`;
* **STAC 1.1** — a per-project catalog (geophysical surveys · rasters · documents)
  you can browse like any other STAC endpoint;
* **Cloud-Optimized GeoTIFFs** — grids you can range-read;
* **GeoParquet** — one-call bulk export of a whole model.

Everything is authenticated with a single project-pinned **access grant**
(`Authorization: Grant gdbg_…`) and is **read-only** — a vendor integrates against
a customer's data without ever being able to change it.

This notebook runs end-to-end against **Project A — St. Francois Iron District,
Missouri** in the public sandbox (real U.S. Geological Survey / CC0 data — see the
project description for full source credit).

### Before you run
```bash
pip install geodb-client          # this repo:  pip install -e .
```
Then set two environment variables:

| var | meaning |
|---|---|
| `GEODB_BASE` | base URL, e.g. `http://127.0.0.1:8001` (local dev) or `https://api.geodb.io` |
| `GEODB_TOKEN` | a `gdbg_…` access grant for Project A |

A demo grant is printed once by
`python manage.py provision_protocol_sandbox --apply`. Optionally set
`GEODB_TOKEN_VALDEZ` for the second sandbox project (last cell).""")

code("""import os
import geodb

BASE = os.environ.get("GEODB_BASE", "http://127.0.0.1:8001")
TOKEN = os.environ["GEODB_TOKEN"]                       # project-scoped, read-only

gx = geodb.Client(TOKEN, base_url=BASE, api_prefix="/api/v2")
ctx = gx.project()                                      # the grant's "who am I"
print("Connected to:", ctx["project"]["name"])
print("Read-only grant:", ctx["read_only"], "| expires:", ctx.get("expires_at"))""")

md("""## 1 · Records → DataFrames

Each accessor is a lazy, paginated iterator; `.to_dataframe()` walks every page.""")

code("""collars = gx.collars().to_dataframe()
assays = gx.assays().to_dataframe()
print(f"{len(collars)} drill collars, {len(assays)} assays")
collars[["name", "hole_type", "total_depth", "latitude", "longitude"]].head()""")

code("""# Geochemistry travels as assays with nested element values.
pts = gx.point_samples()
print(f"{pts.count()} geochemical point samples in this project")
assays.head(3)""")

md("""## 2 · Walk the STAC catalog

A private, per-project STAC 1.1 catalog. Every asset `href` is an authenticated
short-lived redirect — the JSON never embeds a signed URL.""")

code("""cat = gx.stac()
for coll in cat.collections():
    print(f"  {coll['id']:20s} {coll['title']}")

print("\\nGeophysical surveys:")
for item in cat.items("geophysical-surveys"):
    p = item.properties
    print(f"  {item.id}: {p.get('xpl:method')} / {p.get('xpl:acquisition_type')}"
          f" — {p.get('xpl:contractor')}  (footprint: {bool(item.bbox)})")""")

md("""## 3 · Download a Cloud-Optimized GeoTIFF

The `rasters` collection exposes both the original raster and a COG asset
(internal tiling + overviews → HTTP range reads). Downloads follow the
authenticated redirect to a short-lived URL; the grant token never touches
storage.""")

code("""raster = next(cat.items("rasters"))
cog = raster.asset("cog")
out = cog.download("sandbox_cog.tif")
print(f"Downloaded {os.path.getsize(out):,} bytes — {raster.properties.get('title')}")
print("proj:epsg =", raster.properties.get("proj:epsg"), "| bbox =", raster.bbox)

try:
    import rasterio
    with rasterio.open(out) as r:
        print("COG:", r.width, "x", r.height, r.crs, "| overviews:", r.overviews(1)[:4])
except ImportError:
    print("(install rasterio to inspect the COG)")""")

md("""## 4 · Bulk export → GeoParquet

One call creates a server-side export **job** (the only write a read-only grant can
make — it mutates no data), polls to completion, and downloads. The geometry column
is WGS84; native coordinates and EPSG travel alongside (zero-data-loss).""")

code("""job = gx.export("drill_collars", format="geoparquet").wait(timeout=180)
job.download("collars.parquet")

import geopandas as gpd
gdf = gpd.read_parquet("collars.parquet")
print(f"{len(gdf)} collars, {int(gdf.geometry.notna().sum())} with WGS84 geometry")
gdf[["name", "hole_type", "geometry"]].head()""")

md("""## 5 · The two sandbox projects

The public sandbox ships **two** real-data projects. Each grant is scoped to a
single project; set `GEODB_TOKEN_VALDEZ` to explore the second.

| | project | data |
|---|---|---|
| **A** | St. Francois Iron District, Missouri | USGS drillholes + geochemistry (CC0), two airborne magnetic surveys, NURE sediment |
| **B** | Valdez Creek District, Alaska | DGGS airborne EM/mag survey, Caribou Dome Cu drilling (GMC 401/402), USGS AGDB4 geochemistry |""")

code("""for label, tok in [("A · St. Francois", os.environ.get("GEODB_TOKEN")),
                   ("B · Valdez Creek", os.environ.get("GEODB_TOKEN_VALDEZ"))]:
    if not tok:
        print(f"{label}: set its token to explore")
        continue
    c = geodb.Client(tok, base_url=BASE, api_prefix="/api/v2")
    p = c.project()["project"]
    print(f"{label}: {p['name']}")
    print(f"    {c.collars().count()} collars · {c.point_samples().count()} point samples"
          f" · {c.assays().count()} assays · {sum(1 for _ in c.surveys())} surveys")""")

md("""---
**Read-only, by design.** The grant can browse and export, but every write path
(other than creating an export job) returns `403`. **Provenance** lives in each
project's description — this sandbox is real public-domain data, credited to the
USGS and the Alaska DGGS, reshaped into geoDB formats with values unaltered.""")

nb["cells"] = c
nb["metadata"] = {"kernelspec": {"display_name": "Python 3", "language": "python",
                                 "name": "python3"},
                  "language_info": {"name": "python"}}
with open("examples/quickstart.ipynb", "w") as f:
    nbf.write(nb, f)
print("wrote examples/quickstart.ipynb with", len(c), "cells")
