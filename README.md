# BC Parks API — MDS Capstone

A small Python project to download data from the [BC Parks GraphQL API](https://bcparks.api.gov.bc.ca/graphql) for analysis.

## Setup

Pick **one** of the two environments. Both produce an equivalent stack; conda also installs the geospatial libs (geopandas, shapely, pyproj, fiona) cleanly via conda-forge.

### Option A — conda (recommended for geospatial work)

```bash
conda env create -f environment.yml
conda activate bcpark-api
```

To update after `environment.yml` changes:

```bash
conda env update -f environment.yml --prune
```

### Option B — venv + pip

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Add your API key to `.env`:

```
BCPARKS_API_KEY=your_key_here
BCPARKS_API_URL=https://bcparks.api.gov.bc.ca/graphql
```

## Usage

Inspect the GraphQL schema (run this first — the docs in `prompt.md` are not authoritative):

```bash
python -m src.introspect
```

Download data. Each dataset is saved as both JSON and CSV under `data/`:

```bash
python -m src.download                    # all datasets
python -m src.download parks              # just parks
python -m src.download parks advisories   # subset
```

Available datasets:

| Name         | GraphQL root                  | Approx rows |
|--------------|-------------------------------|------------:|
| `parks`      | `protectedAreas_connection`   |       1,052 |
| `advisories` | `publicAdvisories_connection` |         474 |
| `activities` | `parkActivities_connection`   |       4,462 |
| `facilities` | `parkFacilities_connection`   |       1,869 |

### Photos (~1.9 GB if you grab all 1,737)

Metadata always, binaries optional. The downloader is concurrent and resume-safe (skips files already on disk).

```bash
python -m src.download_photos --metadata-only      # just metadata (~2 MB)
python -m src.download_photos --limit 50           # quick sample
python -m src.download_photos                      # full 1,737 images
python -m src.download_photos --workers 16         # tune concurrency
```

Output:
- `data/photos.json` / `data/photos.csv` — metadata (orcs, title, caption, photographer, dateTaken, imageUrl, …)
- `data/photos/<orcs>/<documentId>.jpg` — image binaries
- `data/photos_manifest.csv` — per-file status / size / errors

### Park boundary polygons (geoShapes fallback)

The GraphQL `geoShapes` field is gated behind a higher-tier API key. As a public alternative this script pulls the same authoritative boundaries from the **BC Open Data** Tantalis layer (no auth required) and joins them back to our parks via `ORCS_PRIMARY = orcs`.

```bash
python -m src.download_boundaries
```

Output:
- `data/boundaries.geojson` — 930 polygons, ~37 MB, EPSG:4326
- `data/boundaries.csv` — flat properties (PARK_CLASS, OFFICIAL_AREA_HA, ORCS_PRIMARY, …)

Coverage: ~880/1,052 of our parks have a polygon (the rest are conservancies / recreation areas in other layers).

Open one of the exploratory notebooks:

```bash
jupyter notebook notebooks/explore.ipynb     # parks, advisories, activities, facilities
jupyter notebook notebooks/photos_geo.ipynb  # boundaries + choropleths + photo gallery
```

`photos_geo.ipynb` is committed with cleared outputs (the choropleths embed the 37 MB GeoJSON, which would bloat the file to ~360 MB if executed). Run all cells locally to populate it.

Note: the example query in `prompt.md` (`{ parks { name ... } }`) is **not** the real schema. The API is a Strapi GraphQL backend — use `protectedAreas_connection`, `publicAdvisories_connection`, etc. Run `python -m src.introspect` to see every available query.

## Layout

```
bcpark-api/
├── .env                 # your API key (git-ignored)
├── environment.yml      # conda env (recommended — includes geopandas)
├── requirements.txt     # pip equivalent (no geopandas)
├── src/
│   ├── client.py             # httpx GraphQL client + auth
│   ├── introspect.py         # dump schema to data/schema.json
│   ├── download.py           # parks/advisories/activities/facilities
│   ├── download_photos.py    # photo metadata + binaries
│   └── download_boundaries.py # park polygons (BC Open Data WFS)
├── notebooks/
│   ├── explore.ipynb       # starter EDA on the structured catalog
│   └── photos_geo.ipynb    # choropleth maps + photo gallery
└── data/                # downloaded files (created on first run)
```
