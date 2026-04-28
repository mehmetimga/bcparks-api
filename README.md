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

Open the exploratory notebook (parks/advisories maps, top activities, etc.):

```bash
jupyter notebook notebooks/explore.ipynb
```

Note: the example query in `prompt.md` (`{ parks { name ... } }`) is **not** the real schema. The API is a Strapi GraphQL backend — use `protectedAreas_connection`, `publicAdvisories_connection`, etc. Run `python -m src.introspect` to see every available query.

## Layout

```
bcpark-api/
├── .env                 # your API key (git-ignored)
├── environment.yml      # conda env (recommended — includes geopandas)
├── requirements.txt     # pip equivalent (no geopandas)
├── src/
│   ├── client.py        # httpx GraphQL client + auth
│   ├── introspect.py    # dump schema to data/schema.json
│   └── download.py      # paginated fetch -> JSON + CSV
├── notebooks/
│   └── explore.ipynb    # starter EDA: maps, advisories, activities
└── data/                # downloaded files (created on first run)
```
