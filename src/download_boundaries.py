"""Download BC park polygon boundaries from the BC Open Data catalog.

The GraphQL API blocks `geoShapes` for our key tier, so we fetch the same
authoritative data from the public Tantalis layer published by GeoBC:

    WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW

This is delivered as GeoJSON via WFS, no auth required. The `ORCS_PRIMARY`
property joins back to the `orcs` field in `parks.json`.

Output:
  data/boundaries.geojson   - full GeoJSON FeatureCollection
  data/boundaries.csv       - flat properties table (no geometry)
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW/ows"
TYPENAME = "pub:WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW"
PAGE_SIZE = 1000


def fetch_page(client: httpx.Client, start: int, count: int) -> dict:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": TYPENAME,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "sortBy": "OBJECTID",
        "count": count,
        "startIndex": start,
    }
    r = client.get(WFS_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def fetch_all() -> dict:
    features: list[dict] = []
    start = 0
    crs = None
    with httpx.Client() as client:
        while True:
            page = fetch_page(client, start, PAGE_SIZE)
            crs = crs or page.get("crs")
            batch = page.get("features", [])
            features.extend(batch)
            total = page.get("totalFeatures") or page.get("numberMatched") or len(features)
            print(f"  fetched {len(features):>5} / {total}")
            if len(batch) < PAGE_SIZE:
                break
            start += PAGE_SIZE
    return {
        "type": "FeatureCollection",
        "crs": crs,
        "features": features,
    }


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching park boundaries from BC Open Data WFS...")
    fc = fetch_all()

    geojson_path = DATA_DIR / "boundaries.geojson"
    geojson_path.write_text(json.dumps(fc))
    size_mb = geojson_path.stat().st_size / 1e6
    print(f"\n  {len(fc['features'])} polygons -> data/boundaries.geojson ({size_mb:.1f} MB)")

    df = pd.DataFrame([f["properties"] for f in fc["features"]])
    if "ORCS_PRIMARY" in df.columns:
        df["orcs"] = pd.to_numeric(df["ORCS_PRIMARY"], errors="coerce").astype("Int64")
    csv_path = DATA_DIR / "boundaries.csv"
    df.to_csv(csv_path, index=False)
    print(f"  properties table -> data/boundaries.csv ({len(df)} rows)")


if __name__ == "__main__":
    main()
