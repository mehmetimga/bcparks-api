"""Download BC Parks data via the GraphQL API and save to data/.

The API is a Strapi GraphQL backend. List queries use a `*_connection` field
that returns `{ nodes, pageInfo { total page pageSize pageCount } }` and
accept a `pagination: { page, pageSize }` argument.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .client import BCParksClient

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PAGE_SIZE = 100

# --- Queries -----------------------------------------------------------------

PROTECTED_AREAS_QUERY = """
query ProtectedAreas($page: Int!, $pageSize: Int!) {
  protectedAreas_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      documentId
      orcs
      protectedAreaName
      type
      class
      typeCode
      legalStatus
      latitude
      longitude
      totalArea
      uplandArea
      marineArea
      establishedDate
      url
      slug
      isDisplayed
      hasCampfireBan
      description
      safetyInfo
      locationNotes
    }
  }
}
"""

PUBLIC_ADVISORIES_QUERY = """
query PublicAdvisories($page: Int!, $pageSize: Int!) {
  publicAdvisories_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      documentId
      advisoryNumber
      title
      description
      isSafetyRelated
      latitude
      longitude
      advisoryDate
      effectiveDate
      endDate
      expiryDate
      updatedDate
      accessStatus { accessStatus }
      eventType   { eventType }
      urgency     { urgency }
      advisoryStatus { advisoryStatus }
      protectedAreas_connection(pagination: { pageSize: 100 }) {
        nodes { orcs protectedAreaName }
      }
    }
  }
}
"""

PARK_ACTIVITIES_QUERY = """
query ParkActivities($page: Int!, $pageSize: Int!) {
  parkActivities_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      documentId
      isActive
      isActivityOpen
      description
      activityType { activityName activityCode }
      protectedArea { orcs protectedAreaName }
    }
  }
}
"""

PARK_FACILITIES_QUERY = """
query ParkFacilities($page: Int!, $pageSize: Int!) {
  parkFacilities_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      documentId
      isActive
      isFacilityOpen
      description
      facilityType { facilityName facilityCode }
      protectedArea { orcs protectedAreaName }
    }
  }
}
"""

DATASETS: dict[str, tuple[str, str]] = {
    "parks":      (PROTECTED_AREAS_QUERY,  "protectedAreas_connection"),
    "advisories": (PUBLIC_ADVISORIES_QUERY, "publicAdvisories_connection"),
    "activities": (PARK_ACTIVITIES_QUERY,   "parkActivities_connection"),
    "facilities": (PARK_FACILITIES_QUERY,   "parkFacilities_connection"),
}

# --- Core --------------------------------------------------------------------


def paginate(
    client: BCParksClient, query: str, root: str, page_size: int = PAGE_SIZE
) -> Iterable[dict[str, Any]]:
    page = 1
    while True:
        data = client.query(query, {"page": page, "pageSize": page_size})
        conn = data[root]
        for node in conn["nodes"]:
            yield node
        info = conn["pageInfo"]
        if page >= info["pageCount"] or info["pageCount"] == 0:
            return
        page += 1


def save(name: str, rows: list[dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    json_path = DATA_DIR / f"{name}.json"
    csv_path = DATA_DIR / f"{name}.csv"
    json_path.write_text(json.dumps(rows, indent=2, default=str))
    pd.json_normalize(rows).to_csv(csv_path, index=False)
    print(f"  {name}: {len(rows):>5} rows  ->  data/{name}.json, data/{name}.csv")


def download(names: list[str]) -> None:
    with BCParksClient() as client:
        for name in names:
            if name not in DATASETS:
                print(f"Unknown dataset: {name}. Choose from {list(DATASETS)}")
                continue
            query, root = DATASETS[name]
            rows = list(paginate(client, query, root))
            save(name, rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download BC Parks data.")
    parser.add_argument(
        "datasets",
        nargs="*",
        default=list(DATASETS),
        help=f"Datasets to download. Default: all ({', '.join(DATASETS)})",
    )
    args = parser.parse_args()
    download(args.datasets)


if __name__ == "__main__":
    main()
