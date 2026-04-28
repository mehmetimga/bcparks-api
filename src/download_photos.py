"""Download park photo metadata and binaries.

Metadata -> data/photos.json + data/photos.csv
Images   -> data/photos/<orcs>/<documentId>.<ext>

The downloader is resume-safe: existing files are skipped. Image fetches run
concurrently (default 8 workers).
"""

from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pandas as pd

from .client import BCParksClient

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
PHOTOS_DIR = DATA_DIR / "photos"
PAGE_SIZE = 100

PHOTOS_QUERY = """
query Photos($page: Int!, $pageSize: Int!) {
  parkPhotos_connection(pagination: { page: $page, pageSize: $pageSize }) {
    pageInfo { page pageSize pageCount total }
    nodes {
      documentId
      orcs
      title
      caption
      subject
      photographer
      dateTaken
      isActive
      isFeatured
      sortOrder
      imageUrl
      protectedArea { orcs protectedAreaName }
      site { siteName siteNumber }
    }
  }
}
"""


@dataclass
class PhotoTask:
    document_id: str
    orcs: int | None
    url: str

    @property
    def ext(self) -> str:
        path = urlparse(self.url).path
        m = re.search(r"\.([A-Za-z0-9]{2,5})$", path)
        return f".{m.group(1).lower()}" if m else ".jpg"

    @property
    def dest(self) -> Path:
        bucket = str(self.orcs) if self.orcs is not None else "_unknown"
        return PHOTOS_DIR / bucket / f"{self.document_id}{self.ext}"


def fetch_metadata(client: BCParksClient) -> list[dict]:
    rows: list[dict] = []
    page = 1
    while True:
        data = client.query(
            PHOTOS_QUERY, {"page": page, "pageSize": PAGE_SIZE}
        )
        conn = data["parkPhotos_connection"]
        rows.extend(conn["nodes"])
        info = conn["pageInfo"]
        if page >= info["pageCount"] or info["pageCount"] == 0:
            break
        page += 1
    return rows


def save_metadata(rows: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "photos.json").write_text(json.dumps(rows, indent=2, default=str))
    pd.json_normalize(rows).to_csv(DATA_DIR / "photos.csv", index=False)


def to_tasks(rows: list[dict]) -> list[PhotoTask]:
    tasks: list[PhotoTask] = []
    for r in rows:
        url = r.get("imageUrl")
        if not url:
            continue
        if url.startswith("/"):
            url = "https://bcparks.api.gov.bc.ca" + url
        orcs = r.get("orcs") or (r.get("protectedArea") or {}).get("orcs")
        tasks.append(PhotoTask(document_id=r["documentId"], orcs=orcs, url=url))
    return tasks


def download_one(client: httpx.Client, task: PhotoTask) -> tuple[PhotoTask, str, int]:
    if task.dest.exists() and task.dest.stat().st_size > 0:
        return task, "skip", task.dest.stat().st_size
    task.dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = client.get(task.url, timeout=30, follow_redirects=True)
        r.raise_for_status()
        task.dest.write_bytes(r.content)
        return task, "ok", len(r.content)
    except Exception as exc:
        return task, f"err:{exc}", 0


def download_images(tasks: list[PhotoTask], workers: int = 8) -> None:
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = DATA_DIR / "photos_manifest.csv"
    ok = skip = err = 0
    bytes_total = 0
    rows: list[dict] = []

    with httpx.Client(timeout=30) as http, ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(download_one, http, t) for t in tasks]
        for i, fut in enumerate(as_completed(futures), 1):
            task, status, size = fut.result()
            if status == "ok":
                ok += 1
                bytes_total += size
            elif status == "skip":
                skip += 1
                bytes_total += size
            else:
                err += 1
            rows.append(
                {
                    "documentId": task.document_id,
                    "orcs": task.orcs,
                    "url": task.url,
                    "path": str(task.dest.relative_to(DATA_DIR.parent)),
                    "status": status,
                    "bytes": size,
                }
            )
            if i % 50 == 0 or i == len(futures):
                print(
                    f"  [{i:>4}/{len(futures)}]  ok={ok}  skip={skip}  err={err}  "
                    f"{bytes_total/1e6:.1f} MB"
                )

    pd.DataFrame(rows).to_csv(manifest_path, index=False)
    print(f"\nManifest -> data/{manifest_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download BC Parks photos.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only download the first N images (0 = all)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Concurrent download workers (default 8)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Fetch metadata only, skip image binaries")
    args = parser.parse_args()

    print("Fetching photo metadata...")
    with BCParksClient() as client:
        rows = fetch_metadata(client)
    save_metadata(rows)
    print(f"  {len(rows)} photo records -> data/photos.json, data/photos.csv")

    if args.metadata_only:
        return

    tasks = to_tasks(rows)
    if args.limit:
        tasks = tasks[: args.limit]
    print(f"\nDownloading {len(tasks)} images (workers={args.workers})...")
    download_images(tasks, workers=args.workers)


if __name__ == "__main__":
    main()
