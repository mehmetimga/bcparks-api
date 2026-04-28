"""Download Citywide asset records, attributes, attached-file metadata,
and image binaries — filtered to the 5 profiles BC Parks cares about.

Profile IDs (discovered via /am/profiles):
    337  Boardwalk < 1.2m High
    573  Boardwalk > 1.2m High
    356  Stairs
    253  Trail Bridge
    359  Viewing Platform

Output layout:
    data/citywide/
      assets.json / assets.csv             (one row per asset)
      attributes.json / attributes.csv     (one row per asset×attribute)
      files_manifest.json / .csv           (one row per attached file)
      images_manifest.csv                  (download status per file)
      images/<profile_id>/<asset_id>/<file_id>__<filename>
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from .citywide_client import CitywideClient

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "citywide"

PROFILES: dict[int, str] = {
    337: "Boardwalk < 1.2m High",
    573: "Boardwalk > 1.2m High",
    356: "Stairs",
    253: "Trail Bridge",
    359: "Viewing Platform",
}


# ---------- step 1: assets + attributes + file metadata ----------------


def _fetch_asset_meta(client: CitywideClient, asset_id: int, pid: int
                      ) -> tuple[list[dict], list[dict]]:
    """Fetch attributes + attached_files for a single asset."""
    attrs: list[dict] = []
    files: list[dict] = []
    r = client.get(f"/assets/{asset_id}/attributes")
    if r.status_code == 200:
        for at in r.json():
            at["asset_id"] = asset_id
            at["profile_id"] = pid
            attrs.append(at)
    r = client.get(f"/assets/{asset_id}/attached_files")
    if r.status_code == 200:
        for f in r.json():
            f["asset_id"] = asset_id
            f["profile_id"] = pid
            files.append(f)
    return attrs, files


def fetch_metadata(client: CitywideClient, workers: int = 8
                   ) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (assets, attributes, files_metadata) for every target asset."""
    assets: list[dict] = []
    attributes: list[dict] = []
    files: list[dict] = []

    for pid, pname in PROFILES.items():
        print(f"\n[{pid}] {pname}")
        profile_assets = list(client.list_all("/assets", {"profile_id": pid}))
        print(f"  fetched {len(profile_assets)} assets")
        for a in profile_assets:
            a["profile_id_used"] = pid
            a["profile_name"] = pname
        assets.extend(profile_assets)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_fetch_asset_meta, client, a["id"], pid): a
                    for a in profile_assets}
            done = 0
            for fut in as_completed(futs):
                attrs, fs = fut.result()
                attributes.extend(attrs)
                files.extend(fs)
                done += 1
                if done % 100 == 0 or done == len(profile_assets):
                    print(f"  meta: {done}/{len(profile_assets)}")

        n_attr = sum(1 for at in attributes if at['profile_id'] == pid)
        n_file = sum(1 for f in files if f['profile_id'] == pid)
        print(f"  done: {len(profile_assets)} assets, "
              f"{n_attr} attribute rows, {n_file} file records")

    return assets, attributes, files


def save_metadata(assets: list[dict], attributes: list[dict], files: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for name, rows in [("assets", assets), ("attributes", attributes), ("files_manifest", files)]:
        (DATA_DIR / f"{name}.json").write_text(json.dumps(rows, indent=2, default=str))
        if rows:
            pd.json_normalize(rows).to_csv(DATA_DIR / f"{name}.csv", index=False)
    print(f"\nMetadata saved to {DATA_DIR}/")
    print(f"  assets.csv         {len(assets):>5} rows")
    print(f"  attributes.csv     {len(attributes):>5} rows")
    print(f"  files_manifest.csv {len(files):>5} rows")


# ---------- step 2: image binary download ------------------------------


def safe_filename(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)[:120]


def download_one(client: CitywideClient, file_row: dict) -> tuple[dict, str, int]:
    asset_id = file_row["asset_id"]
    file_id = file_row["id"]
    profile_id = file_row["profile_id"]
    filename = safe_filename(file_row.get("filename") or f"file_{file_id}")
    dest = DATA_DIR / "images" / str(profile_id) / str(asset_id) / f"{file_id}__{filename}"
    if dest.exists() and dest.stat().st_size > 0:
        return file_row, "skip", dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = client.get_binary(f"/assets/{asset_id}/attached_files/{file_id}/content")
        if r.status_code != 200:
            return file_row, f"err:HTTP{r.status_code}", 0
        dest.write_bytes(r.content)
        return file_row, "ok", len(r.content)
    except Exception as exc:
        return file_row, f"err:{type(exc).__name__}", 0


def download_images(client: CitywideClient, files: list[dict],
                    workers: int = 8, only_images: bool = True) -> None:
    targets = [f for f in files
               if (not only_images) or "image" in (f.get("mime_type") or "")]
    print(f"\nDownloading {len(targets)} files (images_only={only_images}, workers={workers})...")

    rows: list[dict] = []
    ok = skip = err = 0
    bytes_total = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(download_one, client, f) for f in targets]
        for i, fut in enumerate(as_completed(futs), 1):
            f, status, size = fut.result()
            if status == "ok":
                ok += 1; bytes_total += size
            elif status == "skip":
                skip += 1; bytes_total += size
            else:
                err += 1
            rows.append({
                "asset_id": f["asset_id"],
                "profile_id": f["profile_id"],
                "file_id": f["id"],
                "filename": f.get("filename"),
                "mime_type": f.get("mime_type"),
                "status": status,
                "bytes": size,
            })
            if i % 50 == 0 or i == len(futs):
                print(f"  [{i:>5}/{len(futs)}]  ok={ok}  skip={skip}  err={err}  "
                      f"{bytes_total/1e6:.0f} MB")

    pd.DataFrame(rows).to_csv(DATA_DIR / "images_manifest.csv", index=False)
    print(f"\nManifest -> {DATA_DIR/'images_manifest.csv'}")


# ---------- main -------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--metadata-only", action="store_true",
                   help="Skip image binary download")
    p.add_argument("--images-only", action="store_true",
                   help="Skip metadata fetch — re-use existing files_manifest.json")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--limit", type=int, default=0,
                   help="If >0, only download first N files (for sampling)")
    args = p.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with CitywideClient() as client:
        if not args.images_only:
            assets, attrs, files = fetch_metadata(client)
            save_metadata(assets, attrs, files)
        else:
            files = json.loads((DATA_DIR / "files_manifest.json").read_text())
            print(f"Loaded {len(files)} files from manifest")

        if args.metadata_only:
            return

        if args.limit:
            files = files[: args.limit]
        download_images(client, files, workers=args.workers)


if __name__ == "__main__":
    main()
