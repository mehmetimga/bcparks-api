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


def _save_profile(pid: int, assets: list[dict], attrs: list[dict], files: list[dict]) -> None:
    """Persist per-profile metadata so a partial run is not lost."""
    pdir = DATA_DIR / "by_profile" / str(pid)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "assets.json").write_text(json.dumps(assets, indent=2, default=str))
    (pdir / "attributes.json").write_text(json.dumps(attrs, indent=2, default=str))
    (pdir / "files.json").write_text(json.dumps(files, indent=2, default=str))


def fetch_metadata(client: CitywideClient) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (assets, attributes, files_metadata) for every target asset.

    Uses /bulk/assets?$linked=Attributes,Files which returns each asset
    together with its inline attributes and attached-file metadata in a
    single response — collapsing what would otherwise be 9000+ calls into
    ~95 list pages. Writes per-profile snapshots to
    data/citywide/by_profile/<pid>/ as it goes (resume-safe).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    assets: list[dict] = []
    attributes: list[dict] = []
    files: list[dict] = []

    for pid, pname in PROFILES.items():
        # Resume: skip profile if already saved
        pdir = DATA_DIR / "by_profile" / str(pid)
        if (pdir / "files.json").exists():
            p_assets = json.loads((pdir / "assets.json").read_text())
            p_attrs  = json.loads((pdir / "attributes.json").read_text())
            p_files  = json.loads((pdir / "files.json").read_text())
            assets.extend(p_assets); attributes.extend(p_attrs); files.extend(p_files)
            print(f"\n[{pid}] {pname}  (already cached: {len(p_assets)} assets)", flush=True)
            continue

        print(f"\n[{pid}] {pname}", flush=True)
        p_assets: list[dict] = []
        p_attrs: list[dict] = []
        p_files: list[dict] = []
        # /bulk/assets supports $linked=Attributes (TitleCase keys) but
        # NOT Files — so we get attributes inline here, and fetch
        # attached_files separately afterwards.
        for a in client.list_all(
            "/bulk/assets",
            {"profile_id": pid, "$linked": "Attributes"},
        ):
            aid = a["id"]
            a["profile_id_used"] = pid
            a["profile_name"] = pname
            linked = a.pop("linked", {}) or {}
            for at in (linked.get("Attributes") or []):
                at["asset_id"] = aid
                at["profile_id"] = pid
                p_attrs.append(at)
            p_assets.append(a)

        # Now fetch attached_files for each asset (one call each).
        # NB: this is the expensive step (~1 call per asset). The script
        # is resume-safe at the profile level.
        print(f"  fetched {len(p_assets)} assets, {len(p_attrs)} attrs; "
              f"now fetching attached_files metadata...", flush=True)
        for i, a in enumerate(p_assets, 1):
            r = client.get(f"/assets/{a['id']}/attached_files")
            if r.status_code == 200:
                for f in r.json():
                    f["asset_id"] = a["id"]
                    f["profile_id"] = pid
                    p_files.append(f)
            if i % 100 == 0 or i == len(p_assets):
                print(f"    files: {i}/{len(p_assets)}  total_files={len(p_files)}",
                      flush=True)

        _save_profile(pid, p_assets, p_attrs, p_files)
        assets.extend(p_assets); attributes.extend(p_attrs); files.extend(p_files)
        print(f"  done: {len(p_assets)} assets, {len(p_attrs)} attrs, "
              f"{len(p_files)} files  -> data/citywide/by_profile/{pid}/",
              flush=True)

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


def probe_quota(client: CitywideClient) -> None:
    """Spend 1 call to read rate-limit headers."""
    r = client.get("/users", params={"$limit": 1})
    print(f"  status: {r.status_code}")
    for h in ("X-Total", "X-Rate-Limit-Limit", "X-Rate-Limit-Remaining",
              "X-Rate-Limit-Reset", "Retry-After"):
        v = r.headers.get(h)
        if v:
            print(f"  {h}: {v}")
    # Print all headers starting with X- in case the names differ
    print("\n  all X-/Retry- headers:")
    for k, v in r.headers.items():
        if k.lower().startswith(("x-", "retry-")):
            print(f"    {k}: {v}")


def consolidate_metadata() -> None:
    """Read every per-profile snapshot and write the combined CSVs."""
    assets: list[dict] = []
    attrs: list[dict] = []
    files: list[dict] = []
    by_profile = DATA_DIR / "by_profile"
    if not by_profile.exists():
        return
    for pdir in sorted(by_profile.iterdir()):
        if not (pdir / "files.json").exists():
            continue
        assets.extend(json.loads((pdir / "assets.json").read_text()))
        attrs.extend(json.loads((pdir / "attributes.json").read_text()))
        files.extend(json.loads((pdir / "files.json").read_text()))
    if assets:
        save_metadata(assets, attrs, files)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--profile", type=int, default=None,
                   help="Run only this profile_id (one of: "
                        + ", ".join(f"{k}={v}" for k, v in PROFILES.items()) + ")")
    p.add_argument("--metadata-only", action="store_true",
                   help="Skip image binary download")
    p.add_argument("--images-only", action="store_true",
                   help="Skip metadata fetch — use cached per-profile JSON")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=0,
                   help="If >0, only download first N files (for sampling)")
    p.add_argument("--max-calls-per-hour", type=int, default=900,
                   help="Client-side rate limit (server cap is 1000/hr)")
    p.add_argument("--probe", action="store_true",
                   help="Issue 1 call and print rate-limit headers")
    args = p.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Optionally restrict to a single profile
    if args.profile is not None:
        if args.profile not in PROFILES:
            raise SystemExit(f"--profile must be one of {list(PROFILES)}")
        only = {args.profile: PROFILES[args.profile]}
        PROFILES.clear()
        PROFILES.update(only)

    with CitywideClient(max_calls_per_hour=args.max_calls_per_hour) as client:
        if args.probe:
            probe_quota(client)
            return

        if not args.images_only:
            assets, attrs, files = fetch_metadata(client)
        else:
            files = []
            for pid in PROFILES:
                pdir = DATA_DIR / "by_profile" / str(pid)
                if (pdir / "files.json").exists():
                    files.extend(json.loads((pdir / "files.json").read_text()))
            print(f"Loaded {len(files)} files from cache")

        consolidate_metadata()

        if args.metadata_only:
            return

        if args.limit:
            files = files[: args.limit]
        download_images(client, files, workers=args.workers)


if __name__ == "__main__":
    main()
