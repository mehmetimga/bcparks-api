"""Dump the GraphQL schema to data/schema.json so we know what's queryable."""

from __future__ import annotations

import json
from pathlib import Path

from .client import BCParksClient

OUT = Path(__file__).resolve().parents[1] / "data" / "schema.json"


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with BCParksClient() as client:
        data = client.introspect()
    OUT.write_text(json.dumps(data, indent=2))

    query_type = data["__schema"]["queryType"]["name"]
    root = next(
        t for t in data["__schema"]["types"] if t["name"] == query_type
    )
    print(f"Root query type: {query_type}")
    print("Available top-level queries:")
    for f in root["fields"] or []:
        t = f["type"]
        type_name = t.get("name") or (t.get("ofType") or {}).get("name")
        print(f"  - {f['name']}: {type_name}")
    print(f"\nFull schema saved to {OUT.relative_to(OUT.parents[1])}")


if __name__ == "__main__":
    main()
