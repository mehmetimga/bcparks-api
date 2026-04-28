"""Minimal GraphQL client for the BC Parks API."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DEFAULT_URL = "https://bcparks.api.gov.bc.ca/graphql"


class BCParksClient:
    def __init__(
        self,
        api_key: str | None = None,
        url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key or os.getenv("BCPARKS_API_KEY")
        self.url = url or os.getenv("BCPARKS_API_URL", DEFAULT_URL)
        if not self.api_key:
            raise RuntimeError(
                "BCPARKS_API_KEY is not set. Add it to .env or pass api_key=..."
            )
        self._client = httpx.Client(
            timeout=timeout,
            headers={
                "Content-Type": "application/json",
                "x-api-key": self.api_key,
            },
        )

    def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = self._client.post(self.url, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if "errors" in body and body["errors"]:
            raise RuntimeError(f"GraphQL errors: {body['errors']}")
        return body.get("data", {})

    def introspect(self) -> dict[str, Any]:
        """Return the schema's top-level types so we know what to query."""
        q = """
        query Introspect {
          __schema {
            queryType { name }
            types {
              name
              kind
              fields { name type { name kind ofType { name kind } } }
            }
          }
        }
        """
        return self.query(q)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "BCParksClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
