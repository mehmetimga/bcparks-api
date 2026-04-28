"""Minimal client for the PSD Citywide Asset Management REST API.

Handles:
  * POST /authenticate exchange (api_key + client_db + username -> bearer token)
  * Auto-refresh of the bearer token before expiry
  * Cursor pagination as exposed by Citywide:
        page 1: send only filters (e.g. {"profile_id": 337})
        page 2+: send {"$cursor": <from Link header>, "$page": N}
        the X-Total response header gives the total count
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DEFAULT_URL = "https://v4.citywidesolutions.com/v4_server/external/v1"


class CitywideClient:
    def __init__(
        self,
        api_key: str | None = None,
        client_db: str | None = None,
        username: str | None = None,
        url: str | None = None,
        timeout: float = 120.0,
    ) -> None:
        self.api_key = api_key or os.getenv("CITYWIDE_API_KEY")
        self.client_db = client_db or os.getenv("CITYWIDE_DB")
        self.username = username or os.getenv("CITYWIDE_USER")
        self.url = (url or os.getenv("CITYWIDE_API_URL") or DEFAULT_URL).rstrip("/")
        for k, v in [("CITYWIDE_API_KEY", self.api_key),
                     ("CITYWIDE_DB", self.client_db),
                     ("CITYWIDE_USER", self.username)]:
            if not v:
                raise RuntimeError(f"{k} is not set in environment / .env")
        self._http = httpx.Client(timeout=timeout)
        self._token: str | None = None
        self._expires_at: float = 0.0

    # ---- auth -----------------------------------------------------------

    def _authenticate(self) -> None:
        r = self._http.post(
            f"{self.url}/authenticate",
            json={
                "api_key": self.api_key,
                "client_db": self.client_db,
                "username": self.username,
            },
        )
        r.raise_for_status()
        body = r.json()
        self._token = body["access_token"]
        self._expires_at = time.time() + int(body.get("expires_in", 3600)) - 60

    def _ensure_token(self) -> str:
        if self._token is None or time.time() >= self._expires_at:
            self._authenticate()
        assert self._token is not None
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._ensure_token()}",
            "Accept": "application/json",
        }

    # ---- requests -------------------------------------------------------

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        url = f"{self.url}{path}" if path.startswith("/") else f"{self.url}/{path}"
        r = self._http.get(url, params=params, headers=self._headers())
        return r

    def get_binary(self, path: str) -> httpx.Response:
        """Same as get() but accepts non-JSON content (e.g. attached_files content)."""
        url = f"{self.url}{path}" if path.startswith("/") else f"{self.url}/{path}"
        r = self._http.get(
            url,
            headers={"Authorization": f"Bearer {self._ensure_token()}"},
            follow_redirects=True,
        )
        return r

    # ---- cursor pagination ---------------------------------------------

    @staticmethod
    def _extract_cursor(link: str | None) -> str | None:
        if not link:
            return None
        m = re.search(r'\$cursor=([^&>]+)', link)
        return m.group(1) if m else None

    def list_all(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        limit: int = 50,   # Citywide caps at 50
    ) -> Iterator[dict[str, Any]]:
        """Yield every record from a list endpoint, following Link cursors.

        params should be FILTER params only — pagination ($cursor/$page/$limit)
        is handled here.
        """
        base = dict(params or {})
        base["$limit"] = limit
        # Page 1 (no $page or $cursor on initial request)
        r = self.get(path, params=base)
        r.raise_for_status()
        batch = r.json()
        yield from batch
        total = int(r.headers.get("X-Total", "0"))
        cursor = self._extract_cursor(r.headers.get("Link"))
        if not cursor or not total or len(batch) >= total:
            return
        seen = len(batch)
        page = 2
        while seen < total:
            r = self.get(path, params={**base, "$cursor": cursor, "$page": page})
            if r.status_code != 200:
                break
            batch = r.json()
            if not batch:
                break
            yield from batch
            seen += len(batch)
            page += 1

    # ---- lifecycle ------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "CitywideClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
