"""Minimal client for the PSD Citywide Asset Management REST API.

Handles:
  * POST /authenticate exchange (api_key + client_db + username -> bearer token)
  * Auto-refresh of the bearer token before expiry
  * Token-bucket rate limiting (server cap is 1000 requests/hour; we cap
    ourselves at 900 to leave headroom)
  * Cursor pagination as exposed by Citywide:
        page 1: send only filters (e.g. {"profile_id": 337})
        page 2+: send {"$cursor": <from Link header>, "$page": N}
        the X-Total response header gives the total count
  * Exponential backoff on 429 / 5xx, honouring Retry-After
"""

from __future__ import annotations

import collections
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DEFAULT_URL = "https://v4.citywidesolutions.com/v4_server/external/v1"


class _RateLimiter:
    """Sliding-window token bucket: at most `max_calls` per `window` seconds."""

    def __init__(self, max_calls: int, window: float = 3600.0) -> None:
        self.max_calls = max_calls
        self.window = window
        self._calls: collections.deque[float] = collections.deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] > self.window:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                wait = self.window - (now - self._calls[0]) + 0.5
            time.sleep(max(0.1, wait))


class CitywideClient:
    def __init__(
        self,
        api_key: str | None = None,
        client_db: str | None = None,
        username: str | None = None,
        url: str | None = None,
        timeout: float = 120.0,
        max_calls_per_hour: int = 900,
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
        self._limiter = _RateLimiter(max_calls=max_calls_per_hour, window=3600)

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

    def _request_with_retry(self, url: str, headers: dict[str, str],
                            params: dict[str, Any] | None = None,
                            max_attempts: int = 8,
                            max_backoff: float = 300.0) -> httpx.Response:
        """GET with exponential backoff on 429 / 5xx / network errors.
        Acquires a rate-limit token before each attempt.
        """
        attempt = 0
        while True:
            attempt += 1
            self._limiter.acquire()
            try:
                r = self._http.get(url, params=params, headers=headers,
                                   follow_redirects=True)
            except (httpx.ReadTimeout, httpx.ConnectTimeout,
                    httpx.RemoteProtocolError):
                if attempt >= max_attempts:
                    raise
                wait = min(max_backoff, 2 ** attempt) + random.random()
                time.sleep(wait)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if attempt >= max_attempts:
                    return r
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else min(max_backoff, 2 ** attempt)
                except ValueError:
                    wait = min(max_backoff, 2 ** attempt)
                # Cap the wait so a single bad response doesn't freeze us forever.
                wait = min(wait, max_backoff) + random.random()
                print(f"    [{r.status_code}] Retry-After={ra!r}, sleeping {wait:.0f}s "
                      f"(attempt {attempt}/{max_attempts})", flush=True)
                time.sleep(wait)
                continue
            return r

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        url = f"{self.url}{path}" if path.startswith("/") else f"{self.url}/{path}"
        return self._request_with_retry(url, self._headers(), params)

    def get_binary(self, path: str) -> httpx.Response:
        """Same as get() but accepts non-JSON content (e.g. attached_files content)."""
        url = f"{self.url}{path}" if path.startswith("/") else f"{self.url}/{path}"
        return self._request_with_retry(
            url, {"Authorization": f"Bearer {self._ensure_token()}"})

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
        progress: bool = True,
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
        if progress:
            print(f"    listing {path}: {len(batch)}/{total}", flush=True)
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
            if progress and (page % 5 == 0 or seen >= total):
                print(f"    listing {path}: {seen}/{total}", flush=True)
            page += 1

    # ---- lifecycle ------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "CitywideClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
