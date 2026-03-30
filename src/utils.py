from __future__ import annotations

import csv
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import requests

_thread_local = threading.local()

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class CsvWriter:
    """Incremental CSV writer for large exports (avoids holding all rows in memory)."""

    def __init__(self, path: Path, fieldnames: list[str]) -> None:
        ensure_dir(path.parent)
        self.path = path
        self.fieldnames = fieldnames
        self._f = path.open("w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=fieldnames, extrasaction="ignore")
        self._w.writeheader()
        self.rows = 0

    def write(self, row: dict[str, Any]) -> None:
        self._w.writerow(row)
        self.rows += 1

    def close(self) -> None:
        self._f.close()

    def __enter__(self) -> "CsvWriter":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
        
    @staticmethod
    def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
        ensure_dir(path.parent)
        rows = list(rows)
        if not rows:
            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
            return
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                w.writerow(row)


GITHUB_HOSTS = {"github.com", "www.github.com"}


def parse_github_owner_repo(repository: Any) -> tuple[str, str] | None:
    """
    Extract (owner, repo) from npm/PyPI-style repository fields.
    Accepts str, or dict with 'url' / 'directory'.
    """
    if repository is None:
        return None
    if isinstance(repository, dict):
        url = repository.get("url") or repository.get("browse")
        if not url:
            return None
        repository = url
    if not isinstance(repository, str):
        return None
    s = repository.strip()
    # github:user/repo
    m = re.match(r"^github:([^/]+)/([^/]+?)(?:\.git)?$", s, re.I)
    if m:
        return m.group(1), re.sub(r"\.git$", "", m.group(2))
    # strip git+ prefix and .git suffix for URL parsing
    s = re.sub(r"^git\+", "", s)
    s = re.sub(r"\.git$", "", s, flags=re.I)
    try:
        u = urlparse(s)
    except Exception:
        return None
    host = (u.netloc or "").lower()
    if host in GITHUB_HOSTS or host.endswith(".github.com"):
        parts = [p for p in u.path.split("/") if p]
        if len(parts) >= 2:
            return parts[0], re.sub(r"\.git$", "", parts[1], flags=re.I)
    # ssh git@github.com:owner/repo
    m = re.match(r"^git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$", s, re.I)
    if m:
        return m.group(1), m.group(2)
    return None


class HttpSession:
    """Thin wrapper with basic backoff for 429/503."""

    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(
            {
                "User-Agent": "cis5500-data-collection/1.0 (educational)",
                "Accept": "application/json",
            }
        )

    def get_json(self, url: str, *, headers: dict[str, str] | None = None, max_retries: int = 5) -> Any:
        h = dict(headers or {})
        last_err: Exception | None = None
        for attempt in range(max_retries):
            try:
                r = self.s.get(url, headers=h, timeout=60)
            except requests.RequestException as e:
                last_err = e
                time.sleep(min(2**attempt, 30))
                continue
            if r.status_code == 429 or r.status_code == 503:
                wait = float(r.headers.get("Retry-After") or (2**attempt))
                time.sleep(min(wait, 60))
                continue
            r.raise_for_status()
            if not r.content:
                return None
            return r.json()
        if last_err:
            raise last_err
        raise RuntimeError(f"GET failed after retries: {url}")


def github_headers() -> dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}


def thread_local_http_session() -> HttpSession:
    """One HttpSession per worker thread (requests.Session is not thread-safe)."""
    if not hasattr(_thread_local, "http"):
        _thread_local.http = HttpSession()
    return _thread_local.http
