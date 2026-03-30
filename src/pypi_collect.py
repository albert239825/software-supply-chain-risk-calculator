from __future__ import annotations

import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from tqdm import tqdm

from .utils import CsvWriter, HttpSession, parse_github_owner_repo, thread_local_http_session


TOP_PYPI_JSON = "https://hugovk.github.io/top-pypi-packages/top-pypi-packages.json"
PYPI_JSON = "https://pypi.org/pypi/{name}/json"


def iter_hugovk_top_packages(session: HttpSession, limit: int) -> list[dict[str, Any]]:
    """Top PyPI projects from hugovk list (used to pick top-N seed names)."""
    data = session.get_json(TOP_PYPI_JSON)
    rows_out: list[dict[str, Any]] = []
    with tqdm(total=limit, desc="pypi: hugovk seed list", unit="pkg") as pbar:
        for row in data.get("rows") or []:
            if isinstance(row, dict):
                p = row.get("project")
            elif isinstance(row, list) and len(row) >= 2:
                p = row[1]
            else:
                continue
            if not p or not isinstance(p, str):
                continue
            name = p.lower()
            rows_out.append(
                {
                    "ecosystem": "pypi",
                    "name": name,
                    "description": "",
                    "latest_version": "",
                }
            )
            pbar.update(1)
            if len(rows_out) >= limit:
                break
    return rows_out[:limit]


def fetch_pypi_package(session: HttpSession, name: str) -> dict[str, Any] | None:
    url = PYPI_JSON.format(name=name)
    try:
        return session.get_json(url)
    except Exception:
        return None


def _info(payload: dict[str, Any]) -> dict[str, Any]:
    inf = payload.get("info")
    return inf if isinstance(inf, dict) else {}


def _parse_requirement_name(req_line: str) -> str | None:
    s = req_line.strip()
    if not s:
        return None
    m = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)", s)
    if not m:
        return None
    base = m.group(1).lower().replace("_", "-")
    return base


def _latest_release_upload_time(payload: dict[str, Any], info: dict[str, Any]) -> str:
    latest = info.get("version") or ""
    releases = payload.get("releases") or {}
    if not latest or not isinstance(releases, dict):
        return ""
    rel_info = releases.get(latest)
    if isinstance(rel_info, list) and rel_info and isinstance(rel_info[0], dict):
        return rel_info[0].get("upload_time") or ""
    return ""


# Matches "Display Name <email@host>" segments (PyPI author/maintainer fields).
_NAME_EMAIL_CHUNK = re.compile(r"([^<]*?)\s*<([^>]+)>")


def _strip_outer_quotes(s: str) -> str:
    """Remove redundant ASCII double quotes from metadata (e.g. \"\"\"Name\"\"\" or \"Name\")."""
    s = s.strip()
    while len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].strip()
    return s


def _parse_name_email_pairs(s: str) -> list[tuple[str, str]]:
    """Extract (display_name, email) from strings containing Name <email> segments."""
    if not s or "<" not in s or ">" not in s:
        return []
    pairs: list[tuple[str, str]] = []
    for m in _NAME_EMAIL_CHUNK.finditer(s):
        nm = _strip_outer_quotes(m.group(1).strip().strip(",").strip())
        em = m.group(2).strip()
        pairs.append((nm, em))
    return pairs


def _split_pypi_people(name_field: str, email_field: str) -> list[tuple[str, str]]:
    """
    PyPI core metadata may store:
    - Multiple people in author_email only: 'Alice <a@x>, Bob <b@y>'
    - One RFC address in maintainer only: 'aiohttp team <team@aiohttp.org>' with email 'team@aiohttp.org'
    - Plain name + email without brackets.
    Returns one (display_name, email) per person.
    """
    name_field = (name_field or "").strip()
    email_field = (email_field or "").strip()
    if not name_field and not email_field:
        return []

    # 1) Structured addresses in *_email (multi-author packages).
    if email_field and "<" in email_field and ">" in email_field:
        pairs = _parse_name_email_pairs(email_field)
        if pairs:
            return pairs

    # 2) Structured address in name/maintainer only (email column is bare address).
    if name_field and "<" in name_field and ">" in name_field:
        pairs = _parse_name_email_pairs(name_field)
        if pairs:
            if len(pairs) == 1:
                n, e = pairs[0]
                if not email_field or email_field == e or email_field.lower() == e.lower():
                    return [(n, e)]
                # Bare email in email_field that matches parsed address, or org uses both columns.
                if "@" in email_field and "<" not in email_field:
                    return [(n, email_field)]
                return [(n, e)]
            return pairs

    # 3) Single-line name + email, no angle brackets.
    if name_field and email_field and "<" not in email_field:
        if "," not in name_field and "," not in email_field:
            return [(_strip_outer_quotes(name_field), email_field)]

    # 4) Parallel comma-separated lists: "A, B" and "a@x, b@y"
    names = [_strip_outer_quotes(x.strip()) for x in name_field.split(",") if x.strip()] if name_field else []
    emails = [x.strip() for x in email_field.split(",") if x.strip()] if email_field else []
    if names and emails and len(names) == len(emails):
        return list(zip(names, emails))

    # 5) Names only or emails only.
    if names and not emails:
        return [(n, "") for n in names]
    if emails and not names:
        return [("", e) for e in emails]

    # 6) Last resort: one row.
    if name_field or email_field:
        return [(_strip_outer_quotes(name_field), email_field)]
    return []


def _pypi_maintainer_rows(package_name: str, info: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for role, name_key, email_key in (
        ("author", "author", "author_email"),
        ("maintainer", "maintainer", "maintainer_email"),
    ):
        for person_name, email in _split_pypi_people(
            str(info.get(name_key) or ""),
            str(info.get(email_key) or ""),
        ):
            rows.append(
                {
                    "ecosystem": "pypi",
                    "package_name": package_name,
                    "username": "",
                    "name": person_name,
                    "role": role,
                    "email": email,
                }
            )
    return rows


def process_pypi_package(name: str, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Turn one /pypi/{name}/json payload into package, version, dependency, and maintainer rows."""
    packages_row: dict[str, Any] = {}
    version_row: dict[str, Any] = {}
    dep_edges: list[dict[str, Any]] = []
    maint_rows: list[dict[str, Any]] = []

    if not payload:
        return packages_row, version_row, dep_edges, maint_rows

    info = _info(payload)
    description = info.get("summary") or ""
    latest = info.get("version") or ""
    packages_row = {
        "ecosystem": "pypi",
        "name": name,
        "description": ((description or "").replace("\n", " ")[:2000]),
        "latest_version": latest,
    }

    maint_rows.extend(_pypi_maintainer_rows(name, info))

    urls = info.get("project_urls") or {}
    home = info.get("home_page") or ""
    gh = None
    if isinstance(urls, dict):
        for label in ("Source", "Source Code", "Repository", "Homepage"):
            u = urls.get(label)
            gh = parse_github_owner_repo(u) if u else None
            if gh:
                break
    if not gh and home:
        gh = parse_github_owner_repo(home)

    released = _latest_release_upload_time(payload, info)
    version_row = {
        "ecosystem": "pypi",
        "package_name": name,
        "version": latest,
        "released": released,
        "github_owner": gh[0] if gh else "",
        "github_repo": gh[1] if gh else "",
    }

    requires = info.get("requires_dist")
    if isinstance(requires, list) and latest:
        for line in requires:
            if not isinstance(line, str):
                continue
            dep_name = _parse_requirement_name(line.split(";")[0])
            if not dep_name:
                continue
            dep_edges.append(
                {
                    "ecosystem": "pypi",
                    "from_package": name,
                    "from_version": latest,
                    "to_package": dep_name,
                    "version_spec": line,
                    "dep_kind": "requires_dist",
                }
            )

    return packages_row, version_row, dep_edges, maint_rows


def _fetch_pypi_worker(package_name: str) -> dict[str, Any] | None:
    """Per-thread fetch (thread-local HttpSession; requests.Session is not thread-safe)."""
    return fetch_pypi_package(thread_local_http_session(), package_name)


def collect_pypi_graph(
    _session: HttpSession,
    seed_names: list[str],
    max_workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    BFS over requires_dist: drain the queue into a request batch, fetch in parallel with
    ThreadPoolExecutor.map, then enqueue only dependencies from this batch.
    """
    del _session

    packages_rows: dict[str, dict[str, Any]] = {}
    version_rows: list[dict[str, Any]] = []
    dep_edges: list[dict[str, Any]] = []
    maint_rows: list[dict[str, Any]] = []

    seen_pkg: set[str] = set()
    q: deque[tuple[str, int]] = deque((n.lower(), 0) for n in seed_names)

    tqdm.write(f"Collecting PyPI graph ({len(q)} seeds)")
    current_depth = -1
    request_queue: list[str] = []
    graph_pbar = tqdm(desc="pypi: warehouse graph", unit="pkg", dynamic_ncols=True)

    while len(q) > 0 or len(request_queue) > 0:
        if len(q) > 0:
            name, depth = q.popleft()
            if depth > current_depth:
                current_depth = depth
                graph_pbar.set_postfix(depth=current_depth)
            if name in seen_pkg:
                continue
            seen_pkg.add(name)
            request_queue.append(name)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                results = pool.map(_fetch_pypi_worker, request_queue)
                batch_edges: list[dict[str, Any]] = []
                for name, payload in zip(request_queue, results):
                    if not payload:
                        continue
                    pkg_row, ver_row, deps, mts = process_pypi_package(name, payload)
                    packages_rows[name] = pkg_row
                    version_rows.append(ver_row)
                    dep_edges.extend(deps)
                    maint_rows.extend(mts)
                    batch_edges.extend(deps)
                for dep in batch_edges:
                    if dep["to_package"] not in seen_pkg:
                        q.append((dep["to_package"], depth + 1))
                graph_pbar.update(len(request_queue))
                request_queue = []

    graph_pbar.close()
    return list(packages_rows.values()), version_rows, dep_edges, maint_rows


def run_pypi_collection(
    out_dir,
    top_n: int,
    max_workers: int,
) -> dict[str, Any]:
    session = HttpSession()
    seeds_meta = iter_hugovk_top_packages(session, limit=top_n)
    seed_names = [p["name"] for p in seeds_meta]
    pkgs, vers, edges, maints = collect_pypi_graph(session, seed_names, max_workers=max_workers)

    CsvWriter.write_csv(
        out_dir / "packages.csv",
        ["ecosystem", "name", "description", "latest_version"],
        pkgs,
    )
    CsvWriter.write_csv(
        out_dir / "versions.csv",
        ["ecosystem", "package_name", "version", "released", "github_owner", "github_repo"],
        vers,
    )
    CsvWriter.write_csv(
        out_dir / "dependencies.csv",
        [
            "ecosystem",
            "from_package",
            "from_version",
            "to_package",
            "version_spec",
            "dep_kind",
        ],
        edges,
    )
    CsvWriter.write_csv(
        out_dir / "maintainers.csv",
        ["ecosystem", "package_name", "username", "name", "role", "email"],
        maints,
    )

    return {
        "seed_count": len(seeds_meta),
        "packages_collected": len(pkgs),
        "versions_rows": len(vers),
        "dependency_edges": len(edges),
        "maintainer_rows": len(maints),
    }
