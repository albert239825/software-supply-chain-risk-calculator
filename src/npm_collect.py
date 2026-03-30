from __future__ import annotations

from collections import deque
from typing import Any

from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from .utils import CsvWriter, HttpSession, parse_github_owner_repo, thread_local_http_session


NPMS_SEARCH = "https://api.npms.io/v2/search"
REGISTRY_PKG = "https://registry.npmjs.org/{name}"


def iter_npms_packages(session: HttpSession, limit: int) -> list[dict[str, Any]]:
    """npms search hits with package metadata (used to pick top-N seed names)."""
    rows: list[dict[str, Any]] = []
    page = 0
    page_size = min(100, max(1, limit))
    with tqdm(total=limit, desc="npm: npms.io seeds", unit="pkg") as pbar:
        while len(rows) < limit:
            url = f"{NPMS_SEARCH}?q=not:deprecated&size={page_size}&from={page * page_size}"
            data = session.get_json(url)
            results = data.get("results") or []
            if not results:
                break
            for item in results:
                pkg = item.get("package") or {}
                name = pkg.get("name")
                if not name:
                    continue
                rows.append(
                    {
                        "ecosystem": "npm",
                        "name": name,
                        "description": ((pkg.get("description") or "")[:2000]),
                        "latest_version": (pkg.get("version") or ""),
                    }
                )
                pbar.update(1)
                if len(rows) >= limit:
                    break
            page += 1
            if len(results) < page_size:
                break
    return rows[:limit]


def _latest_version(meta: dict[str, Any]) -> str | None:
    dist_tags = meta.get("dist-tags") or {}
    return dist_tags.get("latest")


def _version_doc(meta: dict[str, Any], version: str) -> dict[str, Any]:
    versions = meta.get("versions") or {}
    doc = versions.get(version)
    return doc if isinstance(doc, dict) else {}


def fetch_registry_package(session: HttpSession, name: str) -> dict[str, Any] | None:
    url = REGISTRY_PKG.format(name=name)
    try:
        return session.get_json(url)
    except Exception:
        return None


def _fetch_registry_worker(package_name: str) -> dict[str, Any] | None:
    return fetch_registry_package(thread_local_http_session(), package_name)


def process_registry_package(name: str, meta: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    packages_row: dict[str, dict[str, Any]] = {}
    version_row: list[dict[str, Any]] = []
    dep_edges: list[dict[str, Any]] = []
    maint_rows: list[dict[str, Any]] = []
    
    if not meta:
        return packages_row, version_row, dep_edges, maint_rows

    latest = _latest_version(meta)
    desc = meta.get("description")

    packages_row = {
        "ecosystem": "npm",
        "name": name,
        "description": ((desc or "").replace("\n", " ")[:2000]),
        "latest_version": latest or "",
    }

    for m in meta.get("maintainers") or []:
        if isinstance(m, dict):
            maint_rows.append(
                {
                    "ecosystem": "npm",
                    "package_name": name,
                    "username": m.get("name") or "",
                    "name": "",
                    "role": "maintainer",
                    "email": m.get("email") or "",
                }
            )

    doc = _version_doc(meta, latest)
    t = (meta.get("time") or {}).get(latest) if isinstance(meta.get("time"), dict) else None
    repo = doc.get("repository")
    gh = parse_github_owner_repo(repo)
    version_row = {
        "ecosystem": "npm",
        "package_name": name,
        "version": latest,
        "released": t or "",
        "has_repository": bool(repo),
        "github_owner": gh[0] if gh else "",
        "github_repo": gh[1] if gh else "",
    }
    

    dep_sections = [
        ("dependencies", doc.get("dependencies")),
        ("peerDependencies", doc.get("peerDependencies")),
        ("optionalDependencies", doc.get("optionalDependencies")),
    ]
    for dep_type, blob in dep_sections:
        if not isinstance(blob, dict):
            continue
        for dep_name, spec in blob.items():
            dep_edges.append(
                {
                    "ecosystem": "npm",
                    "from_package": name,
                    "from_version": latest,
                    "to_package": dep_name,
                    "version_spec": spec,
                    "dep_kind": dep_type,
                }
            )
    return packages_row, version_row, dep_edges, maint_rows


def collect_npm_graph(
    session: HttpSession,
    seed_names: list[str],
    max_workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    BFS over dependency names (prod + peer + optional) until the frontier is empty.
    Returns: packages_rows, version_rows, dep_edges, maintainer_rows
    """
    packages_rows: dict[str, dict[str, Any]] = {}
    version_rows: list[dict[str, Any]] = []
    dep_edges: list[dict[str, Any]] = []
    maint_rows: list[dict[str, Any]] = []

    seen_pkg: set[str] = set()
    q: deque[tuple[str, int]] = deque((n, 0) for n in seed_names)

    tqdm.write(f"Collecting NPM graph ({len(q)} seeds)")
    current_depth = -1
    request_queue = []
    graph_pbar = tqdm(desc="npm: registry graph", unit="pkg", dynamic_ncols=True)
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
                results = pool.map(_fetch_registry_worker, request_queue)
                batch_edges: list[dict[str, Any]] = []
                for name, meta in zip(request_queue, results):
                    if meta:
                        packages_row, version_row, deps, maints = process_registry_package(name, meta)
                        packages_rows[name] = packages_row
                        version_rows.append(version_row)
                        dep_edges.extend(deps)
                        maint_rows.extend(maints)
                        batch_edges.extend(deps)
                for dep in batch_edges:
                    if dep["to_package"] not in seen_pkg:
                        q.append((dep["to_package"], depth + 1))
                graph_pbar.update(len(request_queue))
                request_queue = []

    graph_pbar.close()
    return list(packages_rows.values()), version_rows, dep_edges, maint_rows

def run_npm_collection(
    out_dir,
    top_n: int,
    max_workers: int,
) -> dict[str, Any]:
    session = HttpSession()
    pkgs_npms = iter_npms_packages(session, limit=top_n)
    seed_names = [p["name"] for p in pkgs_npms]
    pkgs, vers, edges, maints = collect_npm_graph(session, seed_names, max_workers=max_workers)

    CsvWriter.write_csv(
        out_dir / "packages.csv",
        ["ecosystem", "name", "description", "latest_version"],
        pkgs,
    )
    CsvWriter.write_csv(
        out_dir / "versions.csv",
        [
            "ecosystem",
            "package_name",
            "version",
            "released",
            "has_repository",
            "github_owner",
            "github_repo",
        ],
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
        "seed_count": len(pkgs_npms),
        "packages_collected": len(pkgs),
        "versions_rows": len(vers),
        "dependency_edges": len(edges),
        "maintainer_rows": len(maints),
    }
