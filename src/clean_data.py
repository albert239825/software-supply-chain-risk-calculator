"""Normalize raw collector CSVs into deterministic clean tables for import (e.g. Supabase)."""

from __future__ import annotations

import csv
import json
import re
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from .utils import ensure_dir

# Fixed namespace for deterministic UUIDv5 ids (project-specific).
_ID_NAMESPACE = uuid.UUID("018d0000-0000-7000-8000-000000000001")

REQUIRED_PACKAGES = ("ecosystem", "name", "description", "latest_version")
REQUIRED_DEPS = ("ecosystem", "from_package", "from_version", "to_package", "version_spec", "dep_kind")
REQUIRED_MAINT = ("ecosystem", "package_name", "username", "name", "role", "email")
REQUIRED_VERSIONS = ("ecosystem", "package_name", "version")

PACKAGES_CLEAN_FIELDS = ("id", "ecosystem", "name", "description", "latest_version")
VERSIONS_CLEAN_FIELDS = (
    "id",
    "package_id",
    "ecosystem",
    "package_name",
    "version",
    "released",
    "has_repository",
    "github_owner",
    "github_repo",
)
DEPS_CLEAN_FIELDS = (
    "id",
    "ecosystem",
    "from_package",
    "from_version",
    "to_package",
    "version_spec",
    "dep_kind",
    "from_version_id",
    "to_package_id",
)
MAINT_CLEAN_FIELDS = ("id", "ecosystem", "package_name", "package_id", "username", "name", "role", "email")


def _stable_id(*parts: str) -> str:
    return str(uuid.uuid5(_ID_NAMESPACE, "\x1f".join(parts)))


def _norm_ws(s: str) -> str:
    t = (s or "").strip()
    t = re.sub(r"\s+", " ", t.replace("\n", " ").replace("\r", " "))
    return t


def _norm_ecosystem(s: str) -> str:
    e = (s or "").strip().lower()
    if e not in ("npm", "pypi"):
        raise ValueError(f"Invalid ecosystem: {s!r}")
    return e


def _norm_package_name(ecosystem: str, name: str) -> str:
    n = (name or "").strip().lower()
    if ecosystem == "pypi":
        n = n.replace("_", "-")
    return n


def _norm_version(v: str) -> str:
    return (v or "").strip()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return []
        return [dict(row) for row in r]


def _validate_columns(rows: list[dict[str, str]], required: tuple[str, ...], label: str) -> None:
    if not rows:
        return
    for col in required:
        if col not in rows[0]:
            raise ValueError(f"{label}: missing required column {col!r}")


def _merge_packages(group: list[dict[str, str]]) -> dict[str, str]:
    order = list(range(len(group)))

    def desc_len(i: int) -> int:
        return len((group[i].get("description") or "").strip())

    nonempty_desc = [i for i in order if (group[i].get("description") or "").strip()]
    if nonempty_desc:
        best_i = max(nonempty_desc, key=lambda i: (desc_len(i), -i))
        best_desc = _norm_ws(group[best_i].get("description") or "")[:2000]
    else:
        best_desc = _norm_ws(group[0].get("description") or "")[:2000]

    ver_nonempty = [i for i in order if (group[i].get("latest_version") or "").strip()]
    ver_i = ver_nonempty[0] if ver_nonempty else 0
    latest = _norm_version(group[ver_i].get("latest_version") or "")

    eco = _norm_ecosystem(group[0]["ecosystem"])
    name = _norm_package_name(eco, group[0]["name"])
    return {
        "ecosystem": eco,
        "name": name,
        "description": best_desc,
        "latest_version": latest,
    }


def _merge_versions(group: list[dict[str, str]]) -> dict[str, str]:
    order = list(range(len(group)))

    def has_rel(i: int) -> bool:
        return bool((group[i].get("released") or "").strip())

    def gh_score(i: int) -> tuple[int, int]:
        o = (group[i].get("github_owner") or "").strip()
        r = (group[i].get("github_repo") or "").strip()
        return (len(o) + len(r), 1 if (o or r) else 0)

    rel_idx = max(order, key=lambda i: (has_rel(i), gh_score(i), -i))
    g_idx = max(order, key=lambda i: (gh_score(i), has_rel(i), -i))

    eco = _norm_ecosystem(group[0]["ecosystem"])
    pkg = _norm_package_name(eco, group[0]["package_name"])
    ver = _norm_version(group[0]["version"] or "")

    released = (group[rel_idx].get("released") or "").strip()
    gh_o = (group[g_idx].get("github_owner") or "").strip()
    gh_r = (group[g_idx].get("github_repo") or "").strip()

    hr_raw = group[0].get("has_repository")
    if hr_raw is None or str(hr_raw).strip() == "":
        has_repo = "false"
    else:
        s = str(hr_raw).strip().lower()
        if s in ("true", "1", "yes"):
            has_repo = "true"
        elif s in ("false", "0", "no"):
            has_repo = "false"
        else:
            has_repo = "false"

    return {
        "ecosystem": eco,
        "package_name": pkg,
        "version": ver,
        "released": released,
        "has_repository": has_repo,
        "github_owner": gh_o,
        "github_repo": gh_r,
    }


def clean_raw_run(
    *,
    raw_run_dir: Path,
    clean_dir: Path,
    run_id: str,
    ecosystems: list[str],
    top_n: int,
    workers: int,
) -> dict[str, Any]:
    """
    Read raw CSVs from ``raw_run_dir/<ecosystem>/``, merge ecosystems, write clean CSVs + manifest.

    Returns summary dict for CLI printing.
    """
    all_pkg: list[dict[str, str]] = []
    all_ver: list[dict[str, str]] = []
    all_dep: list[dict[str, str]] = []
    all_maint: list[dict[str, str]] = []

    for eco in ecosystems:
        sub = raw_run_dir / eco
        p_rows = _read_csv(sub / "packages.csv")
        _validate_columns(p_rows, REQUIRED_PACKAGES, f"{eco}/packages.csv")
        v_rows = _read_csv(sub / "versions.csv")
        _validate_columns(v_rows, REQUIRED_VERSIONS, f"{eco}/versions.csv")
        d_rows = _read_csv(sub / "dependencies.csv")
        _validate_columns(d_rows, REQUIRED_DEPS, f"{eco}/dependencies.csv")
        m_rows = _read_csv(sub / "maintainers.csv")
        _validate_columns(m_rows, REQUIRED_MAINT, f"{eco}/maintainers.csv")

        for row in p_rows:
            row["ecosystem"] = _norm_ecosystem(row["ecosystem"])
            row["name"] = _norm_package_name(row["ecosystem"], row.get("name") or "")
            row["description"] = _norm_ws(row.get("description") or "")[:2000]
            row["latest_version"] = _norm_version(row.get("latest_version") or "")
        for row in v_rows:
            row["ecosystem"] = _norm_ecosystem(row["ecosystem"])
            row["package_name"] = _norm_package_name(row["ecosystem"], row.get("package_name") or "")
            row["version"] = _norm_version(row.get("version") or "")
            row["released"] = (row.get("released") or "").strip()
            if "has_repository" not in row:
                row["has_repository"] = "false"
            row["github_owner"] = (row.get("github_owner") or "").strip()
            row["github_repo"] = (row.get("github_repo") or "").strip()
        for row in d_rows:
            row["ecosystem"] = _norm_ecosystem(row["ecosystem"])
            fp = _norm_package_name(row["ecosystem"], row.get("from_package") or "")
            tp = _norm_package_name(row["ecosystem"], row.get("to_package") or "")
            row["from_package"] = fp
            row["from_version"] = _norm_version(row.get("from_version") or "")
            row["to_package"] = tp
            row["version_spec"] = _norm_ws(row.get("version_spec") or "")
            row["dep_kind"] = _norm_ws(row.get("dep_kind") or "")
        for row in m_rows:
            row["ecosystem"] = _norm_ecosystem(row["ecosystem"])
            row["package_name"] = _norm_package_name(row["ecosystem"], row.get("package_name") or "")
            row["username"] = (row.get("username") or "").strip()
            row["name"] = _norm_ws(row.get("name") or "")
            row["role"] = _norm_ws(row.get("role") or "")
            row["email"] = (row.get("email") or "").strip()

        all_pkg.extend(p_rows)
        all_ver.extend(v_rows)
        all_dep.extend(d_rows)
        all_maint.extend(m_rows)

    # Dedupe packages
    pkg_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in all_pkg:
        k = (row["ecosystem"], row["name"])
        pkg_groups[k].append(row)
    packages_out: list[dict[str, str]] = []
    for k in sorted(pkg_groups.keys()):
        merged = _merge_packages(pkg_groups[k])
        pid = _stable_id("pkg", merged["ecosystem"], merged["name"])
        packages_out.append(
            {
                "id": pid,
                "ecosystem": merged["ecosystem"],
                "name": merged["name"],
                "description": merged["description"],
                "latest_version": merged["latest_version"],
            }
        )

    pkg_by_key = {(r["ecosystem"], r["name"]): r["id"] for r in packages_out}

    needed: set[tuple[str, str]] = set(pkg_by_key.keys())
    for row in all_ver:
        needed.add((row["ecosystem"], row["package_name"]))
    for row in all_maint:
        needed.add((row["ecosystem"], row["package_name"]))
    for row in all_dep:
        needed.add((row["ecosystem"], row["from_package"]))
        needed.add((row["ecosystem"], row["to_package"]))

    for k in sorted(needed):
        if k not in pkg_by_key:
            pid = _stable_id("pkg", k[0], k[1])
            packages_out.append(
                {
                    "id": pid,
                    "ecosystem": k[0],
                    "name": k[1],
                    "description": "",
                    "latest_version": "",
                }
            )
            pkg_by_key[k] = pid

    packages_out.sort(key=lambda r: (r["ecosystem"], r["name"]))
    pkg_by_key = {(r["ecosystem"], r["name"]): r["id"] for r in packages_out}

    # Dedupe versions
    ver_groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in all_ver:
        k = (row["ecosystem"], row["package_name"], row["version"])
        ver_groups[k].append(row)
    versions_out: list[dict[str, str]] = []
    for k in sorted(ver_groups.keys()):
        merged = _merge_versions(ver_groups[k])
        eco, pn, ver = merged["ecosystem"], merged["package_name"], merged["version"]
        pkg_id = pkg_by_key.get((eco, pn))
        if not pkg_id:
            raise RuntimeError(f"Internal error: missing package for version {k!r}")
        vid = _stable_id("ver", eco, pn, ver)
        versions_out.append(
            {
                "id": vid,
                "package_id": pkg_id,
                "ecosystem": eco,
                "package_name": pn,
                "version": ver,
                "released": merged["released"],
                "has_repository": merged["has_repository"],
                "github_owner": merged["github_owner"],
                "github_repo": merged["github_repo"],
            }
        )
    versions_out.sort(key=lambda r: (r["ecosystem"], r["package_name"], r["version"]))
    ver_by_key = {(r["ecosystem"], r["package_name"], r["version"]): r["id"] for r in versions_out}

    # Dedupe dependencies
    dep_groups: dict[tuple[str, str, str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in all_dep:
        k = (
            row["ecosystem"],
            row["from_package"],
            row["from_version"],
            row["to_package"],
            row["dep_kind"],
            row["version_spec"],
        )
        dep_groups[k].append(row)

    unresolved_to_pkg = 0
    unresolved_from_ver = 0
    deps_out: list[dict[str, str]] = []
    for k in sorted(dep_groups.keys()):
        row = dep_groups[k][0]
        eco, fp, fv, tp = row["ecosystem"], row["from_package"], row["from_version"], row["to_package"]
        fvid = ver_by_key.get((eco, fp, fv), "")
        if not fvid:
            unresolved_from_ver += 1
        to_pid = pkg_by_key.get((eco, tp), "")
        if not to_pid:
            unresolved_to_pkg += 1
        did = _stable_id("dep", *k)
        deps_out.append(
            {
                "id": did,
                "ecosystem": eco,
                "from_package": fp,
                "from_version": fv,
                "to_package": tp,
                "version_spec": row["version_spec"],
                "dep_kind": row["dep_kind"],
                "from_version_id": fvid,
                "to_package_id": to_pid,
            }
        )
    deps_out.sort(
        key=lambda r: (
            r["ecosystem"],
            r["from_package"],
            r["from_version"],
            r["to_package"],
            r["dep_kind"],
            r["version_spec"],
        )
    )

    # Dedupe maintainers
    maint_groups: dict[tuple[str, str, str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in all_maint:
        k = (
            row["ecosystem"],
            row["package_name"],
            row["role"],
            row["username"],
            row["name"],
            row["email"],
        )
        maint_groups[k].append(row)

    unresolved_maint_pkg = 0
    maint_out: list[dict[str, str]] = []
    for k in sorted(maint_groups.keys()):
        row = maint_groups[k][0]
        eco, pn = row["ecosystem"], row["package_name"]
        pkg_id = pkg_by_key.get((eco, pn), "")
        if not pkg_id:
            unresolved_maint_pkg += 1
        mid = _stable_id("maint", *k)
        maint_out.append(
            {
                "id": mid,
                "ecosystem": eco,
                "package_name": pn,
                "package_id": pkg_id,
                "username": row["username"],
                "name": row["name"],
                "role": row["role"],
                "email": row["email"],
            }
        )
    maint_out.sort(key=lambda r: (r["ecosystem"], r["package_name"], r["role"], r["username"], r["name"], r["email"]))

    ensure_dir(clean_dir)
    _write_csv(clean_dir / "packages_clean.csv", PACKAGES_CLEAN_FIELDS, packages_out)
    _write_csv(clean_dir / "versions_clean.csv", VERSIONS_CLEAN_FIELDS, versions_out)
    _write_csv(clean_dir / "dependencies_clean.csv", DEPS_CLEAN_FIELDS, deps_out)
    _write_csv(clean_dir / "maintainers_clean.csv", MAINT_CLEAN_FIELDS, maint_out)

    raw_counts = {
        "packages": len(all_pkg),
        "versions": len(all_ver),
        "dependencies": len(all_dep),
        "maintainers": len(all_maint),
    }
    clean_counts = {
        "packages": len(packages_out),
        "versions": len(versions_out),
        "dependencies": len(deps_out),
        "maintainers": len(maint_out),
    }

    manifest = {
        "run_id": run_id,
        "ecosystems": ecosystems,
        "top_n": top_n,
        "workers": workers,
        "raw_run_dir": str(raw_run_dir.resolve()),
        "clean_dir": str(clean_dir.resolve()),
        "raw_row_counts": raw_counts,
        "clean_row_counts": clean_counts,
        "unresolved_dependencies_to_package_id": unresolved_to_pkg,
        "unresolved_dependencies_from_version_id": unresolved_from_ver,
        "unresolved_maintainers_package_id": unresolved_maint_pkg,
    }
    (clean_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    return {
        "manifest": manifest,
        "clean_counts": clean_counts,
        "raw_counts": raw_counts,
    }


def _write_csv(path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
