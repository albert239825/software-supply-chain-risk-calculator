# CIS 5500 — dependency graph data collection

Fetches NPM and/or PyPI package metadata and dependency graphs in two stages:

1. **Raw** — collector CSVs under `<out>/raw/<run_id>/<ecosystem>/` (immutable per run; `run_id` is UTC `YYYYMMDDTHHMMSSZ`).
2. **Clean** — normalized, deduplicated CSVs plus `manifest.json` under `<out>/clean/` (latest import-ready snapshot).

`--out` (default `data`) is the only output root; the script creates `raw/` and `clean/` under it. One command runs collection and then the clean step.

## Setup

**Conda (recommended)**

```bash
conda env create -f environment.yml
conda activate cis5500
```

**pip only**

```bash
python3 -m venv .venv && source .venv/bin/activate   # optional
pip install -r requirements.txt
```

## Run

```bash
python collect_data.py --npm --pypi
```

Use `--npm` and/or `--pypi`. Optional:

| Flag | Default | Meaning |
|------|---------|---------|
| `--out` | `data` | Output root; subdirs `raw/<run_id>/` and `clean/` are created automatically |
| `--top-n` | `100` | Number of seed packages (per ecosystem you enable) |
| `--workers` | `32` | Max concurrent HTTP requests per BFS level when fetching registry/PyPI JSON |

Examples:

```bash
python collect_data.py --npm --top-n 200
python collect_data.py --pypi --out ./artifacts
```

## Output layout

**Raw (per run)** — under `<out>/raw/<run_id>/`:

- `npm/` — four CSVs if `--npm` was used (same schema as historical single-folder export).
- `pypi/` — four CSVs if `--pypi` was used.

NPM and PyPI no longer overwrite each other; each ecosystem writes to its own subfolder.

**Clean (latest)** — under `<out>/clean/`:

| File | Description |
|------|-------------|
| `packages_clean.csv` | One row per package; includes deterministic `id` (UUIDv5). |
| `versions_clean.csv` | One row per resolved package version; `package_id` FK to packages. |
| `dependencies_clean.csv` | Directed edges; `from_version_id` / `to_package_id` helpers for joins. |
| `maintainers_clean.csv` | Maintainer/author rows; `package_id` when the package exists. |
| `manifest.json` | `run_id`, ecosystems, `top_n`, `workers`, raw/clean row counts, unresolved reference counts, paths. |

### Schema (columns)

**`packages.csv`**

- `ecosystem` — `npm` or `pypi`
- `name` — package name
- `description` — readme-style blurb (PyPI uses the project summary here)
- `latest_version` — semver string for the latest release used in the graph

**`versions.csv`**

- `ecosystem`, `package_name`, `version`, `released`
- `has_repository` — *(NPM only in raw files)* whether a repo URL was present; PyPI raw rows omit this column (clean fills empty)
- `github_owner`, `github_repo` — parsed GitHub location when metadata allows

**`dependencies.csv`**

- `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind` — edge from one package version to a dependency (NPM: dependency / peer / optional; PyPI: `requires_dist`)

**`maintainers.csv`**

- `ecosystem`, `package_name`, `username`, `name`, `role`, `email`

### Clean file schemas (normalized)

Global rules: `ecosystem` lowercased; package names lowercased; PyPI names use `-` instead of `_`; strings trimmed; deterministic sort order; IDs are UUIDv5 strings in a fixed project namespace.

**`packages_clean.csv`**: `id`, `ecosystem`, `name`, `description`, `latest_version` — unique on `(ecosystem, name)`.

**`versions_clean.csv`**: `id`, `package_id`, `ecosystem`, `package_name`, `version`, `released`, `has_repository`, `github_owner`, `github_repo` — unique on `(ecosystem, package_name, version)`.

**`dependencies_clean.csv`**: `id`, `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind`, `from_version_id`, `to_package_id` — unique on the natural edge key; stub package rows are added so `to_package_id` usually resolves when the name appears in the graph.

**`maintainers_clean.csv`**: `id`, `ecosystem`, `package_name`, `package_id`, `username`, `name`, `role`, `email` — deduped on `(ecosystem, package_name, role, username, name, email)`.

See `manifest.json` after each run for row counts and `unresolved_*` diagnostics.
