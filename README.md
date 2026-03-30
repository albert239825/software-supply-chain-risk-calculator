# CIS 5500 — dependency graph data collection

Fetches NPM and/or PyPI package metadata and dependency graphs into CSVs under `data/csv/` (or `--out`).

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
python collect_data.py --npm --pypi --out data/csv
```

Use `--npm` and/or `--pypi`. Optional: `--top-n 100` (seed count), `--workers 32` (parallel requests per BFS batch).

## Output files (in `--out`)

| File | Brief description |
|------|-------------------|
| `packages.csv` | One row per package discovered in the graph; ecosystem tag plus name and short text metadata. |
| `versions.csv` | One row per package’s **latest** resolved version and optional GitHub repo link from package metadata. |
| `dependencies.csv` | Directed edges: a package/version depends on another package (semver or PEP 508 spec). |
| `maintainers.csv` | People linked to a package (NPM registry maintainers; PyPI authors/maintainers). |

### Schema (columns)

**`packages.csv`**

- `ecosystem` — `npm` or `pypi`
- `name` — package name
- `description` — readme-style blurb (PyPI uses the project summary here)
- `latest_version` — semver string for the latest release used in the graph

**`versions.csv`**

- `ecosystem`, `package_name`, `version`, `released` — release time / publish time when available
- `has_repository` — *(NPM only)* whether a repo URL was present on that version
- `github_owner`, `github_repo` — parsed GitHub location when metadata allows

**`dependencies.csv`**

- `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind` — edge from one package version to a dependency (NPM: dependency / peer / optional; PyPI: `requires_dist`)

**`maintainers.csv`**

- `ecosystem`, `package_name`
- `username` — NPM maintainer login *(empty for PyPI)*
- `name` — display name *(PyPI; NPM may leave empty and use `username`)*
- `role` — e.g. `maintainer`, `author` (PyPI)
- `email` — contact when provided
