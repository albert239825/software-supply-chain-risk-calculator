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
