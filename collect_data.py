#!/usr/bin/env python3
"""
CLI: NPM / PyPI data -> <out>/raw/<run_id>/<ecosystem>/, then <out>/clean/.

Environment (conda):
  conda env create -f environment.yml
  conda activate cis5500

Examples:
  python collect_data.py --npm --pypi
  python collect_data.py --npm --top-n 200 --out data
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root))

    from src.clean_data import clean_raw_run
    from src.npm_collect import run_npm_collection
    from src.pypi_collect import run_pypi_collection
    from src.utils import ensure_dir

    p = argparse.ArgumentParser(
        description="Collect NPM / PyPI dependency graphs: raw CSVs per run under <out>/raw/, then clean CSVs under <out>/clean/."
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data"),
        help="Output root; creates raw/<run_id>/ and clean/ under it (default: data)",
    )
    p.add_argument("--npm", action="store_true", help="Collect NPM (npms.io seeds + registry BFS)")
    p.add_argument("--pypi", action="store_true", help="Collect PyPI (hugovk seeds + Warehouse JSON BFS)")
    p.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Number of top packages to use as seeds; full dependency graph is expanded from them",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=32,
        metavar="N",
        help="Max concurrent HTTP requests per BFS level when fetching registry/PyPI JSON (default: 32)",
    )

    args = p.parse_args()
    if not (args.npm or args.pypi):
        p.error("Select at least one of --npm or --pypi")

    out = args.out
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_run_dir = out / "raw" / run_id
    clean_dir = out / "clean"
    ensure_dir(raw_run_dir)
    ensure_dir(clean_dir)

    output_files = [
        "packages.csv",
        "versions.csv",
        "dependencies.csv",
        "maintainers.csv",
    ]

    summary: dict[str, object] = {}
    ecosystems_ran: list[str] = []

    if args.npm:
        ecosystems_ran.append("npm")
        npm_dir = raw_run_dir / "npm"
        ensure_dir(npm_dir)
        for filename in output_files:
            fp = npm_dir / filename
            if fp.exists():
                try:
                    fp.unlink()
                except OSError as e:
                    print(f"Warning: Failed to delete {fp}: {e}")
        summary["npm"] = run_npm_collection(npm_dir, top_n=args.top_n, max_workers=args.workers)

    if args.pypi:
        ecosystems_ran.append("pypi")
        pypi_dir = raw_run_dir / "pypi"
        ensure_dir(pypi_dir)
        for filename in output_files:
            fp = pypi_dir / filename
            if fp.exists():
                try:
                    fp.unlink()
                except OSError as e:
                    print(f"Warning: Failed to delete {fp}: {e}")
        summary["pypi"] = run_pypi_collection(pypi_dir, top_n=args.top_n, max_workers=args.workers)

    clean_result = clean_raw_run(
        raw_run_dir=raw_run_dir,
        clean_dir=clean_dir,
        run_id=run_id,
        ecosystems=ecosystems_ran,
        top_n=args.top_n,
        workers=args.workers,
    )
    summary["clean"] = {
        "raw_row_counts": clean_result["raw_counts"],
        "clean_row_counts": clean_result["clean_counts"],
        "manifest": clean_result["manifest"],
    }

    print("Done.")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"  Output root: {out.resolve()}")
    print(f"  Raw run: {raw_run_dir.resolve()}")
    print(f"  Clean: {clean_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
