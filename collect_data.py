#!/usr/bin/env python3
"""
CLI: NPM / PyPI data -> CSV.

Environment (conda):
  conda env create -f environment.yml
  conda activate cis5500

Examples:
  python collect_data.py --npm --pypi --out data/csv
  python collect_data.py --npm --top-n 200 --out data/csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root))

    from src.npm_collect import run_npm_collection
    from src.pypi_collect import run_pypi_collection
    from src.utils import ensure_dir

    p = argparse.ArgumentParser(description="Collect NPM / PyPI dependency graphs into CSVs.")
    p.add_argument("--out", type=Path, default=Path("data/csv"), help="Output directory for CSV files")
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

    ensure_dir(args.out)
    summary: dict[str, object] = {}
    
    output_files = [
        "packages.csv",
        "versions.csv",
        "dependencies.csv",
        "maintainers.csv",
    ]
    
    # Delete all output_files in output directory
    for filename in output_files:
        file_path = args.out / filename
        if file_path.exists():
            try:
                file_path.unlink()
            except Exception as e:
                print(f"Warning: Failed to delete {file_path}: {e}")

    if args.npm:
        summary["npm"] = run_npm_collection(args.out, top_n=args.top_n, max_workers=args.workers)

    if args.pypi:
        summary["pypi"] = run_pypi_collection(args.out, top_n=args.top_n, max_workers=args.workers)

    print("Done.")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"  CSV directory: {args.out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
