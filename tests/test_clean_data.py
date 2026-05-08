import csv
import json

import pytest

from src.clean_data import clean_raw_run


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_clean_raw_run_normalizes_dedupes_and_writes_manifest(tmp_path):
    raw = tmp_path / "raw" / "run1"
    clean = tmp_path / "clean"
    npm = raw / "npm"
    pypi = raw / "pypi"

    write_csv(
        npm / "packages.csv",
        ["ecosystem", "name", "description", "latest_version"],
        [
            {
                "ecosystem": "NPM",
                "name": "Pkg",
                "description": "short",
                "latest_version": " 1.0.0 ",
            },
            {
                "ecosystem": "npm",
                "name": "pkg",
                "description": "longer\npackage description",
                "latest_version": "",
            },
        ],
    )
    write_csv(
        npm / "versions.csv",
        ["ecosystem", "package_name", "version", "released", "has_repository", "github_owner", "github_repo"],
        [
            {
                "ecosystem": "npm",
                "package_name": "Pkg",
                "version": "1.0.0",
                "released": "",
                "has_repository": "yes",
                "github_owner": "",
                "github_repo": "",
            },
            {
                "ecosystem": "npm",
                "package_name": "pkg",
                "version": "1.0.0",
                "released": "2026-01-01",
                "has_repository": "maybe",
                "github_owner": "org",
                "github_repo": "repo",
            },
        ],
    )
    write_csv(
        npm / "dependencies.csv",
        ["ecosystem", "from_package", "from_version", "to_package", "version_spec", "dep_kind"],
        [
            {
                "ecosystem": "npm",
                "from_package": "Pkg",
                "from_version": "1.0.0",
                "to_package": "Dep",
                "version_spec": " ^2 ",
                "dep_kind": " dependency ",
            }
        ],
    )
    write_csv(
        npm / "maintainers.csv",
        ["ecosystem", "package_name", "username", "name", "role", "email"],
        [
            {
                "ecosystem": "npm",
                "package_name": "Pkg",
                "username": " alice ",
                "name": "Alice\nA.",
                "role": " maintainer ",
                "email": "alice@example.com ",
            }
        ],
    )

    write_csv(
        pypi / "packages.csv",
        ["ecosystem", "name", "description", "latest_version"],
        [
            {
                "ecosystem": "pypi",
                "name": "My_Pkg",
                "description": "",
                "latest_version": "0.1",
            }
        ],
    )
    write_csv(
        pypi / "versions.csv",
        ["ecosystem", "package_name", "version"],
        [{"ecosystem": "pypi", "package_name": "My_Pkg", "version": "0.1"}],
    )
    write_csv(
        pypi / "dependencies.csv",
        ["ecosystem", "from_package", "from_version", "to_package", "version_spec", "dep_kind"],
        [],
    )
    write_csv(
        pypi / "maintainers.csv",
        ["ecosystem", "package_name", "username", "name", "role", "email"],
        [],
    )

    result = clean_raw_run(
        raw_run_dir=raw,
        clean_dir=clean,
        run_id="run1",
        ecosystems=["npm", "pypi"],
        top_n=2,
        workers=4,
    )

    packages = read_csv(clean / "packages_clean.csv")
    versions = read_csv(clean / "versions_clean.csv")
    deps = read_csv(clean / "dependencies_clean.csv")
    maintainers = read_csv(clean / "maintainers_clean.csv")

    assert result["raw_counts"] == {
        "packages": 3,
        "versions": 3,
        "dependencies": 1,
        "maintainers": 1,
    }
    assert {row["name"] for row in packages} == {"dep", "my-pkg", "pkg"}
    assert next(row for row in packages if row["name"] == "pkg")["description"] == "longer package description"
    assert next(row for row in versions if row["ecosystem"] == "pypi")["has_repository"] == "false"
    assert next(row for row in versions if row["ecosystem"] == "npm")["github_owner"] == "org"
    assert deps[0]["to_package"] == "dep"
    assert deps[0]["from_version_id"]
    assert maintainers[0]["name"] == "Alice A."
    assert maintainers[0]["package_id"]

    manifest = json.loads((clean / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["run_id"] == "run1"
    assert manifest["clean_row_counts"] == result["clean_counts"]


def test_clean_raw_run_validates_required_columns(tmp_path):
    raw = tmp_path / "raw" / "run1"
    npm = raw / "npm"
    clean = tmp_path / "clean"

    write_csv(
        npm / "packages.csv",
        ["ecosystem", "description", "latest_version"],
        [{"ecosystem": "npm", "description": "missing name", "latest_version": "1"}],
    )

    with pytest.raises(ValueError, match="missing required column 'name'"):
        clean_raw_run(
            raw_run_dir=raw,
            clean_dir=clean,
            run_id="run1",
            ecosystems=["npm"],
            top_n=1,
            workers=1,
        )
