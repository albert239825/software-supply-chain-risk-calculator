import src.npm_collect as npm_collect
import src.pypi_collect as pypi_collect
from src.npm_collect import (
    collect_npm_graph,
    fetch_registry_package,
    iter_npms_packages,
    process_registry_package,
    run_npm_collection,
)
from src.pypi_collect import (
    collect_pypi_graph,
    fetch_pypi_package,
    iter_hugovk_top_packages,
    process_pypi_package,
    run_pypi_collection,
)


class FakeSession:
    def __init__(self, payload=None, exc=None):
        self.payload = payload
        self.exc = exc
        self.urls = []

    def get_json(self, url):
        self.urls.append(url)
        if self.exc:
            raise self.exc
        return self.payload


def test_iter_npms_packages_paginates_and_filters_missing_names():
    session = FakeSession(
        {
            "results": [
                {"package": {"name": "a", "description": "A", "version": "1.0.0"}},
                {"package": {"description": "missing name"}},
                {"package": {"name": "b", "version": "2.0.0"}},
            ]
        }
    )

    rows = iter_npms_packages(session, limit=2)

    assert [row["name"] for row in rows] == ["a", "b"]
    assert rows[1]["description"] == ""


def test_fetch_registry_package_returns_none_on_errors():
    assert fetch_registry_package(FakeSession(exc=RuntimeError("offline")), "pkg") is None


def test_process_registry_package_extracts_latest_version_dependencies_and_maintainers():
    package, version, deps, maintainers = process_registry_package(
        "pkg",
        {
            "description": "line 1\nline 2",
            "dist-tags": {"latest": "1.2.3"},
            "time": {"1.2.3": "2026-01-01T00:00:00Z"},
            "maintainers": [{"name": "alice", "email": "alice@example.com"}, "bad"],
            "versions": {
                "1.2.3": {
                    "repository": {"url": "https://github.com/org/repo.git"},
                    "dependencies": {"dep": "^1"},
                    "peerDependencies": {"peer": "^2"},
                    "optionalDependencies": {"optional": "^3"},
                }
            },
        },
    )

    assert package == {
        "ecosystem": "npm",
        "name": "pkg",
        "description": "line 1 line 2",
        "latest_version": "1.2.3",
    }
    assert version["github_owner"] == "org"
    assert version["has_repository"] is True
    assert {dep["dep_kind"] for dep in deps} == {
        "dependencies",
        "peerDependencies",
        "optionalDependencies",
    }
    assert maintainers[0]["username"] == "alice"


def test_process_registry_package_handles_empty_metadata():
    assert process_registry_package("pkg", {}) == ({}, [], [], [])


def test_iter_hugovk_top_packages_accepts_dict_and_list_rows():
    session = FakeSession({"rows": [{"project": "Requests"}, [1, "Django"], ["bad"], {}]})

    rows = iter_hugovk_top_packages(session, limit=2)

    assert [row["name"] for row in rows] == ["requests", "django"]
    assert all(row["ecosystem"] == "pypi" for row in rows)


def test_fetch_pypi_package_returns_none_on_errors():
    assert fetch_pypi_package(FakeSession(exc=RuntimeError("offline")), "pkg") is None


def test_process_pypi_package_extracts_repository_people_and_requirements():
    package, version, deps, maintainers = process_pypi_package(
        "demo-pkg",
        {
            "info": {
                "summary": "Demo\npackage",
                "version": "2.0.0",
                "author_email": "Alice <alice@example.com>, Bob <bob@example.com>",
                "maintainer": "Maint Team <team@example.com>",
                "maintainer_email": "team@example.com",
                "project_urls": {
                    "Source": "https://github.com/org/demo-pkg",
                },
                "requires_dist": [
                    "Requests>=2 ; python_version >= '3.11'",
                    "bad req",
                    5,
                ],
            },
            "releases": {
                "2.0.0": [{"upload_time": "2026-02-01T00:00:00"}],
            },
        },
    )

    assert package["description"] == "Demo package"
    assert version["github_repo"] == "demo-pkg"
    assert version["released"] == "2026-02-01T00:00:00"
    assert deps[0] == {
        "ecosystem": "pypi",
        "from_package": "demo-pkg",
        "from_version": "2.0.0",
        "to_package": "requests",
        "version_spec": "Requests>=2 ; python_version >= '3.11'",
        "dep_kind": "requires_dist",
    }
    assert {dep["to_package"] for dep in deps} == {"requests", "bad"}
    assert {row["email"] for row in maintainers} == {
        "alice@example.com",
        "bob@example.com",
        "team@example.com",
    }


def test_process_pypi_package_falls_back_to_homepage_and_plain_people():
    _package, version, _deps, maintainers = process_pypi_package(
        "demo",
        {
            "info": {
                "version": "1",
                "author": '"Alice"',
                "author_email": "alice@example.com",
                "home_page": "git+https://github.com/org/home.git",
                "requires_dist": None,
            },
            "releases": {},
        },
    )

    assert version["github_owner"] == "org"
    assert version["released"] == ""
    assert maintainers == [
        {
            "ecosystem": "pypi",
            "package_name": "demo",
            "username": "",
            "name": "Alice",
            "role": "author",
            "email": "alice@example.com",
        }
    ]


def test_process_pypi_package_handles_empty_payload():
    assert process_pypi_package("pkg", {}) == ({}, {}, [], [])


def test_collect_npm_graph_expands_dependency_queue_without_network(monkeypatch):
    payloads = {
        "root": {
            "dist-tags": {"latest": "1.0.0"},
            "versions": {"1.0.0": {"dependencies": {"child": "^1"}}},
        },
        "child": {
            "dist-tags": {"latest": "2.0.0"},
            "versions": {"2.0.0": {}},
        },
    }

    monkeypatch.setattr(npm_collect, "_fetch_registry_worker", lambda name: payloads.get(name))

    packages, versions, deps, maintainers = collect_npm_graph(FakeSession(), ["root", "root"], max_workers=1)

    assert [row["name"] for row in packages] == ["root", "child"]
    assert [row["version"] for row in versions] == ["1.0.0", "2.0.0"]
    assert deps[0]["to_package"] == "child"
    assert maintainers == []


def test_run_npm_collection_writes_csvs(monkeypatch, tmp_path):
    monkeypatch.setattr(
        npm_collect,
        "iter_npms_packages",
        lambda _session, limit: [{"name": "root"}],
    )
    monkeypatch.setattr(
        npm_collect,
        "collect_npm_graph",
        lambda _session, seed_names, max_workers: (
            [{"ecosystem": "npm", "name": seed_names[0], "description": "", "latest_version": "1"}],
            [
                {
                    "ecosystem": "npm",
                    "package_name": seed_names[0],
                    "version": "1",
                    "released": "",
                    "has_repository": False,
                    "github_owner": "",
                    "github_repo": "",
                }
            ],
            [],
            [],
        ),
    )

    summary = run_npm_collection(tmp_path, top_n=1, max_workers=1)

    assert summary["packages_collected"] == 1
    assert (tmp_path / "packages.csv").read_text(encoding="utf-8").startswith(
        "ecosystem,name,description,latest_version"
    )


def test_collect_pypi_graph_expands_dependency_queue_without_network(monkeypatch):
    payloads = {
        "root": {
            "info": {
                "version": "1.0.0",
                "requires_dist": ["Child>=1"],
            }
        },
        "child": {"info": {"version": "2.0.0", "requires_dist": []}},
    }

    monkeypatch.setattr(pypi_collect, "_fetch_pypi_worker", lambda name: payloads.get(name))

    packages, versions, deps, maintainers = collect_pypi_graph(FakeSession(), ["Root", "root"], max_workers=1)

    assert [row["name"] for row in packages] == ["root", "child"]
    assert [row["version"] for row in versions] == ["1.0.0", "2.0.0"]
    assert deps[0]["to_package"] == "child"
    assert maintainers == []


def test_run_pypi_collection_writes_csvs(monkeypatch, tmp_path):
    monkeypatch.setattr(
        pypi_collect,
        "iter_hugovk_top_packages",
        lambda _session, limit: [{"name": "root"}],
    )
    monkeypatch.setattr(
        pypi_collect,
        "collect_pypi_graph",
        lambda _session, seed_names, max_workers: (
            [{"ecosystem": "pypi", "name": seed_names[0], "description": "", "latest_version": "1"}],
            [
                {
                    "ecosystem": "pypi",
                    "package_name": seed_names[0],
                    "version": "1",
                    "released": "",
                    "has_repository": False,
                    "github_owner": "",
                    "github_repo": "",
                }
            ],
            [],
            [],
        ),
    )

    summary = run_pypi_collection(tmp_path, top_n=1, max_workers=1)

    assert summary["seed_count"] == 1
    assert (tmp_path / "versions.csv").read_text(encoding="utf-8").startswith(
        "ecosystem,package_name,version,released,has_repository,github_owner,github_repo"
    )
