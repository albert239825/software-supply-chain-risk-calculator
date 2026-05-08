import csv

from src.utils import CsvWriter, github_headers, parse_github_owner_repo


def test_parse_github_owner_repo_accepts_common_repository_shapes():
    assert parse_github_owner_repo("github:owner/repo") == ("owner", "repo")
    assert parse_github_owner_repo("git+https://github.com/owner/repo.git") == ("owner", "repo")
    assert parse_github_owner_repo("git@github.com:owner/repo.git") == ("owner", "repo")
    assert parse_github_owner_repo({"url": "https://www.github.com/owner/repo"}) == ("owner", "repo")


def test_parse_github_owner_repo_rejects_unusable_values():
    assert parse_github_owner_repo(None) is None
    assert parse_github_owner_repo({"directory": "packages/app"}) is None
    assert parse_github_owner_repo("https://example.com/owner/repo") is None
    assert parse_github_owner_repo(42) is None


def test_github_headers_uses_token_when_present(monkeypatch):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    assert github_headers() == {}

    monkeypatch.setenv("GITHUB_TOKEN", "  secret  ")
    assert github_headers()["Authorization"] == "Bearer secret"


def test_csv_writer_writes_and_appends_rows(tmp_path):
    path = tmp_path / "nested" / "rows.csv"

    CsvWriter.write_csv(path, ["a", "b"], [{"a": "1", "b": "2", "extra": "x"}])
    CsvWriter.write_csv(path, ["a", "b"], [{"a": "3", "b": "4"}])

    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_csv_writer_rewrites_changed_headers_and_supports_context_manager(tmp_path):
    path = tmp_path / "rows.csv"

    CsvWriter.write_csv(path, ["a"], [{"a": "1"}])
    CsvWriter.write_csv(path, ["b"], [{"b": "2"}])

    with path.open(newline="", encoding="utf-8") as f:
        assert list(csv.DictReader(f)) == [{"b": "2"}]

    with CsvWriter(path, ["a"]) as writer:
        writer.write({"a": "3"})
        assert writer.rows == 1

    with path.open(newline="", encoding="utf-8") as f:
        assert list(csv.DictReader(f)) == [{"a": "3"}]
