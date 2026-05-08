import sys

import pytest

import collect_data
import src.clean_data as clean_data
import src.npm_collect as npm_collect
import src.pypi_collect as pypi_collect


def test_collect_data_main_runs_selected_ecosystems(monkeypatch, tmp_path, capsys):
    calls = []

    def fake_run_npm(out_dir, top_n, max_workers):
        calls.append(("npm", out_dir.name, top_n, max_workers))
        return {"packages_collected": 1}

    def fake_run_pypi(out_dir, top_n, max_workers):
        calls.append(("pypi", out_dir.name, top_n, max_workers))
        return {"packages_collected": 2}

    def fake_clean_raw_run(**kwargs):
        calls.append(("clean", tuple(kwargs["ecosystems"]), kwargs["top_n"], kwargs["workers"]))
        return {
            "raw_counts": {"packages": 3},
            "clean_counts": {"packages": 3},
            "manifest": {"run_id": kwargs["run_id"]},
        }

    monkeypatch.setattr(npm_collect, "run_npm_collection", fake_run_npm)
    monkeypatch.setattr(pypi_collect, "run_pypi_collection", fake_run_pypi)
    monkeypatch.setattr(clean_data, "clean_raw_run", fake_clean_raw_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect_data.py",
            "--npm",
            "--pypi",
            "--out",
            str(tmp_path),
            "--top-n",
            "7",
            "--workers",
            "3",
        ],
    )

    assert collect_data.main() == 0

    assert calls[0][0] == "npm"
    assert calls[1][0] == "pypi"
    assert calls[2] == ("clean", ("npm", "pypi"), 7, 3)
    assert "Done." in capsys.readouterr().out


def test_collect_data_main_requires_an_ecosystem(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["collect_data.py"])

    with pytest.raises(SystemExit):
        collect_data.main()
