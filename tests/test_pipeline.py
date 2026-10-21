"""Tests for the pipeline CLI, run selection and exit status."""

import argparse
import os
import sys

import pytest

import pipeline


class FakeLoadInfo:
    def __init__(self):
        self.metrics = {"load_packages": 1}


@pytest.fixture(autouse=True)
def quiet_pipeline(monkeypatch):
    monkeypatch.setattr(pipeline, "setup_logging", lambda: None)
    monkeypatch.setattr(pipeline, "imgw_historic", lambda: "historic-source")
    monkeypatch.setattr(pipeline, "imgw_real_time", lambda: "real-time-source")
    monkeypatch.delenv("IMGW_FAILED_OUTPUT", raising=False)


def run_pipeline(monkeypatch, argv, failing=()):
    calls = []

    class FakePipeline:
        def __init__(self, dataset_name):
            self.dataset_name = dataset_name

        def run(self, source, **kwargs):
            calls.append({"dataset": self.dataset_name, "source": source, "kwargs": kwargs})
            if self.dataset_name in failing:
                raise RuntimeError("boom")
            return FakeLoadInfo()

    monkeypatch.setattr(
        pipeline,
        "_build_pipeline",
        lambda args, dataset_name: FakePipeline(dataset_name),
    )
    monkeypatch.setattr(sys, "argv", ["pipeline.py", *argv])
    return pipeline.main(), calls


def test_default_runs_both_pipelines(monkeypatch):
    code, calls = run_pipeline(monkeypatch, [])

    assert code == 0
    assert [call["dataset"] for call in calls] == ["imgw_historic", "imgw_real_time"]


def test_historic_flag_runs_only_historic(monkeypatch):
    code, calls = run_pipeline(monkeypatch, ["--historic"])

    assert code == 0
    assert [call["dataset"] for call in calls] == ["imgw_historic"]


def test_real_time_flag_runs_only_real_time(monkeypatch):
    code, calls = run_pipeline(monkeypatch, ["--real-time"])

    assert code == 0
    assert [call["dataset"] for call in calls] == ["imgw_real_time"]


def test_historic_failure_returns_nonzero(monkeypatch):
    code, calls = run_pipeline(monkeypatch, ["--historic"], failing={"imgw_historic"})

    assert code == 1
    assert [call["dataset"] for call in calls] == ["imgw_historic"]


def test_real_time_failure_returns_nonzero_after_historic(monkeypatch):
    code, calls = run_pipeline(monkeypatch, [], failing={"imgw_real_time"})

    assert code == 1
    assert [call["dataset"] for call in calls] == ["imgw_historic", "imgw_real_time"]


def test_refresh_only_applies_to_historic(monkeypatch):
    _, calls = run_pipeline(monkeypatch, ["--refresh"])

    assert calls[0]["kwargs"] == {"refresh": "drop_data"}
    assert calls[1]["kwargs"] == {}


def test_local_and_snowflake_are_mutually_exclusive(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pipeline.py", "--local", "--snowflake"])

    with pytest.raises(SystemExit) as exc:
        pipeline.main()

    assert exc.value.code == 2


def test_failed_output_sets_environment(monkeypatch, tmp_path):
    failed_output = str(tmp_path / "failed")

    run_pipeline(monkeypatch, ["--historic", "--failed-output", failed_output])

    assert os.environ["IMGW_FAILED_OUTPUT"] == failed_output


def test_build_pipeline_honours_local_output(monkeypatch, tmp_path):
    captured = {}
    db_file = str(tmp_path / "imgw.db")

    def fake_local_pipeline(dataset_name, db_file="output/imgw.db"):
        captured["db_file"] = db_file
        return "local-pipeline"

    monkeypatch.setattr(pipeline, "get_dlt_local_pipeline", fake_local_pipeline)
    args = argparse.Namespace(local=True, snowflake=False, local_output=db_file)

    assert pipeline._build_pipeline(args, "imgw_historic") == "local-pipeline"
    assert captured["db_file"] == db_file
