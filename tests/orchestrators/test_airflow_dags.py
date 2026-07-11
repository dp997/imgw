"""Parse-level tests for the Airflow DAG definitions.

These tests load the DAG module through Airflow's own ``DagBag`` parser and
assert that both DAGs are importable, registered, and contain tasks.

Airflow is required: if it is not installed the collection of this module
raises ``ImportError`` rather than being silently skipped, so a broken
orchestration environment cannot pass CI undetected. Install via
``uv sync --extra airflow``.
"""

from __future__ import annotations

import pytest

# Intentionally NOT using pytest.importorskip: a missing Airflow install must
# surface as a hard failure (ImportError) rather than a silent skip.
from airflow.models import DagBag

DAG_FOLDER = "dags/airflow"
EXPECTED_DAG_IDS = {"load_imgw_historical", "load_imgw_real_time"}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_expected_dags_are_registered(dag_bag: DagBag) -> None:
    registered = set(dag_bag.dag_ids)
    missing = EXPECTED_DAG_IDS - registered
    assert not missing, f"Missing expected DAG ids. Registered: {sorted(registered)}"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_tasks(dag_bag: DagBag, dag_id: str) -> None:
    # Use the in-memory `.dags` mapping rather than `get_dag`, which in Airflow 3
    # round-trips through the metadata DB (unavailable in a fresh parse-only env).
    dag = dag_bag.dags.get(dag_id)
    assert dag is not None, f"DAG '{dag_id}' not found in DagBag"
    assert len(dag.task_ids) > 0, f"DAG '{dag_id}' has no tasks"
