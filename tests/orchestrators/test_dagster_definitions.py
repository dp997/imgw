"""Validation tests for the Dagster definitions.

These tests import the ``defs`` object from ``dags.dagster.definitions`` and
validate it structurally via Dagster's own ``Definitions.validate_loadable``
without materializing assets or executing dlt loads. IMGW endpoints and the dlt
pipeline are never invoked here.

Dagster is required: if it is not installed the collection of this module raises
``ImportError`` rather than being silently skipped, so a broken orchestration
environment cannot pass CI undetected. Install via ``uv sync --extra dagster``.
"""

from __future__ import annotations

import pytest

# Intentionally NOT using pytest.importorskip: a missing Dagster install must
# surface as a hard failure (ImportError) rather than a silent skip.
from dagster import Definitions

from dags.dagster.definitions import defs

EXPECTED_JOB_NAMES = {"historic_job", "real_time_job"}
EXPECTED_ASSET_GROUPS = {"imgw_historic", "imgw_real_time"}


def test_definitions_are_loadable() -> None:
    # validate_loadable checks the structure (asset graph, jobs, resources,
    # schedules, ...) without materializing or executing anything.
    Definitions.validate_loadable(defs)


@pytest.fixture(scope="module")
def repository():
    return defs.get_repository_def()


@pytest.mark.parametrize("job_name", sorted(EXPECTED_JOB_NAMES))
def test_expected_jobs_are_defined(repository, job_name: str) -> None:
    assert repository.has_job(job_name), f"Job '{job_name}' not defined. Jobs: {sorted(repository.job_names)}"


def test_expected_asset_groups_are_present() -> None:
    groups = {str(spec.group_name) for spec in defs.resolve_all_asset_specs() if spec.group_name}
    missing = EXPECTED_ASSET_GROUPS - groups
    assert not missing, f"Missing asset groups. Present: {sorted(groups)}"


def test_definitions_have_assets() -> None:
    asset_keys = defs.resolve_all_asset_keys()
    assert len(asset_keys) > 0, "Definitions contain no assets"
