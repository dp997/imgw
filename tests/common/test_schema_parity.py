"""Schema parity tests: ARROW and DLT column definitions must stay aligned.

These tests guard against the kind of key drift the live source-schema audit
(`scripts/audit_source_schema.py`) detected, where DLT status-column names
diverged from the ARROW schema for ``k_d`` and ``k_d_t``. For every recognized
table type the *ordered* ARROW and DLT key lists must be identical, and both
schemas must cover exactly the same set of types.
"""

import pytest

from imgw.common import ARROW_COLUMNS_SCHEMA, DLT_COLUMNS_SCHEMA

RECOGNIZED_TYPES = ["k_d_t", "k_d", "o_d", "s_d", "s_d_t", "k_t", "s_t"]


def test_schemas_cover_same_recognized_types():
    """Both schemas must expose exactly the seven recognized table types."""
    assert set(ARROW_COLUMNS_SCHEMA) == set(RECOGNIZED_TYPES)
    assert set(DLT_COLUMNS_SCHEMA) == set(RECOGNIZED_TYPES)


@pytest.mark.parametrize("table_type", RECOGNIZED_TYPES)
def test_arrow_and_dlt_have_identical_key_lists(table_type):
    """Ordered ARROW and DLT key lists must match for each table type."""
    arrow_keys = list(ARROW_COLUMNS_SCHEMA[table_type].keys())
    dlt_keys = list(DLT_COLUMNS_SCHEMA[table_type].keys())
    assert arrow_keys == dlt_keys


@pytest.mark.parametrize("table_type", RECOGNIZED_TYPES)
def test_dlt_values_are_well_formed_hints(table_type):
    """Each DLT column hint must declare a recognized data_type (sanity guard)."""
    allowed = {"bigint", "text", "double", "bool", "decimal", "timestamp", "date", "time"}
    for column, hint in DLT_COLUMNS_SCHEMA[table_type].items():
        assert isinstance(hint, dict), f"{table_type}.{column} hint must be a dict"
        assert "data_type" in hint, f"{table_type}.{column} missing 'data_type'"
        assert hint["data_type"] in allowed, f"{table_type}.{column} bad data_type {hint['data_type']!r}"
