"""Focused tests for the value-quality profile helper.

The helper lives in ``scripts/audit_source_schema.py`` (a standalone script, not
a package), so it is loaded via ``importlib``. ``imgw`` is installed as a
project package, so it resolves regardless of cwd.

These tests build small pyarrow tables and assert the profile behaviour for each
warning category. Profile findings are warnings only and must never affect the
audit verdict.
"""

import importlib.util
import json
from pathlib import Path

import pyarrow as pa
import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "audit_source_schema.py"
_spec = importlib.util.spec_from_file_location("audit_source_schema", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)
profile_table = audit.profile_table


def _rule_counts(profile: dict) -> dict[str, int]:
    return {w["rule"]: w["violation_count"] for w in profile["plausibility"]["warnings"]}


# ---------------------------- date validation ----------------------------


def test_date_validation_flags_invalid_calendar_dates():
    tbl = pa.table({
        "year": pa.array([2020, 2021, 2020, 2021], pa.int16()),
        "month": pa.array([2, 13, 2, 1], pa.int8()),  # month 13 invalid
        "day": pa.array([30, 1, 31, 1], pa.int8()),  # 2020-02-30 / 2020-02-31 invalid
    })
    profile = profile_table(tbl)
    assert profile["date_validation"]["columns_present"] is True
    assert profile["date_validation"]["invalid_count"] == 3
    assert profile["totals"]["invalid_dates"] == 3
    assert profile["totals"]["warnings"] == 3


def test_date_validation_handles_missing_columns():
    tbl = pa.table({"value": pa.array([1, 2], pa.int32())})
    profile = profile_table(tbl)
    assert profile["date_validation"]["columns_present"] is False
    assert profile["date_validation"]["invalid_count"] == 0


# ---------------------------- hour validation ----------------------------


def test_hour_validation_separates_imgw_24_convention():
    tbl = pa.table({"hour": pa.array([0, 24, 25, None], pa.int8())})
    profile = profile_table(tbl)
    hour = profile["hour_validation"]
    assert hour["present"] is True
    assert hour["hour_24_count"] == 1  # IMGW convention, reported separately
    assert hour["out_of_range_count"] == 1  # only 25 (>24) is a violation
    assert profile["totals"]["hours_24_imgw_convention"] == 1


def test_hour_absent_when_column_missing():
    tbl = pa.table({"value": pa.array([1], pa.int32())})
    profile = profile_table(tbl)
    assert profile["hour_validation"]["present"] is False


# ----------------------- nulls / non-finite / min-max -----------------------


def test_nulls_non_finite_and_numeric_minmax():
    tbl = pa.table({"temp": pa.array([1.0, float("nan"), None, 5.0], pa.float32())})
    profile = profile_table(tbl)
    col = profile["columns"]["temp"]
    assert col["null_count"] == 1
    assert col["null_rate"] == 0.25
    assert col["non_finite"] == 1
    assert col["min"] == 1.0
    assert col["max"] == 5.0
    assert profile["totals"]["null_cells"] == 1
    assert profile["totals"]["non_finite_cells"] == 1
    # nulls are NOT warnings; only the non-finite cell counts as a warning
    assert profile["totals"]["warnings"] == 1


def test_string_columns_have_no_numeric_minmax():
    tbl = pa.table({"cloud_cover_general_oktants": pa.array(["0", "9", "8"], pa.string())})
    profile = profile_table(tbl)
    col = profile["columns"]["cloud_cover_general_oktants"]
    assert col["min"] is None
    assert col["max"] is None
    assert profile["plausibility"]["total"] == 0


# ------------------------------ sentinels ------------------------------


def test_sentinel_counts_per_value_and_column():
    tbl = pa.table({"v": pa.array([-9999.0, -999.0, 9999.0, 999.0, 0.0, 5.0], pa.float32())})
    profile = profile_table(tbl)
    sentinels = profile["suspicious_sentinels"]
    assert sentinels["total"] == 4
    assert sentinels["per_value"] == {-9999: 1, -999: 1, 9999: 1, 999: 1}
    assert sentinels["per_column"]["v"] == 4
    # sentinels are reported separately, not folded into the warnings total
    assert profile["totals"]["warnings"] == 0


# ---------------------- plausibility (name-derived) ----------------------


@pytest.mark.parametrize(
    ("column", "values", "arrow_type", "expected_rule"),
    [
        ("humidity", [50.0, 150.0], pa.float32(), "humidity_0_100"),
        ("relative_humidity_percent", [100.0, 101.0], pa.float32(), "humidity_0_100"),
        ("cloud_cover_general", [8.0, 9.0], pa.float32(), "cloud_cover_0_8"),
        ("air_temperature", [60.0, 61.0], pa.float32(), "temperature_plausible"),
        ("air_temperature", [-80.0, -81.0], pa.float32(), "temperature_plausible"),
        ("wind_speed", [0.0, -1.0], pa.float32(), "wind_nonnegative"),
        ("wind_gust_ms", [0.0, -2.0], pa.float32(), "wind_nonnegative"),
        ("precipitation_daily_total", [0.0, -1.0], pa.float32(), "nonnegative"),
        ("snow_cover_height", [0.0, -1.0], pa.float32(), "nonnegative"),
        ("sunshine_duration", [0.0, -1.0], pa.float32(), "nonnegative"),
        ("duration_rain", [0.0, -1.0], pa.float32(), "nonnegative"),
        ("wind_direction_deg", [360.0, 361.0], pa.float32(), "wind_direction_0_360"),
        ("max_gust_hour", [24, 25], pa.int8(), "hour_0_24"),
    ],
)
def test_plausibility_rules_derived_from_names(column, values, arrow_type, expected_rule):
    tbl = pa.table({column: pa.array(values, arrow_type)})
    profile = profile_table(tbl)
    counts = _rule_counts(profile)
    assert counts.get(expected_rule) == 1


def test_occurrence_flag_must_be_zero_or_one():
    tbl = pa.table({"occurrence_snow_cover": pa.array([0, 1, 2, 0], pa.int8())})
    profile = profile_table(tbl)
    assert _rule_counts(profile).get("occurrence_flag_0_1") == 1


def test_dew_occurrence_flag_rule_via_dew_flag_name():
    tbl = pa.table({"dew_occurrence_flag": pa.array([0, 1, 5], pa.int8())})
    profile = profile_table(tbl)
    assert _rule_counts(profile).get("occurrence_flag_0_1") == 1


def test_primary_hour_column_not_double_counted_in_plausibility():
    # 'hour' is owned by hour_validation; plausibility only handles *_hour columns
    tbl = pa.table({"hour": pa.array([24, 25], pa.int8())})
    profile = profile_table(tbl)
    assert "hour_0_24" not in _rule_counts(profile)
    assert profile["hour_validation"]["out_of_range_count"] == 1


# --------------------- verdict / aggregation / rendering ---------------------


def test_value_profile_warnings_do_not_fail_verdict():
    # Only missing types, parse failures, or schema diffs fail the audit.
    report = {"missing": [], "parse_failures": [], "schema_diffs": []}
    assert audit._verdict(report) == "PASS"


def test_aggregate_value_profiles_sums_totals():
    results = {
        "k_d": {"value_profile": profile_table(pa.table({"hour": pa.array([25], pa.int8())}))},
    }
    aggregated = audit._aggregate_value_profiles(results)
    assert aggregated["sampled_tables"] == 1
    assert aggregated["hours_out_of_range"] == 1
    assert aggregated["warnings"] == 1


def test_profile_is_json_serializable():
    tbl = pa.table({
        "hour": pa.array([24, 25], pa.int8()),
        "humidity": pa.array([150.0, None], pa.float32()),
        "year": pa.array([2020, 2021], pa.int16()),
        "month": pa.array([1, 13], pa.int8()),
        "day": pa.array([1, 1], pa.int8()),
    })
    profile = profile_table(tbl)
    # Must round-trip through JSON (the audit --json output path).
    json.loads(json.dumps(profile))


def test_render_text_includes_value_profile_blocks():
    table = pa.table({"humidity": pa.array([150.0], pa.float32())})

    def record():
        return {
            "source_url": "https://example/x.zip",
            "source_file": "k_d_2020.csv",
            "parsed_type_returned": "k_d",
            "parsed_row_count": table.num_rows,
            "parsed_column_count": len(table.column_names),
            "parsed_column_names": list(table.column_names),
            "arrow_column_count": len(table.column_names),
            "parsed_names_equal_arrow_schema": True,
            "arrow_vs_dlt": {"arrow_only": [], "dlt_only": [], "match": True},
            "value_profile": profile_table(table),
        }

    results = {"k_d": record()}
    report = {
        "root_urls": ["https://example/x/"],
        "results": results,
        "sampled": ["k_d"],
        "missing": [],
        "parse_failures": [],
        "failures": [],
        "schema_diffs": [],
        "verdict": "PASS",
        "value_profile_totals": audit._aggregate_value_profiles(results),
    }
    output = audit.render_text(report)
    assert "value profile" in output
    assert "humidity_0_100" in output
    assert "VALUE PROFILE TOTALS" in output
    assert "PASS" in output


def test_empty_table_profile_is_well_formed():
    tbl = pa.table({"value": pa.array([], pa.int32())})
    profile = profile_table(tbl)
    assert profile["totals"]["rows"] == 0
    assert profile["columns"]["value"]["null_rate"] == 0.0
    assert profile["columns"]["value"]["min"] is None
