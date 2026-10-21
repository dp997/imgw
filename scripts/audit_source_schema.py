#!/usr/bin/env python3
"""Bounded, reusable source-schema audit for IMGW historic archives.

What it does
------------
1. Reads the project's configured ``sources.root_urls`` from dlt config
   (``.dlt/config.toml``), with a direct ``tomllib`` fallback so the script is
   not dependent on the current working directory.
2. For every root URL, discovers ZIP archives via the existing
   :func:`imgw.extract.find_zip_links`.
3. Downloads archives per root using the existing
   :func:`imgw.extract.fetch_zip_data` + :func:`imgw.extract.unzip`. The probe
   order prefers the *newest discovered* archive but also reaches the *oldest*
   archives within the same ``--max-archives-per-root`` total cap (duplicate
   links skipped), so historic-only companions (``k_d_t``/``s_d_t``) are sampled
   alongside the current ``k_d``/``s_d`` archives.
4. For each of the seven recognized table types
   (``k_d_t, k_d, o_d, s_d, s_d_t, k_t, s_t``) samples exactly one matching CSV
   and parses it with the existing :func:`imgw.extract.parse_table` (which uses
   ``ARROW_COLUMNS_SCHEMA``).
5. Reports, per type: source URL/file, parsed row count, parsed column names and
   count, whether the parsed names equal the declared ARROW schema, and the key
   set differences between the ARROW and DLT schemas.
6. Exits non-zero when any recognized type cannot be sampled, any parsing fails,
   or the declared ARROW / DLT key sets differ for any type.

No new dependencies are introduced (only the stdlib ``tomllib`` plus existing
project deps: ``dlt``, ``pyarrow``, ``beautifulsoup4``).

Run (from project root)::

    uv run python scripts/audit_source_schema.py
    uv run python scripts/audit_source_schema.py --json   # machine-readable output

Notes
-----
- This script is **read-only** with respect to production schemas and tests; it
  only *imports* and *calls* existing extract helpers and reports findings.
- The existing ``parse_table`` writes unparseable files to ``./failed_files``
  (its own behaviour). The audit pre-filters to recognized table types so this
  should rarely trigger, but the directory may appear next to where you run the
  script if a parse error occurs.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
from pyarrow import compute as pc

from imgw.common import ARROW_COLUMNS_SCHEMA, DLT_COLUMNS_SCHEMA, get_logger
from imgw.extract import fetch_zip_data, find_zip_links, parse_table, unzip

PROJECT_ROOT = Path(__file__).resolve().parents[1]

logger = get_logger("audit_source_schema")

# The seven recognized IMGW historic table types, in canonical reporting order.
RECOGNIZED_TYPES: list[str] = ["k_d_t", "k_d", "o_d", "s_d", "s_d_t", "k_t", "s_t"]


def load_root_urls() -> list[str]:
    """Load ``sources.root_urls`` from the project dlt config.

    Primary path is ``dlt.config.get`` (honours env vars / secrets too); if that
    yields nothing (e.g. cwd is not the project root), fall back to parsing
    ``.dlt/config.toml`` directly via the stdlib ``tomllib``.
    """
    urls: list[str] | None = None
    try:
        value = dlt.config.get("sources.root_urls", list)
        if value:
            urls = [str(u) for u in value]
    except Exception:
        logger.debug("dlt.config.get failed; falling back to direct config.toml parse", exc_info=True)
        urls = None

    if not urls:
        # tomllib is stdlib only since Python 3.11; the project supports >=3.10.
        # Don't add a hard dependency: if it's unavailable (3.10) and dlt config
        # already failed, surface a clear RuntimeError rather than ImportError.
        try:
            import tomllib  # stdlib since Python 3.11
        except ImportError as exc:  # pragma: no cover - Python 3.10 only
            msg = (
                "Could not resolve 'sources.root_urls' via dlt config and stdlib "
                "'tomllib' is unavailable on Python 3.10; run on Python >=3.11 or "
                "ensure dlt config resolves 'sources.root_urls'."
            )
            raise RuntimeError(msg) from exc

        config_path = PROJECT_ROOT / ".dlt" / "config.toml"
        if not config_path.is_file():
            raise RuntimeError(f"No dlt config found at {config_path}")  # noqa: TRY003
        with config_path.open("rb") as fh:
            data = tomllib.load(fh)
        urls = [str(u) for u in data.get("sources", {}).get("root_urls", [])]

    if not urls:
        raise RuntimeError("No 'sources.root_urls' found in project dlt config")  # noqa: TRY003
    return urls


def filename_table_type(filename: str) -> str | None:
    """Derive a table type from a CSV filename, mirroring ``parse_table``.

    ``parse_table`` uses ``re.match(r"^\\D+", filename).group().rstrip("_")``.
    We replicate that so pre-filtering aligns exactly with what ``parse_table``
    will detect.
    """
    match = re.match(r"^\D+", filename)
    if not match:
        return None
    return match.group().rstrip("_")


def expected_types_for_root(root_url: str) -> list[str]:
    """Heuristic mapping of a root URL path to the table types it should offer.

    Used only to bound how many archives we download per root (so we don't keep
    pulling older archives once a root has nothing left to contribute). If the
    URL structure is unrecognized we conservatively assume any type may appear.
    """
    path = root_url.lower()
    is_dobowe = "dobowe" in path
    is_terminowe = "terminowe" in path
    if is_dobowe and "klimat" in path:
        return ["k_d", "k_d_t"]
    if is_dobowe and "opad" in path:
        return ["o_d"]
    if is_dobowe and "synop" in path:
        return ["s_d", "s_d_t"]
    if is_terminowe and "klimat" in path:
        return ["k_t"]
    if is_terminowe and "synop" in path:
        return ["s_t"]
    return list(RECOGNIZED_TYPES)


def _compare_arrow_dlt(table_type: str) -> dict[str, Any]:
    """Compute key-set differences between ARROW and DLT schemas for a type."""
    arrow_keys = set(ARROW_COLUMNS_SCHEMA[table_type].keys())
    dlt_keys = set(DLT_COLUMNS_SCHEMA[table_type].keys())
    arrow_only = sorted(arrow_keys - dlt_keys)
    dlt_only = sorted(dlt_keys - arrow_keys)
    return {
        "arrow_only": arrow_only,
        "dlt_only": dlt_only,
        "match": not arrow_only and not dlt_only,
    }


def _discover_zip_links(root_url: str, failures: list[str]) -> list[str]:
    """Discover and lexicographically sort zip links under ``root_url``.

    On failure or empty result, records a message in ``failures`` and returns an
    empty list.
    """
    logger.info("Discovering zip links under %s", root_url)
    try:
        zip_links = sorted(find_zip_links(root_url))  # lexicographic; latest = last
    except Exception:
        msg = f"discovery failed for {root_url}"
        logger.exception(msg)
        failures.append(msg)
        return []

    if not zip_links:
        msg = f"no zip links found under {root_url}"
        logger.warning(msg)
        failures.append(msg)
        return []

    return zip_links


def _probe_order(zip_links: list[str], cap: int) -> list[str]:
    """Build an archive probe order that prefers newest but also reaches oldest.

    ``zip_links`` is sorted ascending (oldest first, newest last). The returned
    order starts at the newest end, then alternates to the oldest end, then back,
    so that both recent archives (which carry ``k_d``/``s_d``) and the historic
    oldest archives (which carry the ``k_d_t``/``s_d_t`` companions) are reachable
    within a single ``cap`` budget. Duplicate links are removed. At most ``cap``
    links are returned.
    """
    order: list[str] = []
    seen: set[str] = set()
    left = 0
    right = len(zip_links) - 1
    take_newest = True
    while left <= right and len(order) < cap:
        idx = right if take_newest else left
        link = zip_links[idx]
        if link not in seen:
            seen.add(link)
            order.append(link)
        if take_newest:
            right -= 1
        else:
            left += 1
        take_newest = not take_newest
    return order


def _fetch_and_unzip(link: str, failures: list[str]) -> list[Any]:
    """Download ``link`` and return its CSV files, or ``[]`` on failure."""
    logger.info("Fetching archive: %s", link)
    try:
        zip_file = fetch_zip_data(link)
        return unzip(zip_file)
    except Exception:
        msg = f"fetch/unzip failed: {link}"
        logger.exception(msg)
        failures.append(msg)
        return []


def _sample_type(
    csv_file: Any,
    link: str,
    table_type: str,
    failures: list[str],
    parse_failures: list[str],
) -> tuple[dict[str, Any] | None, bool]:
    """Parse ``csv_file`` as ``table_type`` and build a result record.

    Returns ``(result_record, schema_diff_found)``. On parse failure the record
    is ``None``, messages are appended to ``failures``/``parse_failures``, and
    ``schema_diff_found`` is ``False``.
    """
    try:
        table, parsed_type = parse_table(csv_file)
    except Exception:
        msg = f"parse failed: {csv_file.filename} from {link}"
        logger.exception(msg)
        parse_failures.append(msg)
        failures.append(msg)
        return None, False

    parsed_names: list[str] = list(table.column_names)
    arrow_names: list[str] = list(ARROW_COLUMNS_SCHEMA[table_type].keys())
    diff = _compare_arrow_dlt(table_type)
    record = {
        "table_type": table_type,
        "source_url": link,
        "source_file": csv_file.filename,
        "parsed_type_returned": parsed_type,
        "parsed_row_count": int(table.num_rows),
        "parsed_column_count": len(parsed_names),
        "parsed_column_names": parsed_names,
        "arrow_column_count": len(arrow_names),
        "parsed_names_equal_arrow_schema": parsed_names == arrow_names,
        "arrow_vs_dlt": diff,
        "value_profile": _safe_profile(table, csv_file.filename),
    }
    logger.info(
        "Sampled %s from %s (%s): rows=%d, cols=%d, arrow_match=%s, arrow_dlt_match=%s",
        table_type,
        link,
        csv_file.filename,
        int(table.num_rows),
        len(parsed_names),
        parsed_names == arrow_names,
        diff["match"],
    )
    return record, not diff["match"]


def _sample_matching_csvs(
    csv_files: list[Any],
    link: str,
    missing: set[str],
    results: dict[str, dict[str, Any]],
    failures: list[str],
    parse_failures: list[str],
    schema_diffs: list[str],
) -> None:
    """Parse every CSV whose (still missing) table type is recognized."""
    for csv_file in csv_files:
        ttype = filename_table_type(csv_file.filename)
        if ttype is None or ttype not in missing or ttype not in ARROW_COLUMNS_SCHEMA:
            continue
        record, diff_found = _sample_type(csv_file, link, ttype, failures, parse_failures)
        if record is None:
            continue
        results[ttype] = record
        if diff_found:
            schema_diffs.append(ttype)
        missing.discard(ttype)


### value-quality profiling
#
# Profiles are purely informational warnings. They never affect the audit
# verdict (some IMGW conventions, e.g. hour == 24 or sentinel missing-value
# codes, may be documented historically). Only pyarrow compute / stdlib is used.


SENTINEL_VALUES: list[int] = [-9999, -999, 9999, 999]
_MAX_PROFILE_EXAMPLES = 5


def _count_true(mask: pa.Array | pa.ChunkedArray) -> int:
    """Count True values in a boolean pyarrow array (0 for empty input)."""
    return int(pc.sum(pc.cast(mask, pa.int64()), min_count=0).as_py())


def _finite_base(column: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Return the non-null, finite subset of a numeric column (for stats/rules)."""
    non_null = pc.filter(column, pc.is_valid(column))
    if pa.types.is_floating(column.type):
        return pc.filter(non_null, pc.is_finite(non_null))
    return non_null


def _date_validation(table: pa.Table) -> dict[str, Any]:
    """Validate calendar dates built from year/month/day columns."""
    needed = ("year", "month", "day")
    present = all(c in table.column_names for c in needed)
    result: dict[str, Any] = {"columns_present": present, "invalid_count": 0, "examples": []}
    if not present:
        return result

    years = table.column("year").to_pylist()
    months = table.column("month").to_pylist()
    days = table.column("day").to_pylist()
    invalid = 0
    examples: list[list[int]] = []
    for y, m, d in zip(years, months, days, strict=False):
        if y is None or m is None or d is None:
            continue
        try:
            datetime.date(int(y), int(m), int(d))
        except (ValueError, OverflowError):
            invalid += 1
            if len(examples) < _MAX_PROFILE_EXAMPLES:
                examples.append([int(y), int(m), int(d)])
    result["invalid_count"] = invalid
    result["examples"] = examples
    return result


def _hour_validation(table: pa.Table) -> dict[str, Any]:
    """Validate the ``hour`` column; report IMGW convention ``24`` separately."""
    present = "hour" in table.column_names
    result: dict[str, Any] = {"present": present, "out_of_range_count": 0, "hour_24_count": 0}
    if not present:
        return result

    out_of_range = 0
    hour_24 = 0
    for value in table.column("hour").to_pylist():
        if value is None:
            continue
        hour = int(value)
        if hour == 24:  # IMGW documented convention; reported separately, not a violation
            hour_24 += 1
        if hour < 0 or hour > 24:
            out_of_range += 1
    result["out_of_range_count"] = out_of_range
    result["hour_24_count"] = hour_24
    return result


def _sentinel_profile(
    table: pa.Table,
    sentinels: list[int] | None = None,
) -> dict[str, Any]:
    """Count suspicious sentinel missing-value codes across numeric columns."""
    values = sentinels if sentinels is not None else SENTINEL_VALUES
    per_value: dict[int, int] = dict.fromkeys(values, 0)
    per_column: dict[str, int] = {}
    for name in table.column_names:
        column = table.column(name)
        if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
            continue
        base = _finite_base(column)
        column_total = 0
        for sentinel in values:
            count = _count_true(pc.equal(base, sentinel))
            if count:
                per_value[sentinel] += count
                column_total += count
        if column_total:
            per_column[name] = column_total
    return {
        "values": list(values),
        "total": sum(per_value.values()),
        "per_value": per_value,
        "per_column": per_column,
    }


def _plausibility_rules_for(column_name: str) -> list[dict[str, Any]]:
    """Generic semantic plausibility rules derived from a column's name."""
    low = column_name.lower()
    rules: list[dict[str, Any]] = []

    def add(rule: str, constraint: str, **bounds: Any) -> None:
        rules.append({"rule": rule, "constraint": constraint, **bounds})

    if "humidity" in low:
        add("humidity_0_100", "0 <= v <= 100", min=0, max=100)
    if "cloud_cover" in low or "oktant" in low:
        add("cloud_cover_0_8", "0 <= v <= 8", min=0, max=8)
    if "occurrence" in low or (low.endswith("_flag") and "dew" in low):
        add("occurrence_flag_0_1", "v in {0, 1}", allowed=[0, 1])
    if ("wind" in low and "speed" in low) or "gust" in low:
        add("wind_nonnegative", "v >= 0", min=0)
    if any(k in low for k in ("precipitation", "snow", "rain", "sleet", "hail", "sunshine", "duration")):
        add("nonnegative", "v >= 0", min=0)
    if "temp" in low:
        add("temperature_plausible", "-80 <= v <= 60", min=-80, max=60)
    if "direction" in low and "deg" in low:
        add("wind_direction_0_360", "0 <= v <= 360", min=0, max=360)
    # The primary ``hour`` column is handled by _hour_validation (which separates
    # the IMGW hour==24 convention); here only auxiliary *_hour columns apply.
    if low != "hour" and low.endswith("_hour"):
        add("hour_0_24", "0 <= v <= 24", min=0, max=24)
    return rules


def _apply_plausibility_rule(base: pa.Array | pa.ChunkedArray, rule: dict[str, Any]) -> int:
    """Count violations of one plausibility rule against the finite base array."""
    if len(base) == 0:
        return 0
    if "allowed" in rule:
        acc = None
        for value in rule["allowed"]:
            match = pc.equal(base, value)
            acc = match if acc is None else pc.or_(acc, match)
        violations = pc.invert(acc) if acc is not None else pc.is_valid(base)
        return _count_true(violations)

    mask = None
    low = rule.get("min")
    high = rule.get("max")
    if low is not None:
        mask = pc.less(base, low)
    if high is not None:
        above = pc.greater(base, high)
        mask = above if mask is None else pc.or_(mask, above)
    return _count_true(mask) if mask is not None else 0


def profile_table(table: pa.Table) -> dict[str, Any]:
    """Build a reusable value-quality profile for a sampled pyarrow table.

    Covers: per-column null counts/rates and numeric min/max; non-finite numeric
    counts; valid-calendar-date checks from year/month/day; hour checks (with the
    IMGW ``24`` convention reported separately); suspicious sentinel counts; and
    name-derived semantic plausibility violations. All findings are warnings and
    never affect the audit verdict.
    """
    row_count = int(table.num_rows)

    columns: dict[str, Any] = {}
    non_finite_total = 0
    null_cells_total = 0
    for name in table.column_names:
        column = table.column(name)
        null_count = int(column.null_count)
        null_cells_total += null_count
        entry: dict[str, Any] = {
            "data_type": str(column.type),
            "null_count": null_count,
            "null_rate": round(null_count / row_count, 6) if row_count else 0.0,
            "non_finite": 0,
            "min": None,
            "max": None,
        }
        if pa.types.is_integer(column.type) or pa.types.is_floating(column.type):
            base = _finite_base(column)
            if pa.types.is_floating(column.type):
                non_finite = int(pc.sum(pc.cast(pc.is_valid(column), pa.int64()), min_count=0).as_py()) - len(base)
                entry["non_finite"] = non_finite
                non_finite_total += non_finite
            if len(base) > 0:
                min_max = pc.min_max(base)
                entry["min"] = min_max["min"].as_py()
                entry["max"] = min_max["max"].as_py()
        columns[name] = entry

    date_profile = _date_validation(table)
    hour_profile = _hour_validation(table)
    sentinel_profile = _sentinel_profile(table)

    plausibility_warnings: list[dict[str, Any]] = []
    plausibility_total = 0
    for name in table.column_names:
        column = table.column(name)
        if not (pa.types.is_integer(column.type) or pa.types.is_floating(column.type)):
            continue
        base = _finite_base(column)
        if len(base) == 0:
            continue
        for rule in _plausibility_rules_for(name):
            count = _apply_plausibility_rule(base, rule)
            if count:
                plausibility_warnings.append({
                    "rule": rule["rule"],
                    "column": name,
                    "violation_count": count,
                    "constraint": rule["constraint"],
                })
                plausibility_total += count

    warnings_total = (
        date_profile["invalid_count"] + hour_profile["out_of_range_count"] + non_finite_total + plausibility_total
    )

    return {
        "row_count": row_count,
        "columns": columns,
        "date_validation": date_profile,
        "hour_validation": hour_profile,
        "non_finite_total": non_finite_total,
        "null_cells_total": null_cells_total,
        "suspicious_sentinels": sentinel_profile,
        "plausibility": {"warnings": plausibility_warnings, "total": plausibility_total},
        "totals": {
            "rows": row_count,
            "null_cells": null_cells_total,
            "non_finite_cells": non_finite_total,
            "invalid_dates": date_profile["invalid_count"],
            "hours_out_of_range": hour_profile["out_of_range_count"],
            "hours_24_imgw_convention": hour_profile["hour_24_count"],
            "sentinel_cells": sentinel_profile["total"],
            "plausibility_violations": plausibility_total,
            "warnings": warnings_total,
        },
    }


def _safe_profile(table: pa.Table, filename: str) -> dict[str, Any]:
    """Run :func:`profile_table` defensively so profiling never breaks the audit."""
    try:
        return profile_table(table)
    except Exception:
        logger.exception("value profiling failed for %s", filename)
        return {"error": "value profiling failed"}


def _aggregate_value_profiles(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Sum value-profile warning totals across all sampled table types."""
    totals = {
        "sampled_tables": 0,
        "rows": 0,
        "null_cells": 0,
        "non_finite_cells": 0,
        "invalid_dates": 0,
        "hours_out_of_range": 0,
        "hours_24_imgw_convention": 0,
        "sentinel_cells": 0,
        "plausibility_violations": 0,
        "warnings": 0,
    }
    for record in results.values():
        profile = record.get("value_profile") or {}
        if not isinstance(profile, dict) or "totals" not in profile:
            continue
        totals["sampled_tables"] += 1
        for key in (
            "rows",
            "null_cells",
            "non_finite_cells",
            "invalid_dates",
            "hours_out_of_range",
            "hours_24_imgw_convention",
            "sentinel_cells",
            "plausibility_violations",
            "warnings",
        ):
            totals[key] += int(profile["totals"].get(key, 0))
    return totals


def audit(
    *,
    max_archives_per_root: int = 5,
) -> dict[str, Any]:
    """Run the bounded source-schema audit and return a structured report.

    Parameters
    ----------
    max_archives_per_root:
        Total per-root budget of archives to download. The probe order prefers
        the newest archive but also reaches the oldest archives within this cap,
        so historic-only companions (``k_d_t``/``s_d_t``) are sampled alongside
        the current ``k_d``/``s_d`` archives. Duplicate links are skipped.
    """
    root_urls = load_root_urls()
    logger.info("Auditing %d root URL(s)", len(root_urls))

    results: dict[str, dict[str, Any]] = {}
    missing: set[str] = set(RECOGNIZED_TYPES)
    failures: list[str] = []
    parse_failures: list[str] = []
    schema_diffs: list[str] = []

    for root_url in root_urls:
        if not missing:
            break

        if not [t for t in expected_types_for_root(root_url) if t in missing]:
            logger.debug("Root %s offers no missing types; skipping", root_url)
            continue

        zip_links = _discover_zip_links(root_url, failures)
        if not zip_links:
            continue

        # newest-first, but interleaved with oldest so both ends are reached
        # within the same total cap (deduped).
        probe_links = _probe_order(zip_links, max_archives_per_root)
        tried = 0
        for link in probe_links:
            offers = [t for t in expected_types_for_root(root_url) if t in missing]
            if not offers:
                break
            tried += 1

            csv_files = _fetch_and_unzip(link, failures)
            if not csv_files:
                continue

            _sample_matching_csvs(csv_files, link, missing, results, failures, parse_failures, schema_diffs)

        if [t for t in expected_types_for_root(root_url) if t in missing]:
            still_missing = sorted(t for t in expected_types_for_root(root_url) if t in missing)
            logger.warning(
                "Exhausted archive budget (%d/%d probed) for %s; still missing %s",
                tried,
                max_archives_per_root,
                root_url,
                still_missing,
            )

    report: dict[str, Any] = {
        "recognized_types": list(RECOGNIZED_TYPES),
        "root_urls": root_urls,
        "results": results,
        "sampled": sorted(results.keys()),
        "missing": sorted(missing),
        "failures": failures,
        "parse_failures": parse_failures,
        "schema_diffs": sorted(set(schema_diffs)),
        "value_profile_totals": _aggregate_value_profiles(results),
    }
    report["verdict"] = _verdict(report)
    return report


def _verdict(report: dict[str, Any]) -> str:
    """Compute PASS/FAIL based on the audit rules."""
    failed = bool(report.get("missing") or report.get("parse_failures") or report.get("schema_diffs"))
    return "FAIL" if failed else "PASS"


def render_text(report: dict[str, Any]) -> str:
    """Render a human-readable text report."""
    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("IMGW source-schema audit")
    lines.append("=" * 70)
    lines.append(f"Root URLs: {len(report['root_urls'])}")
    for url in report["root_urls"]:
        lines.append(f"  - {url}")
    lines.append("")

    for ttype in RECOGNIZED_TYPES:
        lines.append("-" * 70)
        lines.append(f"[{ttype}]")
        if ttype in report["results"]:
            r = report["results"][ttype]
            lines.append(f"  source_url           : {r['source_url']}")
            lines.append(f"  source_file          : {r['source_file']}")
            lines.append(f"  parsed_type_returned : {r['parsed_type_returned']}")
            lines.append(f"  parsed_row_count     : {r['parsed_row_count']}")
            lines.append(
                f"  parsed_column_count  : {r['parsed_column_count']} (arrow declares {r['arrow_column_count']})"
            )
            lines.append(f"  parsed_column_names  : {r['parsed_column_names']}")
            lines.append(f"  parsed_names == arrow: {r['parsed_names_equal_arrow_schema']}")
            diff = r["arrow_vs_dlt"]
            lines.append(
                f"  arrow vs dlt keys    : match={diff['match']} "
                f"arrow_only={diff['arrow_only']} dlt_only={diff['dlt_only']}"
            )
            lines.extend(_render_value_profile(r.get("value_profile")))
        else:
            lines.append("  <NOT SAMPLED>")
        lines.append("")

    lines.append("=" * 70)
    lines.append("SUMMARY")
    lines.append("=" * 70)
    lines.append(f"  sampled       : {len(report['sampled'])}/{len(RECOGNIZED_TYPES)} {report['sampled']}")
    lines.append(f"  missing       : {report['missing']}")
    lines.append(f"  parse_failures: {len(report['parse_failures'])} {report['parse_failures']}")
    lines.append(f"  other failures: {len(report['failures']) - len(report['parse_failures'])}")
    lines.append(f"  schema_diffs  : {report['schema_diffs']}")
    lines.append(f"  VERDICT       : {report['verdict']}")
    lines.append("")
    lines.append("Exit non-zero when: any type missing, any parse failure, or ARROW/DLT keys differ.")

    vp = report.get("value_profile_totals") or {}
    if vp:
        lines.append("")
        lines.append("=" * 70)
        lines.append("VALUE PROFILE TOTALS (warnings only, never fatal)")
        lines.append("=" * 70)
        lines.append(f"  sampled tables       : {vp.get('sampled_tables', 0)}")
        lines.append(f"  total rows           : {vp.get('rows', 0)}")
        lines.append(f"  invalid dates        : {vp.get('invalid_dates', 0)}")
        lines.append(
            f"  hours out of range   : {vp.get('hours_out_of_range', 0)} "
            f"(imgw hour==24: {vp.get('hours_24_imgw_convention', 0)})"
        )
        lines.append(f"  non-finite cells     : {vp.get('non_finite_cells', 0)}")
        lines.append(f"  null cells           : {vp.get('null_cells', 0)}")
        lines.append(f"  suspicious sentinels : {vp.get('sentinel_cells', 0)}")
        lines.append(f"  plausibility viol.   : {vp.get('plausibility_violations', 0)}")
        lines.append(f"  total warnings       : {vp.get('warnings', 0)}")
    return "\n".join(lines)


def _render_value_profile(profile: dict[str, Any] | None) -> list[str]:
    """Render a concise per-table value-profile block (warnings only)."""
    if not profile:
        return []
    if "error" in profile:
        return [f"  -- value profile -- : ERROR ({profile['error']})"]

    totals = profile.get("totals", {})
    lines = ["  -- value profile (warnings only, never fatal) --"]
    lines.append(f"  rows                 : {totals.get('rows', 0)}")
    lines.append(f"  invalid dates        : {totals.get('invalid_dates', 0)}")
    lines.append(
        f"  hours out of range   : {totals.get('hours_out_of_range', 0)} "
        f"(imgw hour==24: {totals.get('hours_24_imgw_convention', 0)})"
    )
    lines.append(f"  non-finite cells     : {totals.get('non_finite_cells', 0)}")
    lines.append(f"  null cells           : {totals.get('null_cells', 0)}")
    sentinels = profile.get("suspicious_sentinels", {})
    lines.append(f"  suspicious sentinels : {sentinels.get('total', 0)} {sentinels.get('per_value', {})}")
    plaus = profile.get("plausibility", {})
    lines.append(f"  plausibility viol.   : {plaus.get('total', 0)}")
    for warning in plaus.get("warnings", [])[:_MAX_PROFILE_EXAMPLES]:
        lines.append(
            f"    - {warning['rule']}[{warning['column']}]={warning['violation_count']} ({warning['constraint']})"
        )
    extra = max(0, len(plaus.get("warnings", [])) - _MAX_PROFILE_EXAMPLES)
    if extra:
        lines.append(f"    ... ({extra} more plausibility rule(s))")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit IMGW source archives against declared ARROW/DLT schemas.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the report as JSON to stdout instead of human-readable text.",
    )
    parser.add_argument(
        "--max-archives-per-root",
        type=int,
        default=5,
        help="Max archives to download per root when falling back (default: 5).",
    )
    args = parser.parse_args(argv)

    report = audit(max_archives_per_root=args.max_archives_per_root)

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(render_text(report))

    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
