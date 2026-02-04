#!/usr/bin/env python3
"""Lightweight validation for generated outputs in data/output."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

DATASET_PREFIXES = (
    "vdem__",
    "fh__",
    "hrmi__",
    "rsf__",
    "wgi__",
    "cpi__",
    "gsi__",
    "afi__",
)

CORE_FILES = (
    "country_2020_2026.csv",
    "country_2020_2026.sqlite",
    "column_map.csv",
    "substate_entities_template.csv",
    "substate_positions_template.csv",
    "issue_catalog.csv",
)

OPTIONAL_FILES = (
    "country_2020_2026.parquet",
    "country_robustness_2020_2026.csv",
    "robustness_thresholds.json",
    "vparty_entities.csv",
    "vparty_party_year.csv",
    "pipeline_status.json",
)


class ValidationContext:
    def __init__(self) -> None:
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)


def _read_csv_header_and_rows(path: Path, max_rows: int = 5) -> Tuple[List[str], List[List[str]]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows: List[List[str]] = []
        for row in reader:
            rows.append(row)
            if len(rows) >= max_rows:
                break
        return header, rows


def _ensure_required_columns(ctx: ValidationContext, path: Path, header: Sequence[str], required: Iterable[str]) -> None:
    missing = [col for col in required if col not in header]
    if missing:
        ctx.error(f"{path}: missing required columns: {', '.join(missing)}")


def _sample_cell(header: Sequence[str], row: Sequence[str], col: str) -> str:
    try:
        return row[header.index(col)]
    except (ValueError, IndexError):
        return ""


def _validate_country_csv(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    _ensure_required_columns(ctx, path, header, ["iso3", "year", "country_name"])
    if not any(col.startswith(DATASET_PREFIXES) for col in header):
        ctx.error(f"{path}: missing dataset-prefixed columns (expected one of {', '.join(DATASET_PREFIXES)})")

    if not rows:
        ctx.error(f"{path}: no data rows found")
        return

    sample = rows[0]
    iso3 = _sample_cell(header, sample, "iso3").strip()
    if len(iso3) != 3:
        ctx.error(f"{path}: sample iso3 value should be 3 characters, got '{iso3}'")
    year_raw = _sample_cell(header, sample, "year").strip()
    if year_raw:
        try:
            int(float(year_raw))
        except ValueError:
            ctx.error(f"{path}: sample year value not numeric: '{year_raw}'")
    else:
        ctx.error(f"{path}: sample year value is empty")


def _validate_column_map(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path, max_rows=200)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    _ensure_required_columns(ctx, path, header, ["dataset", "source_column", "output_column"])
    if not rows:
        ctx.error(f"{path}: no mappings found")
        return
    dataset_idx = header.index("dataset") if "dataset" in header else None
    if dataset_idx is not None:
        datasets = {row[dataset_idx] for row in rows if len(row) > dataset_idx and row[dataset_idx]}
        expected = {prefix.rstrip("__") for prefix in DATASET_PREFIXES}
        unknown = sorted(datasets - expected)
        if unknown:
            ctx.warn(f"{path}: unexpected dataset names in sample: {', '.join(unknown)}")


def _validate_country_sqlite(path: Path, ctx: ValidationContext) -> None:
    if path.stat().st_size == 0:
        ctx.warn(f"{path}: sqlite file is empty (likely due to column limits)")
        return
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        ctx.error(f"{path}: unable to open sqlite db: {exc}")
        return

    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        if "country_year" not in tables:
            ctx.error(f"{path}: missing country_year table")
            return
        cur.execute("PRAGMA table_info(country_year)")
        columns = {row[1] for row in cur.fetchall()}
        for col in ("iso3", "year"):
            if col not in columns:
                ctx.error(f"{path}: country_year missing column '{col}'")
        cur.execute("SELECT 1 FROM country_year LIMIT 1")
        if cur.fetchone() is None:
            ctx.error(f"{path}: country_year table has no rows")
    except sqlite3.Error as exc:
        ctx.error(f"{path}: sqlite validation error: {exc}")
    finally:
        conn.close()


def _validate_template(path: Path, ctx: ValidationContext, expected_header: Sequence[str]) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    if list(header) != list(expected_header):
        ctx.error(
            f"{path}: unexpected header. Expected {', '.join(expected_header)}; got {', '.join(header)}"
        )
    if not rows:
        ctx.warn(f"{path}: template has no data rows (header-only is expected)")


def _validate_issue_catalog(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    _ensure_required_columns(ctx, path, header, ["issue_code", "issue_label", "description"])
    if not rows:
        ctx.error(f"{path}: issue catalog has no rows")
        return
    code_idx = header.index("issue_code") if "issue_code" in header else None
    if code_idx is not None:
        codes = [row[code_idx] for row in rows if len(row) > code_idx]
        if len(set(codes)) != len(codes):
            ctx.error(f"{path}: duplicate issue_code values in sample")


def _validate_vparty_party_year(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    _ensure_required_columns(ctx, path, header, ["iso3", "year", "party_name"])
    if not any(col.startswith("vparty__") for col in header):
        ctx.error(f"{path}: missing vparty__ prefixed columns")
    if not rows:
        ctx.error(f"{path}: no data rows found")
        return
    sample = rows[0]
    iso3 = _sample_cell(header, sample, "iso3").strip()
    if len(iso3) != 3:
        ctx.error(f"{path}: sample iso3 value should be 3 characters, got '{iso3}'")


def _validate_vparty_entities(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    _ensure_required_columns(ctx, path, header, ["entity_id", "name", "country_iso3", "entity_type"])
    if not rows:
        ctx.error(f"{path}: no data rows found")


def _validate_country_robustness(path: Path, ctx: ValidationContext) -> None:
    header, rows = _read_csv_header_and_rows(path)
    if not header:
        ctx.error(f"{path}: missing header")
        return
    required = [
        "iso3",
        "year",
        "A",
        "G",
        "M",
        "P",
        "S_norm",
        "decline_norm",
        "risk_score",
        "risk_band",
    ]
    _ensure_required_columns(ctx, path, header, required)
    if not rows:
        ctx.error(f"{path}: no data rows found")


def _validate_robustness_thresholds(path: Path, ctx: ValidationContext) -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        ctx.error(f"{path}: unable to read JSON: {exc}")
        return
    if not isinstance(payload, dict):
        ctx.error(f"{path}: expected JSON object")
        return
    for key in ("thresholds", "quantiles"):
        if key not in payload:
            ctx.error(f"{path}: missing '{key}' section")


def _validate_pipeline_status(path: Path, ctx: ValidationContext) -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        ctx.error(f"{path}: unable to read JSON: {exc}")
        return
    for key in ("pipeline_id", "name", "timestamp", "ingestion_enabled"):
        if key not in payload:
            ctx.error(f"{path}: missing '{key}' field")


def _validate_parquet(path: Path, ctx: ValidationContext) -> None:
    if path.stat().st_size <= 0:
        ctx.error(f"{path}: parquet file is empty")


def _dispatch_validators() -> Dict[str, Callable[[Path, ValidationContext], None]]:
    return {
        "country_2020_2026.csv": _validate_country_csv,
        "country_2020_2026.sqlite": _validate_country_sqlite,
        "column_map.csv": _validate_column_map,
        "substate_entities_template.csv": lambda p, c: _validate_template(
            p,
            c,
            [
                "entity_id",
                "name",
                "country_iso3",
                "entity_type",
                "founded_year",
                "member_count",
                "member_count_year",
                "funding_usd",
                "funding_year",
                "funding_type",
                "source_name",
                "source_url",
                "source_date",
                "confidence",
            ],
        ),
        "substate_positions_template.csv": lambda p, c: _validate_template(
            p,
            c,
            [
                "entity_id",
                "year",
                "issue_code",
                "stance",
                "evidence_type",
                "evidence_url",
                "evidence_snippet",
                "source_name",
                "source_date",
                "confidence",
            ],
        ),
        "issue_catalog.csv": _validate_issue_catalog,
        "vparty_entities.csv": _validate_vparty_entities,
        "vparty_party_year.csv": _validate_vparty_party_year,
        "country_robustness_2020_2026.csv": _validate_country_robustness,
        "robustness_thresholds.json": _validate_robustness_thresholds,
        "pipeline_status.json": _validate_pipeline_status,
        "country_2020_2026.parquet": _validate_parquet,
    }


def _validate_outputs(output_dir: Path, require_optional: bool) -> ValidationContext:
    ctx = ValidationContext()
    validators = _dispatch_validators()

    for filename in CORE_FILES:
        path = output_dir / filename
        if not path.exists():
            ctx.error(f"{path}: missing required output")
            continue
        validators[filename](path, ctx)

    for filename in OPTIONAL_FILES:
        path = output_dir / filename
        if not path.exists():
            if require_optional:
                ctx.error(f"{path}: missing optional output (required by flag)")
            else:
                ctx.warn(f"{path}: optional output missing")
            continue
        validator = validators.get(filename)
        if validator:
            validator(path, ctx)

    return ctx


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate generated outputs in data/output")
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument(
        "--require-optional",
        action="store_true",
        help="Treat missing optional outputs as errors.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    ctx = _validate_outputs(output_dir, args.require_optional)

    for warning in ctx.warnings:
        print(f"[warn] {warning}")
    for error in ctx.errors:
        print(f"[error] {error}")

    if ctx.errors:
        print(f"Validation failed with {len(ctx.errors)} error(s).")
        return 1

    if ctx.warnings:
        print(f"Validation passed with {len(ctx.warnings)} warning(s).")
    else:
        print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
