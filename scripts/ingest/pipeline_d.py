#!/usr/bin/env python3
"""Influence-first ingestion (Pipeline D).

Merges seed CSVs from data/raw/seeds with V-Party entities (if present),
computes an influence score from available signals, and writes
`data/output/top_actors_influence.csv`.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

SIGNAL_COLUMNS = [
    "revenue_usd",
    "assets_usd",
    "budget_usd",
    "users",
    "audience",
    "member_count",
    "employee_count",
    "years_active",
]


def _load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_seed_files(seed_dir: Path) -> pd.DataFrame:
    files = sorted(glob.glob(str(seed_dir / "*.csv")))
    frames: List[pd.DataFrame] = []
    for file in files:
        df = pd.read_csv(file)
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _vparty_entities(vparty_entities_path: Path, vparty_party_year_path: Path) -> pd.DataFrame:
    if not vparty_entities_path.exists() or not vparty_party_year_path.exists():
        return pd.DataFrame()

    entities = pd.read_csv(vparty_entities_path)
    party_year = pd.read_csv(vparty_party_year_path, low_memory=False)

    # Derive years_active as count of distinct years per party name + iso3
    if "party_name" in party_year.columns and "iso3" in party_year.columns and "year" in party_year.columns:
        years = (
            party_year[["iso3", "party_name", "year"]]
            .dropna()
            .drop_duplicates()
            .groupby(["iso3", "party_name"])
            .size()
            .reset_index(name="years_active")
        )
    else:
        years = pd.DataFrame(columns=["iso3", "party_name", "years_active"])

    entities = entities.rename(
        columns={
            "country_iso3": "country_iso3",
            "name": "name",
            "entity_id": "entity_id",
            "entity_type": "entity_type",
        }
    )
    if "country_iso3" in entities.columns:
        entities["country_iso3"] = entities["country_iso3"].astype(str).str.upper()

    entities = entities.merge(
        years,
        how="left",
        left_on=["country_iso3", "name"],
        right_on=["iso3", "party_name"],
    )

    entities = entities.drop(columns=[c for c in ["iso3", "party_name"] if c in entities.columns])
    return entities


def _normalize(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    min_v = s.min()
    max_v = s.max()
    if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
        return pd.Series([np.nan] * len(s), index=s.index)
    return (s - min_v) / (max_v - min_v)


def _compute_influence(df: pd.DataFrame, weights: Dict[str, float]) -> pd.Series:
    score = pd.Series(0.0, index=df.index, dtype="float64")
    total_weight = pd.Series(0.0, index=df.index, dtype="float64")
    for signal, weight in weights.items():
        if signal not in df.columns:
            continue
        norm = _normalize(df[signal])
        mask = norm.notna()
        if mask.sum() == 0:
            continue
        w = float(weight)
        score = score + norm.fillna(0.0) * w
        total_weight = total_weight + mask.astype("float64") * w
    total_weight = total_weight.replace(0.0, np.nan)
    return score / total_weight


def _ensure_required(df: pd.DataFrame, ctx: List[str]) -> None:
    required = ["entity_id", "name", "country_iso3", "entity_type"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        ctx.append(f"Missing required columns: {', '.join(missing)}")


def _map_seed_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "employee_count" not in df.columns and "employees_raw" in df.columns:
        df["employee_count"] = pd.to_numeric(df["employees_raw"], errors="coerce")

    if "revenue_usd" not in df.columns and "revenue_raw" in df.columns:
        revenue = pd.to_numeric(df["revenue_raw"], errors="coerce")
        if "revenue_unit" in df.columns:
            unit = df["revenue_unit"].astype(str)
            is_usd = unit.str.contains("Q4917", na=False) | unit.str.contains("USD", case=False, na=False)
            df["revenue_usd"] = revenue.where(is_usd)
        else:
            df["revenue_usd"] = pd.Series([np.nan] * len(df), index=df.index)

    return df


def _dedupe_entities(df: pd.DataFrame) -> pd.DataFrame:
    if "entity_id" not in df.columns:
        return df

    signal_cols = [c for c in SIGNAL_COLUMNS if c in df.columns]
    extra_cols = [c for c in ["revenue_usd", "revenue_raw", "employee_count", "years_active"] if c in df.columns]
    score_cols = list(dict.fromkeys(signal_cols + extra_cols))
    if score_cols:
        df["_signal_count"] = df[score_cols].notna().sum(axis=1)
    else:
        df["_signal_count"] = 0

    revenue = pd.Series([np.nan] * len(df), index=df.index)
    if "revenue_usd" in df.columns:
        revenue = pd.to_numeric(df["revenue_usd"], errors="coerce")
    if "revenue_raw" in df.columns:
        revenue = revenue.fillna(pd.to_numeric(df["revenue_raw"], errors="coerce"))
    df["_revenue_rank"] = revenue

    df = df.sort_values(
        ["_signal_count", "_revenue_rank"],
        ascending=[False, False],
        na_position="last",
    )
    df = df.drop_duplicates(subset=["entity_id"], keep="first").copy()
    return df.drop(columns=["_signal_count", "_revenue_rank"], errors="ignore")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline D ingestion")
    parser.add_argument("--config", default="config/pipelines/pipeline_d.json")
    parser.add_argument("--seed-dir", default="data/raw/seeds")
    parser.add_argument("--vparty-entities", default="data/output/vparty_entities.csv")
    parser.add_argument("--vparty-party-year", default="data/output/vparty_party_year.csv")
    parser.add_argument("--output", default="data/output/top_actors_influence.csv")
    args = parser.parse_args()

    config = _load_config(Path(args.config))
    weights = config.get("influence_overlay", {}).get("weights", {})
    if not weights:
        # default weights
        weights = {
            "revenue_usd": 0.3,
            "assets_usd": 0.2,
            "budget_usd": 0.2,
            "users": 0.15,
            "audience": 0.1,
            "member_count": 0.05,
            "employee_count": 0.1,
            "years_active": 0.05,
        }

    seed_df = _read_seed_files(Path(args.seed_dir))
    vparty_df = _vparty_entities(Path(args.vparty_entities), Path(args.vparty_party_year))

    combined = pd.DataFrame()
    if not seed_df.empty:
        combined = seed_df.copy()
    if not vparty_df.empty:
        combined = pd.concat([combined, vparty_df], ignore_index=True) if not combined.empty else vparty_df.copy()

    if combined.empty:
        raise SystemExit("No seed data found. Add CSVs to data/raw/seeds or provide V-Party outputs.")

    combined["country_iso3"] = combined["country_iso3"].astype(str).str.upper()
    combined = _map_seed_columns(combined)
    combined = _dedupe_entities(combined)
    for col in SIGNAL_COLUMNS:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # Compute influence score
    combined["influence_score"] = _compute_influence(combined, weights)

    # Rank with nulls last
    combined["rank"] = combined["influence_score"].rank(ascending=False, method="dense", na_option="bottom")

    # Ensure output columns
    base_cols = [
        "entity_id",
        "name",
        "country_iso3",
        "entity_type",
        "influence_score",
        "rank",
    ]
    output_cols = base_cols.copy()
    if "sector_code" in combined.columns:
        output_cols.insert(4, "sector_code")
    output_cols += [c for c in SIGNAL_COLUMNS if c in combined.columns]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined[output_cols].to_csv(out_path, index=False)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
