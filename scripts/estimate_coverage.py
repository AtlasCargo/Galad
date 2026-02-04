#!/usr/bin/env python3
"""Estimate coverage gaps for org datasets by country and overlay tags."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


def _load_un_members(raw_dir: Path) -> List[str]:
    csv_path = raw_dir / "un_members.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        if "iso3" in df.columns:
            return sorted(df["iso3"].astype(str).str.upper().unique())
    # Fallback to any country output if present
    fallback = Path("data/output/country_2020_2026.csv")
    if fallback.exists():
        df = pd.read_csv(fallback, usecols=["iso3"])
        return sorted(df["iso3"].astype(str).str.upper().unique())
    return []


def _load_overlays(path: Path) -> List[str]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return [str(o.get("id")) for o in payload.get("overlays", [])]


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate coverage gaps by country and overlay")
    parser.add_argument("--input", default="data/output/org_classification_map.csv")
    parser.add_argument("--output", default="data/output/org_coverage_gaps.csv")
    parser.add_argument("--iso-col", default="iso3")
    parser.add_argument("--overlay-col", default="overlay_tags")
    parser.add_argument("--overlays", default="config/taxonomy/overlays.json")
    parser.add_argument("--raw-dir", default="data/raw")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    overlays = _load_overlays(Path(args.overlays))
    iso3_list = _load_un_members(Path(args.raw_dir))

    df = pd.read_csv(in_path)
    if args.iso_col not in df.columns:
        raise SystemExit(f"Missing ISO3 column '{args.iso_col}' in {in_path}")

    df[args.iso_col] = df[args.iso_col].astype(str).str.upper()
    df[args.overlay_col] = df.get(args.overlay_col, "").fillna("")

    # Expand overlay tags
    rows = []
    for _, row in df.iterrows():
        tags = [t for t in str(row[args.overlay_col]).split(",") if t]
        if not tags:
            tags = ["none"]
        for tag in tags:
            rows.append({"iso3": row[args.iso_col], "overlay": tag})

    tag_df = pd.DataFrame(rows)
    counts = tag_df.groupby(["iso3", "overlay"], dropna=False).size().reset_index(name="entity_count")

    out_rows: List[Dict[str, str]] = []
    target_overlays = overlays + ["none"] if overlays else ["none"]
    for iso3 in (iso3_list or sorted(df[args.iso_col].unique())):
        for overlay in target_overlays:
            subset = counts[(counts["iso3"] == iso3) & (counts["overlay"] == overlay)]
            count = int(subset["entity_count"].iloc[0]) if not subset.empty else 0
            out_rows.append(
                {
                    "iso3": iso3,
                    "overlay": overlay,
                    "entity_count": str(count),
                    "coverage_flag": "missing" if count == 0 else "present",
                }
            )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["iso3", "overlay", "entity_count", "coverage_flag"])
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
