#!/usr/bin/env python3
"""Filter sub-state entities by membership or funding thresholds."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Filter entities by min members or funding")
    parser.add_argument("--input", default="data/output/substate_entities.csv")
    parser.add_argument("--output", default="data/output/substate_entities_filtered.csv")
    parser.add_argument("--min-members", type=int, default=1000)
    parser.add_argument("--min-funding-usd", type=float, default=1_000_000_000)
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    df = pd.read_csv(in_path)

    if "member_count" not in df.columns:
        df["member_count"] = pd.NA
    if "funding_usd" not in df.columns:
        df["funding_usd"] = pd.NA

    members_ok = df["member_count"].fillna(0).astype(float) >= args.min_members
    funding_ok = df["funding_usd"].fillna(0).astype(float) >= args.min_funding_usd

    filtered = df[members_ok | funding_ok].copy()
    filtered.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
