#!/usr/bin/env python3
"""Fetch SEC company tickers as an open seed list (US public companies)."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

SEC_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT = "GaladData/0.1 (contact: data@galad.local)"


def _fetch_sec() -> List[Dict[str, str]]:
    resp = requests.get(
        SEC_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        },
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    rows: List[Dict[str, str]] = []
    for entry in payload.values():
        cik = str(entry.get("cik_str", "")).strip()
        name = str(entry.get("title", "")).strip()
        ticker = str(entry.get("ticker", "")).strip()
        if not cik or not name:
            continue
        entity_id = f"sec:{int(cik):010d}"
        rows.append(
            {
                "entity_id": entity_id,
                "name": name,
                "country_iso3": "USA",
                "entity_type": "company",
                "sector_code": "",
                "ticker": ticker,
                "source": "sec",
                "source_url": SEC_URL,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch SEC company tickers")
    parser.add_argument("--output", default="data/raw/seeds/sec_company_tickers.csv")
    parser.add_argument("--limit", type=int, default=0, help="Optional max rows to write")
    args = parser.parse_args()

    rows = _fetch_sec()
    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("SEC company tickers fetch returned no rows")
    df = df.drop_duplicates(subset=["entity_id"], keep="first")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
