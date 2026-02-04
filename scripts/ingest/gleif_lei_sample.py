#!/usr/bin/env python3
"""Fetch a sample of GLEIF LEI records as an open seed list."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

GLEIF_URL = "https://api.gleif.org/api/v1/lei-records"
ISO_MAP_URL = (
    "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/"
    "master/all/all.csv"
)
USER_AGENT = "GaladData/0.1 (contact: data@galad.local)"


def _ensure_iso_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        resp = requests.get(ISO_MAP_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
        resp.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(resp.content)
    df = pd.read_csv(path, dtype=str)
    iso2 = df.get("alpha-2")
    iso3 = df.get("alpha-3")
    if iso2 is None or iso3 is None:
        return {}
    mapping = {a.strip().upper(): b.strip().upper() for a, b in zip(iso2, iso3) if isinstance(a, str)}
    return mapping


def _fetch_page(page_number: int, page_size: int) -> dict:
    params = {
        "page[number]": page_number,
        "page[size]": page_size,
        "filter[registration.status]": "ISSUED",
    }
    resp = requests.get(
        GLEIF_URL,
        params=params,
        headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.api+json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch GLEIF LEI sample")
    parser.add_argument("--output", default="data/raw/seeds/gleif_lei_sample.csv")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--iso-map", default="data/raw/iso2_iso3.csv")
    args = parser.parse_args()

    iso_map = _ensure_iso_map(Path(args.iso_map))

    rows: List[Dict[str, str]] = []
    total = args.limit
    page_number = 1
    while total > 0:
        page_size = min(args.page_size, total)
        payload = _fetch_page(page_number, page_size)
        data = payload.get("data", [])
        if not data:
            break
        for record in data:
            lei = record.get("id", "")
            attrs = record.get("attributes", {})
            entity = attrs.get("entity", {})
            name = entity.get("legalName", {}).get("name", "")
            legal_addr = entity.get("legalAddress", {}) or {}
            hq_addr = entity.get("headquartersAddress", {}) or {}
            country_iso2 = (
                str(legal_addr.get("country", "")) or str(hq_addr.get("country", ""))
            ).upper()
            country_iso3 = iso_map.get(country_iso2, "")
            if not lei or not name:
                continue
            rows.append(
                {
                    "entity_id": f"lei:{lei}",
                    "name": name,
                    "country_iso3": country_iso3,
                    "entity_type": "company",
                    "sector_code": "",
                    "source": "gleif",
                    "source_url": GLEIF_URL,
                }
            )
        total -= page_size
        page_number += 1
        time.sleep(max(args.delay, 0.0))

    if not rows:
        raise SystemExit("GLEIF fetch returned no rows")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["entity_id"], keep="first")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
