#!/usr/bin/env python3
"""Fetch revenue-ranked company lists from Wikipedia tables.

This is a lightweight, open seed source. It scrapes a handful of Wikipedia
list pages and extracts company, revenue, employees, and headquarters.
"""

from __future__ import annotations

import argparse
import re
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests

PAGES = [
    {
        "id": "largest_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_by_revenue",
        "label": "Largest companies by revenue (global)",
    },
    {
        "id": "largest_us_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue",
        "label": "Largest companies in the United States by revenue",
    },
    {
        "id": "largest_europe_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Europe_by_revenue",
        "label": "Largest companies in Europe by revenue",
    },
    {
        "id": "largest_asia_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Asia",
        "label": "Largest companies in Asia by revenue",
    },
    {
        "id": "largest_private_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_private_non-governmental_companies_by_revenue",
        "label": "Largest private non-governmental companies by revenue",
    },
    {
        "id": "largest_manufacturing_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_manufacturing_companies_by_revenue",
        "label": "Largest manufacturing companies by revenue",
    },
]

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"

US_STATES = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "district of columbia",
}

COUNTRY_ALIASES = {
    "united states": "USA",
    "united states of america": "USA",
    "u.s.": "USA",
    "usa": "USA",
    "uk": "GBR",
    "united kingdom": "GBR",
    "south korea": "KOR",
    "north korea": "PRK",
    "russia": "RUS",
    "russian federation": "RUS",
    "iran": "IRN",
    "vietnam": "VNM",
    "laos": "LAO",
    "bolivia": "BOL",
    "venezuela": "VEN",
    "tanzania": "TZA",
    "czech republic": "CZE",
    "czechia": "CZE",
    "taiwan": "TWN",
    "hong kong": "HKG",
    "macau": "MAC",
}


def _normalize(text: str) -> str:
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r"\[.*?\]", "", text)
    text = text.replace("&", "and")
    text = re.sub(r"[^A-Za-z0-9\s-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _load_country_map() -> Dict[str, str]:
    candidates = []
    output = Path("data/output/country_2020_2026.csv")
    raw = Path("data/raw/un_members.csv")
    if output.exists():
        candidates.append(output)
    if raw.exists():
        candidates.append(raw)
    mapping: Dict[str, str] = {}
    for path in candidates:
        try:
            header = pd.read_csv(path, nrows=0)
        except Exception:
            continue
        cols = list(header.columns)
        usecols = [c for c in cols if c in ("iso3", "country_name", "name")]
        if not usecols:
            continue
        df = pd.read_csv(path, usecols=usecols, low_memory=False)
        if "iso3" not in df.columns:
            continue
        name_cols = [c for c in df.columns if "country" in c.lower() or c.lower() == "name"]
        if not name_cols:
            continue
        name_col = name_cols[0]
        for _, row in df[[name_col, "iso3"]].dropna().iterrows():
            key = _normalize(row[name_col])
            if key and key not in mapping:
                mapping[key] = str(row["iso3"]).upper()
    for alias, iso3 in COUNTRY_ALIASES.items():
        mapping[_normalize(alias)] = iso3
    return mapping


def _extract_country(headquarters: str) -> str:
    if not headquarters:
        return ""
    raw = re.sub(r"\[.*?\]", "", str(headquarters))
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    candidate = parts[-1] if parts else raw.strip()
    candidate_norm = _normalize(candidate)
    if candidate_norm in US_STATES:
        return "United States"
    return candidate.strip()


def _parse_revenue(value: str, col_name: str) -> Tuple[float | None, str]:
    if value is None:
        return None, ""
    raw = str(value)
    raw = re.sub(r"\[.*?\]", "", raw).strip()
    if not raw:
        return None, raw
    factor = 1.0
    col_lower = str(col_name).lower()
    if "billion" in col_lower:
        factor = 1e9
    elif "million" in col_lower:
        factor = 1e6
    elif "usd" in col_lower and "million" in col_lower:
        factor = 1e6
    elif "usd" in col_lower and "billion" in col_lower:
        factor = 1e9
    # try to detect explicit unit in value text
    raw_lower = raw.lower()
    if "billion" in raw_lower:
        factor = 1e9
    elif "million" in raw_lower:
        factor = 1e6

    cleaned = raw.replace("$", "").replace(",", " ")
    cleaned = cleaned.replace("(", " ").replace(")", " ")
    nums = re.findall(r"[-+]?\d*\.?\d+", cleaned)
    if not nums:
        return None, raw
    try:
        value_num = float(nums[0])
    except ValueError:
        return None, raw
    return value_num * factor, raw


def _parse_employees(value: str) -> float | None:
    if value is None:
        return None
    raw = str(value)
    raw = re.sub(r"\[.*?\]", "", raw)
    raw = raw.replace(",", "")
    nums = re.findall(r"[-+]?\d*\.?\d+", raw)
    if not nums:
        return None
    try:
        return float(nums[0])
    except ValueError:
        return None


def _select_table(tables: Iterable[pd.DataFrame]) -> pd.DataFrame | None:
    for table in tables:
        cols = [str(c).lower() for c in table.columns]
        if any("revenue" in c for c in cols) and any(
            "company" in c or "name" in c for c in cols
        ):
            return table
    return None


def _slugify(name: str) -> str:
    slug = _normalize(name)
    slug = slug.replace(" ", "-")
    return slug


def _fetch_tables(url: str) -> List[pd.DataFrame]:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    html = resp.text
    return pd.read_html(StringIO(html))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Wikipedia revenue lists")
    parser.add_argument("--output", default="data/raw/seeds/wikipedia_revenue_lists.csv")
    args = parser.parse_args()

    country_map = _load_country_map()
    rows: List[Dict[str, str]] = []

    for page in PAGES:
        try:
            tables = _fetch_tables(page["url"])
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] failed to fetch {page['url']}: {exc}")
            continue
        table = _select_table(tables)
        if table is None:
            print(f"[warn] no revenue table found for {page['url']}")
            continue

        cols = {str(c).lower(): c for c in table.columns}
        name_col = next((cols[c] for c in cols if "company" in c or "name" in c), None)
        revenue_col = next((cols[c] for c in cols if "revenue" in c), None)
        employees_col = next((cols[c] for c in cols if "employees" in c), None)
        hq_col = next((cols[c] for c in cols if "headquarters" in c), None)

        if name_col is None or revenue_col is None:
            continue

        for _, row in table.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or name.lower() == "nan":
                continue
            revenue_usd, revenue_raw = _parse_revenue(row.get(revenue_col, ""), str(revenue_col))
            employees = _parse_employees(row.get(employees_col, "")) if employees_col else None
            headquarters = str(row.get(hq_col, "")) if hq_col else ""
            country = _extract_country(headquarters)
            iso3 = country_map.get(_normalize(country), "") if country else ""

            entity_id = f"wiki:{page['id']}:{_slugify(name)}"
            rows.append(
                {
                    "entity_id": entity_id,
                    "name": name,
                    "country_iso3": iso3,
                    "entity_type": "company",
                    "sector_code": "",
                    "revenue_usd": "" if revenue_usd is None else f"{revenue_usd:.2f}",
                    "revenue_raw": revenue_raw,
                    "revenue_unit": "USD",
                    "employee_count": "" if employees is None else f"{employees:.0f}",
                    "employees_raw": "" if employees is None else f"{employees:.0f}",
                    "source": f"wikipedia:{page['id']}",
                    "source_url": page["url"],
                }
            )

    if not rows:
        raise SystemExit("No rows extracted from Wikipedia tables")

    df = pd.DataFrame(rows)
    df["name_norm"] = df["name"].apply(_normalize)
    df["revenue_usd_num"] = pd.to_numeric(df["revenue_usd"], errors="coerce")
    df = df.sort_values(["revenue_usd_num"], ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["name_norm"], keep="first")
    df = df.drop(columns=["name_norm", "revenue_usd_num"])

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
