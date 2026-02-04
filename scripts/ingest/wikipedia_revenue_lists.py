#!/usr/bin/env python3
"""Fetch revenue-ranked company lists from Wikipedia tables.

This is a lightweight, open seed source. It scrapes a handful of Wikipedia
list pages and extracts company, revenue, employees, and headquarters.
"""

from __future__ import annotations

import argparse
import re
import urllib.parse
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
        "id": "fortune_global_500",
        "url": "https://en.wikipedia.org/wiki/Fortune_Global_500",
        "label": "Fortune Global 500",
    },
    {
        "id": "fortune_500",
        "url": "https://en.wikipedia.org/wiki/Fortune_500",
        "label": "Fortune 500 (US)",
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
        "id": "largest_uk_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_Kingdom",
        "label": "Largest companies in the United Kingdom",
    },
    {
        "id": "largest_germany_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_German_companies",
        "label": "Largest companies in Germany",
    },
    {
        "id": "largest_france_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_French_companies",
        "label": "Largest companies in France",
    },
    {
        "id": "largest_italy_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_Italian_companies",
        "label": "Largest companies in Italy",
    },
    {
        "id": "largest_spain_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_Spanish_companies",
        "label": "Largest companies in Spain",
    },
    {
        "id": "largest_asia_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Asia",
        "label": "Largest companies in Asia by revenue",
    },
    {
        "id": "largest_japan_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Japan",
        "label": "Largest companies in Japan",
    },
    {
        "id": "largest_india_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_India",
        "label": "Largest companies in India",
    },
    {
        "id": "largest_canada_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Canada",
        "label": "Largest companies in Canada",
    },
    {
        "id": "largest_australia_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_companies_in_Australia",
        "label": "Largest companies in Australia",
    },
    {
        "id": "largest_brazil_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_Brazilian_companies",
        "label": "Largest companies in Brazil",
    },
    {
        "id": "largest_mexico_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_Mexican_companies",
        "label": "Largest companies in Mexico",
    },
    {
        "id": "largest_russia_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_companies_of_Russia",
        "label": "Largest companies in Russia (List of companies of Russia)",
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
    {
        "id": "forbes_global_2000",
        "url": "https://en.wikipedia.org/wiki/Forbes_Global_2000",
        "label": "Forbes Global 2000",
    },
    {
        "id": "largest_technology_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue",
        "label": "Largest technology companies by revenue",
    },
    {
        "id": "largest_oil_and_gas_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_oil_and_gas_companies_by_revenue",
        "label": "Largest oil and gas companies by revenue",
    },
    {
        "id": "largest_energy_companies_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_energy_companies_by_revenue",
        "label": "Largest energy companies by revenue",
    },
    {
        "id": "largest_telecommunications_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_telecommunications_companies",
        "label": "Largest telecommunications companies",
    },
    {
        "id": "largest_retail_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_retail_companies",
        "label": "Largest retail companies",
    },
    {
        "id": "largest_banks",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_banks",
        "label": "Largest banks",
    },
    {
        "id": "largest_insurance_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_insurance_companies",
        "label": "Largest insurance companies",
    },
    {
        "id": "largest_pharmaceutical_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_pharmaceutical_companies",
        "label": "Largest pharmaceutical companies",
    },
    {
        "id": "largest_automotive_manufacturers_by_revenue",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_automotive_manufacturers_by_revenue",
        "label": "Largest automotive manufacturers by revenue",
    },
    {
        "id": "largest_software_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_software_companies",
        "label": "Largest software companies",
    },
    {
        "id": "largest_media_companies",
        "url": "https://en.wikipedia.org/wiki/List_of_largest_media_companies",
        "label": "Largest media companies",
    },
]

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
API_URL = "https://en.wikipedia.org/w/api.php"
DEFAULT_DISCOVER_QUERIES = [
    "list of largest companies by revenue",
    "list of largest companies in",
    "list of largest companies of",
    "largest companies by revenue list",
    "largest companies list",
    "largest technology companies by revenue",
    "largest retail companies",
]

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
        has_value_col = any("revenue" in c for c in cols) or any("sales" in c for c in cols) or any(
            "turnover" in c for c in cols
        )
        has_name_col = any("company" in c or "name" in c for c in cols)
        if has_value_col and has_name_col:
            return table
    return None


def _slugify(name: str) -> str:
    slug = _normalize(name)
    slug = slug.replace(" ", "-")
    return slug


def _discover_pages(queries: List[str], limit: int) -> List[Dict[str, str]]:
    discovered: List[Dict[str, str]] = []
    seen_urls = {p["url"] for p in PAGES}
    for query in queries:
        params = {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "srlimit": limit,
            "format": "json",
        }
        resp = requests.get(API_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=60)
        if resp.status_code != 200:
            continue
        payload = resp.json()
        for item in payload.get("query", {}).get("search", []):
            title = str(item.get("title", "")).strip()
            if not title:
                continue
            title_lc = title.lower()
            if "list of" not in title_lc or "company" not in title_lc:
                continue
            url = "https://en.wikipedia.org/wiki/" + urllib.parse.quote(title.replace(" ", "_"))
            if url in seen_urls:
                continue
            discovered.append(
                {
                    "id": f"discovered_{_slugify(title)}",
                    "url": url,
                    "label": title,
                }
            )
            seen_urls.add(url)
    return discovered


def _fetch_tables(url: str) -> List[pd.DataFrame]:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    html = resp.text
    return pd.read_html(StringIO(html))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Wikipedia revenue lists")
    parser.add_argument("--output", default="data/raw/seeds/wikipedia_revenue_lists.csv")
    parser.add_argument("--discover", action="store_true", help="Search Wikipedia for additional list pages")
    parser.add_argument("--discover-limit", type=int, default=30, help="Max results per search query")
    parser.add_argument(
        "--discover-query",
        action="append",
        default=[],
        help="Additional discovery query (repeatable)",
    )
    args = parser.parse_args()

    country_map = _load_country_map()
    rows: List[Dict[str, str]] = []

    pages = list(PAGES)
    if args.discover:
        queries = args.discover_query or DEFAULT_DISCOVER_QUERIES
        pages.extend(_discover_pages(queries, args.discover_limit))

    for page in pages:
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
        if revenue_col is None:
            revenue_col = next((cols[c] for c in cols if "sales" in c), None)
        if revenue_col is None:
            revenue_col = next((cols[c] for c in cols if "turnover" in c), None)
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
