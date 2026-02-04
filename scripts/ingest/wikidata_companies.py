#!/usr/bin/env python3
"""Fetch company data from Wikidata SPARQL.

Outputs a seed CSV usable by Pipeline D.
"""

from __future__ import annotations

import argparse
import csv
import random
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

WDQS_URL = "https://query.wikidata.org/sparql"
USD_QIDS = {"Q4917"}
USD_LABELS = {
    "usd",
    "us dollar",
    "u.s. dollar",
    "united states dollar",
    "us dollars",
    "u.s. dollars",
}


def _query_sparql(
    query: str,
    retries: int,
    backoff: float,
    max_backoff: float,
    jitter: float,
    timeout: int,
) -> tuple[dict, int]:
    headers = {"Accept": "application/sparql-results+json"}
    params = {"query": query}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(WDQS_URL, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as exc:
            sleep_for = min(backoff * (2 ** (attempt - 1)), max_backoff)
            sleep_for = sleep_for * (1.0 + random.random() * jitter)
            time.sleep(sleep_for)
            if attempt == retries:
                raise RuntimeError(f"WDQS request failed after retries: {exc}") from exc
            continue
        if resp.status_code == 200:
            return resp.json(), attempt
        if resp.status_code in (429, 503, 504):
            sleep_for = min(backoff * (2 ** (attempt - 1)), max_backoff)
            sleep_for = sleep_for * (1.0 + random.random() * jitter)
            time.sleep(sleep_for)
            continue
        raise RuntimeError(f"WDQS error {resp.status_code}: {resp.text[:200]}")
    raise RuntimeError("WDQS query failed after retries")


def _build_query(limit: int, offset: int, order_by_revenue: bool) -> str:
    order_clause = "ORDER BY DESC(?revenue)" if order_by_revenue else ""
    return f"""
SELECT ?company ?companyLabel ?revenue ?employees ?iso3 WHERE {{
  ?company wdt:P31/wdt:P279* wd:Q4830453 .
  ?company wdt:P2139 ?revenue .
  OPTIONAL {{ ?company wdt:P1128 ?employees . }}
  OPTIONAL {{ ?company wdt:P17 ?country .
             OPTIONAL {{ ?country wdt:P298 ?iso3 . }}
           }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
{order_clause}
LIMIT {limit}
OFFSET {offset}
"""


def _parse_value(binding: dict, key: str) -> str:
    if key not in binding:
        return ""
    return binding[key].get("value", "")


def _extract_qid(value: str) -> str:
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value.rsplit("/", 1)[-1]
    return value


def _is_usd(unit_value: str) -> bool:
    if not unit_value:
        return False
    qid = _extract_qid(unit_value)
    if qid in USD_QIDS:
        return True
    unit_lc = unit_value.lower()
    return unit_lc in USD_LABELS


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch company list from Wikidata")
    parser.add_argument("--output", default="data/raw/seeds/wikidata_companies.csv")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--min-page-size", type=int, default=100)
    parser.add_argument("--no-order", action="store_true", help="Disable ORDER BY (faster, not revenue-ranked)")
    parser.add_argument("--adaptive", action="store_true", help="Use adaptive backoff and dynamic page size")
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--base-backoff", type=float, default=2.0)
    parser.add_argument("--max-backoff", type=float, default=60.0)
    parser.add_argument("--jitter", type=float, default=0.25)
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between page requests (seconds)")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, str]] = []
    page_size = max(args.page_size, 1)
    backoff = max(args.base_backoff, 0.1)
    remaining = args.limit
    offset = 0
    while remaining > 0:
        page = min(page_size, remaining)
        query = _build_query(page, offset, order_by_revenue=not args.no_order)
        payload, attempts = _query_sparql(
            query,
            retries=args.retries,
            backoff=backoff,
            max_backoff=args.max_backoff,
            jitter=args.jitter,
            timeout=args.timeout,
        )
        bindings = payload.get("results", {}).get("bindings", [])
        if not bindings:
            break
        for b in bindings:
            company_uri = _parse_value(b, "company")
            qid = company_uri.rsplit("/", 1)[-1] if company_uri else ""
            revenue = _parse_value(b, "revenue")
            revenue_unit = _parse_value(b, "revenueUnit")
            revenue_time = _parse_value(b, "revenueTime")
            employees = _parse_value(b, "employees")
            iso3 = _parse_value(b, "iso3")
            revenue_unit_qid = _extract_qid(revenue_unit)
            revenue_usd = revenue if _is_usd(revenue_unit) else ""
            rows.append(
                {
                    "entity_id": f"wd:{qid}" if qid else "",
                    "name": _parse_value(b, "companyLabel"),
                    "country_iso3": iso3.upper(),
                    "entity_type": "company",
                    "sector_code": "",
                    "revenue_usd": revenue_usd,
                    "revenue_raw": revenue,
                    "revenue_unit": revenue_unit_qid,
                    "revenue_time": revenue_time,
                    "employee_count": employees,
                    "employees_raw": employees,
                    "industry": _parse_value(b, "industryLabel"),
                    "source": "wikidata",
                }
            )
        remaining -= page
        offset += page
        if args.adaptive:
            if attempts > 1:
                backoff = min(backoff * 1.5, args.max_backoff)
                if page_size > args.min_page_size:
                    page_size = max(args.min_page_size, int(page_size * 0.8))
            else:
                backoff = max(args.base_backoff, backoff * 0.9)
        time.sleep(max(args.delay, 0.0))

    fieldnames = [
        "entity_id",
        "name",
        "country_iso3",
        "entity_type",
        "sector_code",
        "revenue_usd",
        "revenue_raw",
        "revenue_unit",
        "revenue_time",
        "employee_count",
        "employees_raw",
        "industry",
        "source",
    ]
    if rows:
        df = pd.DataFrame(rows)
        df = df[df["entity_id"].astype(str).str.len() > 0].copy()
        revenue_usd = pd.to_numeric(df["revenue_usd"].replace("", pd.NA), errors="coerce")
        revenue_raw = pd.to_numeric(df["revenue_raw"].replace("", pd.NA), errors="coerce")
        employees = pd.to_numeric(df["employee_count"].replace("", pd.NA), errors="coerce")
        df["revenue_numeric"] = revenue_usd.fillna(revenue_raw)
        df["employee_numeric"] = employees
        df = df.sort_values(
            ["revenue_numeric", "employee_numeric"],
            ascending=[False, False],
            na_position="last",
        )
        df = df.drop_duplicates(subset=["entity_id"], keep="first")
        rows = df[fieldnames].to_dict(orient="records")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {out_path} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
