#!/usr/bin/env python3
"""Build a neutral country-year dataset from multiple public sources.

Outputs CSV/SQLite/Parquet for UN member states over a year range.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

UNMEMBERS_URL = "https://raw.githubusercontent.com/mledoze/countries/master/dist/countries.json"

DATASET_PREFIXES = {
    "vdem": "vdem__",
    "fh": "fh__",
    "hrmi": "hrmi__",
    "rsf": "rsf__",
    "wgi": "wgi__",
    "cpi": "cpi__",
    "gsi": "gsi__",
    "afi": "afi__",
}


def _safe_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def _normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = name.replace("&", "and")
    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    for prefix in ("the ",):
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name


class CountryMatcher:
    def __init__(self, countries: List[dict]):
        self.name_to_iso3: Dict[str, str] = {}
        for c in countries:
            iso3 = c.get("cca3")
            if not iso3:
                continue
            names: List[str] = []
            name_obj = c.get("name", {})
            for key in ("common", "official"):
                if isinstance(name_obj, dict) and name_obj.get(key):
                    names.append(name_obj[key])
            alt = c.get("altSpellings") or []
            if isinstance(alt, list):
                names.extend(alt)
            for n in names:
                key = _normalize_name(str(n))
                if key and key not in self.name_to_iso3:
                    self.name_to_iso3[key] = iso3

    def match(self, name: str) -> Optional[str]:
        if not name:
            return None
        key = _normalize_name(name)
        return self.name_to_iso3.get(key)



def _read_un_members(raw_dir: Path) -> Tuple[pd.DataFrame, List[dict]]:
    csv_path = raw_dir / "un_members.csv"
    json_path = raw_dir / "un_members.json"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        if "iso3" not in df.columns:
            raise ValueError("un_members.csv must include an 'iso3' column")
        df["iso3"] = df["iso3"].astype(str).str.upper()
        return df, []
    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = []
        for c in data:
            if not c.get("unMember"):
                continue
            rows.append({
                "iso3": c.get("cca3"),
                "name": c.get("name", {}).get("common"),
                "official": c.get("name", {}).get("official"),
            })
        df = pd.DataFrame(rows)
        df["iso3"] = df["iso3"].astype(str).str.upper()
        return df, data

    # Attempt to fetch from mledoze if not provided
    try:
        import urllib.request
        with urllib.request.urlopen(UNMEMBERS_URL, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        rows = []
        for c in data:
            if not c.get("unMember"):
                continue
            rows.append({
                "iso3": c.get("cca3"),
                "name": c.get("name", {}).get("common"),
                "official": c.get("name", {}).get("official"),
            })
        df = pd.DataFrame(rows)
        df["iso3"] = df["iso3"].astype(str).str.upper()
        df.to_csv(csv_path, index=False)
        return df, data
    except Exception as exc:
        raise RuntimeError(
            "Unable to load UN members list. Provide data/raw/un_members.csv or data/raw/un_members.json."
        ) from exc



def _detect_iso3(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "iso3",
        "iso3c",
        "country_text_id",
        "country_code",
        "country_iso3",
        "cca3",
        "ISO3",
        "Country Code",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _detect_year(df: pd.DataFrame) -> Optional[str]:
    candidates = ["year", "Year", "edition", "Edition", "date"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _prefix_columns(df: pd.DataFrame, prefix: str, key_cols: Iterable[str], colmap: List[Tuple[str, str, str]]) -> pd.DataFrame:
    rename = {}
    used = set(df.columns)
    for c in df.columns:
        if c in key_cols:
            continue
        safe = _safe_col(c)
        new = f"{prefix}{safe}"
        # ensure uniqueness
        if new in used:
            i = 2
            while f"{new}_{i}" in used:
                i += 1
            new = f"{new}_{i}"
        rename[c] = new
        used.add(new)
        colmap.append((prefix.rstrip("__"), c, new))
    return df.rename(columns=rename)


def _coerce_year(df: pd.DataFrame, year_col: str) -> pd.DataFrame:
    df = df.copy()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    return df


def _filter_years(df: pd.DataFrame, year_col: str, start_year: int, end_year: int) -> pd.DataFrame:
    return df[(df[year_col] >= start_year) & (df[year_col] <= end_year)].copy()


def load_vdem(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    if not iso_col or not year_col:
        raise ValueError("V-Dem file missing iso3 or year columns")
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    df[iso_col] = df[iso_col].astype(str).str.upper()
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["vdem"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def load_freedom_house(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]], matcher: CountryMatcher) -> pd.DataFrame:
    # Freedom House files often include an index sheet and a data sheet with a title row.
    # Try to locate the sheet that contains a year/edition column and country names.
    df = None
    try:
        xl = pd.ExcelFile(path)
        for sheet in xl.sheet_names:
            for header in (1, 0):
                try:
                    candidate = pd.read_excel(path, sheet_name=sheet, header=header)
                except Exception:
                    continue
                year_col = _detect_year(candidate)
                if not year_col:
                    continue
                if any(c in candidate.columns for c in ["Country/Territory", "Country", "Territory"]):
                    df = candidate
                    break
            if df is not None:
                break
    except Exception:
        df = None

    if df is None:
        df = pd.read_excel(path)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    if not year_col:
        # try to infer from "Edition" or fall back to a constant
        raise ValueError("Freedom House file missing year column")
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    if iso_col:
        df[iso_col] = df[iso_col].astype(str).str.upper()
    else:
        # attempt to match by country name
        name_col = None
        for c in ["Country/Territory", "Country", "country", "Territory"]:
            if c in df.columns:
                name_col = c
                break
        if not name_col:
            raise ValueError("Freedom House file missing ISO3 and country name columns")
        df["iso3"] = df[name_col].apply(lambda x: matcher.match(str(x)) if pd.notna(x) else None)
        iso_col = "iso3"
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["fh"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def load_hrmi(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.read_csv(path)
    iso_col = _detect_iso3(df) or "iso3"
    if iso_col not in df.columns:
        raise ValueError("HRMI file missing ISO3 column")
    year_col = _detect_year(df)
    if not year_col:
        raise ValueError("HRMI file missing year column")
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    df[iso_col] = df[iso_col].astype(str).str.upper()

    # If long format with 'indicator' and 'score' columns, pivot to wide
    if "indicator" in df.columns:
        value_cols = [c for c in df.columns if c not in [iso_col, year_col, "indicator"]]
        pivot_frames = []
        for val in value_cols:
            tmp = df.pivot_table(index=[iso_col, year_col], columns="indicator", values=val, aggfunc="first")
            tmp.columns = [f"{val}__{_safe_col(str(c))}" for c in tmp.columns]
            pivot_frames.append(tmp)
        wide = pd.concat(pivot_frames, axis=1).reset_index()
        df = wide
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["hrmi"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def load_rsf(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]], matcher: CountryMatcher) -> pd.DataFrame:
    df = pd.read_csv(path)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    if not year_col:
        raise ValueError("RSF file missing year column")
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    if iso_col:
        df[iso_col] = df[iso_col].astype(str).str.upper()
    else:
        name_col = None
        for c in ["Country", "country", "Country name", "Country Name"]:
            if c in df.columns:
                name_col = c
                break
        if not name_col:
            raise ValueError("RSF file missing ISO3 and country name columns")
        df["iso3"] = df[name_col].apply(lambda x: matcher.match(str(x)) if pd.notna(x) else None)
        iso_col = "iso3"
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["rsf"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def load_wgi(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.read_excel(path)
    # Expected columns (single-sheet WGI export):
    # Country Name, Country Code, Indicator Name, Indicator Code, Year, Estimate, etc.
    if "Country Code" in df.columns and "Year" in df.columns and "Indicator Code" in df.columns:
        df = _coerce_year(df, "Year")
        df = _filter_years(df, "Year", start_year, end_year)
        df["Country Code"] = df["Country Code"].astype(str).str.upper()

        id_cols = ["Country Code", "Year", "Indicator Code"]
        value_cols = [c for c in df.columns if c not in id_cols + ["Country Name", "Indicator Name"]]
        frames = []
        for val in value_cols:
            tmp = df.pivot_table(index=["Country Code", "Year"], columns="Indicator Code", values=val, aggfunc="first")
            tmp.columns = [f"{val}__{_safe_col(str(c))}" for c in tmp.columns]
            frames.append(tmp)
        wide = pd.concat(frames, axis=1).reset_index()
        key_cols = ["Country Code", "Year"]
        wide = _prefix_columns(wide, DATASET_PREFIXES["wgi"], key_cols, colmap)
        wide = wide.rename(columns={"Country Code": "iso3", "Year": "year"})
        return wide

    # Multi-sheet WGI replication package format (one sheet per indicator)
    xl = pd.ExcelFile(path)
    frames = []
    for sheet in xl.sheet_names:
        tmp = pd.read_excel(path, sheet_name=sheet)
        if "Economy (code)" not in tmp.columns or "Year" not in tmp.columns:
            continue
        tmp = _coerce_year(tmp, "Year")
        tmp = _filter_years(tmp, "Year", start_year, end_year)
        tmp["Economy (code)"] = tmp["Economy (code)"].astype(str).str.upper()

        drop_cols = [
            "ID variable (economy code/ gov. dimension/ year)",
            "Economy (name)",
            "Region",
            "Income classification",
            "Governance dimension",
        ]
        cols = [c for c in tmp.columns if c not in drop_cols]
        tmp = tmp[cols]

        rename = {}
        for c in tmp.columns:
            if c in ("Economy (code)", "Year"):
                continue
            rename[c] = f"{sheet}__{_safe_col(str(c))}"
        tmp = tmp.rename(columns=rename)
        tmp = tmp.rename(columns={"Economy (code)": "iso3", "Year": "year"})
        frames.append(tmp)

    if not frames:
        raise ValueError("WGI file missing expected columns or sheets")

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["iso3", "year"], how="outer")
    key_cols = ["iso3", "year"]
    merged = _prefix_columns(merged, DATASET_PREFIXES["wgi"], key_cols, colmap)
    return merged


def load_cpi(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]], matcher: CountryMatcher) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)

    if year_col and iso_col:
        df = _coerce_year(df, year_col)
        df = _filter_years(df, year_col, start_year, end_year)
        df[iso_col] = df[iso_col].astype(str).str.upper()
        key_cols = [iso_col, year_col]
        df = _prefix_columns(df, DATASET_PREFIXES["cpi"], key_cols, colmap)
        df = df.rename(columns={iso_col: "iso3", year_col: "year"})
        return df

    # Try to reshape wide format with year columns
    # Identify year columns like 2020, 2021, ...
    year_cols = [c for c in df.columns if re.fullmatch(r"20\d{2}", str(c))]
    if not year_cols:
        raise ValueError("CPI file missing ISO3/year columns and no year columns detected")

    name_col = None
    for c in ["Country", "country", "Jurisdiction", "jurisdiction", "Country/Territory"]:
        if c in df.columns:
            name_col = c
            break
    if not name_col:
        raise ValueError("CPI file missing country name column")

    melted = df.melt(id_vars=[name_col], value_vars=year_cols, var_name="year", value_name="cpi_score")
    melted["year"] = pd.to_numeric(melted["year"], errors="coerce").astype("Int64")
    melted = _filter_years(melted, "year", start_year, end_year)
    melted["iso3"] = melted[name_col].apply(lambda x: matcher.match(str(x)) if pd.notna(x) else None)
    key_cols = ["iso3", "year"]
    melted = _prefix_columns(melted, DATASET_PREFIXES["cpi"], key_cols, colmap)
    return melted


def load_gsi(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]], matcher: CountryMatcher) -> pd.DataFrame:
    df = pd.read_csv(path)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    if not year_col:
        # if no year, assume 2023
        df["year"] = 2023
        year_col = "year"
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    if iso_col:
        df[iso_col] = df[iso_col].astype(str).str.upper()
    else:
        name_col = None
        for c in ["Country", "country", "Country name", "Country Name"]:
            if c in df.columns:
                name_col = c
                break
        if not name_col:
            raise ValueError("GSI file missing ISO3 and country name columns")
        df["iso3"] = df[name_col].apply(lambda x: matcher.match(str(x)) if pd.notna(x) else None)
        iso_col = "iso3"
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["gsi"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def load_afi(path: Path, start_year: int, end_year: int, colmap: List[Tuple[str, str, str]]) -> pd.DataFrame:
    df = pd.read_csv(path)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    if not iso_col or not year_col:
        raise ValueError("AFI file missing iso3 or year columns")
    df = _coerce_year(df, year_col)
    df = _filter_years(df, year_col, start_year, end_year)
    df[iso_col] = df[iso_col].astype(str).str.upper()
    key_cols = [iso_col, year_col]
    df = _prefix_columns(df, DATASET_PREFIXES["afi"], key_cols, colmap)
    df = df.rename(columns={iso_col: "iso3", year_col: "year"})
    return df


def _load_if_exists(path: Path, loader, *args):
    if path.exists():
        return loader(path, *args)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Build country-year dataset from multiple sources")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--vdem", default="data/raw/vdem_cy_full.csv")
    parser.add_argument("--vdem-core", default="data/raw/vdem_cy_core.csv")
    parser.add_argument("--freedom-house", default="data/raw/freedom_house_all_data.xlsx")
    parser.add_argument("--freedom-house-alt", default="data/raw/freedom_house_ratings.xlsx")
    parser.add_argument("--hrmi", default="data/raw/hrmi_rights_tracker.csv")
    parser.add_argument("--rsf", default="data/raw/rsf_press_freedom.csv")
    parser.add_argument("--wgi", default="data/raw/wgi.xlsx")
    parser.add_argument("--cpi", default="data/raw/cpi.xlsx")
    parser.add_argument("--cpi-alt", default="data/raw/cpi.csv")
    parser.add_argument("--gsi", default="data/raw/gsi_2023.csv")
    parser.add_argument("--afi", default="data/raw/afi_core.csv")
    parser.add_argument("--un-members", default="data/raw/un_members.csv")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Allow missing source files (default is to require all sources).",
    )

    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Enforce presence of all sources unless --allow-missing is used.
    missing = []
    def pick_path(primary: str, alt: Optional[str] = None) -> Optional[Path]:
        p = Path(primary)
        if p.exists():
            return p
        if alt:
            a = Path(alt)
            if a.exists():
                return a
        return None

    vdem_path = pick_path(args.vdem, args.vdem_core)
    if vdem_path is None:
        missing.append("vdem")
    fh_path = pick_path(args.freedom_house, args.freedom_house_alt)
    if fh_path is None:
        missing.append("freedom_house")
    rsf_path = Path(args.rsf) if Path(args.rsf).exists() else None
    if rsf_path is None:
        missing.append("rsf")
    hrmi_path = Path(args.hrmi) if Path(args.hrmi).exists() else None
    if hrmi_path is None:
        missing.append("hrmi")
    wgi_path = Path(args.wgi) if Path(args.wgi).exists() else None
    if wgi_path is None:
        missing.append("wgi")
    cpi_path = pick_path(args.cpi, args.cpi_alt)
    if cpi_path is None:
        missing.append("cpi")
    gsi_path = Path(args.gsi) if Path(args.gsi).exists() else None
    if gsi_path is None:
        missing.append("gsi")
    afi_path = Path(args.afi) if Path(args.afi).exists() else None
    if afi_path is None:
        missing.append("afi")

    if missing and not args.allow_missing:
        print("[error] Missing required sources: " + ", ".join(sorted(missing)), file=sys.stderr)
        print("        Add the files to data/raw/ or re-run with --allow-missing.", file=sys.stderr)
        return 2
    if missing:
        print("[warn] Missing sources: " + ", ".join(sorted(missing)), file=sys.stderr)

    un_df, countries_json = _read_un_members(raw_dir)
    un_iso3 = set(un_df["iso3"].dropna().astype(str).str.upper())

    # matcher uses countries.json if available; otherwise build from un_members.csv names if present
    countries = countries_json
    if not countries:
        countries = []
        if "name" in un_df.columns:
            for _, r in un_df.iterrows():
                countries.append({
                    "cca3": r.get("iso3"),
                    "name": {"common": r.get("name"), "official": r.get("official")},
                    "altSpellings": [],
                })
    matcher = CountryMatcher(countries)

    start_year = args.start_year
    end_year = args.end_year

    base = pd.MultiIndex.from_product([sorted(un_iso3), range(start_year, end_year + 1)], names=["iso3", "year"]).to_frame(index=False)
    # Attach country names if available
    if "name" in un_df.columns:
        base = base.merge(un_df[["iso3", "name"]], on="iso3", how="left")
        base = base.rename(columns={"name": "country_name"})

    colmap: List[Tuple[str, str, str]] = []

    # Load datasets if present
    datasets = []

    vdem_df = _load_if_exists(vdem_path, load_vdem, start_year, end_year, colmap)
    if vdem_df is not None:
        datasets.append(vdem_df)

    fh_df = _load_if_exists(fh_path, load_freedom_house, start_year, end_year, colmap, matcher)
    if fh_df is not None:
        datasets.append(fh_df)

    hrmi_df = _load_if_exists(hrmi_path, load_hrmi, start_year, end_year, colmap)
    if hrmi_df is not None:
        datasets.append(hrmi_df)

    rsf_df = _load_if_exists(rsf_path, load_rsf, start_year, end_year, colmap, matcher)
    if rsf_df is not None:
        datasets.append(rsf_df)

    wgi_df = _load_if_exists(wgi_path, load_wgi, start_year, end_year, colmap)
    if wgi_df is not None:
        datasets.append(wgi_df)

    cpi_df = _load_if_exists(cpi_path, load_cpi, start_year, end_year, colmap, matcher)
    if cpi_df is not None:
        datasets.append(cpi_df)

    gsi_df = _load_if_exists(gsi_path, load_gsi, start_year, end_year, colmap, matcher)
    if gsi_df is not None:
        datasets.append(gsi_df)

    afi_df = _load_if_exists(afi_path, load_afi, start_year, end_year, colmap)
    if afi_df is not None:
        datasets.append(afi_df)

    # Merge everything into base
    merged = base.copy()
    for df in datasets:
        df = df[df["iso3"].isin(un_iso3)]
        merged = merged.merge(df, on=["iso3", "year"], how="left")

    # Save outputs
    csv_path = out_dir / f"country_{start_year}_{end_year}.csv"
    merged.to_csv(csv_path, index=False)

    # Column map
    if colmap:
        map_path = out_dir / "column_map.csv"
        with open(map_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["dataset", "source_column", "output_column"])
            writer.writerows(colmap)

    # SQLite
    sqlite_path = out_dir / f"country_{start_year}_{end_year}.sqlite"
    conn = sqlite3.connect(sqlite_path)
    try:
        merged.to_sql("country_year", conn, if_exists="replace", index=False)
    except sqlite3.OperationalError as exc:
        print(f"[warn] SQLite not written: {exc}", file=sys.stderr)
        try:
            conn.close()
        finally:
            conn = None
    else:
        conn.close()

    # Parquet (optional)
    parquet_path = out_dir / f"country_{start_year}_{end_year}.parquet"
    try:
        merged.to_parquet(parquet_path, index=False)
    except Exception as exc:
        print(f"[warn] Parquet not written: {exc}", file=sys.stderr)

    print(f"Wrote {csv_path}")
    if sqlite_path.exists():
        print(f"Wrote {sqlite_path}")
    if parquet_path.exists():
        print(f"Wrote {parquet_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
