#!/usr/bin/env python3
"""Create sub-state templates and optional V-Party outputs (neutral schema)."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd


def _safe_col(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def _detect_iso3(df: pd.DataFrame) -> str:
    for c in ["iso3", "ISO3", "country_text_id", "country_code", "country_iso3", "cca3", "Country Code"]:
        if c in df.columns:
            return c
    raise ValueError("V-Party file missing ISO3 column")


def _detect_year(df: pd.DataFrame) -> str:
    for c in ["year", "Year", "date", "Edition"]:
        if c in df.columns:
            return c
    raise ValueError("V-Party file missing year column")


def _detect_party_name(df: pd.DataFrame) -> str:
    for c in [
        "party_name",
        "party",
        "party_name_english",
        "party_name_en",
        "partyname",
        "v2paenname",
        "v2paorname",
        "v2pashname",
    ]:
        if c in df.columns:
            return c
    raise ValueError("V-Party file missing party name column")


def _detect_party_id(df: pd.DataFrame) -> str:
    for c in ["party_id", "vparty_id", "party_id_vdem", "party_id_v", "v2paid", "pf_party_id"]:
        if c in df.columns:
            return c
    return ""


def write_templates(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    entities_path = out_dir / "substate_entities_template.csv"
    positions_path = out_dir / "substate_positions_template.csv"
    issues_path = out_dir / "issue_catalog.csv"

    with open(entities_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
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
        ])

    with open(positions_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
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
        ])

    issues = [
        ("expression", "Expression & media freedom", "Policies or actions affecting speech, media, and access to information."),
        ("academic_freedom", "Academic/scientific freedom", "Policies or actions affecting research, teaching, and scientific practice."),
        ("labor_rights", "Labor rights & forced labor safeguards", "Policies or actions affecting labor protections, forced labor, and unions."),
        ("participation", "Participatory representation", "Policies or actions affecting elections, party competition, and civic participation."),
        ("due_process", "Due process & detention", "Policies or actions affecting legal process, detention, and judicial protections."),
        ("fiscal_transparency", "Fiscal transparency & accountability", "Policies or actions affecting budget openness, corruption, and accountability."),
    ]
    with open(issues_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["issue_code", "issue_label", "description"])
        writer.writerows(issues)


def build_vparty(vparty_path: Path, out_dir: Path) -> None:
    df = pd.read_csv(vparty_path, low_memory=False)
    iso_col = _detect_iso3(df)
    year_col = _detect_year(df)
    party_name_col = _detect_party_name(df)
    party_id_col = _detect_party_id(df)

    df[iso_col] = df[iso_col].astype(str).str.upper()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")

    # Output a party-year dataset with prefixed columns
    key_cols = [iso_col, year_col, party_name_col]
    if party_id_col:
        key_cols.append(party_id_col)
    rename = {}
    for c in df.columns:
        if c in key_cols:
            continue
        rename[c] = f"vparty__{_safe_col(c)}"
    party_year = df.rename(columns=rename)
    rename_keys = {iso_col: "iso3", year_col: "year", party_name_col: "party_name"}
    if party_id_col:
        rename_keys[party_id_col] = "party_id"
    party_year = party_year.rename(columns=rename_keys)

    out_dir.mkdir(parents=True, exist_ok=True)
    party_year_path = out_dir / "vparty_party_year.csv"
    party_year.to_csv(party_year_path, index=False)

    # Entities table
    ent_cols = ["iso3", "party_name"]
    if party_id_col:
        ent_cols.append("party_id")
    entities = party_year[ent_cols].dropna().drop_duplicates()

    def _make_entity_id(row) -> str:
        if party_id_col and pd.notna(row.get("party_id")):
            return f"vparty_{int(row.get('party_id'))}"
        return f"vparty_{row['iso3']}_{_safe_col(str(row['party_name']))}"

    entities = entities.copy()
    entities["entity_id"] = entities.apply(_make_entity_id, axis=1)
    entities["name"] = entities["party_name"]
    entities["country_iso3"] = entities["iso3"]
    entities["entity_type"] = "political_party"

    entities_out = entities[["entity_id", "name", "country_iso3", "entity_type"]]
    entities_out_path = out_dir / "vparty_entities.csv"
    entities_out.to_csv(entities_out_path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build sub-state templates and optional V-Party outputs")
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument("--vparty", default="")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    write_templates(out_dir)

    if args.vparty:
        vparty_path = Path(args.vparty)
        if vparty_path.exists():
            build_vparty(vparty_path, out_dir)
        else:
            print(f"[warn] V-Party file not found: {vparty_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
