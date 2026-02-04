#!/usr/bin/env python3
"""Assign sector labels and overlay tags to entity records.

Input CSV should include at least:
- entity_id
- name
Optional:
- sector_code (NAICS 2-digit or range like 31-33)

Output: org_classification_map.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Sequence


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_name(name: str) -> str:
    return " ".join(str(name).lower().strip().split())


def _sector_label(sector_code: str, sectors: Sequence[dict]) -> str:
    if not sector_code:
        return ""
    for sector in sectors:
        if sector_code == sector.get("code"):
            return str(sector.get("label", ""))
    return ""


def _match_overlays(name: str, sector_code: str, overlays: Sequence[dict]) -> List[str]:
    matches: List[str] = []
    name_lc = _normalize_name(name)
    for overlay in overlays:
        overlay_id = overlay.get("id")
        keywords = [k.lower() for k in overlay.get("keywords", [])]
        overlay_sectors = overlay.get("naics_codes", [])
        keyword_hit = any(k in name_lc for k in keywords)
        sector_hit = sector_code in overlay_sectors if sector_code else False
        if keyword_hit or sector_hit:
            matches.append(str(overlay_id))
    return matches


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify entities with sector labels and overlay tags")
    parser.add_argument("--input", default="data/output/top_10k_corporations.csv")
    parser.add_argument("--output", default="data/output/org_classification_map.csv")
    parser.add_argument("--name-col", default="name")
    parser.add_argument("--entity-id-col", default="entity_id")
    parser.add_argument("--sector-col", default="sector_code")
    parser.add_argument("--taxonomy", default="config/taxonomy/sectors_naics2.json")
    parser.add_argument("--overlays", default="config/taxonomy/overlays.json")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    taxonomy = _load_json(Path(args.taxonomy))
    overlays = _load_json(Path(args.overlays))

    sectors = taxonomy.get("sectors", [])
    overlay_defs = overlays.get("overlays", [])

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(in_path, newline="", encoding="utf-8") as fin, open(
        out_path, "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        fieldnames = [
            "entity_id",
            "name",
            "sector_code",
            "sector_label",
            "overlay_tags",
        ]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            entity_id = row.get(args.entity_id_col, "").strip()
            name = row.get(args.name_col, "").strip()
            sector_code = row.get(args.sector_col, "").strip()
            label = _sector_label(sector_code, sectors)
            overlays_matched = _match_overlays(name, sector_code, overlay_defs)
            writer.writerow(
                {
                    "entity_id": entity_id,
                    "name": name,
                    "sector_code": sector_code,
                    "sector_label": label,
                    "overlay_tags": ",".join(overlays_matched),
                }
            )

    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
