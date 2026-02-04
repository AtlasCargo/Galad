# Current State Snapshot

Date: 2026-02-04

## What exists now
- Country-level dataset (2020–2026)
  - File: `data/output/country_2020_2026.csv`
  - Rows: 1,370  | Columns: 6,933 | ISO3s: 194
- Sub-state (V-Party)
  - Party-year: `data/output/vparty_party_year.csv` (11,898 rows, 384 cols)
  - Entities: `data/output/vparty_entities.csv` (3,467 rows, 178 ISO3s)
- Robustness model outputs
  - Thresholds: `data/output/robustness_thresholds.json`
  - Scores: `data/output/country_robustness_2020_2026.csv` (1,394 rows, 16 cols)
- Influence-first pipeline outputs
  - `data/output/top_actors_influence.csv` (4,485 rows, 10 cols)
  - `data/output/org_classification_map.csv` (4,485 rows, 6 cols)
  - `data/output/org_coverage_gaps.csv` (1,746 rows, 4 cols)
- Templates
  - `data/output/substate_entities_template.csv`
  - `data/output/substate_positions_template.csv`
  - `data/output/issue_catalog.csv`

## Key scripts
- `scripts/build_country_dataset.py`
- `scripts/build_substate_dataset.py`
- `scripts/compute_robustness_thresholds.py`
- `scripts/assess_country_robustness.py`

## Known limitations
- SQLite output for country dataset fails due to SQLite column limit; CSV is the primary output.
- Parquet not written (pyarrow/fastparquet not installed).
- V-Party coverage ends before 2020; robustness model carries forward latest pre-2020 values.

## Inputs acquired
- V-Dem, V-Party, Freedom House, WGI, RSF, HRMI, AFI, CPI (2020–2023), GSI 2023
- Wikidata seed (companies with revenue property; ~385 entities, generated locally)
- Wikidata revenue-ordered list (small; 16 entities)
- Wikipedia revenue lists (combined; ~619 entities, generated locally)
- See `data/raw/` for exact files.
