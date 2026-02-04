# Democracy Data Pipeline (Neutral, Evidence-Based)

This project builds two datasets for the 2020-2026 window:

1) **Country-level indicators** (UN member states only) merged from reputable public sources.
2) **Sub-state schema + templates** for organizations and institutions (no ideological labels).

The pipeline is intentionally neutral: it stores measured indicators and documented positions on specific issues rather than labeling groups or people.

## What you get

- `data/output/country_2020_2026.csv`
- `data/output/country_2020_2026.sqlite` (table `country_year`)
- `data/output/country_2020_2026.parquet` (if `pyarrow` is installed)
- `data/output/column_map.csv` (maps original columns to output names)

Sub-state outputs:

- `data/output/substate_entities_template.csv`
- `data/output/substate_positions_template.csv`
- `data/output/issue_catalog.csv`
- `data/output/vparty_party_year.csv` (optional, if V-Party provided)
- `data/output/vparty_entities.csv` (optional)

Pipeline D outputs (optional):

- `data/output/top_actors_influence.csv`
- `data/output/org_classification_map.csv`
- `data/output/org_coverage_gaps.csv`

## Sources (official or primary)

Country-level indicators:

- V-Dem Dataset (Country-Year, Core or Full+Others)
- Freedom House: Freedom in the World historical data files
- HRMI Rights Tracker dataset
- Reporters Without Borders (RSF) World Press Freedom Index
- World Bank Worldwide Governance Indicators (WGI)
- Transparency International CPI (annual data)
- Walk Free Global Slavery Index (country-level dataset)
- Academic Freedom Index (AFI) core dataset

UN member list:

- Derived from `mledoze/countries` (`unMember` flag) unless you provide your own list

## Licenses & access notes

- Some sources require acknowledgement, registration, or request access (e.g., HRMI, Walk Free GSI, RSF dataset files).
- This pipeline **does not redistribute** source data. You download raw files to `data/raw/`.

## Folder structure

```
/ home/x/Astor/democracy-data
  data/
    raw/        # place raw downloads here
    output/     # generated outputs
  schema/       # SQLite schema templates
  scripts/      # build scripts
```

## Quick start

1) Install dependencies:

```
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2) Download the raw source files into `data/raw/` using the names below.

3) Build country-level dataset:

```
python scripts/build_country_dataset.py \
  --start-year 2020 \
  --end-year 2026
```

By default, the build requires **all** sources to be present in `data/raw/`.
If you want to proceed with partial data, add `--allow-missing`.

4) Build sub-state templates (and optional V-Party outputs):

```
python scripts/build_substate_dataset.py \
  --vparty data/raw/vparty_country_party_date.csv
```

5) Optional: seed Pipeline D using open data (Wikidata), then run ingestion:

```
python scripts/ingest/wikidata_companies.py --limit 2000 --no-order
python scripts/run_pipeline.py
python scripts/classify_overlays.py --input data/output/top_actors_influence.csv
python scripts/estimate_coverage.py --input data/output/org_classification_map.csv
```

Optional: add Wikipedia revenue lists as an additional seed source:

```
python scripts/ingest/wikipedia_revenue_lists.py --discover --discover-limit 40
python scripts/ingest/sec_company_tickers.py
python scripts/ingest/gleif_lei_sample.py --limit 2000
```

## Required raw files (place in `data/raw/`)

**V-Dem** (CSV extracted from ZIP):
- `vdem_cy_full.csv` (preferred) **or** `vdem_cy_core.csv`

**Freedom House** (Excel download):
- `freedom_house_all_data.xlsx` **or** `freedom_house_ratings.xlsx`

**HRMI**:
- `hrmi_rights_tracker.csv`

**RSF Press Freedom Index** (combined file you create):
- `rsf_press_freedom.csv`
  - If you download yearly CSVs, merge into one file with columns: `iso3`, `year`, `score`, `rank` (other columns okay).

**WGI**:
- `wgi.xlsx` (World Bank WGI data file)

**CPI**:
- `cpi.xlsx` or `cpi.csv`
  - If your file lacks ISO3 codes, the script will attempt name-to-ISO3 matching.

**Global Slavery Index**:
- `gsi_2023.csv`

**Academic Freedom Index**:
- `afi_core.csv`

**UN Members** (optional override):
- `un_members.csv` with at least `iso3` column

If `un_members.csv` is missing, the script attempts to download from `mledoze/countries` and caches a derived `un_members.csv`.

## Year coverage note (2020-2026)

Most sources publish with a 1-2 year lag. Expect 2026 columns to be mostly empty until 2026 releases become available.

## Sub-state scope & thresholds

The schema supports organizations/institutions (parties, unions, NGOs, SOEs, religious orgs, media, agencies, etc.).
You can filter entities after populating data using:

```
python scripts/filter_entities.py \
  --min-members 1000 \
  --min-funding-usd 1000000000
```

## Naming & column conventions

- Output columns are prefixed with dataset identifiers (e.g., `vdem__`, `fh__`, `hrmi__`, `rsf__`, `wgi__`, `cpi__`, `gsi__`, `afi__`).
- Original column names are normalized to ASCII snake_case; mapping is saved in `data/output/column_map.csv`.
