# Plan for Tomorrow

## Goals
1) Decide the target ranking metric for "top 10k corporations" (revenue vs assets vs market cap).
2) Choose the ingestion pipeline and confirm allowed sources (open-only vs mixed).
3) Add non-corporate large actors (NGOs/INGOs, universities, unions, SOEs) with membership/funding where possible.

## Decisions needed
- Ranking metric: revenue / assets / market cap.
- Scope: public only vs public + private + SOEs.
- Data sources: open-only vs commercial augment.

## Execution plan (if open-only)
- Build entity backbone:
  - GLEIF LEI open data
  - OpenCorporates identifiers where allowed
- Extract financials:
  - Public filings (SEC EDGAR and comparable national registries)
  - National registries where bulk financials exist
- Classification:
  - NAICS sector + influence overlays (media, tech platforms, telecom, defense, security)

## Outputs to target
- `data/output/top_10k_corporations.csv`
- `data/output/org_classification_map.csv`
- `data/output/org_financials_normalized.csv`
- `data/output/org_coverage_gaps.csv` (coverage estimates by country/sector)

## Open questions
- Which countries/regions are priority if global coverage is not feasible?
- Do we allow commercial data to reach 10k reliably?
