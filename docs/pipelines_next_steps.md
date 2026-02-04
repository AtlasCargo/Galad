# Pipelines: Next Steps

This repo now includes scaffold configs for pipelines A–D and an active pipeline pointer.

## What exists
- `config/pipelines/pipeline_{a,b,c,d}.json`
- `config/pipelines/active.json` (defaults to pipeline D)
- `config/taxonomy/` for sector + overlay classification
- `scripts/run_pipeline.py` (status output only)
- `scripts/classify_overlays.py`
- `scripts/estimate_coverage.py`
- `scripts/ingest/pipeline_d.py` (seed-driven ingestion)
- `scripts/ingest/wikidata_companies.py` (optional open seed helper)
- `scripts/ingest/wikipedia_revenue_lists.py` (optional open seed helper)

## What to decide
1) Ranking metric (revenue / assets / market cap / influence score).
2) Source list and licensing policy.
3) Whether to prioritize open-only or allow commercial sources.

## Recommended first execution
```
python scripts/run_pipeline.py
```

This writes `data/output/pipeline_status.json` with a checklist.

## After sources are confirmed
- Implement ingestion steps per pipeline in `scripts/ingest/`.
- Generate `data/output/top_10k_corporations.csv` or `data/output/top_actors_influence.csv`.
- Run:
  - `python scripts/classify_overlays.py --input <entities.csv>`
  - `python scripts/estimate_coverage.py --input data/output/org_classification_map.csv`

## Quick seed run (Pipeline D)
```
python scripts/ingest/wikidata_companies.py --limit 2000 --no-order
python scripts/ingest/wikipedia_revenue_lists.py
python scripts/run_pipeline.py
python scripts/classify_overlays.py --input data/output/top_actors_influence.csv
python scripts/estimate_coverage.py --input data/output/org_classification_map.csv
```
