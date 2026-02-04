# Seed Inputs (Pipeline D)

Place CSV files in `data/raw/seeds/` to seed the influence-first actor list.

## Required columns
- `entity_id`
- `name`
- `country_iso3`
- `entity_type`

## Optional columns (signals)
- `sector_code` (NAICS 2-digit or ranges like 31-33)
- `revenue_usd`
- `assets_usd`
- `budget_usd`
- `users`
- `audience`
- `member_count`
- `employee_count`

## Notes
- Seeds are merged with V-Party entities (if present).
- The pipeline computes `influence_score` from available signals.

## Optional helper (open seed list)
Fetch a company seed list from Wikidata:

```
python scripts/ingest/wikidata_companies.py --limit 2000 --no-order --adaptive
```

The script writes `data/raw/seeds/wikidata_companies.csv` with:
- `revenue_usd` populated only when Wikidata provides USD units (often blank with the truthy revenue query)
- `employee_count` derived from Wikidata employee counts

Note: `ORDER BY` revenue can time out on WDQS; add `--no-order` for faster, non-ranked retrieval.

Revenue-ordered small list:

```
python scripts/ingest/wikidata_companies.py --limit 50 --page-size 25 \
  --output data/raw/seeds/wikidata_companies_revenue_order.csv
```

Wikipedia revenue lists (open, CC BY-SA):

```
python scripts/ingest/wikipedia_revenue_lists.py
```

This writes `data/raw/seeds/wikipedia_revenue_lists.csv` by combining multiple
Wikipedia list tables (global + regional + private/manufacturing).
