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
