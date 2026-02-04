# Data Outputs (Tracked)

This repo **tracks generated outputs** in `data/output/` and **does not** track raw source files in `data/raw/`.

## Included outputs
- `data/output/country_2020_2026.csv`
- `data/output/country_2020_2026.sqlite` (may be empty due to SQLite column limits)
- `data/output/column_map.csv`
- `data/output/country_robustness_2020_2026.csv`
- `data/output/robustness_thresholds.json`
- `data/output/vparty_party_year.csv`
- `data/output/vparty_entities.csv`
- `data/output/substate_entities_template.csv`
- `data/output/substate_positions_template.csv`
- `data/output/issue_catalog.csv`

## Not included
- Raw source datasets (`data/raw/`) due to licensing and size constraints.
- Parquet outputs (requires pyarrow/fastparquet).

## Regeneration
Run these from the repo root:

```
python scripts/build_country_dataset.py --start-year 2020 --end-year 2026
python scripts/build_substate_dataset.py --vparty data/raw/vparty_country_party_date.csv
python scripts/compute_robustness_thresholds.py
python scripts/assess_country_robustness.py
```

If some raw files are missing, use `--allow-missing` for the country build.
