# Configs

This repo uses JSON configuration files for pipeline behavior. Current configs are scaffolding only and do not trigger any data ingestion.

## Pipeline scaffolds
Candidate pipeline configs live in `config/pipelines/`:
- `config/pipelines/pipeline_a.json` (open-only global, LEI + registries)
- `config/pipelines/pipeline_b.json` (public-company centric)
- `config/pipelines/pipeline_c.json` (mixed open + commercial)
- `config/pipelines/pipeline_d.json` (influence-first)

Each scaffold keeps `status: candidate`, `stage: scaffold`, and `ingestion.enabled: false` until a pipeline is selected and build steps are added.

## Other configs
- `config/robustness_config.json` (country robustness scoring)

## Editing notes
- Keep `last_updated` current when a config changes.
- Only enable ingestion after sources, licensing, and output targets are finalized.
