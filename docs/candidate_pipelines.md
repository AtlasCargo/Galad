# Four Candidate Pipelines (Pick One Tomorrow)

## Pipeline A — Open‑Only Global (LEI + Registries)
**Goal:** maximum openness, consistent IDs.
- Backbone: GLEIF LEI data (global IDs)
- Companies: OpenCorporates + national registries where bulk downloads exist
- NGOs/INGOs: IRS EO BMF, UK Charity Commission, IATI registry
- Pros: open licensing, auditable provenance
- Cons: uneven financial coverage; 10k ranking may be incomplete

## Pipeline B — Public‑Company Centric (Market Cap/Revenue)
**Goal:** accurate, high‑quality top list for public firms.
- Backbone: stock exchanges + SEC EDGAR (US) + national securities filings
- Ranking: market cap or revenue
- Coverage: public firms only; add SOEs if listed
- Pros: clean financials, consistent reporting
- Cons: misses large private firms and many SOEs

## Pipeline C — Mixed Open + Commercial Augment
**Goal:** reach 10k reliably across public + private + SOEs.
- Backbone: GLEIF LEI
- Primary list: commercial data provider (revenue/asset ranking)
- Validate with open filings where possible
- Pros: best coverage for a true global top‑10k
- Cons: licensing cost and redistribution limits

## Pipeline D — Influence‑First (Meso‑Scale Actors)
**Goal:** prioritize influence‑sensitive sectors over strict size.
- Start with: media groups, tech platforms, telecoms, defense, security
- Add: largest NGOs/INGOs, universities, unions, SOEs
- Ranking: size + public influence score (reach, users, budget)
- Pros: aligns with robustness/risk modeling needs
- Cons: subjective weighting; harder to standardize globally

## Recommendation logic
- If you want a strictly open dataset: pick Pipeline A.
- If you want a clean top list fast: Pipeline B.
- If you need a real global top‑10k: Pipeline C.
- If you want maximum analytic value for the robustness model: Pipeline D.
