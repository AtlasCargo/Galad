# Sub-state Actor Scope (Neutral, Evidence-based)

Goal: capture measurable attributes and issue-specific stances for large
organizations. No ideological labels are asserted in the base dataset.

## Inclusion thresholds
An entity is in-scope if it meets at least one of:
- Membership >= 1,000 (members, employees, adherents, students, or affiliates)
- Annual budget / revenue / assets >= USD 1,000,000,000

If a source does not report membership or funding, the entity can still be
included if it is clearly a large, widely recognized organization.

## Entity types (examples)
- Political parties and party alliances (already covered via V-Party)
- Labor unions and union confederations
- Religious denominations and large congregational networks
- Professional/industry associations and chambers
- Large NGOs, INGOs, and advocacy networks
- Foundations and charitable trusts
- Universities and university systems
- Corporations and corporate groups (public and private)
- State-owned enterprises and autonomous public agencies
- Media groups and large publishers
- Financial institutions and investment funds

## Exclusions
- Individual people
- Informal networks without an identifiable legal entity
- Central government as a whole (already covered at country level)

## De-duplication and entity identity
- Prefer a stable legal identifier when available (LEI, national registry ID).
- Roll up subsidiaries to the ultimate parent unless the subsidiary is
  independently governed and meets thresholds on its own.

## Required fields (core)
- entity_id
- name
- country_iso3
- entity_type
- member_count, member_count_year
- funding_usd, funding_year, funding_type (budget / revenue / assets)
- source_name, source_url, source_date, confidence

## Issue stance fields (optional, evidence-based)
Issue stances are recorded only when there is explicit, citable evidence.
No organization is labeled as "aligned" or "opposed" in the base dataset.

Fields:
- issue_code (see issue_catalog)
- stance: support / restrict / mixed / unknown
- evidence_type: policy / statute / official_statement / vote / enforcement_action
- evidence_url / evidence_snippet
- source_name / source_date / confidence

## Derived fields (optional, user-defined)
- alignment_score: weighted sum across issue stances
- alignment_band: aligned / neutral / opposed

These are computed from issue stances and user-supplied weights; they are not
asserted by default.
