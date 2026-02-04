-- Neutral sub-state schema (no ideological labels)

CREATE TABLE IF NOT EXISTS entities (
  entity_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  country_iso3 TEXT,
  entity_type TEXT,
  founded_year INTEGER,
  member_count INTEGER,
  member_count_year INTEGER,
  funding_usd REAL,
  funding_year INTEGER,
  funding_type TEXT,
  source_name TEXT,
  source_url TEXT,
  source_date TEXT,
  confidence REAL
);

CREATE TABLE IF NOT EXISTS issue_catalog (
  issue_code TEXT PRIMARY KEY,
  issue_label TEXT NOT NULL,
  description TEXT
);

CREATE TABLE IF NOT EXISTS positions (
  entity_id TEXT NOT NULL,
  year INTEGER,
  issue_code TEXT NOT NULL,
  stance TEXT, -- support | restrict | mixed | unknown
  evidence_type TEXT,
  evidence_url TEXT,
  evidence_snippet TEXT,
  source_name TEXT,
  source_date TEXT,
  confidence REAL,
  FOREIGN KEY(entity_id) REFERENCES entities(entity_id),
  FOREIGN KEY(issue_code) REFERENCES issue_catalog(issue_code)
);

