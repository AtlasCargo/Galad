#!/usr/bin/env python3
"""Pipeline runner (scaffold).

Reads a pipeline config and produces a status report. Ingestion is disabled
by default; enable it only after sources and licenses are confirmed.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a pipeline config (scaffold)")
    parser.add_argument("--pipeline", default="config/pipelines/active.json")
    parser.add_argument("--status-out", default="data/output/pipeline_status.json")
    args = parser.parse_args()

    pipeline_ref = Path(args.pipeline)
    if not pipeline_ref.exists():
        raise SystemExit(f"Pipeline config not found: {pipeline_ref}")

    if pipeline_ref.name == "active.json":
        active = _load_json(pipeline_ref)
        pipeline_path = Path(active.get("config_path", ""))
        if not pipeline_path.exists():
            raise SystemExit(f"Active pipeline config not found: {pipeline_path}")
        pipeline = _load_json(pipeline_path)
    else:
        pipeline = _load_json(pipeline_ref)

    ingestion = pipeline.get("ingestion", {})
    enabled = bool(ingestion.get("enabled", False))

    status = {
        "pipeline_id": pipeline.get("id"),
        "name": pipeline.get("name"),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ingestion_enabled": enabled,
        "stage": pipeline.get("stage"),
        "notes": pipeline.get("ingestion", {}).get("notes", ""),
        "next_actions": [
            "Confirm ranking metric",
            "Confirm sources and licenses",
            "Enable ingestion in config",
        ],
    }

    _write_json(Path(args.status_out), status)
    print(f"Wrote {args.status_out}")

    if enabled:
        raise SystemExit(
            "Ingestion is not implemented in scaffold mode. Configure sources and implement ingestion steps."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
