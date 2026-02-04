from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_outputs.py"
OUTPUT_DIR = ROOT / "data" / "output"

OPTIONAL_OUTPUTS = (
    "country_2020_2026.parquet",
    "country_robustness_2020_2026.csv",
    "robustness_thresholds.json",
    "vparty_entities.csv",
    "vparty_party_year.csv",
)


def _run_validator(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


def test_validate_outputs_default() -> None:
    result = _run_validator()
    assert result.returncode == 0, f"Validator failed:\n{result.stdout}\n{result.stderr}"


def test_validate_outputs_optional_when_present() -> None:
    if not all((OUTPUT_DIR / name).exists() for name in OPTIONAL_OUTPUTS):
        pytest.skip("Optional outputs are not all present")
    result = _run_validator("--require-optional")
    assert result.returncode == 0, f"Validator failed:\n{result.stdout}\n{result.stderr}"
