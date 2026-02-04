#!/usr/bin/env python3
"""Compute data-driven thresholds for country robustness modeling.

This script derives quantile-based thresholds from the merged country dataset
and V-Party party-year data. It writes a JSON file with recommended thresholds
that can be used by assess_country_robustness.py.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def _load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    min_v = s.min()
    max_v = s.max()
    if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
        return pd.Series([np.nan] * len(s), index=s.index)
    return (s - min_v) / (max_v - min_v)


def _compute_index(
    df: pd.DataFrame, metrics: List[dict], orientation: str
) -> Tuple[pd.Series, List[str]]:
    parts = []
    weights = []
    used = []
    for metric in metrics:
        col = metric.get("column")
        if not col or col not in df.columns:
            continue
        weight = float(metric.get("weight", 1.0))
        higher_is_better = bool(metric.get("higher_is_better", True))
        norm = _normalize(df[col])
        if norm.notna().sum() == 0:
            continue
        if orientation == "good":
            if not higher_is_better:
                norm = 1 - norm
        elif orientation == "bad":
            if higher_is_better:
                norm = 1 - norm
        else:
            raise ValueError("orientation must be 'good' or 'bad'")
        parts.append(norm * weight)
        weights.append(weight)
        used.append(col)

    if not parts:
        return pd.Series([np.nan] * len(df), index=df.index), []

    total_weight = float(sum(weights))
    index = sum(parts) / total_weight
    return index, used


def _compute_vparty_metrics(vparty_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if vparty_df is None or vparty_df.empty:
        return pd.DataFrame(columns=["iso3", "year", "m_raw", "p_raw", "v_raw"])

    anticol = cfg.get("vparty", {}).get("antiplural_col")
    populcol = cfg.get("vparty", {}).get("popul_col")
    violcol = cfg.get("vparty", {}).get("violence_col")

    for col in [anticol, populcol, violcol]:
        if col and col in vparty_df.columns:
            vparty_df[col] = pd.to_numeric(vparty_df[col], errors="coerce")

    group = vparty_df.groupby(["iso3", "year"], dropna=False)

    ant_mean = group[anticol].mean() if anticol in vparty_df.columns else None
    ant_max = group[anticol].max() if anticol in vparty_df.columns else None
    pop_std = group[populcol].std() if populcol in vparty_df.columns else None
    ant_std = group[anticol].std() if anticol in vparty_df.columns else None
    viol_mean = group[violcol].mean() if violcol in vparty_df.columns else None

    out = pd.DataFrame(index=group.size().index)
    if ant_mean is not None:
        out["antiplural_mean"] = ant_mean
    if ant_max is not None:
        out["antiplural_max"] = ant_max

    # Anti-system influence mass proxy: emphasize maximum + mean
    if "antiplural_max" in out.columns and "antiplural_mean" in out.columns:
        out["m_raw"] = 0.7 * out["antiplural_max"] + 0.3 * out["antiplural_mean"]
    elif "antiplural_max" in out.columns:
        out["m_raw"] = out["antiplural_max"]
    elif "antiplural_mean" in out.columns:
        out["m_raw"] = out["antiplural_mean"]
    else:
        out["m_raw"] = np.nan

    # Polarization proxy: dispersion of party positions
    if pop_std is not None:
        out["p_raw"] = pop_std
    elif ant_std is not None:
        out["p_raw"] = ant_std
    else:
        out["p_raw"] = np.nan

    if viol_mean is not None:
        out["v_raw"] = viol_mean
    else:
        out["v_raw"] = np.nan

    out = out.reset_index()
    return out


def _merge_vparty_asof(country: pd.DataFrame, vparty_metrics: pd.DataFrame) -> pd.DataFrame:
    if vparty_metrics is None or vparty_metrics.empty:
        country[["m_raw", "p_raw", "v_raw"]] = np.nan
        return country

    vparty_metrics = vparty_metrics.sort_values(["iso3", "year"]).copy()
    country = country.sort_values(["iso3", "year"]).copy()

    merged_parts = []
    for iso3, group in country.groupby("iso3"):
        vp = vparty_metrics[vparty_metrics["iso3"] == iso3]
        if vp.empty:
            g = group.copy()
            g[["m_raw", "p_raw", "v_raw"]] = np.nan
            merged_parts.append(g)
            continue
        g = pd.merge_asof(
            group.sort_values("year"),
            vp.sort_values("year"),
            on="year",
            by="iso3",
            direction="backward",
            suffixes=("", "_vp"),
        )
        merged_parts.append(g)

    merged = pd.concat(merged_parts, ignore_index=True)
    return merged


def _compute_trend(df: pd.DataFrame, value_col: str, window: int) -> pd.DataFrame:
    results = []
    for iso3, group in df.groupby("iso3"):
        g = group.sort_values("year")
        slopes = []
        for _, row in g.iterrows():
            end_year = row["year"]
            start_year = end_year - window + 1
            sub = g[(g["year"] >= start_year) & (g["year"] <= end_year)].copy()
            sub = sub.dropna(subset=[value_col])
            if sub["year"].nunique() < 3:
                slopes.append(0.0)
                continue
            first = sub.iloc[0]
            last = sub.iloc[-1]
            denom = float(last["year"] - first["year"])
            if denom == 0:
                slopes.append(0.0)
                continue
            slope = float(last[value_col] - first[value_col]) / denom
            slopes.append(slope)
        out = g[["iso3", "year"]].copy()
        out["slope"] = slopes
        results.append(out)
    trend = pd.concat(results, ignore_index=True)
    return trend


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/robustness_config.json")
    parser.add_argument("--country", default="data/output/country_2020_2026.csv")
    parser.add_argument("--vparty", default="data/output/vparty_party_year.csv")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    cfg = _load_config(Path(args.config))
    out_path = Path(args.out or cfg.get("thresholds_file", "data/output/robustness_thresholds.json"))

    country = pd.read_csv(args.country, low_memory=False)
    vparty = pd.read_csv(args.vparty, low_memory=False) if Path(args.vparty).exists() else None

    # Compute A, G, S
    a, a_cols = _compute_index(country, cfg.get("alignment_metrics", []), "good")
    g, g_cols = _compute_index(country, cfg.get("guardrail_metrics", []), "good")
    s, s_cols = _compute_index(country, cfg.get("stress_metrics", []), "bad")

    metrics = country[["iso3", "year"]].copy()
    metrics["A"] = a
    metrics["G"] = g
    metrics["S"] = s

    vparty_metrics = _compute_vparty_metrics(vparty, cfg)
    metrics = _merge_vparty_asof(metrics, vparty_metrics[["iso3", "year", "m_raw", "p_raw", "v_raw"]])

    # Normalize M, P, S
    metrics["M"] = _normalize(metrics["m_raw"])
    metrics["P"] = _normalize(metrics["p_raw"])
    metrics["S_norm"] = _normalize(metrics["S"])

    metrics["MP"] = metrics["M"] * metrics["P"]

    # Trend decline (A)
    trend = _compute_trend(metrics, "A", cfg.get("horizon_years", 5))
    metrics = metrics.merge(trend, on=["iso3", "year"], how="left")
    metrics = metrics.rename(columns={"slope": "trend_slope"})
    metrics["decline"] = metrics["trend_slope"].apply(lambda x: max(0.0, -float(x)) if pd.notna(x) else np.nan)
    metrics["decline_norm"] = _normalize(metrics["decline"])

    def q(series: pd.Series, p: float) -> float:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return float("nan")
        return float(s.quantile(p))

    thresholds = {
        "guardrail_critical": {"value": q(metrics["G"], 0.25), "method": "q25"},
        "alignment_low": {"value": q(metrics["A"], 0.25), "method": "q25"},
        "mp_percolation": {"value": q(metrics["MP"], 0.75), "method": "q75"},
        "shock_high": {"value": q(metrics["S_norm"], 0.75), "method": "q75"},
        "decline_high": {"value": q(metrics["decline_norm"], 0.75), "method": "q75"},
    }

    out = {
        "thresholds": thresholds,
        "notes": {
            "alignment_columns_used": a_cols,
            "guardrail_columns_used": g_cols,
            "stress_columns_used": s_cols,
            "vparty_metrics": {
                "antiplural_col": cfg.get("vparty", {}).get("antiplural_col"),
                "popul_col": cfg.get("vparty", {}).get("popul_col"),
                "violence_col": cfg.get("vparty", {}).get("violence_col"),
            },
            "orientation": "A/G are higher=better, M/P/S are higher=worse",
        },
        "quantiles": {
            "A": {"p25": q(metrics["A"], 0.25), "p50": q(metrics["A"], 0.50), "p75": q(metrics["A"], 0.75)},
            "G": {"p25": q(metrics["G"], 0.25), "p50": q(metrics["G"], 0.50), "p75": q(metrics["G"], 0.75)},
            "M": {"p25": q(metrics["M"], 0.25), "p50": q(metrics["M"], 0.50), "p75": q(metrics["M"], 0.75)},
            "P": {"p25": q(metrics["P"], 0.25), "p50": q(metrics["P"], 0.50), "p75": q(metrics["P"], 0.75)},
            "S_norm": {"p25": q(metrics["S_norm"], 0.25), "p50": q(metrics["S_norm"], 0.50), "p75": q(metrics["S_norm"], 0.75)},
            "MP": {"p25": q(metrics["MP"], 0.25), "p50": q(metrics["MP"], 0.50), "p75": q(metrics["MP"], 0.75)},
            "decline_norm": {"p25": q(metrics["decline_norm"], 0.25), "p50": q(metrics["decline_norm"], 0.50), "p75": q(metrics["decline_norm"], 0.75)},
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
