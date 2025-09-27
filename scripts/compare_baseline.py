#!/usr/bin/env python3
# scripts/compare_baseline.py (ASCII only)
import sys
import pandas as pd

def load_engine(p: str) -> pd.DataFrame:
    df = pd.read_csv(p, dtype={"ticker": str})
    # Use available columns, handle missing intraday_push_pct
    cols = ["ticker", "event_date"]
    if "intraday_push_pct" in df.columns:
        cols.append("intraday_push_pct")
    return df[cols].drop_duplicates()

def load_baseline(p: str) -> pd.DataFrame:
    df = pd.read_csv(p, dtype={"symbol": str})

    # normalize columns: symbol->ticker, gap_date->event_date
    if "gap_date" in df.columns:
        df = df.rename(columns={"symbol": "ticker", "gap_date": "event_date"})
    elif "date" in df.columns:
        df = df.rename(columns={"symbol": "ticker", "date": "event_date"})

    return df[["ticker", "event_date"]].drop_duplicates()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python -m scripts.compare_baseline engine_hits.csv baseline_hits.csv out_prefix")
        sys.exit(2)

    eng = load_engine(sys.argv[1])
    base = load_baseline(sys.argv[2])

    eng["key"] = eng["ticker"] + "|" + eng["event_date"]
    base["key"] = base["ticker"] + "|" + base["event_date"]

    misses = base[~base["key"].isin(eng["key"])].copy()
    overlap = base[base["key"].isin(eng["key"])].copy()

    misses.to_csv(sys.argv[3] + "_misses_detail.csv", index=False)
    overlap.to_csv(sys.argv[3] + "_overlap.csv", index=False)

    print(f"baseline: {len(base)} engine: {len(eng)} overlap: {len(overlap)} baseline-only: {len(misses)}")