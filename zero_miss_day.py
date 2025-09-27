# -*- coding: ascii -*-
"""Minimal CLI shim to run zero-miss discovery using run_discovery_compare helpers."""
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from run_discovery_compare import (
    build_daily_universe,
    zero_miss_discovery_pass1,
    zero_miss_discovery_pass2b,
    create_completeness_metrics,
)

def run_zero_miss_discovery(day_iso: str, providers: list | None = None) -> bool:
    """Execute zero-miss discovery orchestrator for a single day."""
    provider_list = providers if providers is not None else []
    universe = build_daily_universe(day_iso, provider_list)
    market_data, r4_metrics = zero_miss_discovery_pass1(day_iso, provider_list)
    pass2_results = zero_miss_discovery_pass2b(day_iso, market_data, provider_list)
    metrics = create_completeness_metrics(day_iso, universe, market_data, pass2_results, r4_metrics)
    return bool(metrics.get("audit_passed", True))

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run zero-miss discovery for a single day")
    parser.add_argument("--date", required=True, help="Trading day in YYYY-MM-DD format")
    args = parser.parse_args(argv)
    ok = run_zero_miss_discovery(args.date)
    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
