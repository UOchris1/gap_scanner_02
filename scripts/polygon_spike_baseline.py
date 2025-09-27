#!/usr/bin/env python3
"""
Polygon Spike Scanner - Baseline Version
Lightweight spike detection for baseline comparison with primary zero-miss discovery system.

Usage: python scripts/polygon_spike_baseline.py --date 2025-09-11 --min_spike 50.0 --min_vol 500000 --output baseline_spikes_2025-09-11.csv
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
import json

def get_polygon_roster(date: str) -> List[Dict[str, Any]]:
    """Get Polygon grouped daily roster for the date"""
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: POLYGON_API_KEY environment variable required")
        sys.exit(1)

    try:
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {
            "adjusted": "false",
            "include_otc": "false",
            "apiKey": api_key
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        print(f"Retrieved {len(results)} symbols from Polygon grouped daily for {date}")
        return results

    except Exception as e:
        print(f"Error fetching Polygon roster for {date}: {e}")
        return []

def calc_spike_pct(high: Optional[float], open_price: Optional[float]) -> Optional[float]:
    """Calculate spike percentage (intraday push)"""
    if high is None or open_price is None or open_price <= 0:
        return None
    return ((high / open_price) - 1) * 100

def find_baseline_spikes(date: str, min_spike: float = 50.0, min_vol: int = 500_000) -> List[Dict[str, Any]]:
    """Find spike events using Polygon baseline method"""
    print(f"Scanning for baseline spikes on {date} (min_spike={min_spike}%, min_vol={min_vol:,})")

    # Get roster from Polygon grouped daily
    roster_data = get_polygon_roster(date)
    if not roster_data:
        print("No roster data available for baseline spike scan")
        return []

    baseline_hits = []

    for i, bar in enumerate(roster_data):
        if i % 1000 == 0:
            print(f"Processing symbol {i+1}/{len(roster_data)}")

        symbol = bar.get("T")  # Polygon uses "T" for ticker
        open_price = bar.get("o")
        high = bar.get("h")
        volume = bar.get("v")

        if not symbol or volume is None or volume < min_vol:
            continue

        # Calculate spike percentage (equivalent to R3 intraday push)
        spike_pct = calc_spike_pct(high, open_price)

        if spike_pct is not None and spike_pct >= min_spike:
            baseline_hits.append({
                "symbol": symbol,
                "date": date,
                "event_type": "INTRADAY_PUSH_50",
                "prev_close": bar.get("c"),  # Previous close not available in grouped daily
                "open": open_price,
                "high": high,
                "volume": volume,
                "pct_value": spike_pct,
                "source": "polygon_baseline"
            })

    print(f"Found {len(baseline_hits)} baseline spike events")
    return baseline_hits

def write_baseline_csv(hits: List[Dict[str, Any]], output_file: str):
    """Write baseline hits to CSV"""
    if not hits:
        # Write empty file with header
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["symbol", "date", "event_type", "prev_close", "open", "high", "volume", "pct_value", "source"])
        return

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "date", "event_type", "prev_close", "open", "high", "volume", "pct_value", "source"])

        for hit in hits:
            writer.writerow([
                hit["symbol"],
                hit["date"],
                hit["event_type"],
                hit.get("prev_close", ""),  # May be empty for Polygon grouped daily
                hit["open"],
                hit["high"],
                hit["volume"],
                hit["pct_value"],
                hit["source"]
            ])

    print(f"Wrote {len(hits)} baseline hits to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Polygon Spike Scanner - Baseline Version")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--min_spike", type=float, default=50.0, help="Minimum spike percentage (default: 50.0)")
    parser.add_argument("--min_vol", type=int, default=500_000, help="Minimum volume (default: 500,000)")
    parser.add_argument("--output", required=True, help="Output CSV file")

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("ERROR: Date must be in YYYY-MM-DD format")
        sys.exit(1)

    # Find baseline spikes
    hits = find_baseline_spikes(args.date, args.min_spike, args.min_vol)

    # Write results
    write_baseline_csv(hits, args.output)

    print(f"Baseline spike scan complete for {args.date}")
    print(f"Results written to {args.output}")

if __name__ == "__main__":
    main()