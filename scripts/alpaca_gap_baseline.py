#!/usr/bin/env python3
"""
Alpaca Gap Scanner - Baseline Version
Parameterized for baseline comparison with primary zero-miss discovery system.

Usage: python scripts/alpaca_gap_baseline.py --date 2025-09-11 --min_gap 50.0 --min_vol 1000000 --output baseline_gaps_2025-09-11.csv
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
import json

# Alpaca API configuration
ALPACA_BASE_URL = "https://data.alpaca.markets/v2"

def get_alpaca_headers() -> Dict[str, str]:
    """Get Alpaca API headers with credentials from environment"""
    api_key = os.environ.get("ALPACA_API_KEY")
    api_secret = os.environ.get("ALPACA_SECRET_KEY")

    if not api_key or not api_secret:
        print("ERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables required")
        sys.exit(1)

    return {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret
    }

def get_nasdaq_symbols() -> List[str]:
    """Get NASDAQ traded symbols for baseline scanning"""
    try:
        headers = get_alpaca_headers()
        url = f"{ALPACA_BASE_URL}/stocks/meta/exchanges"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # For baseline, use a simplified approach - get active assets
        assets_url = f"{ALPACA_BASE_URL}/assets"
        assets_response = requests.get(assets_url, headers=headers, timeout=30)
        assets_response.raise_for_status()

        assets = assets_response.json()
        nasdaq_symbols = []

        for asset in assets:
            if (asset.get("exchange") == "NASDAQ" and
                asset.get("status") == "active" and
                asset.get("tradable", False)):
                nasdaq_symbols.append(asset.get("symbol"))

        print(f"Found {len(nasdaq_symbols)} NASDAQ symbols for baseline scan")
        return nasdaq_symbols[:1000]  # Limit for baseline comparison

    except Exception as e:
        print(f"Error fetching NASDAQ symbols: {e}")
        # Fallback to a small sample for testing
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX"]

def get_bars(symbol: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
    """Get daily bars for symbol from Alpaca"""
    try:
        headers = get_alpaca_headers()
        url = f"{ALPACA_BASE_URL}/stocks/{symbol}/bars"

        params = {
            "start": f"{start_date}T00:00:00Z",
            "end": f"{end_date}T23:59:59Z",
            "timeframe": "1Day",
            "adjustment": "raw",  # Unadjusted to match primary pipeline
            "feed": "iex"  # Use IEX feed for consistency
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        bars = data.get("bars", [])

        if len(bars) >= 2:
            # Get prev_close and current day data
            prev_bar = bars[-2]  # Previous day
            curr_bar = bars[-1]  # Current day (target date)

            return {
                "symbol": symbol,
                "prev_close": prev_bar.get("c"),
                "open": curr_bar.get("o"),
                "high": curr_bar.get("h"),
                "low": curr_bar.get("l"),
                "close": curr_bar.get("c"),
                "volume": curr_bar.get("v"),
                "date": start_date
            }

        return None

    except Exception as e:
        print(f"Error getting bars for {symbol}: {e}")
        return None

def calc_gap_pct(open_price: Optional[float], prev_close: Optional[float]) -> Optional[float]:
    """Calculate opening gap percentage"""
    if open_price is None or prev_close is None or prev_close <= 0:
        return None
    return ((open_price / prev_close) - 1) * 100

def calc_intraday_push_pct(high: Optional[float], open_price: Optional[float]) -> Optional[float]:
    """Calculate intraday push percentage"""
    if high is None or open_price is None or open_price <= 0:
        return None
    return ((high / open_price) - 1) * 100

def find_baseline_gaps(date: str, min_gap: float = 50.0, min_vol: int = 1_000_000) -> List[Dict[str, Any]]:
    """Find gap events using Alpaca baseline method"""
    print(f"Scanning for baseline gaps on {date} (min_gap={min_gap}%, min_vol={min_vol:,})")

    # Calculate date range (need prev day for gap calculation)
    target_date = datetime.strptime(date, "%Y-%m-%d")
    start_date = (target_date - timedelta(days=5)).strftime("%Y-%m-%d")  # 5 days back for safety
    end_date = date

    symbols = get_nasdaq_symbols()
    baseline_hits = []

    for i, symbol in enumerate(symbols):
        if i % 100 == 0:
            print(f"Processing symbol {i+1}/{len(symbols)}: {symbol}")

        bar_data = get_bars(symbol, start_date, end_date)
        if not bar_data:
            continue

        # Check volume filter
        volume = bar_data.get("volume", 0)
        if volume < min_vol:
            continue

        prev_close = bar_data.get("prev_close")
        open_price = bar_data.get("open")
        high = bar_data.get("high")

        # Calculate R2 (opening gap)
        gap_pct = calc_gap_pct(open_price, prev_close)
        if gap_pct is not None and gap_pct >= min_gap:
            baseline_hits.append({
                "symbol": symbol,
                "date": date,
                "event_type": "OPEN_GAP_50",
                "prev_close": prev_close,
                "open": open_price,
                "high": high,
                "volume": volume,
                "pct_value": gap_pct,
                "source": "alpaca_baseline"
            })

        # Calculate R3 (intraday push) - only if significant gap already
        push_pct = calc_intraday_push_pct(high, open_price)
        if push_pct is not None and push_pct >= min_gap:
            baseline_hits.append({
                "symbol": symbol,
                "date": date,
                "event_type": "INTRADAY_PUSH_50",
                "prev_close": prev_close,
                "open": open_price,
                "high": high,
                "volume": volume,
                "pct_value": push_pct,
                "source": "alpaca_baseline"
            })

    print(f"Found {len(baseline_hits)} baseline events")
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
                hit["prev_close"],
                hit["open"],
                hit["high"],
                hit["volume"],
                hit["pct_value"],
                hit["source"]
            ])

    print(f"Wrote {len(hits)} baseline hits to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Alpaca Gap Scanner - Baseline Version")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--min_gap", type=float, default=50.0, help="Minimum gap percentage (default: 50.0)")
    parser.add_argument("--min_vol", type=int, default=1_000_000, help="Minimum volume (default: 1,000,000)")
    parser.add_argument("--output", required=True, help="Output CSV file")

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("ERROR: Date must be in YYYY-MM-DD format")
        sys.exit(1)

    # Find baseline gaps
    hits = find_baseline_gaps(args.date, args.min_gap, args.min_vol)

    # Write results
    write_baseline_csv(hits, args.output)

    print(f"Baseline scan complete for {args.date}")
    print(f"Results written to {args.output}")

if __name__ == "__main__":
    main()