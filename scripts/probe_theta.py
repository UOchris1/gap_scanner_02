#!/usr/bin/env python
# -*- coding: ascii -*-
"""
ThetaData v3 Health Check Probe
Per universe_03.txt requirements - 5-second health check

Tests the four critical v3 endpoints before running main scan
"""
import requests
import sys

THETA_BASE = "http://127.0.0.1:25503"

def probe_endpoint(name, url, params=None):
    """Test a ThetaData v3 endpoint and return OK/FAIL status"""
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data:  # Non-empty response
                print(f"{name}: OK")
                return True
            else:
                print(f"{name}: FAIL (empty response)")
                return False
        else:
            print(f"{name}: FAIL ({response.status_code})")
            return False
    except Exception as e:
        print(f"{name}: FAIL ({e})")
        return False

def main():
    """Run all ThetaData v3 endpoint health checks"""
    print("ThetaData v3 Health Check")
    print("=" * 40)

    all_passed = True

    # Test 1: Symbols list
    all_passed &= probe_endpoint(
        "Symbols",
        f"{THETA_BASE}/v3/stock/list/symbols",
        {"format": "json"}
    )

    # Test 2: Dates/trade (returns dates, not roster)
    all_passed &= probe_endpoint(
        "Dates/trade",
        f"{THETA_BASE}/v3/stock/list/dates/trade",
        {"symbol": "*"}
    )

    # Test 3: EOD sample
    all_passed &= probe_endpoint(
        "EOD sample",
        f"{THETA_BASE}/v3/stock/history/eod",
        {"symbol": "AAPL", "start_date": "2025-01-02", "end_date": "2025-01-10"}
    )

    # Test 4: OHLC premarket
    all_passed &= probe_endpoint(
        "OHLC premarket",
        f"{THETA_BASE}/v3/stock/history/ohlc",
        {
            "symbol": "AAPL",
            "date": "2025-09-11",
            "interval": "1m",
            "start_time": "04:00:00",
            "end_time": "09:29:59"
        }
    )

    print("=" * 40)
    if all_passed:
        print("ALL CHECKS PASSED - Ready for scan")
        sys.exit(0)
    else:
        print("SOME CHECKS FAILED - Review endpoints")
        sys.exit(1)

if __name__ == "__main__":
    main()