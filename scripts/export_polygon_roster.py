#!/usr/bin/env python3
# scripts/export_polygon_roster.py (ASCII only)
import os
import sys
import csv
import time
import datetime
import requests
from typing import List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

API = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

def daterange(start: datetime.date, end: datetime.date) -> List[str]:
    d = start
    out = []
    while d <= end:
        if d.weekday() < 5:  # skip weekends; holidays will 200 with empty results
            out.append(d.strftime("%Y-%m-%d"))
        d += datetime.timedelta(days=1)
    return out

class Transient(Exception):
    pass

@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=1, max=30),
       retry=retry_if_exception_type(Transient))
def fetch(date_str: str, api_key: str) -> List[str]:
    url = API.format(date=date_str)
    params = {"adjusted": "false", "include_otc": "false", "apiKey": api_key}

    r = requests.get(url, params=params, timeout=45)

    if r.status_code in (429, 500, 502, 503, 504):
        raise Transient(f"{r.status_code} {r.text[:120]}")

    r.raise_for_status()
    j = r.json()
    results = j.get("results") or []

    return [row["T"] for row in results if not row.get("otc")]

def main(start: str, end: str, out_csv: str):
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: set POLYGON_API_KEY in .env")
        sys.exit(1)

    start_d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])  # Single column for roster

        for d in daterange(start_d, end_d):
            syms = fetch(d, api_key)
            for s in syms:
                w.writerow([s])  # Single symbol per line
            print(f"{d}: {len(syms)} symbols")

def main_single_day(date: str, out_csv: str):
    """Export roster for a single day (used by test plan)"""
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        print("ERROR: set POLYGON_API_KEY in .env")
        sys.exit(1)

    syms = fetch(date, api_key)

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        for s in syms:
            w.writerow([s])

    print(f"{date}: {len(syms)} symbols exported to {out_csv}")
    return len(syms)

if __name__ == "__main__":
    if len(sys.argv) == 3:
        # Single day mode: python PROBLEM_SCRIPT_POLYGON_EXPORT.py 2025-09-11 out\roster_2025-09-11.csv
        main_single_day(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        # Date range mode: python -m scripts.export_polygon_roster 2025-08-29 2025-09-12 roster.csv
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print("Usage: python PROBLEM_SCRIPT_POLYGON_EXPORT.py <date> <output.csv>")
        print("   or: python -m scripts.export_polygon_roster <start_date> <end_date> <output.csv>")
        sys.exit(2)