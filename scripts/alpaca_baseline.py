#!/usr/bin/env python
# -*- coding: ascii -*-
"""
Alpaca Baseline Scanner for Cross-Validation
Per universe_03.txt requirements

Requires: APCA-API-KEY-ID, APCA-API-SECRET-KEY in env
Usage: python scripts/alpaca_baseline.py 2025-09-11 roster.csv out_baseline.csv
"""
import os, sys, csv, requests, datetime as dt, time
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root (scripts folder is one level down from root)
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

# Load API keys at module level
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "").strip()
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "").strip()

BASE = "https://data.alpaca.markets/v2"
KEY = APCA_API_KEY_ID or ALPACA_API_KEY
SECRET = APCA_API_SECRET_KEY or ALPACA_SECRET_KEY
HEADERS = {"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SECRET}

def bars(symbol, start_iso, end_iso, timeframe):
    url = f"{BASE}/stocks/{symbol}/bars"
    p = {"start": start_iso, "end": end_iso, "timeframe": timeframe, "limit": 10000}
    r = requests.get(url, params=p, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("bars", [])

def iso_et(date_str, hhmmss):
    # convert an ET wall time to UTC ISO; Alpaca requires RFC-3339 with TZ
    # naive approach: ET=UTC-4 in Sep 2025; adjust if you want robust tz handling
    y,m,d = map(int, date_str.split("-"))
    h,mm,ss = map(int, hhmmss.split(":"))
    t = dt.datetime(y,m,d,h,mm,ss) + dt.timedelta(hours=4)  # ET->UTC (+4 in DST)
    return t.isoformat() + "Z"

def main(date_iso, roster_csv, out_csv):
    # prev day
    d = dt.date.fromisoformat(date_iso)
    prev = (d - dt.timedelta(days=1)).isoformat()

    with open(roster_csv, newline="") as f, open(out_csv, "w", newline="") as g:
        rdr = csv.DictReader(f)
        wr = csv.writer(g)
        wr.writerow(["symbol","date","r1_pm_gap_pct","r2_open_gap_pct","r3_push_pct",
                     "prev_close","open","hod","pm_high"])

        for row in rdr:
            sym = row["symbol"]
            try:
                # prev close (daily bar for prev)
                prev_bars = bars(sym, prev+"T00:00:00Z", prev+"T23:59:59Z", "1Day")
                if not prev_bars:
                    continue
                prev_close = float(prev_bars[-1]["c"])

                # day 1-min to get open and HOD
                day_bars = bars(sym, date_iso+"T09:30:00Z", date_iso+"T16:00:00Z", "1Min")
                if not day_bars:
                    continue
                day_open = float(day_bars[0]["o"])
                hod = max(float(b["h"]) for b in day_bars)

                # premarket 1-min
                pm_bars = bars(sym, iso_et(date_iso,"04:00:00"), iso_et(date_iso,"09:29:59"), "1Min")
                pm_high = max((float(b["h"]) for b in pm_bars), default=None)

                def pct(a,b):
                    return None if a is None or b<=0 else (a/b-1.0)*100.0

                r1 = pct(pm_high, prev_close) if pm_high is not None else None
                r2 = pct(day_open, prev_close)
                r3 = pct(hod, day_open)

                wr.writerow([sym, date_iso,
                           f"{r1:.2f}" if r1 is not None else "",
                           f"{r2:.2f}" if r2 is not None else "",
                           f"{r3:.2f}" if r3 is not None else "",
                           f"{prev_close:.4f}", f"{day_open:.4f}", f"{hod:.4f}",
                           f"{pm_high:.4f}" if pm_high is not None else ""])

            except Exception:
                # keep going on any single-symbol failure
                continue

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])