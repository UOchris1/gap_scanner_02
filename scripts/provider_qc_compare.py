# scripts/provider_qc_compare.py
import os, csv, datetime as dt, sqlite3
from typing import Dict, List
from src.providers.polygon_provider import grouped_daily, prev_close as poly_prev, splits as poly_splits
# If you have FMP and Theta wrappers, import them here

def daterange(a: str, b: str) -> List[str]:
    s, e = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
    out=[]; d=s
    while d<=e:
        if d.weekday()<5: out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out

def main(start, end, db, out_csv):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    rows=[]
    with sqlite3.connect(db) as c:
        for day in daterange(start,end):
            pdaily = grouped_daily(day)  # whole market; fast
            symbol_set = {r["symbol"] for r in pdaily}
            # Placeholder hooks: plug Theta/FMP retrievals here if needed
            # Compute per-day metrics (counts, overlaps, missing prev_close, split events, etc.)
            rows.append([day, len(symbol_set), "OK"])
    with open(out_csv,"w",newline="") as f:
        w=csv.writer(f); w.writerow(["date","polygon_universe","status"]); w.writerows(rows)

if __name__=="__main__":
    import sys
    if len(sys.argv)!=5:
        print("usage: python -m scripts.provider_qc_compare START END DB OUTCSV"); raise SystemExit(2)
    main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])