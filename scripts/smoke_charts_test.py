# -*- coding: utf-8 -*-
# scripts/smoke_charts_test.py
import argparse, sqlite3, pandas as pd, sys, subprocess, os, json
from pathlib import Path

def sql(cx, q, params=()):
    return pd.read_sql(q, cx, params=params)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="db/scanner.db")
    ap.add_argument("--start", default="2025-09-22")
    ap.add_argument("--end", default="2025-09-26")
    ap.add_argument("--out", default="data/input/hits_smoke.csv")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    Path("data/input").mkdir(parents=True, exist_ok=True)

    # 1) Build a CSV of hits for the range
    print("BUILD CSV...")
    rc = subprocess.call([sys.executable, "scripts/build_hits_csv.py",
                          "--db", args.db, "--start", args.start, "--end", args.end,
                          "--out", args.out])
    if rc != 0: sys.exit(rc)

    # 2) Ingest a limited number (fast smoke)
    print("INGEST...")
    rc = subprocess.call([sys.executable, "scripts/gapctl.py", "charts-ingest",
                          "--csv", args.out, "--db", args.db, "--limit", str(args.limit)])
    if rc != 0: sys.exit(rc)

    # 3) Basic DB checks
    print("CHECK DB...")
    with sqlite3.connect(args.db) as cx:
        c1 = sql(cx, "SELECT COUNT(*) AS n FROM bars_1m")["n"].iloc[0]
        c5 = sql(cx, "SELECT COUNT(*) AS n FROM bars_5m")["n"].iloc[0]
        cd = sql(cx, "SELECT COUNT(*) AS n FROM bars_1d")["n"].iloc[0]
        print(json.dumps({"bars_1m": int(c1), "bars_5m": int(c5), "bars_1d": int(cd)}, indent=2))

        # pick any symbol/date present
        sym = sql(cx, "SELECT symbol FROM bars_5m GROUP BY symbol ORDER BY COUNT(*) DESC LIMIT 1")["symbol"]
        if not sym.empty:
            sym = sym.iloc[0]
            ds = sql(cx, "SELECT DISTINCT substr(ts_utc,1,10) AS d FROM bars_5m WHERE symbol=? ORDER BY d DESC LIMIT 1", (sym,))
            d0 = ds["d"].iloc[0] if not ds.empty else None
            if d0:
                n5 = sql(cx, "SELECT COUNT(*) AS n FROM bars_5m WHERE symbol=? AND substr(ts_utc,1,10)=?", (sym,d0))["n"].iloc[0]
                print(f"Sample: {sym} {d0} -> 5m bars: {n5}")

if __name__ == "__main__":
    main()
