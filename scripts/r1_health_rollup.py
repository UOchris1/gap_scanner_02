#!/usr/bin/env python3
# -*- coding: ascii -*-
import argparse, json, os, csv
from datetime import datetime, timedelta

def daterange(a, b):
    d1 = datetime.strptime(a, "%Y-%m-%d")
    d2 = datetime.strptime(b, "%Y-%m-%d")
    cur = d1
    while cur <= d2:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)

def load_diag(day_iso):
    p = os.path.join("project_state","artifacts",f"pm_diag_{day_iso}.json")
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="ascii", errors="ignore") as f:
            return json.load(f)
    except:
        return None

def health_of(diag):
    if not isinstance(diag, dict):
        return 0.0, 0, 0, 0
    s200 = sum(int(v.get("200",0)) for v in diag.values() if isinstance(v, dict))
    s204 = sum(int(v.get("204",0)) for v in diag.values() if isinstance(v, dict))
    s472 = sum(int(v.get("472",0)) for v in diag.values() if isinstance(v, dict))
    den = max(1, s200+s204+s472)
    return (float(s200)/float(den), s200, s204, s472)

def main(start_iso, end_iso, out_csv):
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","health","sum200","sum204","sum472"])
        for day in daterange(start_iso, end_iso):
            diag = load_diag(day)
            if diag is None:
                w.writerow([day,"",0,0,0])
                continue
            h, a, b, c = health_of(diag)
            w.writerow([day, f"{h:.3f}", a, b, c])
    print("[OK] wrote", out_csv)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="exports/r1_health_rollup.csv")
    args = ap.parse_args()
    main(args.start, args.end, args.out)
