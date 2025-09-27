#!/usr/bin/env python3
# -*- coding: ascii -*-
import argparse, sqlite3, csv, os

def main(db_path, start_iso, end_iso, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"provenance_summary_{start_iso}_{end_iso}.csv")
    sql = """
    SELECT d.event_date,
           COALESCE(d.pm_high_source,'') AS pm_high_source,
           COALESCE(d.pm_high_venue,'')  AS pm_high_venue,
           COUNT(*) AS hits
    FROM discovery_hits d
    WHERE d.event_date BETWEEN ? AND ?
      AND d.pm_high_source IS NOT NULL
      AND d.pm_high_venue  IS NOT NULL
    GROUP BY d.event_date, pm_high_source, pm_high_venue
    ORDER BY d.event_date, pm_high_source, pm_high_venue
    """
    with sqlite3.connect(db_path) as c, open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","pm_high_source","pm_high_venue","hits"])
        for row in c.execute(sql, (start_iso, end_iso)):
            w.writerow(row)
    print("[OK] wrote", out_csv)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="db/scanner.db")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="exports")
    args = ap.parse_args()
    main(args.db, args.start, args.end, args.out)
