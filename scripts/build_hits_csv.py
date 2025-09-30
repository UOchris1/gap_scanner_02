# -*- coding: utf-8 -*-
# scripts/build_hits_csv.py
import argparse, sqlite3, pandas as pd, sys
from pathlib import Path

REQS = [["symbol","ticker"], ["trigger_date","date","event_date"]]

def find_cols(cols):
    s = next((c for c in REQS[0] if c in cols), None)
    d = next((c for c in REQS[1] if c in cols), None)
    return s, d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="db/scanner.db")
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD
    ap.add_argument("--out", default="data/input/hits_range.csv")
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(args.db) as cx:
        # try canonical table/columns first
        q = """
        SELECT * FROM discovery_hits
        WHERE event_date BETWEEN ? AND ?
        ORDER BY event_date, ticker
        """
        df = pd.read_sql(q, cx, params=(args.start, args.end))

    if df.empty:
        print("No hits found in that range.", file=sys.stderr); sys.exit(2)

    s_col, d_col = find_cols(df.columns)
    if not s_col or not d_col:
        print(f"Cannot find symbol/date columns in discovery_hits; cols={list(df.columns)}", file=sys.stderr)
        sys.exit(2)

    out = df[[s_col, d_col]].dropna().drop_duplicates().rename(
        columns={s_col:"symbol", d_col:"trigger_date"}
    )
    out.to_csv(args.out, index=False)
    print(f"Wrote {len(out)} rows -> {args.out}")

if __name__ == "__main__":
    main()
