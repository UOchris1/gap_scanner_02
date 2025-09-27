# scripts/export_unique_pairs.py
import csv, argparse, sqlite3

def export_unique_date_ticker_pairs(conn, start, end, path):
    """Export all unique date-ticker pairs for the month period (no duplicates)."""

    query = """
    SELECT DISTINCT
        d.event_date AS date,
        d.ticker
    FROM discovery_hits d
    WHERE d.event_date >= ? AND d.event_date <= ?
    ORDER BY d.event_date, d.ticker
    """

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "ticker"])
        for row in conn.execute(query, (start, end)):
            w.writerow(row)

def main(start, end, db, out_path):
    conn = sqlite3.connect(db)
    export_unique_date_ticker_pairs(conn, start, end, out_path)
    conn.close()
    print(f"Exported unique date-ticker pairs to: {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--db", default="db/scanner.db")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    main(a.start, a.end, a.db, a.out)