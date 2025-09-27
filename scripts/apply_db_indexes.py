#!/usr/bin/env python3
# scripts/apply_db_indexes.py (ASCII only)
import sqlite3
import os
import sys

INDEX_SQL = [
    # Discovery + lookups
    "CREATE INDEX IF NOT EXISTS idx_discovery_hits_ticker_date ON discovery_hits(ticker, event_date)",
    "CREATE INDEX IF NOT EXISTS idx_discovery_hits_date ON discovery_hits(event_date)",
    "CREATE INDEX IF NOT EXISTS idx_discovery_rules_hitid ON discovery_hit_rules(hit_id)",

    # Raw + caches
    "CREATE INDEX IF NOT EXISTS idx_daily_raw_date_symbol ON daily_raw(date, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_prev_close_date_symbol ON prev_close_cache(date, symbol)",

    # Baseline/audit
    "CREATE INDEX IF NOT EXISTS idx_baseline_hits_symbol_date ON baseline_hits(symbol, date)",
    "CREATE INDEX IF NOT EXISTS idx_enhanced_audit_date ON enhanced_audit_log(date)",
    "CREATE INDEX IF NOT EXISTS idx_audit_missed_hits_date ON audit_missed_hits(date)",

    # Universe + diffs
    "CREATE INDEX IF NOT EXISTS idx_universe_day_date ON universe_day(date)",
    "CREATE INDEX IF NOT EXISTS idx_diffs_date ON diffs(date)",
]

def apply(db_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # Pragmas for faster writes; acceptable for batch backfill
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA mmap_size=30000000000")  # 30 GB if available; ignored if not

        # Get existing tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cur.fetchall()}

        # Filter indexes to only apply to existing tables
        applicable_indexes = []
        for sql in INDEX_SQL:
            # Extract table name from CREATE INDEX statement
            table_name = None
            if " ON " in sql:
                table_part = sql.split(" ON ")[1].split("(")[0].strip()
                table_name = table_part

            if table_name and table_name in existing_tables:
                applicable_indexes.append(sql)
            elif table_name:
                print(f"Skipping index for non-existent table: {table_name}")

        # Do everything in one txn
        cur.execute("BEGIN IMMEDIATE")
        for sql in applicable_indexes:
            try:
                cur.execute(sql)
            except Exception as e:
                print(f"Warning: Failed to create index: {e}")
        cur.execute("COMMIT")

        # Analyze/optimize per fix1.txt Section 2.3
        print("Running ANALYZE...")
        cur.execute("ANALYZE")
        print("Running VACUUM...")
        cur.execute("VACUUM")
        cur.execute("PRAGMA optimize")

        conn.commit()
        print(f"OK: {len(applicable_indexes)} indexes + pragmas applied")

    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m scripts.apply_db_indexes db/production_3year.db")
        sys.exit(2)
    apply(sys.argv[1])