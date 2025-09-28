#!/usr/bin/env python3
"""
Enhanced Database Schema with Baseline Tracking
Per universe_04.txt requirements for baseline comparison and diffs tracking.
"""
import sqlite3
from typing import Any, Dict, List


def ensure_enhanced_db_schema(db_path: str) -> None:
    """Create SQLite tables with enhanced baseline tracking support"""
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")

        # Original discovery tables (unchanged)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discovery_hits (
                hit_id INTEGER PRIMARY KEY,
                ticker TEXT NOT NULL,
                event_date TEXT NOT NULL,
                volume INTEGER,
                intraday_push_pct REAL,
                is_near_reverse_split INTEGER
            )
        """)

        # Create unique constraint for upsert operations
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS unique_index_hits
            ON discovery_hits(ticker, event_date)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS discovery_hit_rules (
                hit_rule_id INTEGER PRIMARY KEY,
                hit_id INTEGER NOT NULL,
                trigger_rule TEXT NOT NULL,
                rule_value REAL,
                FOREIGN KEY(hit_id) REFERENCES discovery_hits(hit_id)
            )
        """)

        # Raw daily and completeness bookkeeping (unchanged)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_raw (
                provider TEXT,
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                vwap REAL,
                PRIMARY KEY(provider, date, symbol)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS universe_day (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                delisted_utc TEXT,
                primary_exchange TEXT,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(date, symbol)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS completeness_log (
                date TEXT PRIMARY KEY,
                total_universe INTEGER,
                polygon_count INTEGER,
                cand_pass1 INTEGER,
                r1_checked INTEGER,
                r1_hits INTEGER,
                miss_audit_sample INTEGER,
                miss_audit_hits INTEGER,
                audit_failed INTEGER
            )
        """)

        # NEW: Baseline hits table per universe_04.txt
        conn.execute("""
            CREATE TABLE IF NOT EXISTS baseline_hits (
                baseline_id INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rule TEXT NOT NULL,
                pct_value REAL,
                source TEXT NOT NULL,
                volume INTEGER,
                prev_close REAL,
                open_price REAL,
                high REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, symbol, rule, source)
            )
        """)

        # NEW: Baseline comparison diffs table per universe_04.txt
        conn.execute("""
            CREATE TABLE IF NOT EXISTS diffs (
                diff_id INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                rule TEXT NOT NULL,
                primary_only_count INTEGER DEFAULT 0,
                baseline_only_count INTEGER DEFAULT 0,
                overlap_count INTEGER DEFAULT 0,
                total_primary INTEGER DEFAULT 0,
                total_baseline INTEGER DEFAULT 0,
                coverage_rate REAL DEFAULT 0.0,
                comparison_passed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, rule)
            )
        """)

        # NEW: Enhanced audit results table with rule of three metrics
        conn.execute("""
            CREATE TABLE IF NOT EXISTS enhanced_audit_log (
                audit_id INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                exchange_roster_size INTEGER,
                undiscovered_count INTEGER,
                required_sample_size INTEGER,
                actual_sample_size INTEGER,
                samples_checked INTEGER,
                observed_misses INTEGER,
                miss_rate_bound REAL,
                target_miss_rate REAL,
                confidence_level REAL,
                audit_passed INTEGER,
                audit_errors INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date)
            )
        """)

        # NEW: Missed hits discovered by audit (for investigation)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_missed_hits (
                miss_id INTEGER PRIMARY KEY,
                audit_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                rule TEXT NOT NULL,
                pct_value REAL,
                premarket_high REAL,
                prev_close REAL,
                date TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(audit_id) REFERENCES enhanced_audit_log(audit_id)
            )
        """)

        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_hits_date ON discovery_hits(event_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_hits_ticker ON discovery_hits(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_baseline_hits_date ON baseline_hits(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_baseline_hits_symbol ON baseline_hits(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_baseline_hits_rule ON baseline_hits(rule)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_diffs_date ON diffs(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_date ON enhanced_audit_log(date)")

        conn.commit()
        print("Enhanced database schema initialized with baseline tracking")

    # Ensure day_completeness table exists
    from src.core.database_operations import ensure_day_completeness_schema_db
    ensure_day_completeness_schema_db(db_path)


def store_baseline_hits(db_path: str, baseline_hits: List[Dict[str, Any]]) -> int:
    """Store baseline hits in database"""
    if not baseline_hits:
        return 0

    with sqlite3.connect(db_path) as conn:
        insert_sql = """
            INSERT OR REPLACE INTO baseline_hits
            (date, symbol, rule, pct_value, source, volume, prev_close, open_price, high)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        inserted = 0
        for hit in baseline_hits:
            try:
                conn.execute(insert_sql, (
                    hit.get("date"),
                    hit.get("symbol"),
                    hit.get("rule"),
                    hit.get("pct_value"),
                    hit.get("source"),
                    hit.get("volume"),
                    hit.get("prev_close"),
                    hit.get("open"),
                    hit.get("high")
                ))
                inserted += 1
            except Exception as e:
                print(f"Error storing baseline hit: {e}")

        conn.commit()
        print(f"Stored {inserted} baseline hits")
        return inserted


def store_baseline_comparison(db_path: str, date: str, comparison_results: Dict[str, Any]) -> None:
    """Store baseline comparison results in database"""
    with sqlite3.connect(db_path) as conn:
        # Store overall comparison summary
        insert_sql = """
            INSERT OR REPLACE INTO diffs
            (date, rule, primary_only_count, baseline_only_count, overlap_count,
             total_primary, total_baseline, coverage_rate, comparison_passed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        summary = comparison_results.get("summary", {})

        # For now, store aggregated results (could be enhanced to store per-rule)
        conn.execute(insert_sql, (
            date,
            "ALL",  # Aggregate across all rules
            summary.get("primary_only_count", 0),
            summary.get("baseline_only_count", 0),
            summary.get("overlap_count", 0),
            summary.get("total_primary", 0),
            summary.get("total_baseline", 0),
            summary.get("coverage_rate", 0.0),
            1 if comparison_results.get("comparison_passed", False) else 0
        ))

        conn.commit()
        print(f"Stored baseline comparison for {date}")


def store_enhanced_audit_results(db_path: str, date: str, audit_results: Dict[str, Any]) -> int:
    """Store enhanced audit results with rule of three metrics"""
    with sqlite3.connect(db_path) as conn:
        # Store audit summary
        insert_audit_sql = """
            INSERT OR REPLACE INTO enhanced_audit_log
            (date, exchange_roster_size, undiscovered_count, required_sample_size,
             actual_sample_size, samples_checked, observed_misses, miss_rate_bound,
             target_miss_rate, confidence_level, audit_passed, audit_errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor = conn.execute(insert_audit_sql, (
            date,
            audit_results.get("exchange_roster_size", 0),
            audit_results.get("undiscovered_count", 0),
            audit_results.get("required_sample_size", 0),
            audit_results.get("sample_size", 0),
            audit_results.get("samples_checked", 0),
            len(audit_results.get("missed_r1_hits", [])),
            audit_results.get("miss_rate_bound", 0.0),
            audit_results.get("target_miss_rate", 0.01),
            audit_results.get("confidence_level", 0.95),
            1 if audit_results.get("audit_passed", False) else 0,
            audit_results.get("audit_errors", 0)
        ))

        # Get the audit_id
        audit_id = cursor.lastrowid

        # Store any missed hits discovered by audit
        missed_hits = audit_results.get("missed_r1_hits", [])
        if missed_hits:
            insert_miss_sql = """
                INSERT INTO audit_missed_hits
                (audit_id, symbol, rule, pct_value, premarket_high, prev_close, date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            for miss in missed_hits:
                conn.execute(insert_miss_sql, (
                    audit_id,
                    miss.get("symbol"),
                    miss.get("rule", "R1"),
                    miss.get("value"),
                    miss.get("premarket_high"),
                    miss.get("prev_close"),
                    date
                ))

        conn.commit()
        print(f"Stored enhanced audit results for {date} (audit_id: {audit_id})")
        return audit_id


def get_baseline_comparison_summary(db_path: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    """Get baseline comparison summary across date range"""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        where_clause = ""
        params = []

        if start_date and end_date:
            where_clause = "WHERE date BETWEEN ? AND ?"
            params = [start_date, end_date]
        elif start_date:
            where_clause = "WHERE date >= ?"
            params = [start_date]

        query = f"""
            SELECT date, rule, primary_only_count, baseline_only_count, overlap_count,
                   total_primary, total_baseline, coverage_rate, comparison_passed,
                   created_at
            FROM diffs
            {where_clause}
            ORDER BY date DESC, rule
        """

        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_enhanced_audit_summary(db_path: str, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    """Get enhanced audit summary across date range"""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        where_clause = ""
        params = []

        if start_date and end_date:
            where_clause = "WHERE date BETWEEN ? AND ?"
            params = [start_date, end_date]
        elif start_date:
            where_clause = "WHERE date >= ?"
            params = [start_date]

        query = f"""
            SELECT date, exchange_roster_size, undiscovered_count, required_sample_size,
                   actual_sample_size, samples_checked, observed_misses, miss_rate_bound,
                   target_miss_rate, confidence_level, audit_passed, audit_errors,
                   created_at
            FROM enhanced_audit_log
            {where_clause}
            ORDER BY date DESC
        """

        cursor = conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def cleanup_old_data(db_path: str, days_to_keep: int = 90) -> None:
    """Clean up old baseline and audit data to prevent database bloat"""
    with sqlite3.connect(db_path) as conn:
        cutoff_sql = "SELECT date('now', '-{} days')".format(days_to_keep)
        cutoff_date = conn.execute(cutoff_sql).fetchone()[0]

        # Clean up old baseline hits
        conn.execute("DELETE FROM baseline_hits WHERE date < ?", (cutoff_date,))
        baseline_deleted = conn.rowcount

        # Clean up old diffs
        conn.execute("DELETE FROM diffs WHERE date < ?", (cutoff_date,))
        diffs_deleted = conn.rowcount

        # Clean up old audit results (cascade will handle missed hits)
        conn.execute("DELETE FROM enhanced_audit_log WHERE date < ?", (cutoff_date,))
        audit_deleted = conn.rowcount

        conn.commit()
        print(f"Cleaned up old data: {baseline_deleted} baseline hits, {diffs_deleted} diffs, {audit_deleted} audits")