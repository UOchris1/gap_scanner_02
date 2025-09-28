# -*- coding: ascii -*-
# Reuse your existing schema and index code. Minimal wrappers only.

import os
import sqlite3
from typing import Dict, Iterable, List, Optional, Tuple

def ensure_schema_and_indexes(db_path: str) -> None:
    # Use your production-ready schema + index scripts
    from enhanced_db_schema import ensure_enhanced_db_schema  # existing file
    ensure_enhanced_db_schema(db_path)  # creates discovery_hits, daily_raw, etc.
    try:
        from scripts.apply_db_indexes import apply  # existing file
        apply(db_path)
    except Exception as e:
        print(f"[WARN] Index optimization not applied: {e}")

    # NEW: make sure split columns exist
    with sqlite3.connect(db_path) as c:
        from src.core.database_operations import _ensure_split_context_columns, _ensure_daily_vw, _ensure_exchange_column, ensure_symbol_exchange_table, _ensure_pm_provenance_columns
        # Backfill/migrate columns that may be missing in pre-existing DBs
        _ensure_daily_vw(c)
        _ensure_split_context_columns(c)
        _ensure_pm_provenance_columns(c)
        _ensure_exchange_column(c)
        ensure_symbol_exchange_table(c)

        # De-duplicate rule rows and enforce uniqueness going forward
        try:
            cur = c.cursor()
            # Remove duplicates (keep first per (hit_id, trigger_rule))
            cur.execute(
                """
                DELETE FROM discovery_hit_rules
                WHERE rowid NOT IN (
                  SELECT MIN(rowid) FROM discovery_hit_rules
                  GROUP BY hit_id, trigger_rule
                )
                """
            )
            # Enforce uniqueness going forward
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_hit_rule
                ON discovery_hit_rules(hit_id, trigger_rule)
                """
            )
            # Helpful composite index for common filters
            try:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_disc_hits_date_venue_src ON discovery_hits(event_date, pm_high_source, pm_high_venue)"
                )
            except Exception:
                pass
            c.commit()
        except Exception as e:
            print(f"[WARN] Could not enforce rule uniqueness: {e}")

        # Optional enrichment: Fill legacy polygon_prev NULL fields with T+1 daily_raw values
        # Rationale: make polygon_prev more informative for manual inspection without
        # affecting the active pipeline (which does not use this table).
        try:
            cur = c.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='polygon_prev'")
            if cur.fetchone():
                # Discover available columns in polygon_prev
                cur.execute("PRAGMA table_info(polygon_prev)")
                pp_cols = {row[1] for row in cur.fetchall()}
                # Map target polygon_prev columns to daily_raw source columns
                mappings = [
                    ("open", "open"),
                    ("high", "high"),
                    ("low", "low"),
                    ("close_t1", "close"),  # only fill if a separate column exists; avoid overwriting prev close
                    ("volume", "volume"),
                    ("vwap", "vwap"),
                    ("vw", "vwap"),
                ]
                for target, source in mappings:
                    if target in pp_cols:
                        # Update NULL targets from daily_raw of next day (date + 1 day)
                        sql = (
                            f"UPDATE polygon_prev AS p "
                            f"SET {target} = COALESCE({target}, "
                            f"  (SELECT dr.{source} FROM daily_raw dr "
                            f"   WHERE dr.symbol = p.symbol AND dr.date = date(p.date, '+1 day') LIMIT 1)) "
                            f"WHERE {target} IS NULL"
                        )
                        try:
                            cur.execute(sql)
                        except Exception:
                            # Ignore if daily_raw lacks the source column or other minor issues
                            pass
                c.commit()
        except Exception as e:
            print(f"[WARN] polygon_prev enrichment skipped: {e}")

def store_daily_raw(conn: sqlite3.Connection, date_iso: str, rows: Iterable[Dict]) -> int:
    cur = conn.cursor()
    n = 0
    for r in rows:
        cur.execute(
            "INSERT OR REPLACE INTO daily_raw(provider,date,symbol,open,high,low,close,volume,vwap) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            ("polygon", date_iso, r["symbol"], r["open"], r["high"], r["low"], r["close"], r["volume"], r.get("vwap"))
        )
        n += 1
    conn.commit()
    return n

def fetch_prev_close_map(conn: sqlite3.Connection, prev_date_iso: str) -> Dict[str, float]:
    cur = conn.cursor()
    cur.execute("SELECT symbol, close FROM daily_raw WHERE date = ?", (prev_date_iso,))
    return {s: float(c) for s, c in cur.fetchall()}

def upsert_hit(
    conn: sqlite3.Connection,
    date_iso: str,
    symbol: str,
    volume: int,
    intraday_push_pct: Optional[float],
    near_rs: int,
    rs_exec_date: Optional[str] = None,
    rs_days_after: Optional[int] = None,
    exchange: Optional[str] = None,
    pm_high_source: Optional[str] = None,
    pm_high_venue: Optional[str] = None,
) -> int:
    """
    One row per (ticker, event_date) in discovery_hits.
    If row exists, merge fields; return canonical hit_id.
    """
    from src.core.database_operations import _ensure_split_context_columns
    _ensure_split_context_columns(conn)

    cur = conn.cursor()
    cur.execute("""
      INSERT INTO discovery_hits(ticker,event_date,volume,intraday_push_pct,is_near_reverse_split,rs_exec_date,rs_days_after,exchange,pm_high_source,pm_high_venue)
      VALUES(?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(ticker,event_date) DO UPDATE SET
        volume = MAX(discovery_hits.volume, excluded.volume),
        intraday_push_pct = COALESCE(discovery_hits.intraday_push_pct, excluded.intraday_push_pct),
        is_near_reverse_split = MAX(discovery_hits.is_near_reverse_split, excluded.is_near_reverse_split),
        rs_exec_date = COALESCE(excluded.rs_exec_date, discovery_hits.rs_exec_date),
        rs_days_after = COALESCE(excluded.rs_days_after, discovery_hits.rs_days_after),
        exchange = COALESCE(excluded.exchange, discovery_hits.exchange),
        pm_high_source = COALESCE(excluded.pm_high_source, discovery_hits.pm_high_source),
        pm_high_venue = COALESCE(excluded.pm_high_venue, discovery_hits.pm_high_venue)
      RETURNING hit_id
    """, (
        symbol,
        date_iso,
        int(volume or 0),
        intraday_push_pct,
        int(near_rs),
        rs_exec_date,
        rs_days_after,
        exchange,
        pm_high_source,
        pm_high_venue,
    ))
    hit_id = cur.fetchone()[0]
    conn.commit()
    return hit_id

def insert_rules(conn: sqlite3.Connection, rules: List[Tuple[int, str, float]]) -> None:
    if not rules:
        return
    cur = conn.cursor()
    cur.executemany("INSERT OR IGNORE INTO discovery_hit_rules(hit_id,trigger_rule,rule_value) VALUES(?,?,?)", rules)
    conn.commit()

def log_completeness(conn: sqlite3.Connection, date_iso: str, total_universe: int, polygon_count: int,
                     cand_pass1: int, r1_checked: int, r1_hits: int, miss_audit_sample: int,
                     miss_audit_hits: int, audit_failed: bool) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO completeness_log(date,total_universe,polygon_count,cand_pass1,"
        "r1_checked,r1_hits,miss_audit_sample,miss_audit_hits,audit_failed) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (date_iso, total_universe, polygon_count, cand_pass1, r1_checked, r1_hits,
         miss_audit_sample, miss_audit_hits, int(audit_failed))
    )
    conn.commit()
