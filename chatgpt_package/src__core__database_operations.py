# -*- coding: ascii -*-
# Day completeness schema and metrics (no new root-level files)

import json as _json
import sqlite3 as _sqlite3
from typing import Optional as _Optional, Dict as _Dict
import sqlite3 as sqlite3

def ensure_day_completeness_schema_conn(conn: _sqlite3.Connection) -> None:
    """
    Create the day_completeness table and supporting index if they do not exist.
    This is idempotent and safe to invoke repeatedly.
    """
    cur = conn.cursor()

    # CREATE TABLE IF NOT EXISTS is the canonical way to conditionally create tables in SQLite.
    # See SQLite docs for CREATE TABLE and IF NOT EXISTS.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS day_completeness (
            date TEXT PRIMARY KEY,
            total_universe INTEGER,
            daily_raw_rows INTEGER,
            daily_raw_symbols INTEGER,
            coverage_pct REAL,
            discoveries INTEGER,
            discovery_rule_rows INTEGER,
            r1_hits INTEGER,
            r2_hits INTEGER,
            r3_hits INTEGER,
            r4_hits INTEGER,
            cand_pass1 INTEGER,
            r1_checked INTEGER,
            r1_hits_log INTEGER,
            miss_audit_sample INTEGER,
            miss_audit_hits INTEGER,
            audit_failed INTEGER,
            provider_status TEXT,
            created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_day_completeness_date
        ON day_completeness(date)
    """)

    conn.commit()

def ensure_day_completeness_schema_db(db_path: str) -> None:
    """
    Convenience wrapper when a connection is not yet open.
    """
    with _sqlite3.connect(db_path) as conn:
        ensure_day_completeness_schema_conn(conn)

def _count_scalar(cur: _sqlite3.Cursor, sql: str, args: tuple) -> int:
    cur.execute(sql, args)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def compute_day_completeness_metrics_conn(conn: _sqlite3.Connection, date_iso: str) -> dict:
    """
    Compute daily completeness metrics from existing tables:
    - universe_day (deterministic universe)
    - daily_raw (market backbone)
    - discovery_hits + discovery_hit_rules (R1..R4 results)
    - completeness_log (your per-day operational log, if present)

    All joins align with your current column names: discovery_hits.ticker and daily_raw.symbol.
    """
    ensure_day_completeness_schema_conn(conn)
    cur = conn.cursor()

    # Universe size for the day (deterministic loader)
    total_universe = _count_scalar(
        cur, "SELECT COUNT(*) FROM universe_day WHERE date = ?", (date_iso,)
    )

    # Backbone coverage for the day
    daily_raw_rows = _count_scalar(
        cur, "SELECT COUNT(*) FROM daily_raw WHERE date = ?", (date_iso,)
    )

    daily_raw_symbols = _count_scalar(
        cur, "SELECT COUNT(DISTINCT symbol) FROM daily_raw WHERE date = ?", (date_iso,)
    )

    coverage_pct = (float(daily_raw_symbols) / float(total_universe) * 100.0) if total_universe else None

    # Discovery rows and rule rows for the day
    discoveries = _count_scalar(
        cur, "SELECT COUNT(*) FROM discovery_hits WHERE event_date = ?", (date_iso,)
    )

    discovery_rule_rows = _count_scalar(
        cur, """
            SELECT COUNT(*) FROM discovery_hit_rules r
            JOIN discovery_hits h ON h.hit_id = r.hit_id
            WHERE h.event_date = ?
        """, (date_iso,),
    )

    # Rule-specific counts
    def _count_rule(rule_name: str) -> int:
        return _count_scalar(
            cur, """
                SELECT COUNT(*) FROM discovery_hit_rules r
                JOIN discovery_hits h ON h.hit_id = r.hit_id
                WHERE h.event_date = ? AND r.trigger_rule = ?
            """, (date_iso, rule_name),
        )

    r1_hits = _count_rule("PM_GAP_50")
    r2_hits = _count_rule("OPEN_GAP_50")
    r3_hits = _count_rule("INTRADAY_PUSH_50")
    r4_hits = _count_rule("SURGE_7D_300")

    # Pull optional operational fields from completeness_log, if present
    try:
        cur.execute("""
            SELECT total_universe, polygon_count, cand_pass1, r1_checked, r1_hits,
                   miss_audit_sample, miss_audit_hits, audit_failed
            FROM completeness_log WHERE date = ?
        """, (date_iso,))
        row = cur.fetchone()
        if row:
            # Keep r1_hits from log separate (r1_hits_log) from derived r1_hits above
            cand_pass1 = int(row[2]) if row[2] is not None else None
            r1_checked = int(row[3]) if row[3] is not None else None
            r1_hits_log = int(row[4]) if row[4] is not None else None
            miss_audit_sample = int(row[5]) if row[5] is not None else None
            miss_audit_hits = int(row[6]) if row[6] is not None else None
            audit_failed = int(row[7]) if row[7] is not None else None
        else:
            cand_pass1 = r1_checked = r1_hits_log = miss_audit_sample = miss_audit_hits = audit_failed = None
    except Exception:
        # If completeness_log does not exist yet, keep these as None
        cand_pass1 = r1_checked = r1_hits_log = miss_audit_sample = miss_audit_hits = audit_failed = None

    return {
        "date": date_iso,
        "total_universe": total_universe,
        "daily_raw_rows": daily_raw_rows,
        "daily_raw_symbols": daily_raw_symbols,
        "coverage_pct": coverage_pct,
        "discoveries": discoveries,
        "discovery_rule_rows": discovery_rule_rows,
        "r1_hits": r1_hits,
        "r2_hits": r2_hits,
        "r3_hits": r3_hits,
        "r4_hits": r4_hits,
        "cand_pass1": cand_pass1,
        "r1_checked": r1_checked,
        "r1_hits_log": r1_hits_log,
        "miss_audit_sample": miss_audit_sample,
        "miss_audit_hits": miss_audit_hits,
        "audit_failed": audit_failed
    }

def upsert_day_completeness_conn(conn: _sqlite3.Connection, metrics: dict, provider_status: _Optional[_Dict[str, bool]] = None) -> None:
    """
    Insert or update the metrics row for a day. Primary key is the date.
    """
    ensure_day_completeness_schema_conn(conn)

    provider_status_text = None
    if provider_status is not None:
        try:
            provider_status_text = _json.dumps(provider_status, separators=(",", ":"), ensure_ascii=True)
        except Exception:
            provider_status_text = None

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO day_completeness (
            date, total_universe, daily_raw_rows, daily_raw_symbols, coverage_pct,
            discoveries, discovery_rule_rows, r1_hits, r2_hits, r3_hits, r4_hits,
            cand_pass1, r1_checked, r1_hits_log, miss_audit_sample, miss_audit_hits,
            audit_failed, provider_status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            total_universe = excluded.total_universe,
            daily_raw_rows = excluded.daily_raw_rows,
            daily_raw_symbols = excluded.daily_raw_symbols,
            coverage_pct = excluded.coverage_pct,
            discoveries = excluded.discoveries,
            discovery_rule_rows = excluded.discovery_rule_rows,
            r1_hits = excluded.r1_hits,
            r2_hits = excluded.r2_hits,
            r3_hits = excluded.r3_hits,
            r4_hits = excluded.r4_hits,
            cand_pass1 = excluded.cand_pass1,
            r1_checked = excluded.r1_checked,
            r1_hits_log = excluded.r1_hits_log,
            miss_audit_sample = excluded.miss_audit_sample,
            miss_audit_hits = excluded.miss_audit_hits,
            audit_failed = excluded.audit_failed,
            provider_status = excluded.provider_status
    """, (
        metrics.get("date"),
        metrics.get("total_universe"),
        metrics.get("daily_raw_rows"),
        metrics.get("daily_raw_symbols"),
        metrics.get("coverage_pct"),
        metrics.get("discoveries"),
        metrics.get("discovery_rule_rows"),
        metrics.get("r1_hits"),
        metrics.get("r2_hits"),
        metrics.get("r3_hits"),
        metrics.get("r4_hits"),
        metrics.get("cand_pass1"),
        metrics.get("r1_checked"),
        metrics.get("r1_hits_log"),
        metrics.get("miss_audit_sample"),
        metrics.get("miss_audit_hits"),
        metrics.get("audit_failed"),
        provider_status_text
    ))

    conn.commit()

def recompute_and_upsert_day_completeness(db_path: str, date_iso: str, provider_status: _Optional[_Dict[str, bool]] = None) -> None:
    """
    One-call helper used by pipeline and exporters.
    """
    with _sqlite3.connect(db_path) as conn:
        metrics = compute_day_completeness_metrics_conn(conn, date_iso)
        upsert_day_completeness_conn(conn, metrics, provider_status)

# =============================================================================
# DAILY RAW VWAP COLUMN MIGRATION
# =============================================================================

def _ensure_daily_vw(conn: _sqlite3.Connection) -> None:
    """
    Add vw (VWAP) column to daily_raw table if missing, idempotently.
    Required for storing VWAP from Polygon grouped daily API.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(daily_raw)")
    cols = {row[1] for row in cur.fetchall()}
    # Preferred column name is 'vwap'
    added = False
    if "vwap" not in cols:
        cur.execute("ALTER TABLE daily_raw ADD COLUMN vwap REAL")
        added = True
    # If legacy 'vw' exists, migrate values into 'vwap' when present
    if "vw" in cols:
        try:
            cur.execute("UPDATE daily_raw SET vwap = COALESCE(vwap, vw)")
        except Exception:
            pass
    if added:
        conn.commit()

# =============================================================================
# EXCHANGE COLUMN AND CACHE
# =============================================================================

def _ensure_exchange_column(conn: _sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(discovery_hits)")
    cols = {row[1] for row in cur.fetchall()}
    if "exchange" not in cols:
        cur.execute("ALTER TABLE discovery_hits ADD COLUMN exchange TEXT")
        conn.commit()

def ensure_symbol_exchange_table(conn: _sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_exchange (
            symbol TEXT PRIMARY KEY,
            primary_exchange TEXT,
            normalized_exchange TEXT,
            security_type TEXT,
            ticker_suffix TEXT,
            last_updated_utc TEXT
        )
        """
    )
    # Backfill missing columns if table existed previously
    try:
        cur.execute("PRAGMA table_info(symbol_exchange)")
        cols = {row[1] for row in cur.fetchall()}
        changed = False
        if "security_type" not in cols:
            cur.execute("ALTER TABLE symbol_exchange ADD COLUMN security_type TEXT")
            changed = True
        if "ticker_suffix" not in cols:
            cur.execute("ALTER TABLE symbol_exchange ADD COLUMN ticker_suffix TEXT")
            changed = True
        if changed:
            conn.commit()
    except Exception:
        pass
    conn.commit()

def get_cached_exchange(conn: _sqlite3.Connection, symbol: str) -> _Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT normalized_exchange FROM symbol_exchange WHERE symbol=?", (symbol,))
    row = cur.fetchone()
    return row[0] if row else None

def get_cached_meta(conn: _sqlite3.Connection, symbol: str) -> _Optional[dict]:
    """Return cached metadata for symbol or None.
    Keys: exchange, security_type, primary_exchange, ticker_suffix
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT primary_exchange, normalized_exchange, security_type, ticker_suffix FROM symbol_exchange WHERE symbol=?",
        (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None
    mic, ex, st, suf = row
    return {
        "primary_exchange": mic,
        "exchange": ex,
        "security_type": st,
        "ticker_suffix": suf,
    }

def upsert_symbol_exchange(conn: _sqlite3.Connection, symbol: str, mic: _Optional[str], norm: _Optional[str], *, security_type: _Optional[str] = None, ticker_suffix: _Optional[str] = None) -> None:
    from datetime import datetime as _dt
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO symbol_exchange(symbol, primary_exchange, normalized_exchange, security_type, ticker_suffix, last_updated_utc)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
          primary_exchange=excluded.primary_exchange,
          normalized_exchange=excluded.normalized_exchange,
          security_type=COALESCE(excluded.security_type, symbol_exchange.security_type),
          ticker_suffix=COALESCE(excluded.ticker_suffix, symbol_exchange.ticker_suffix),
          last_updated_utc=excluded.last_updated_utc
        """,
        (symbol, mic, norm, security_type, ticker_suffix, _dt.utcnow().isoformat()),
    )
    conn.commit()

# =============================================================================
# SPLIT CONTEXT COLUMNS MIGRATION
# =============================================================================

def _ensure_split_context_columns(conn: _sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(discovery_hits)")
    cols = {row[1] for row in cur.fetchall()}
    changed = False
    if "rs_exec_date" not in cols:
        cur.execute("ALTER TABLE discovery_hits ADD COLUMN rs_exec_date TEXT")
        changed = True
    if "rs_days_after" not in cols:
        cur.execute("ALTER TABLE discovery_hits ADD COLUMN rs_days_after INTEGER")
        changed = True
    if changed:
        conn.commit()

# =============================================================================
# PM HIGH PROVENANCE COLUMNS MIGRATION
# =============================================================================

def _ensure_pm_provenance_columns(conn: _sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(discovery_hits)")
    cols = {row[1] for row in cur.fetchall()}
    changed = False
    if "pm_high_source" not in cols:
        cur.execute("ALTER TABLE discovery_hits ADD COLUMN pm_high_source TEXT")
        changed = True
    if "pm_high_venue" not in cols:
        cur.execute("ALTER TABLE discovery_hits ADD COLUMN pm_high_venue TEXT")
        changed = True
    if changed:
        conn.commit()

# =============================================================================
# NEXT-DAY OUTCOMES (T+1) LABELS
# =============================================================================

def _ensure_next_day_outcomes_schema(conn: _sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS next_day_outcomes (
            date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            close REAL,
            next_date TEXT,
            next_close REAL,
            next_return_pct REAL,
            next_positive INTEGER,
            PRIMARY KEY(date, symbol)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_next_outcomes_date ON next_day_outcomes(date)"
    )
    conn.commit()


def recompute_next_day_outcomes_range(db_path: str, start_date: str, end_date: str) -> int:
    """
    Compute next-day outcomes from daily_raw for all (date,symbol) between start and end.
    - next_date is the next available date for the same symbol in daily_raw (skips weekends/holidays).
    - next_return_pct = (next_close/close - 1) * 100.
    - next_positive = 1 if next_return_pct > 0 else 0 (0 if unavailable).
    Returns number of rows upserted.
    """
    with _sqlite3.connect(db_path) as conn:
        _ensure_next_day_outcomes_schema(conn)
        cur = conn.cursor()

        # Pull all (date, symbol, close) in range
        cur.execute(
            """
            SELECT date, symbol, close
            FROM daily_raw
            WHERE date BETWEEN ? AND ?
            ORDER BY date, symbol
            """,
            (start_date, end_date),
        )
        rows = cur.fetchall()
        upserts = 0
        for date_iso, symbol, close in rows:
            # Find the next trading date for this symbol (first future date with data)
            cur.execute(
                """
                SELECT date, close
                FROM daily_raw
                WHERE symbol = ? AND date > ?
                ORDER BY date ASC
                LIMIT 1
                """,
                (symbol, date_iso),
            )
            nxt = cur.fetchone()
            if not nxt:
                next_date, next_close = None, None
                next_ret, next_pos = None, 0
            else:
                next_date, next_close = nxt[0], nxt[1]
                try:
                    if close and next_close:
                        next_ret = (float(next_close) / float(close) - 1.0) * 100.0
                        next_pos = 1 if next_ret > 0 else 0
                    else:
                        next_ret, next_pos = None, 0
                except Exception:
                    next_ret, next_pos = None, 0

            cur.execute(
                """
                INSERT INTO next_day_outcomes(date, symbol, close, next_date, next_close, next_return_pct, next_positive)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    close = excluded.close,
                    next_date = excluded.next_date,
                    next_close = excluded.next_close,
                    next_return_pct = excluded.next_return_pct,
                    next_positive = excluded.next_positive
                """,
                (date_iso, symbol, close, next_date, next_close, next_ret, next_pos),
            )
            upserts += 1

        conn.commit()
        return upserts

# =============================================================================
# SPLIT CONTEXT SCHEMA AND OPERATIONS
# =============================================================================

def ensure_discovery_hit_split_context(conn: _sqlite3.Connection) -> None:
    """
    Create discovery_hit_split_context table if it doesn't exist.
    This table stores reverse split context for each gap discovery.
    """
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS discovery_hit_split_context (
            hit_id INTEGER PRIMARY KEY,
            rs_exec_date TEXT,             -- YYYY-MM-DD
            rs_split_from REAL,            -- numeric from ratio
            rs_split_to REAL,              -- numeric to ratio
            rs_ratio REAL,                 -- rs_split_from / rs_split_to
            rs_days_from_event INTEGER,    -- event_date - rs_exec_date in days (can be negative)
            rs_is_reverse_split INTEGER,   -- 1 if from > to
            FOREIGN KEY(hit_id) REFERENCES discovery_hits(hit_id)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_split_hit_id
        ON discovery_hit_split_context(hit_id)
    """)

    conn.commit()

def upsert_hit_split_context(conn: _sqlite3.Connection,
                             hit_id: int,
                             rs_exec_date: _Optional[str],
                             split_from: _Optional[float],
                             split_to: _Optional[float],
                             rs_days_from_event: _Optional[int],
                             is_reverse: int) -> None:
    """
    Insert or update split context data for a discovery hit.
    """
    ensure_discovery_hit_split_context(conn)

    ratio = (float(split_from) / float(split_to)) if (split_from and split_to and split_to != 0) else None
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO discovery_hit_split_context
        (hit_id, rs_exec_date, rs_split_from, rs_split_to, rs_ratio, rs_days_from_event, rs_is_reverse_split)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(hit_id) DO UPDATE SET
            rs_exec_date=excluded.rs_exec_date,
            rs_split_from=excluded.rs_split_from,
            rs_split_to=excluded.rs_split_to,
            rs_ratio=excluded.rs_ratio,
            rs_days_from_event=excluded.rs_days_from_event,
            rs_is_reverse_split=excluded.rs_is_reverse_split
    """, (hit_id, rs_exec_date, split_from, split_to, ratio, rs_days_from_event, int(is_reverse)))

    conn.commit()

# =============================================================================
# FUNDAMENTALS SCHEMA AND OPERATIONS
# =============================================================================

def ensure_discovery_hit_fundamentals(conn: _sqlite3.Connection) -> None:
    """
    Create discovery_hit_fundamentals table if it doesn't exist.
    This table stores as-of fundamental data for each gap discovery.
    """
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS discovery_hit_fundamentals (
            hit_id INTEGER PRIMARY KEY,
            shares_outstanding REAL,
            market_cap REAL,
            float_shares REAL,
            dollar_volume REAL,
            data_source TEXT,
            retrieved_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
            FOREIGN KEY (hit_id) REFERENCES discovery_hits (hit_id)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fundamentals_hit_id
        ON discovery_hit_fundamentals(hit_id)
    """)

    conn.commit()

def upsert_hit_fundamentals(conn: _sqlite3.Connection, hit_id: int, shares_outstanding: _Optional[float] = None,
                           market_cap: _Optional[float] = None, float_shares: _Optional[float] = None,
                           dollar_volume: _Optional[float] = None, data_source: str = "unknown") -> None:
    """
    Insert or update fundamentals data for a discovery hit.
    """
    ensure_discovery_hit_fundamentals(conn)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO discovery_hit_fundamentals
        (hit_id, shares_outstanding, market_cap, float_shares, dollar_volume, data_source)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(hit_id) DO UPDATE SET
            shares_outstanding = COALESCE(excluded.shares_outstanding, discovery_hit_fundamentals.shares_outstanding),
            market_cap = COALESCE(excluded.market_cap, discovery_hit_fundamentals.market_cap),
            float_shares = COALESCE(excluded.float_shares, discovery_hit_fundamentals.float_shares),
            dollar_volume = COALESCE(excluded.dollar_volume, discovery_hit_fundamentals.dollar_volume),
            data_source = excluded.data_source,
            retrieved_at = excluded.retrieved_at
    """, (hit_id, shares_outstanding, market_cap, float_shares, dollar_volume, data_source))

    conn.commit()

# =============================================================================
# NOTIONAL AND VWAP REPAIR UTILITY
# =============================================================================

def repair_notional_and_vw(conn: _sqlite3.Connection, start_date: str, end_date: str, polygon_api_key: str) -> dict:
    """
    For discovery hits in [start_date,end_date], ensure daily_raw.vw is set.
    Recompute dollar_volume as volume*vw and upsert into discovery_hit_fundamentals.
    """
    import datetime as dt
    cur = conn.cursor()
    # Ensure vw column exists
    _ensure_daily_vw(conn)

    # Pull all (symbol,date) we need
    cur.execute("""
      SELECT DISTINCT d.ticker, d.event_date
      FROM discovery_hits d
      WHERE d.event_date BETWEEN ? AND ?
    """, (start_date, end_date))
    pairs = cur.fetchall()

    fixed, missing = 0, 0
    for sym, d in pairs:
        # Get daily_raw row
        row = cur.execute(
          "SELECT volume, vw, open, high, low, close FROM daily_raw WHERE symbol=? AND date=?",
          (sym, d)
        ).fetchone()
        if not row:
            # try to fetch and insert from Polygon per-ticker
            from src.providers.polygon_provider import daily_symbol
            v, vw = daily_symbol(d, sym, polygon_api_key)
            if v is None:
                missing += 1
                continue
            # Insert a minimal daily_raw if absent
            cur.execute("INSERT OR IGNORE INTO daily_raw(symbol,date,volume,vw) VALUES(?,?,?,?)",
                        (sym, d, v, vw))
            conn.commit()
            row = (v, vw, None, None, None, None)

        vol, vw, o, h, l, c = row
        if vw is None:
            # fallback: try per-ticker
            from src.providers.polygon_provider import daily_symbol
            v2, vw2 = daily_symbol(d, sym, polygon_api_key)
            if vw2 is not None:
                cur.execute("UPDATE daily_raw SET vw=? WHERE symbol=? AND date=?", (vw2, sym, d))
                conn.commit()
                vw = vw2
            else:
                # last-resort proxy
                if all(x is not None for x in (o,h,l,c)):
                    vw = (o+h+l+c)/4.0
                    cur.execute("UPDATE daily_raw SET vw=? WHERE symbol=? AND date=?", (vw, sym, d))
                    conn.commit()

        if vw is None or vol is None:
            missing += 1
            continue

        dollar_volume = float(vol) * float(vw)

        # Upsert into fundamentals table (keep source tag)
        cur.execute("""
          INSERT INTO discovery_hit_fundamentals(hit_id, shares_outstanding, market_cap, float_shares, dollar_volume, data_source)
          SELECT d.hit_id, f.shares_outstanding, f.market_cap, f.float_shares, ?, 'calc_unadj_polygon'
          FROM discovery_hits d
          LEFT JOIN discovery_hit_fundamentals f ON f.hit_id=d.hit_id
          WHERE d.ticker=? AND d.event_date=?
          ON CONFLICT(hit_id) DO UPDATE SET dollar_volume=excluded.dollar_volume, data_source='calc_unadj_polygon'
        """, (dollar_volume, sym, d))
        conn.commit()
        fixed += 1

    return {"fixed": fixed, "missing": missing}
