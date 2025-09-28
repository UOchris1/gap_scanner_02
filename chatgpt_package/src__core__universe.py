# -*- coding: ascii -*-
# Deterministic universe management for zero-miss gap scanning

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.providers.polygon_provider import get_universe_symbols

def ensure_universe_day_table(db_path: str) -> None:
    """Ensure universe_day table exists in database"""
    with sqlite3.connect(db_path) as conn:
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

        # Create index for efficient queries
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_day_date
            ON universe_day(date)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_universe_day_symbol
            ON universe_day(symbol)
        """)

def populate_universe_for_date(db_path: str, date_iso: str, force_refresh: bool = False) -> int:
    """
    Populate universe_day table for a specific date using Polygon API.
    Returns number of symbols loaded.
    Per plan2.txt: Include delisted symbols for deterministic coverage.
    """
    ensure_universe_day_table(db_path)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Check if already populated for this date
        if not force_refresh:
            cur.execute("SELECT COUNT(*) FROM universe_day WHERE date = ?", (date_iso,))
            existing_count = cur.fetchone()[0]
            if existing_count > 0:
                print(f"[UNIVERSE] {date_iso} already has {existing_count} symbols")
                return existing_count

        # Clear existing data for this date if force refresh
        if force_refresh:
            cur.execute("DELETE FROM universe_day WHERE date = ?", (date_iso,))

        print(f"[UNIVERSE] Loading symbols for {date_iso} (including delisted)...")

        # Get universe from Polygon API
        symbols = get_universe_symbols(include_delisted=True)

        if not symbols:
            print("[UNIVERSE] Warning: No symbols returned from Polygon API")
            return 0

        # Filter for US stocks only and exclude problematic tickers
        valid_symbols = []
        for symbol_data in symbols:
            symbol = symbol_data.get("symbol", "").strip()
            market = symbol_data.get("market", "").lower()
            ticker_type = symbol_data.get("type", "").lower()

            # Basic filtering
            if (not symbol or
                len(symbol) > 10 or  # Exclude overly long symbols
                market != "stocks" or
                ticker_type not in ["cs", "common stock", "stock", ""]):
                continue

            # Exclude certain symbol patterns
            if any(pattern in symbol for pattern in [".", "/", " ", "-WT", "-RT", "-UN", "^"]):
                continue

            valid_symbols.append(symbol_data)

        print(f"[UNIVERSE] Filtered to {len(valid_symbols)} valid symbols")

        # Insert into database
        inserted_count = 0
        for symbol_data in valid_symbols:
            try:
                cur.execute("""
                    INSERT OR REPLACE INTO universe_day
                    (date, symbol, active, delisted_utc, primary_exchange)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    date_iso,
                    symbol_data["symbol"],
                    1 if symbol_data.get("active", True) else 0,
                    symbol_data.get("delisted_utc"),
                    symbol_data.get("primary_exchange", "")
                ))
                inserted_count += 1

            except Exception as e:
                print(f"[UNIVERSE] Error inserting {symbol_data.get('symbol', 'UNKNOWN')}: {e}")
                continue

        conn.commit()

        print(f"[UNIVERSE] Loaded {inserted_count} symbols for {date_iso}")
        return inserted_count

def get_universe_for_date(db_path: str, date_iso: str) -> List[str]:
    """
    Get deterministic list of symbols to scan for a given date.
    Ensures complete coverage by using universe_day table.
    """
    ensure_universe_day_table(db_path)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT symbol FROM universe_day
            WHERE date = ? AND primary_exchange IN ('XNAS', 'XNYS', 'XASE')
            AND symbol NOT LIKE '%W' AND symbol NOT LIKE '%.WS'
            ORDER BY symbol
        """, (date_iso,))

        symbols = [row[0] for row in cur.fetchall()]

        if not symbols:
            print(f"[UNIVERSE] No symbols found for {date_iso}, attempting to populate...")
            symbol_count = populate_universe_for_date(db_path, date_iso)
            if symbol_count > 0:
                # Retry query after population
                cur.execute("""
                    SELECT symbol FROM universe_day
                    WHERE date = ? AND primary_exchange IN ('XNAS', 'XNYS', 'XASE')
                    AND symbol NOT LIKE '%W' AND symbol NOT LIKE '%.WS'
                    ORDER BY symbol
                """, (date_iso,))
                symbols = [row[0] for row in cur.fetchall()]

        print(f"[UNIVERSE] Found {len(symbols)} symbols for scanning on {date_iso}")
        return symbols

def get_universe_stats(db_path: str, date_iso: str) -> Dict:
    """Get universe statistics for completeness reporting"""
    ensure_universe_day_table(db_path)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Total symbols
        cur.execute("SELECT COUNT(*) FROM universe_day WHERE date = ?", (date_iso,))
        total_symbols = cur.fetchone()[0]

        # Active vs delisted
        cur.execute("SELECT COUNT(*) FROM universe_day WHERE date = ? AND active = 1", (date_iso,))
        active_symbols = cur.fetchone()[0]

        # Exchange breakdown
        cur.execute("""
            SELECT primary_exchange, COUNT(*) as count
            FROM universe_day
            WHERE date = ?
            GROUP BY primary_exchange
            ORDER BY count DESC
        """, (date_iso,))
        exchange_breakdown = cur.fetchall()

        return {
            "date": date_iso,
            "total_symbols": total_symbols,
            "active_symbols": active_symbols,
            "delisted_symbols": total_symbols - active_symbols,
            "exchange_breakdown": {exchange: count for exchange, count in exchange_breakdown}
        }

def bulk_populate_universe(db_path: str, start_date: str, end_date: str) -> Dict:
    """
    Populate universe_day for a range of dates.
    Reuses the same universe data to avoid excessive API calls.
    """
    print(f"[UNIVERSE] Bulk populating {start_date} to {end_date}")

    # Get universe once and reuse for all dates
    symbols = get_universe_symbols(include_delisted=True)
    if not symbols:
        return {"success": False, "error": "No symbols from API"}

    # Generate date range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    current_dt = start_dt

    dates_populated = []
    total_symbols_loaded = 0

    while current_dt <= end_dt:
        date_iso = current_dt.strftime("%Y-%m-%d")

        # Skip weekends (basic filter)
        if current_dt.weekday() < 5:  # Monday=0, Sunday=6
            ensure_universe_day_table(db_path)

            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()

                # Clear existing for this date
                cur.execute("DELETE FROM universe_day WHERE date = ?", (date_iso,))

                # Insert all symbols for this date
                inserted = 0
                for symbol_data in symbols:
                    symbol = symbol_data.get("symbol", "").strip()
                    if len(symbol) > 0 and len(symbol) <= 10:
                        try:
                            cur.execute("""
                                INSERT INTO universe_day
                                (date, symbol, active, delisted_utc, primary_exchange)
                                VALUES (?, ?, ?, ?, ?)
                            """, (
                                date_iso,
                                symbol,
                                1 if symbol_data.get("active", True) else 0,
                                symbol_data.get("delisted_utc"),
                                symbol_data.get("primary_exchange", "")
                            ))
                            inserted += 1
                        except Exception:
                            continue

                conn.commit()
                dates_populated.append(date_iso)
                total_symbols_loaded += inserted
                print(f"[UNIVERSE] {date_iso}: {inserted} symbols")

        current_dt += timedelta(days=1)

    return {
        "success": True,
        "dates_populated": dates_populated,
        "total_dates": len(dates_populated),
        "total_symbols_loaded": total_symbols_loaded,
        "symbols_per_day": len(symbols)
    }