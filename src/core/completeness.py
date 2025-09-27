# -*- coding: ascii -*-
# Completeness guardrails: miss auditing and provider overlap reporting

import sqlite3
import os
import re
import csv
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from src.core.rules import r1_pm, r2_open_gap
from src.providers.polygon_provider import grouped_daily
from src.providers.theta_provider import ThetaDataClient

def create_miss_audit_table(db_path: str) -> None:
    """Create table to track miss audit results"""
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS miss_audit (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                audit_type TEXT NOT NULL,
                missed_initially BOOLEAN NOT NULL,
                rule_triggered TEXT,
                rule_value REAL,
                audit_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(date, symbol, audit_type)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_miss_audit_date
            ON miss_audit(date)
        """)

def post_scan_miss_audit(db_path: str, date_iso: str, top_n: int = 150) -> Dict:
    """
    Post-scan miss auditor per plan2.txt Phase D.
    Reruns targeted R1 and R2 passes on top N gainers to detect any missed discoveries.
    Returns audit results and marks day for retry if misses found.
    """
    create_miss_audit_table(db_path)

    # Get daily data for top gainers analysis
    daily = grouped_daily(date_iso)
    if not daily:
        return {"status": "no_daily_data", "misses_found": 0}

    # Calculate gainers: symbols with highest (high/prev_close - 1) ratios
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get previous day closes from daily_raw table
        prev_date = (datetime.fromisoformat(date_iso) - timedelta(days=1)).date().isoformat()
        cur.execute("""
            SELECT symbol, close FROM daily_raw
            WHERE date = ? AND close > 0
        """, (prev_date,))
        prev_closes = dict(cur.fetchall())

        # Calculate gain ratios and sort
        gainers = []
        for row in daily:
            symbol = row["symbol"]
            high = row["high"]
            prev_close = prev_closes.get(symbol)

            if prev_close and prev_close > 0 and high > 0:
                gain_ratio = (high / prev_close) - 1.0
                gainers.append((symbol, gain_ratio, high, prev_close))


        # Sort by gain ratio descending and allow env to cap audit scope
        gainers.sort(key=lambda x: x[1], reverse=True)
        try:
            env_cap = os.getenv("MISS_AUDIT_TOP_N")
            audit_cap = int(env_cap) if env_cap else top_n
        except Exception:
            audit_cap = top_n
        audit_cap = max(0, audit_cap)
        top_gainers = gainers[:audit_cap]

        allowed_exchanges = set((os.getenv("ALLOWED_EXCHANGES") or "NYSE,NASDAQ,AMEX").split(","))
        try:
            min_vol = int(os.getenv("DISCOVERY_MIN_VOL", "100000"))
        except Exception:
            min_vol = 100000
        exclude_deriv = os.getenv("EXCLUDE_DERIVATIVES", "true").strip().lower() == "true"

        vol_by_symbol = {}
        for row in daily:
            sym = row.get("symbol") or row.get("ticker") or row.get("t")
            if not sym:
                continue
            v = row.get("v") if "v" in row else row.get("volume")
            try:
                vol_by_symbol[sym] = int(v or 0)
            except Exception:
                vol_by_symbol[sym] = 0

        deriv_pat = re.compile(r"([\.\- ]W(S|T)?$|[\.\- ]WS$|[\.\- ]WT$|[\.\- ]W$|[\.\- ]U(N)?$|RIGHTS?$)", re.IGNORECASE)

        def _is_derivative(symbol: str) -> bool:
            if not exclude_deriv:
                return False
            return bool(deriv_pat.search((symbol or "").upper()))

        def _lookup_exchange(symbol: str) -> str:
            cur.execute("SELECT exchange FROM discovery_hits WHERE ticker=? AND event_date=? LIMIT 1", (symbol, date_iso))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
            try:
                cur.execute("SELECT exchange FROM symbol_exchange WHERE symbol=? ORDER BY as_of DESC LIMIT 1", (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
            except sqlite3.OperationalError:
                pass
            return ""

        c_deriv = 0
        c_exchange = 0
        c_unknown_exch = 0
        c_minvol = 0
        filtered = []
        for sym, gain_ratio, hi, pv in top_gainers:
            if _is_derivative(sym):
                c_deriv += 1
                continue
            ex = _lookup_exchange(sym)
            if not ex:
                c_unknown_exch += 1
                continue
            if ex not in allowed_exchanges:
                c_exchange += 1
                continue
            if vol_by_symbol.get(sym, 0) < min_vol:
                c_minvol += 1
                continue
            filtered.append((sym, gain_ratio, hi, pv))

        filtered_out = len(top_gainers) - len(filtered)
        top_gainers = filtered

        print(f"[MISS-AUDIT] Filtered audit list: kept={len(top_gainers)} filtered_out={filtered_out} (exch in {sorted(list(allowed_exchanges))}, min_vol={min_vol}, exclude_deriv={exclude_deriv})")

        filter_summary = {
            "date": date_iso,
            "kept": len(top_gainers),
            "filtered_total": filtered_out,
            "filtered_derivative": c_deriv,
            "filtered_exchange_not_allowed": c_exchange,
            "filtered_unknown_exchange": c_unknown_exch,
            "filtered_min_volume": c_minvol,
            "allowed_exchanges": sorted(list(allowed_exchanges)),
            "min_volume": min_vol,
            "exclude_derivatives": exclude_deriv
        }

        # Get existing discoveries for this date
        # Get existing discoveries for this date
        cur.execute("""
            SELECT h.ticker, GROUP_CONCAT(r.trigger_rule) as rules
            FROM discovery_hits h
            LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
            WHERE h.event_date = ?
            GROUP BY h.ticker
        """, (date_iso,))
        existing_discoveries = {row[0]: (row[1] or "").split(",") for row in cur.fetchall()}

        # Re-audit R1 and R2 for top gainers
        theta_env = os.getenv("MISS_AUDIT_THETA", "true").strip().lower()
        theta_enabled = theta_env not in ("false", "0", "no")
        theta = ThetaDataClient() if theta_enabled else None
        theta_ok = theta.ok() if theta_enabled and theta is not None else False
        if not theta_enabled:
            print("[MISS-AUDIT] Theta checks disabled via MISS_AUDIT_THETA=false")
        elif not theta_ok:
            print("[MISS-AUDIT] Theta unavailable during miss audit; skipping R1 checks")
        misses_found = 0
        audit_results = []

        for symbol, gain_ratio, high, prev_close in top_gainers:
            symbol_misses = []

            # Check R2 (open gap) miss
            for row in daily:
                if row["symbol"] == symbol:
                    r2_value = r2_open_gap(prev_close, row["open"], 50.0)
                    if r2_value is not None:
                        existing_rules = existing_discoveries.get(symbol, [])
                        if "OPEN_GAP_50" not in existing_rules:
                            symbol_misses.append(("R2", "OPEN_GAP_50", r2_value))
                            misses_found += 1
                    break

            # Check R1 (premarket) miss if Theta available
            if theta_ok:
                premarket_high = theta.get_premarket_high(symbol, date_iso)
                if premarket_high:
                    r1_value = r1_pm(prev_close, premarket_high, 50.0)
                    if r1_value is not None:
                        existing_rules = existing_discoveries.get(symbol, [])
                        if "PM_GAP_50" not in existing_rules:
                            symbol_misses.append(("R1", "PM_GAP_50", r1_value))
                            misses_found += 1

            # Record audit results
            for audit_type, rule_code, rule_value in symbol_misses:
                audit_results.append((date_iso, symbol, audit_type, True, rule_code, rule_value))
                print(f"[MISS-AUDIT] Found missed {rule_code}: {symbol} = {rule_value:.1f}%")

            # Record clean audits (no misses)
            if not symbol_misses:
                audit_results.append((date_iso, symbol, "CLEAN", False, None, None))

        # Store audit results
        if audit_results:
            cur.executemany("""
                INSERT OR REPLACE INTO miss_audit
                (date, symbol, audit_type, missed_initially, rule_triggered, rule_value)
                VALUES (?, ?, ?, ?, ?, ?)
            """, audit_results)
            conn.commit()

        # Mark day status and emit filter diagnostics
        retry_needed = misses_found > 0
        day_status = "RETRY_NEEDED" if retry_needed else "COMPLETE"

        try:
            os.makedirs(os.path.join('project_state', 'artifacts'), exist_ok=True)
            out_path = os.path.join('project_state', 'artifacts', f"miss_audit_filters_{date_iso}.json")
            with open(out_path, 'w', encoding='ascii', errors='replace') as f:
                json.dump(filter_summary, f, indent=2)
            print(f"[MISS-AUDIT] Wrote filter diagnostics {out_path}")
        except Exception as exc:
            print(f"[MISS-AUDIT] Warning: could not write filter diagnostics: {exc}")

        return {
            'status': 'audit_complete',
            'date': date_iso,
            'top_gainers_checked': len(top_gainers),
            'misses_found': misses_found,
            'day_status': day_status,
            'retry_needed': retry_needed
        }

def generate_provider_overlap_report(db_path: str, date_iso: str, output_path: str) -> str:
    """
    Generate denormalized CSV per day with provider overlap analysis.
    Columns: [symbol,date,hit,r1_hit,r2_hit,r3_hit,r4_hit,theta_used,polygon_used,split_window_flag]
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get all symbols that had any activity for this date
        cur.execute("""
            SELECT DISTINCT symbol FROM daily_raw WHERE date = ?
            UNION
            SELECT DISTINCT ticker FROM discovery_hits WHERE event_date = ?
            ORDER BY symbol
        """, (date_iso, date_iso))
        symbols = [row[0] for row in cur.fetchall()]

        # Get discovery data
        cur.execute("""
            SELECT h.ticker, h.hit_id,
                   GROUP_CONCAT(CASE WHEN r.trigger_rule = 'PM_GAP_50' THEN r.rule_value END) as r1_value,
                   GROUP_CONCAT(CASE WHEN r.trigger_rule = 'OPEN_GAP_50' THEN r.rule_value END) as r2_value,
                   GROUP_CONCAT(CASE WHEN r.trigger_rule = 'INTRADAY_PUSH_50' THEN r.rule_value END) as r3_value,
                   GROUP_CONCAT(CASE WHEN r.trigger_rule = 'SURGE_7D_300' THEN r.rule_value END) as r4_value
            FROM discovery_hits h
            LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
            WHERE h.event_date = ?
            GROUP BY h.ticker, h.hit_id
        """, (date_iso,))
        discovery_data = {row[0]: row[1:] for row in cur.fetchall()}

        # Generate CSV report
        csv_file = f"{output_path}/provider_overlap_{date_iso}.csv"
        with open(csv_file, 'w', newline='', encoding='ascii') as f:
            writer = csv.writer(f)
            writer.writerow([
                'symbol', 'date', 'hit', 'r1_hit', 'r2_hit', 'r3_hit', 'r4_hit',
                'theta_used', 'polygon_used', 'split_window_flag'
            ])

            for symbol in symbols:
                discovery = discovery_data.get(symbol)
                if discovery:
                    hit_id, r1_val, r2_val, r3_val, r4_val = discovery
                    hit = 1
                    r1_hit = 1 if r1_val else 0
                    r2_hit = 1 if r2_val else 0
                    r3_hit = 1 if r3_val else 0
                    r4_hit = 1 if r4_val else 0
                    theta_used = 1 if r1_hit else 0  # R1 requires Theta
                    polygon_used = 1  # All symbols use Polygon for daily data
                else:
                    hit = 0
                    r1_hit = r2_hit = r3_hit = r4_hit = 0
                    theta_used = 0
                    polygon_used = 1  # Still used for daily data

                # Check for split window flag (simplified - could be enhanced)
                split_window_flag = 0  # Would need split analysis integration

                writer.writerow([
                    symbol, date_iso, hit, r1_hit, r2_hit, r3_hit, r4_hit,
                    theta_used, polygon_used, split_window_flag
                ])

        print(f"[OVERLAP-REPORT] Generated {csv_file} with {len(symbols)} symbols")
        return csv_file

def generate_day_completeness_csv(db_path: str, date_iso: str, output_path: str) -> str:
    """Generate day_completeness.csv with zero 'missed_after_audit' requirement"""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get completeness metrics
        cur.execute("""
            SELECT COUNT(*) as missed_count FROM miss_audit
            WHERE date = ? AND missed_initially = 1
        """, (date_iso,))
        missed_after_audit = cur.fetchone()[0]

        # Get other metrics from completeness log
        cur.execute("""
            SELECT * FROM day_completeness WHERE date = ?
        """, (date_iso,))
        completeness_row = cur.fetchone()

        csv_file = f"{output_path}/day_completeness.csv"
        with open(csv_file, 'w', newline='', encoding='ascii') as f:
            writer = csv.writer(f)
            writer.writerow([
                'date', 'total_symbols', 'daily_coverage', 'candidates_screened',
                'r1_checked', 'r1_hits', 'audit_sample', 'audit_hits',
                'missed_after_audit', 'status'
            ])

            if completeness_row:
                writer.writerow([
                    date_iso,
                    completeness_row[2],  # total_symbols
                    completeness_row[3],  # daily_coverage
                    completeness_row[4],  # candidates_screened
                    completeness_row[5],  # r1_checked
                    completeness_row[6],  # r1_hits
                    completeness_row[7],  # audit_sample
                    completeness_row[8],  # audit_hits
                    missed_after_audit,
                    "PASS" if missed_after_audit == 0 else "FAIL"
                ])
            else:
                writer.writerow([
                    date_iso, 0, 0, 0, 0, 0, 0, 0, missed_after_audit,
                    "PASS" if missed_after_audit == 0 else "FAIL"
                ])

        print(f"[COMPLETENESS] Generated {csv_file} - Status: {'PASS' if missed_after_audit == 0 else 'FAIL'}")
        return csv_file


