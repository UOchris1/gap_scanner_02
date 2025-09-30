# -*- coding: ascii -*-
# Single "scan_day" with Pass-1 (Polygon grouped-daily), R2/R3, R1 (Theta premarket), R4 (7-day surge).
# Keeps requests bounded for Theta Standard plan (4 in-flight).

import datetime as dt
import random
import sqlite3
import time
import os
import faulthandler
import threading
import concurrent.futures as cf
from typing import Dict, List, Optional, Tuple

from src.core.rules import r1_pm, r2_open_gap, r3_push, r4_surge7
from src.core.db import ensure_schema_and_indexes, store_daily_raw, fetch_prev_close_map, upsert_hit, insert_rules, log_completeness
from src.core.universe import populate_universe_for_date, get_universe_for_date, get_universe_stats
from src.core.completeness import post_scan_miss_audit, generate_provider_overlap_report, generate_day_completeness_csv
from src.providers.polygon_provider import grouped_daily, prev_close as poly_prev_close, prev_close_bulk_map as poly_prev_close_bulk, splits as poly_splits
from src.providers.theta_provider import ThetaDataClient

def _stage_log(day_iso, label):
    path = os.path.join("project_state", "artifacts", f"scan_{day_iso}.log")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {label}\n"
    # both file and console, flush immediately
    with open(path, "a", encoding="ascii", errors="replace") as f:
        f.write(line)
    print(f"[SCAN] {label}", flush=True)

def _start_hang_watchdog(day_iso, seconds=120):
    # dump all thread tracebacks after N seconds; works on Windows by calling dump_traceback from a timer thread
    out = os.path.join("project_state", "artifacts", f"hang_trace_{day_iso}.txt")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fh = open(out, "w", encoding="ascii", errors="replace")
    def _dump():
        time.sleep(seconds)
        try:
            faulthandler.dump_traceback(file=fh)
            fh.flush()
        except Exception:
            pass
    threading.Thread(target=_dump, daemon=True).start()

# Tunables (keep it simple; move to YAML later if you prefer)
R2_TH = 50.0
R3_TH = 50.0
R1_TH = 50.0
R4_TH = 300.0
R1_WINDOW_ET = ("04:00:00", "09:29:59")  # enforced inside provider

HEAVY_RUNNER_DV = 10_000_000.0  # override gate
HEAVY_RUNNER_PUSH_MIN = 50.0

# Exchange and volume gates
ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}
MIN_DISCOVERY_VOL = int(os.getenv("DISCOVERY_MIN_VOL", "100000"))

# Derivative exclusion knobs
EXCLUDE_DERIVATIVES = os.getenv("EXCLUDE_DERIVATIVES", "true").strip().lower() == "true"
ALLOWED_SECURITY_TYPES = set(
    t.strip().upper() for t in os.getenv(
        "ALLOW_SECURITY_TYPES",
        "CS,ADRC,ADRP,ADRR,ADRW,GDR"
    ).split(",") if t.strip()
)

def _compute_prev_close(conn: sqlite3.Connection, date_iso: str, daily_rows: List[Dict]) -> Tuple[Dict[str, float], List[str]]:
    prev_date = (dt.date.fromisoformat(date_iso) - dt.timedelta(days=1)).isoformat()
    m = fetch_prev_close_map(conn, prev_date)
    try:
        bulk_map = poly_prev_close_bulk(prev_date)
        for sym, close in (bulk_map or {}).items():
            if sym not in m:
                m[sym] = close
    except Exception:
        pass
    missing = [r.get("symbol") for r in daily_rows if r.get("symbol") not in m]
    for sym in (missing[:25] if missing else []):
        try:
            pc = poly_prev_close(sym, prev_date)
        except Exception:
            pc = None
        if pc is not None:
            m[sym] = pc
    return m, missing

def _reverse_split_gate(symbol: str, date_iso: str, dv: float, push_pct: float) -> Tuple[int, str]:
    """Enhanced reverse split gating with 1 trading-day window per plan3_suggestions.txt"""
    # Get 1 trading day window around event date
    event_date = dt.date.fromisoformat(date_iso)
    start_check = event_date - dt.timedelta(days=3)  # Buffer for weekends
    end_check = event_date + dt.timedelta(days=3)

    events = poly_splits(symbol) or []

    # Find reverse splits within 1 trading day window
    relevant_splits = []
    for e in events:
        try:
            sf = float(e.get("split_from") or 0)
            st = float(e.get("split_to") or 0)
            exec_date = e.get("execution_date")

            if sf > st and exec_date:  # Reverse split
                exec_dt = dt.date.fromisoformat(exec_date)
                # Check if within reasonable window (allowing for weekends)
                days_diff = abs((exec_dt - event_date).days)
                if days_diff <= 3:  # Within 3 calendar days (1 trading day)
                    relevant_splits.append((e, days_diff))
        except Exception:
            continue

    if not relevant_splits:
        return 0, ""

    # Get closest reverse split to event date
    closest_split, days_away = min(relevant_splits, key=lambda x: x[1])

    # Heavy runner override: $10M+ dollar volume AND 50%+ intraday push
    if dv >= HEAVY_RUNNER_DV and push_pct >= HEAVY_RUNNER_PUSH_MIN:
        return 1, f"heavy_runner_override_split_{days_away}d"

    return 1, f"split_artifact_{days_away}d"

def scan_day(date_iso: str, db_path: str) -> Dict:
    # watchdog + first breadcrumb
    _start_hang_watchdog(date_iso, seconds=120)
    _stage_log(date_iso, "START scan_day")

    t0 = time.time()
    ensure_schema_and_indexes(db_path)

    # 0) Pin the deterministic universe
    _stage_log(date_iso, "UNIVERSE:begin")
    total_universe = populate_universe_for_date(db_path, date_iso)
    symbols = get_universe_for_date(db_path, date_iso)
    _stage_log(date_iso, "UNIVERSE:done")
    print(f"[UNIVERSE] Loaded {total_universe} symbols for deterministic scanning")

    # 1) Pass-1 market sweep (bounded, no infinite loops)
    _stage_log(date_iso, "POLYGON:grouped_daily:begin")
    try:
        rows = grouped_daily(date_iso, adjusted=False, include_otc=False, timeout_sec=45, max_retries=3)
    except Exception as e:
        return {"status": "no_grouped_daily", "error": str(e), "discoveries": 0}

    # 1a) Convert and persist once; do not re-call grouped_daily in a loop
    daily = []
    for r in rows:
        # Already filtered by grouped_daily, but ensure no OTC
        daily.append({
            "symbol": r["symbol"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
        })

    _stage_log(date_iso, f"POLYGON:grouped_daily:done count={len(daily)}")

    _stage_log(date_iso, "DB:store_daily_raw:begin")
    with sqlite3.connect(db_path) as conn:
        # WAL + NORMAL help, but avoid holding locks across long loops
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        store_daily_raw(conn, date_iso, daily)
    _stage_log(date_iso, "DB:store_daily_raw:done")

    # Log universe vs daily coverage for completeness tracking
    universe_symbols = set(get_universe_for_date(db_path, date_iso))
    daily_symbols = set(row["symbol"] for row in daily)
    coverage_pct = len(daily_symbols & universe_symbols) / len(universe_symbols) * 100 if universe_symbols else 0
    print(f"[COVERAGE] Daily data covers {len(daily_symbols & universe_symbols)}/{len(universe_symbols)} symbols ({coverage_pct:.1f}%)")

    # Prev close map from DB (prev day) - use scoped connection
    with sqlite3.connect(db_path) as conn:
        prev_map, missing_prev = _compute_prev_close(conn, date_iso, daily)

    # R2 and R3 candidates
    _stage_log(date_iso, "R2R3:compute:begin")
    r2_flags: Dict[str, float] = {}
    r3_flags: Dict[str, float] = {}
    for row in daily:
        sym, o, h = row["symbol"], row["open"], row["high"]
        pv = prev_map.get(sym)
        r2v = r2_open_gap(pv, o, R2_TH)
        if r2v is not None:
            r2_flags[sym] = r2v
        r3v = r3_push(o, h, R3_TH)
        if r3v is not None:
            r3_flags[sym] = r3v
    _stage_log(date_iso, f"R2R3:compute:done r2={len(r2_flags)} r3={len(r3_flags)}")

    # ---- R1 Premarket (Theta) ----
    # R1 premarket: cap the work; standard tier has 2 threads
    _stage_log(date_iso, "R1:theta:begin")
    theta = ThetaDataClient()
    r1_flags: Dict[str, float] = {}
    r1_meta: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    r1_checked = 0
    miss_audit_sample = 0
    miss_audit_hits = 0
    audit_failed = False

    if theta.ok():
        # Build candidate list quickly with bounded audit
        candidate = set(list(r2_flags.keys()) + list(r3_flags.keys()))
        for row in daily:
            pv = prev_map.get(row["symbol"])
            if pv and pv > 0 and (row["high"] / pv) >= 1.2:
                candidate.add(row["symbol"])

        # Audit sample - keep bounded
        universe_syms = [r["symbol"] for r in daily]
        remainder = [s for s in universe_syms if s not in candidate]
        random.seed(12345)
        sample = set(random.sample(remainder, k=min(50, len(remainder)))) if remainder else set()  # reduced from 200 to 50
        to_check = sorted(candidate | sample)

        # Absolute cap to prevent runaway on heavy days
        to_check = to_check[:750]
        miss_audit_sample = len(sample)

        # Hard time cap at 8 minutes for Theta premarket phase
        start_pm = time.time()
        # Match worker count to semaphore limit to prevent deadlock
        # Default to 4 (THETA_V3_MAX_OUTSTANDING default is 2, but allow some buffer)
        worker_env = os.getenv("R1_THREAD_WORKERS", "4")
        try:
            workers = int(worker_env)
        except Exception:
            workers = 4
        if workers < 1:
            workers = 1
        # Cap at 8 to prevent overwhelming the semaphore (default limit is 2)
        workers = min(workers, 8)

        def _fetch_theta(symbol: str):
            pmh, pm_src, pm_ven = (None, None, None)
            try:
                pmh, pm_src, pm_ven = theta.get_premarket_high_with_meta(symbol, date_iso)
            except Exception:
                pmh = theta.get_premarket_high(symbol, date_iso)
            return pmh, pm_src, pm_ven

        futures = {}
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for sym in to_check:
                if time.time() - start_pm > 8 * 60:
                    break
                try:
                    futures[ex.submit(_fetch_theta, sym)] = sym
                except Exception:
                    continue
            for fut in cf.as_completed(futures):
                sym = futures[fut]
                pmh, pm_src, pm_ven = (None, None, None)
                try:
                    pmh, pm_src, pm_ven = fut.result()
                except Exception:
                    pass
                r1_checked += 1
                if pmh is None:
                    continue
                if pm_src is None:
                    pm_src = "legacy"
                if pm_ven is None:
                    pm_ven = ""
                pv = prev_map.get(sym)
                r1v = r1_pm(pv, pmh, R1_TH)
                if r1v is not None:
                    r1_flags[sym] = r1v
                    r1_meta[sym] = (pm_src, pm_ven)
                    if sym in sample:
                        miss_audit_hits += 1
                        audit_failed = True
    else:
        # Theta down: skip R1 gracefully (logged by caller)
        pass

    _stage_log(date_iso, f"R1:theta:done hits={len(r1_flags)}")
    try:
        theta.flush_pm_diag(date_iso)
    except Exception:
        pass

    # ---- R4 Seven-day surge with enhanced reverse split gating ----
    # Compute R4 for all symbols with interesting action (R1|R2|R3) plus high performers
    interesting = set(r1_flags) | set(r2_flags) | set(r3_flags)

    # Add symbols with significant daily moves to R4 candidate set
    for row in daily:
        sym = row["symbol"]
        if sym not in interesting:
            pv = prev_map.get(sym)
            if pv and pv > 0:
                daily_change = ((row["high"] / pv) - 1.0) * 100.0
                if daily_change >= 20.0:  # 20%+ daily move
                    interesting.add(sym)

    r4_flags: Dict[str, float] = {}
    reverse_split_context: Dict[str, Dict] = {}

    def _get_last_7_enhanced(symbol: str, end_date: str) -> Optional[Tuple[float, float]]:
        """Enhanced 7-day lookback with multiple data sources per plan2.txt"""
        # Try database first (fastest) with scoped connection
        with sqlite3.connect(db_path) as db_conn:
            cur = db_conn.cursor()
            cur.execute(
                "SELECT low, high FROM daily_raw WHERE symbol=? AND date<=? ORDER BY date DESC LIMIT 7",
                (symbol, end_date),
            )
            rows = cur.fetchall()

        # Filter out rows with missing values and take the latest 7 valid entries
        valid: List[Tuple[float, float]] = []
        for low, high in rows:
            try:
                if low is None or high is None:
                    continue
                valid.append((float(low), float(high)))
            except Exception:
                continue

        if len(valid) >= 7:
            use = valid[:7]
            lows = [lo for lo, _ in use]
            highs = [hi for _, hi in use]
            return min(lows), max(highs)

        # Fallback to Polygon grouped-daily backbone
        try:
            from src.providers.polygon_provider import get_daily_ohlc_range
            end_d = dt.date.fromisoformat(end_date)
            start_d = end_d - dt.timedelta(days=14)  # Buffer for weekends/holidays

            polygon_data = get_daily_ohlc_range(symbol, start_d.isoformat(), end_d.isoformat())
            if len(polygon_data) >= 7:
                # Take last 7 trading days
                sorted_data = sorted(polygon_data, key=lambda x: x["date"])[-7:]
                lows = [bar["low"] for bar in sorted_data]
                highs = [bar["high"] for bar in sorted_data]
                return min(lows), max(highs)

        except Exception:
            pass

        # Final fallback to ThetaData for missing data
        try:
            end_d = dt.date.fromisoformat(end_date)
            start_d = end_d - dt.timedelta(days=14)

            theta_data = theta.get_daily_ohlc_range(symbol, start_d.isoformat(), end_d.isoformat())
            if len(theta_data) >= 7:
                sorted_data = sorted(theta_data, key=lambda x: x["date"])[-7:]
                lows = [bar["low"] for bar in sorted_data]
                highs = [bar["high"] for bar in sorted_data]
                return min(lows), max(highs)

        except Exception:
            pass

        return None

    def _analyze_reverse_split_context(symbol: str, event_date: str) -> Dict:
        """Analyze reverse split context around event date per plan2.txt"""
        try:
            from src.providers.polygon_provider import splits
            # Check splits within 3 days of event
            event_dt = dt.date.fromisoformat(event_date)
            start_check = (event_dt - dt.timedelta(days=3)).isoformat()
            end_check = (event_dt + dt.timedelta(days=3)).isoformat()

            split_events = splits(symbol, start_check, end_check)

            reverse_splits = [s for s in split_events if s.get("is_reverse_split", False)]

            if reverse_splits:
                # Get most recent reverse split
                latest_split = max(reverse_splits, key=lambda x: x.get("execution_date", ""))
                return {
                    "has_reverse_split": True,
                    "execution_date": latest_split.get("execution_date"),
                    "split_ratio": latest_split.get("split_ratio"),
                    "split_from": latest_split.get("split_from"),
                    "split_to": latest_split.get("split_to"),
                    "days_from_event": abs((dt.date.fromisoformat(latest_split.get("execution_date", event_date)) - event_dt).days)
                }

            return {"has_reverse_split": False}

        except Exception:
            return {"has_reverse_split": False, "error": "split_analysis_failed"}

    # Process R4 candidates
    for sym in sorted(interesting):
        lohi = _get_last_7_enhanced(sym, date_iso)
        if not lohi:
            continue

        lo7, hi7 = lohi
        r4v = r4_surge7(lo7, hi7, R4_TH)

        if r4v is not None:
            # Analyze reverse split context for gating
            split_context = _analyze_reverse_split_context(sym, date_iso)

            # Derive rs fields for each symbol
            rs_exec_date = None
            rs_days_after = None
            try:
                if split_context.get("has_reverse_split"):
                    exec_date = split_context.get("execution_date")
                    if exec_date:
                        rs_exec_date = exec_date
                        # signed: event minus exec_date (days)
                        ev = dt.date.fromisoformat(date_iso)
                        ex = dt.date.fromisoformat(exec_date)
                        rs_days_after = (ev - ex).days
            except Exception:
                rs_exec_date = None
                rs_days_after = None

            # Keep track for persistence
            reverse_split_context[sym] = {
                **split_context,
                "rs_exec_date": rs_exec_date,
                "rs_days_after": rs_days_after
            }

            # Apply reverse split gating with heavy runner override
            if split_context.get("has_reverse_split", False):
                # Check heavy runner override criteria
                for row in daily:
                    if row["symbol"] == sym:
                        dollar_volume = (row.get("vwap") or row["close"] or 0.0) * (row["volume"] or 0)
                        intraday_push = ((row["high"] / row["open"] - 1.0) * 100.0) if (row["open"] and row["open"] > 0) else 0

                        if dollar_volume >= HEAVY_RUNNER_DV and intraday_push >= HEAVY_RUNNER_PUSH_MIN:
                            # Heavy runner override - keep the R4 hit
                            r4_flags[sym] = r4v
                            print(f"[R4-HEAVY-RUNNER] {sym}: ${dollar_volume:,.0f} volume, {intraday_push:.1f}% push, R4={r4v:.1f}%")
                        else:
                            # Suppress due to reverse split
                            print(f"[R4-SPLIT-GATE] {sym}: R4 suppressed due to reverse split on {split_context.get('execution_date')}")
                        break
            else:
                # No reverse split - include R4 hit
                r4_flags[sym] = r4v

    # ---- Persist discoveries ----
    hits = 0
    discoveries = []  # Collect discoveries first
    for row in daily:
        sym, o, h, v = row["symbol"], row["open"], row["high"], row["volume"]
        r1 = r1_flags.get(sym)
        r2 = r2_flags.get(sym)
        r3 = r3_flags.get(sym)
        r4 = r4_flags.get(sym)
        if not any([r1, r2, r3, r4]):
            continue

        push_pct = ((h / o - 1.0) * 100.0) if (o and o > 0) else None
        # crude dollar volume for gate
        dv = (row["close"] or 0.0) * float(v or 0)
        near_rs, _rs_reason = _reverse_split_gate(sym, date_iso, dv, push_pct or 0.0)

        discoveries.append((sym, v, push_pct, near_rs, r1, r2, r3, r4))

    # Persist all discoveries in one scoped connection
    with sqlite3.connect(db_path) as conn:
        # Lazy imports to avoid circulars at module import time
        from src.core.database_operations import get_cached_exchange, upsert_symbol_exchange, get_cached_meta
        from src.providers.polygon_provider import get_exchange as poly_get_exchange, get_symbol_meta
        # Clear existing discoveries for this date to avoid stale rows failing new gates
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM discovery_hit_rules WHERE hit_id IN (SELECT hit_id FROM discovery_hits WHERE event_date=?)", (date_iso,))
            cur.execute("DELETE FROM discovery_hits WHERE event_date=?", (date_iso,))
            conn.commit()
        except Exception:
            pass
        for sym, v, push_pct, near_rs, r1, r2, r3, r4 in discoveries:
            # NEW: pull the split context for this symbol (if any)
            sc = reverse_split_context.get(sym, {})

            # Exchange + security type lookup with micro-cache
            meta = get_cached_meta(conn, sym)
            ex = (meta.get("exchange") if meta else None)
            sec_type = (meta.get("security_type") if meta else None)

            # If missing required info, fetch details as-of date
            if not ex or (EXCLUDE_DERIVATIVES and not sec_type):
                info = get_symbol_meta(sym, date_iso)
                if info:
                    ex = info.get("exchange") or ex
                    sec_type = info.get("security_type") or sec_type
                    upsert_symbol_exchange(
                        conn, sym,
                        info.get("primary_exchange"),
                        ex,
                        security_type=sec_type,
                        ticker_suffix=info.get("ticker_suffix")
                    )

            # Only keep requested exchanges; eliminate OTC & others implicitly
            if ex not in ALLOWED_EXCHANGES:
                continue

            # Derivative/type gate
            if EXCLUDE_DERIVATIVES:
                if sec_type and sec_type.upper() not in ALLOWED_SECURITY_TYPES:
                    continue
                if not sec_type:
                    continue

            # Enforce minimal volume gate
            if int(v or 0) < MIN_DISCOVERY_VOL:
                continue

            pm_src, pm_ven = r1_meta.get(sym, (None, None))
            hit_id = upsert_hit(
                conn,
                date_iso,
                sym,
                v,
                push_pct,
                near_rs,
                sc.get("rs_exec_date"),
                sc.get("rs_days_after"),
                ex,
                pm_src,
                pm_ven,
            )
            rules = []
            if r1 is not None:
                rules.append((hit_id, "PM_GAP_50", r1))
            if r2 is not None:
                rules.append((hit_id, "OPEN_GAP_50", r2))
            if r3 is not None:
                rules.append((hit_id, "INTRADAY_PUSH_50", r3))
            if r4 is not None:
                rules.append((hit_id, "SURGE_7D_300", r4))
            insert_rules(conn, rules)
            hits += len(rules)

        # ---- Fundamentals Enrichment ----
        _stage_log(date_iso, "FUNDAMENTALS:enrich:begin")
        from src.core.database_operations import upsert_hit_fundamentals, ensure_discovery_hit_split_context, upsert_hit_split_context
        from src.providers.fundamentals_provider import get_fundamentals_for_hit

        # Ensure split context schema exists
        ensure_discovery_hit_split_context(conn)

        for sym, v, push_pct, near_rs, r1, r2, r3, r4 in discoveries:
            # Get hit_id from discovery_hits table (already persisted above)
            cursor = conn.cursor()
            cursor.execute("SELECT hit_id FROM discovery_hits WHERE ticker = ? AND event_date = ?", (sym, date_iso))
            result = cursor.fetchone()
            if result:
                hit_id = result[0]

                # Get fundamentals data
                fundamentals = get_fundamentals_for_hit(sym, date_iso)

                # Calculate dollar volume using VWAP (fallback to close)
                dollar_volume = None
                for row in daily:
                    if row["symbol"] == sym:
                        vwap = row.get("vwap") or row.get("close")
                        if vwap and v:
                            dollar_volume = float(vwap) * float(v)
                        break

                # Upsert fundamentals data
                upsert_hit_fundamentals(
                    conn,
                    hit_id,
                    shares_outstanding=fundamentals.get("shares_outstanding"),
                    market_cap=fundamentals.get("market_cap"),
                    float_shares=fundamentals.get("float_shares"),
                    dollar_volume=dollar_volume,
                    data_source=fundamentals.get("data_source", "unknown")
                )

                # ---- Split Context Tracking ----
                # Get split context from reverse_split_context dict (computed earlier for R4 candidates)
                split_context = reverse_split_context.get(sym, {})
                if split_context.get("has_reverse_split", False):
                    # Extract split context data
                    exec_date = split_context.get("execution_date")
                    split_from = split_context.get("split_from")
                    split_to = split_context.get("split_to")
                    days_from_event = split_context.get("days_from_event")
                    is_reverse = 1 if split_from and split_to and float(split_from) > float(split_to) else 0

                    # Persist split context
                    upsert_hit_split_context(
                        conn,
                        hit_id,
                        exec_date,
                        split_from,
                        split_to,
                        days_from_event,
                        is_reverse
                    )
                else:
                    # For non-R4 candidates, still check for splits using Polygon 1-trading-day window
                    try:
                        event_dt = dt.date.fromisoformat(date_iso)
                        start_check = (event_dt - dt.timedelta(days=3)).isoformat()
                        end_check = (event_dt + dt.timedelta(days=3)).isoformat()

                        split_events = poly_splits(sym) or []

                        # Look for reverse splits within window
                        for split_event in split_events:
                            try:
                                sf = float(split_event.get("split_from", 0))
                                st = float(split_event.get("split_to", 0))
                                exec_date = split_event.get("execution_date")

                                if sf > st and exec_date:  # Reverse split
                                    exec_dt = dt.date.fromisoformat(exec_date)
                                    days_diff = (event_dt - exec_dt).days

                                    # Check if within 1 trading day (3 calendar days buffer)
                                    if abs(days_diff) <= 3:
                                        # Update both the separate table AND main discovery_hits columns
                                        upsert_hit_split_context(
                                            conn,
                                            hit_id,
                                            exec_date,
                                            sf,
                                            st,
                                            days_diff,
                                            1
                                        )

                                        # ALSO update main discovery_hits table for CSV export
                                        cursor.execute("""
                                            UPDATE discovery_hits
                                            SET rs_exec_date = ?, rs_days_after = ?
                                            WHERE hit_id = ?
                                        """, (exec_date, days_diff, hit_id))

                                        break  # Only record the first/closest reverse split
                            except Exception:
                                continue
                    except Exception:
                        # If split lookup fails, continue without split context
                        pass

        # ---- Sync Split Context to Main Table ----
        # Ensure any split context in separate table is copied to main discovery_hits columns
        _stage_log(date_iso, "SYNC:split_context:begin")
        cur = conn.cursor()
        cur.execute("""
            UPDATE discovery_hits
            SET rs_exec_date = (
                SELECT sc.rs_exec_date FROM discovery_hit_split_context sc
                WHERE sc.hit_id = discovery_hits.hit_id AND sc.rs_exec_date IS NOT NULL
            ),
            rs_days_after = (
                SELECT sc.rs_days_from_event FROM discovery_hit_split_context sc
                WHERE sc.hit_id = discovery_hits.hit_id AND sc.rs_days_from_event IS NOT NULL
            )
            WHERE discovery_hits.event_date = ?
            AND (discovery_hits.rs_exec_date IS NULL OR discovery_hits.rs_exec_date = '')
            AND EXISTS (
                SELECT 1 FROM discovery_hit_split_context sc
                WHERE sc.hit_id = discovery_hits.hit_id
            )
        """, (date_iso,))
        synced_rows = cur.rowcount
        conn.commit()
        _stage_log(date_iso, f"SYNC:split_context:done synced={synced_rows}")

        _stage_log(date_iso, f"FUNDAMENTALS:enrich:done count={len(discoveries)}")
        # Log completeness metrics with universe tracking
        universe_stats = get_universe_stats(db_path, date_iso)
        log_completeness(conn, date_iso, len(daily), universe_stats["total_symbols"],
                         len(set(r2_flags) | set(r3_flags)), r1_checked, len(r1_flags),
                         miss_audit_sample, miss_audit_hits, audit_failed)

    # Compute and store day_completeness metrics
    from src.core.database_operations import recompute_and_upsert_day_completeness
    provider_status = {
        "theta_v3": theta.ok(),
        "theta_v1": theta.ok(),  # Using same status for both since ThetaDataClient abstracts this
        "polygon_ok": len(daily) > 0,
        "fmp_ok": False  # Not used in current implementation
    }
    recompute_and_upsert_day_completeness(db_path, date_iso, provider_status)

    # Post-scan completeness audit per plan2.txt Phase D
    _stage_log(date_iso, "AUDIT:post_scan:begin")
    print(f"[COMPLETENESS] Running miss audit for {date_iso}")
    audit_result = post_scan_miss_audit(db_path, date_iso, top_n=150)
    _stage_log(date_iso, "AUDIT:post_scan:done")

    # Generate completeness reports
    reports_dir = os.path.join(os.path.dirname(db_path), "reports")
    os.makedirs(reports_dir, exist_ok=True)

    overlap_report = generate_provider_overlap_report(db_path, date_iso, reports_dir)
    completeness_report = generate_day_completeness_csv(db_path, date_iso, reports_dir)

    print(f"[REPORTS] Generated: {overlap_report}, {completeness_report}")

    _stage_log(date_iso, "COMPLETE:ok")

    return {
        "status": "ok",
        "discoveries": hits,
        "audit_failed": audit_failed,
        "universe_symbols": universe_stats["total_symbols"],
        "daily_symbols": len(daily),
        "coverage_pct": coverage_pct,
        "miss_audit": audit_result,
        "reports": {
            "overlap": overlap_report,
            "completeness": completeness_report
        }
    }
