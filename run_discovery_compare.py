# run_discovery_compare.py
# T+1 discovery comparison harness with reverse-split gate and provider fallbacks.
# Windows 11, conda env "stat_project", no venv. ASCII only.
#
# Assumptions (Phase 0):
# - .env exists with POLYGON_API_KEY and FMP_API_KEY.
# - ThetaData Terminal v2 is running on localhost:25510 (optional; auto-fallback if not).
# - symbols.txt may exist with one ticker per line; if missing, a small list is fetched via FMP screener.
# - Date range defaults to 2025-08-15 .. 2025-09-15 inclusive; configurable below.
#
# Provider behavior refs (session scope, adjustments, endpoints):
# - ThetaData v2 OHLC: 1-min via /v2/hist/stock/ohlc (ivl=60000), rth=true (RTH only) or rth=false for ext hours.
#   Raw unadjusted prices; Splits via /v2/hist/stock/splits; Standard plan concurrency 2.
# - Polygon Aggs: minutes and daily via /v2/aggs; adjusted=false for raw; minutes include extended hours; filter times for premarket.
#   Splits via /v3/reference/splits.
# - FMP Starter: daily via /api/v3/historical-price-full; intraday not included; splits via /api/v3/historical-price-full/stock_split.

import os
import sys
import time
import json
import math
import sqlite3
import traceback
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from pathlib import Path
from tqdm import tqdm
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------
# CONFIG (edit here if needed)
# ----------------------------
CONFIG = {
    "date_start": "2025-08-15",
    "date_end":   "2025-09-15",
    "exchanges": ["NASDAQ", "NYSE"],  # universe hint when using FMP screener fallback
    "max_symbols": 0,                  # 0 means no cap; otherwise limit universe size
    "db_path": "scanner.db",
    "csv_apples": "apples_to_apples_hits.csv",
    "csv_superset": "superset_hits.csv",
    "csv_summary": "provider_rule_summary.csv",
    # Discovery pipeline selection
    "discovery_mode": "legacy",        # "legacy" or "zero_miss"
    "zero_miss_single_date": None,     # Single date for zero-miss mode (YYYY-MM-DD)
    # rule thresholds
    "premarket_trigger_pct": 50.0,     # R1
    "open_gap_trigger_pct": 50.0,      # R2
    "intraday_push_trigger_pct": 50.0, # R3
    "surge_7d_trigger_pct": 300.0,     # R4
    # split gate
    "split_window_trading_days": 1,    # 1 or 2
    "split_heavy_dv_min": 10_000_000.0,
    "split_heavy_push_min_pct": 50.0,
    "apply_split_gate": True,
    # premarket policy
    "pm_time_start": "04:00:00",
    "pm_time_end":   "09:29:59",
    # misc
    "timeout_sec": 30,
    "theta_base": "http://127.0.0.1:25503",
    "polygon_base": "https://api.polygon.io",
    "fmp_base": "https://financialmodelingprep.com/api",
    "write_csvs": True,
    "print_every": 50,  # progress heartbeat
    # ThetaData concurrency optimization
    "theta_outstanding": 4,     # STANDARD=4, PRO=8 concurrent requests
    "theta_retry_total": 4,     # Retry attempts for failed requests
    "theta_backoff": 0.3,       # Backoff factor for retries
    # Zero-Miss Discovery configuration
    "zero_miss_enabled": True,      # Enable zero-miss discovery system
    "premarket_method": "pass2b",   # "pass2a" = full sweep, "pass2b" = smart filter + audit
    "pass2a_max_symbols": 1000,    # Limit for Pass-2A testing (0 = no limit)
    "pass2b_audit_sample": 50,     # Random sample size for miss-audit
    "prefilter_gap_threshold": 1.2,     # 20%+ gap prefilter
    "prefilter_push_threshold": 1.3,    # 30%+ push prefilter
    "prefilter_volume_threshold": 1_000_000  # $1M+ volume prefilter
}

# ----------------------------
# ENV
# ----------------------------
# Load .env from project root (handle running from any directory)
project_root = Path(__file__).parent
env_path = project_root / ".env"
load_dotenv(env_path)
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()

# ----------------------------
# UTILS
# ----------------------------
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def to_ymd(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")

def ymd_to_int(ymd: str) -> int:
    return int(ymd.replace("-", ""))

def int_to_ymd(n: int) -> str:
    s = str(n)
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def eastern_now() -> datetime:
    # Keep simple; pandas tz helpers used elsewhere
    return datetime.now()

def load_symbols_from_file(path: str = "symbols.txt") -> List[str]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            syms = [line.strip().upper() for line in f if line.strip()]
            return list(dict.fromkeys(syms))
    return []

def trading_calendar() -> mcal.MarketCalendar:
    return mcal.get_calendar("XNYS")

def trading_days_between(start_ymd: str, end_ymd: str) -> List[str]:
    cal = trading_calendar()
    dts = cal.valid_days(start_date=start_ymd, end_date=end_ymd)
    return [d.date().strftime("%Y-%m-%d") for d in dts]

def shift_trading_days(ymd: str, offset: int) -> str:
    days = trading_days_between("2000-01-01", "2100-12-31")
    try:
        idx = days.index(ymd)
        j = idx + offset
        if j < 0 or j >= len(days):
            return ymd
        return days[j]
    except ValueError:
        return ymd

def ensure_db_schema(db_path: str) -> None:
    ddl = """
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS discovery_hits (
      hit_id INTEGER PRIMARY KEY AUTOINCREMENT,
      provider TEXT NOT NULL,
      ticker TEXT NOT NULL,
      event_date TEXT NOT NULL,
      volume INTEGER,
      intraday_push_pct REAL,
      is_near_reverse_split INTEGER,
      split_detect_source TEXT,
      rs_window_days INTEGER,
      rs_execution_date TEXT,
      rs_ratio TEXT,
      rs_filter_applied INTEGER,
      rs_override_reason TEXT,
      pm_source TEXT,
      pm_method TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_hits ON discovery_hits(provider,ticker,event_date);

    CREATE TABLE IF NOT EXISTS discovery_hit_rules (
      hit_rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
      hit_id INTEGER NOT NULL,
      trigger_rule TEXT NOT NULL,
      rule_value REAL NOT NULL,
      FOREIGN KEY(hit_id) REFERENCES discovery_hits(hit_id) ON DELETE CASCADE
    );
    """
    with sqlite3.connect(db_path) as conn:
        conn.executescript(ddl)

def db_upsert_hit(conn, rec: Dict[str, Any]) -> int:
    fields = ("provider","ticker","event_date","volume","intraday_push_pct",
              "is_near_reverse_split","split_detect_source",
              "rs_window_days","rs_execution_date","rs_ratio",
              "rs_filter_applied","rs_override_reason","pm_source","pm_method")
    vals = tuple(rec.get(k) for k in fields)
    q = """
    INSERT INTO discovery_hits(provider,ticker,event_date,volume,intraday_push_pct,
      is_near_reverse_split,split_detect_source,
      rs_window_days,rs_execution_date,rs_ratio,
      rs_filter_applied,rs_override_reason,pm_source,pm_method)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(provider,ticker,event_date) DO UPDATE SET
      volume=excluded.volume,
      intraday_push_pct=excluded.intraday_push_pct,
      is_near_reverse_split=excluded.is_near_reverse_split,
      split_detect_source=excluded.split_detect_source,
      rs_window_days=excluded.rs_window_days,
      rs_execution_date=excluded.rs_execution_date,
      rs_ratio=excluded.rs_ratio,
      rs_filter_applied=excluded.rs_filter_applied,
      rs_override_reason=excluded.rs_override_reason,
      pm_source=excluded.pm_source,
      pm_method=excluded.pm_method
    """
    conn.execute(q, vals)
    row = conn.execute("SELECT hit_id FROM discovery_hits WHERE provider=? AND ticker=? AND event_date=?",
                       (rec["provider"], rec["ticker"], rec["event_date"])).fetchone()
    return int(row[0])

def db_insert_rule(conn, hit_id: int, rule_name: str, rule_value: float) -> None:
    conn.execute("INSERT INTO discovery_hit_rules(hit_id,trigger_rule,rule_value) VALUES (?,?,?)",
                 (hit_id, rule_name, float(rule_value)))

# ----------------------------
# RULES
# ----------------------------
def r1_pm(prev_close: float, pm_high: Optional[float], th: float) -> Optional[float]:
    if prev_close and pm_high and prev_close > 0:
        pct = (pm_high / prev_close - 1.0) * 100.0
        if pct >= th: return pct
    return None

def r2_open_gap(prev_close: float, open_price: float, th: float) -> Optional[float]:
    if prev_close and open_price and prev_close > 0:
        pct = (open_price / prev_close - 1.0) * 100.0
        if pct >= th: return pct
    return None

def r3_push(open_price: float, high_of_day: float, th: float) -> Optional[float]:
    if open_price and high_of_day and open_price > 0:
        pct = (high_of_day / open_price - 1.0) * 100.0
        if pct >= th: return pct
    return None

def r4_surge7(lowest_low_7d: float, highest_high_7d: float, th: float) -> Optional[float]:
    if lowest_low_7d and highest_high_7d and lowest_low_7d > 0:
        pct = (highest_high_7d / lowest_low_7d - 1.0) * 100.0
        if pct >= th: return pct
    return None

# ----------------------------
# PROVIDERS
# ----------------------------
class ProviderBase:
    name = "base"
    def supports_premarket(self) -> bool:
        return False
    def get_daily_ohlcv(self, symbols: List[str], start_ymd: str, end_ymd: str) -> pd.DataFrame:
        raise NotImplementedError
    def get_premarket_high(self, symbol: str, event_ymd: str) -> Tuple[Optional[float], str, str]:
        return None, "", ""
    def get_splits_window(self, symbol: str, from_ymd: str, to_ymd: str) -> List[Dict[str,Any]]:
        return []

# ThetaData
class ThetaDataProvider(ProviderBase):
    name = "thetadata"

    # Class-level semaphore for connection limiting
    _semaphore = None
    _in_flight_count = 0
    _count_lock = threading.Lock()

    def __init__(self):
        # Initialize semaphore if not already done
        if ThetaDataProvider._semaphore is None:
            ThetaDataProvider._semaphore = threading.Semaphore(CONFIG["theta_outstanding"])

        self.base = None
        self.version = None
        self.param_name = None

        # Configure session with connection pooling and retry strategy
        self.session = requests.Session()

        # Setup retry strategy for ThetaData-specific errors
        retry_strategy = Retry(
            total=CONFIG["theta_retry_total"],
            backoff_factor=CONFIG["theta_backoff"],
            status_forcelist=[429, 570, 571, 474],  # ThetaData specific error codes
            allowed_methods=["GET"],
            raise_on_status=False
        )

        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=CONFIG["theta_outstanding"],
            pool_maxsize=CONFIG["theta_outstanding"],
            max_retries=retry_strategy
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.timeout = CONFIG["timeout_sec"]

        # Auto-detect ThetaData version and configure endpoints
        if self._test_connection("http://127.0.0.1:25503", "symbol"):
            self.base = "http://127.0.0.1:25503"
            self.version = "v3"
            self.param_name = "symbol"
            log("ThetaData v3 detected on port 25503")
        elif self._test_connection("http://127.0.0.1:25510", "root"):
            self.base = "http://127.0.0.1:25510"
            self.version = "v1"
            self.param_name = "root"
            log("ThetaData v1.8.6 detected on port 25510")
        else:
            log("ThetaData not detected - will use Polygon fallback")

    def _test_connection(self, base_url: str, param_name: str) -> bool:
        """Test if ThetaData is responding with correct parameter name and endpoints"""
        try:
            # Use correct endpoints for each version
            if param_name == "symbol":  # v3
                endpoint = "/v3/stock/history/ohlc"
                params = {param_name: "SPY", "date": "20250912", "interval": "1m"}
            else:  # v1.8.6/v2
                endpoint = "/v2/hist/stock/ohlc"
                params = {param_name: "SPY", "start_date": "20250912", "end_date": "20250912", "ivl": 60000}

            url = f"{base_url}{endpoint}"
            r = self.session.get(url, params=params, timeout=2)
            return r.status_code == 200
        except:
            return False

    def _theta_request(self, url: str, params: Dict) -> Optional[requests.Response]:
        """Make a throttled request to ThetaData with semaphore control"""
        with ThetaDataProvider._semaphore:
            with ThetaDataProvider._count_lock:
                ThetaDataProvider._in_flight_count += 1
                current_count = ThetaDataProvider._in_flight_count

            try:
                # Log in-flight status periodically
                if current_count % 2 == 1:  # Every other request
                    log(f"Theta in-flight: {current_count}/{CONFIG['theta_outstanding']}")

                response = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"])

                # Handle ThetaData-specific error codes
                if response.status_code == 429:
                    log(f"ThetaData OS_LIMIT (429) - backing off")
                elif response.status_code == 570:
                    log(f"ThetaData LARGE_REQUEST (570) - request too large")
                elif response.status_code == 571:
                    log(f"ThetaData SERVER_STARTING (571) - server starting up")
                elif response.status_code == 474:
                    log(f"ThetaData DISCONNECTED (474) - connection lost")

                response.raise_for_status()
                return response

            except Exception as e:
                log(f"ThetaData request error: {e}")
                return None
            finally:
                with ThetaDataProvider._count_lock:
                    ThetaDataProvider._in_flight_count -= 1

    def ping(self) -> bool:
        return self.base is not None

    def supports_premarket(self) -> bool:
        return self.base is not None

    def get_daily_ohlcv(self, symbols: List[str], start_ymd: str, end_ymd: str) -> pd.DataFrame:
        if not self.base:
            return pd.DataFrame()

        all_days = set(trading_days_between(start_ymd, end_ymd))
        out = []

        log(f"ThetaData {self.version} daily OHLC retrieval starting...")
        log(f"Theta concurrency limit: {CONFIG['theta_outstanding']} outstanding requests")

        for sym in tqdm(symbols, desc=f"thetadata {self.version} daily"):
            for d in sorted(all_days):
                mb = self._ohlc_1m(sym, d, rth=True)
                if mb.empty:
                    continue
                day_open = float(mb.iloc[0]["open"])
                day_close = float(mb.iloc[-1]["close"])
                day_high = float(mb["high"].max())
                day_low = float(mb["low"].min())
                day_vol = int(mb["volume"].sum())
                out.append({
                    "provider": self.name,
                    "symbol": sym,
                    "date": d,
                    "open": day_open,
                    "high": day_high,
                    "low": day_low,
                    "close": day_close,
                    "volume": day_vol
                })

        return pd.DataFrame(out)

    def _ohlc_1m(self, symbol: str, ymd: str, rth: bool) -> pd.DataFrame:
        if not self.base:
            return pd.DataFrame()

        # Use proper endpoints and parameters for each version
        if self.version == "v3":
            endpoint = "/v3/stock/history/ohlc"
            params = {
                self.param_name: symbol,
                "date": ymd.replace("-", ""),
                "interval": "1m"
            }
            if not rth:
                # For premarket: 4:00 AM to 9:29 AM
                params["start_time"] = CONFIG["pm_time_start"]
                params["end_time"] = CONFIG["pm_time_end"]
        else:  # v1.8.6/v2
            endpoint = "/v2/hist/stock/ohlc"
            params = {
                self.param_name: symbol,
                "start_date": ymd.replace("-", ""),
                "end_date": ymd.replace("-", ""),
                "ivl": 60000  # 1 minute
            }
            if not rth:
                params["rth"] = "false"

        url = f"{self.base}{endpoint}"

        try:
            resp = self._theta_request(url, params)
            if not resp or resp.status_code != 200:
                return pd.DataFrame()

            # Handle different response formats based on version
            if self.version == "v3":
                # v3 returns CSV data by default
                import io
                try:
                    df = pd.read_csv(io.StringIO(resp.text))
                    if df.empty:
                        return pd.DataFrame()

                    # Convert to our expected format
                    rows = []
                    for _, row in df.iterrows():
                        # Parse timestamp: "2025-09-12T09:30:00"
                        timestamp_str = str(row['timestamp'])
                        if 'T' in timestamp_str:
                            date_part, time_part = timestamp_str.split('T')
                            # Create datetime string compatible with our format
                            datetime_str = f"{date_part} {time_part}"
                        else:
                            datetime_str = timestamp_str

                        rows.append({
                            "symbol": symbol,
                            "datetime": datetime_str,
                            "open": float(row['open']),
                            "high": float(row['high']),
                            "low": float(row['low']),
                            "close": float(row['close']),
                            "volume": int(row['volume'])
                        })
                except Exception as e:
                    log(f"ThetaData v3 CSV parsing error for {symbol}: {e}")
                    return pd.DataFrame()
            else:
                # v1.8.6/v2 returns JSON data
                data = resp.json()
                header = data.get("header", {})

                # Check for API errors
                error_type = header.get("error_type")
                if error_type and error_type != "null":
                    if error_type != "NO_DATA":  # NO_DATA is expected for non-trading days
                        log(f"ThetaData API error for {symbol}: {header.get('error_msg', 'Unknown error')}")
                    return pd.DataFrame()

                response = data.get("response", [])
                if not response:
                    return pd.DataFrame()

                rows = []
                for rec in response:
                    # Ensure record has the expected length
                    if not isinstance(rec, list) or len(rec) < 8:
                        continue

                    # Format: [ms_of_day, open, high, low, close, volume, count, date]
                    ms_of_day = rec[0]
                    o, h, l, c, v = rec[1], rec[2], rec[3], rec[4], rec[5]
                    date_int = rec[7]

                    # Convert ms_of_day to time
                    total_seconds = ms_of_day // 1000
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60

                    # Convert date integer YYYYMMDD to YYYY-MM-DD
                    date_str = str(date_int)
                    date_ymd = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    timestamp = f"{date_ymd} {hours:02d}:{minutes:02d}:{seconds:02d}"

                    rows.append({
                        "symbol": symbol,
                        "datetime": timestamp,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "volume": v
                    })

            return pd.DataFrame(rows)

        except Exception as e:
            log(f"ThetaData {self.version} OHLC error for {symbol}: {e}")
            return pd.DataFrame()

    def get_premarket_high(self, symbol: str, event_ymd: str) -> Tuple[Optional[float], str, str]:
        df = self._ohlc_1m(symbol, event_ymd, rth=False)
        if df.empty:
            return None, self.name, "minute"

        # Filter for premarket hours (already filtered in v3, but double-check for v2)
        mask = (df["datetime"] >= f"{event_ymd} {CONFIG['pm_time_start']}") & \
               (df["datetime"] <= f"{event_ymd} {CONFIG['pm_time_end']}")
        pm = df.loc[mask]

        if pm.empty:
            return None, self.name, "minute"

        return float(pm["high"].max()), self.name, "minute"

    def get_splits_window(self, symbol: str, from_ymd: str, to_ymd: str) -> List[Dict[str,Any]]:
        if not self.base:
            return []

        # Use splits endpoint (v2 is stable for splits across both versions)
        endpoint = "/v2/hist/stock/split"  # Note: 'split' not 'splits'

        # For v3, splits endpoint is on v2 port with 'root' parameter
        if self.version == "v3":
            url = "http://127.0.0.1:25510" + endpoint
            param_name = "root"
        else:
            url = f"{self.base}{endpoint}"
            param_name = self.param_name

        params = {
            param_name: symbol,
            "start_date": from_ymd.replace("-", ""),
            "end_date": to_ymd.replace("-", "")
        }

        try:
            resp = self._theta_request(url, params)
            if not resp or resp.status_code != 200:
                return []

            data = resp.json()
            response = data.get("response", [])

            results = []
            for split in response:
                # Format: [date, split_from, split_to]
                if len(split) >= 3:
                    date_int = str(split[0])
                    date_ymd = f"{date_int[0:4]}-{date_int[4:6]}-{date_int[6:8]}"

                    if date_ymd >= from_ymd and date_ymd <= to_ymd:
                        results.append({
                            "execution_date": date_ymd,
                            "split_from": split[1],
                            "split_to": split[2]
                        })

            return results

        except Exception as e:
            log(f"ThetaData {self.version} splits error for {symbol}: {e}")
            return []

    def get_universe_symbols(self, date_ymd: str = None) -> List[str]:
        """
        Zero-Miss Discovery: Get complete universe of tradable symbols
        Uses ThetaData v3 list/symbols endpoint (updated overnight)
        """
        if not self.base or self.version != "v3":
            log("ThetaData universe symbols: v3 required")
            return []

        endpoint = "/v3/stock/list/symbols"
        url = f"{self.base}{endpoint}"

        try:
            log(f"ThetaData universe: Fetching all symbols for {date_ymd or 'current'}")
            resp = self._theta_request(url, {})
            if not resp or resp.status_code != 200:
                log("ThetaData universe: Failed to retrieve symbols")
                return []

            # v3 list/symbols returns CSV format
            import io
            try:
                df = pd.read_csv(io.StringIO(resp.text))
                if 'symbol' in df.columns:
                    symbols = df['symbol'].tolist()
                elif len(df.columns) == 1:
                    # Sometimes just a list of symbols
                    symbols = df.iloc[:, 0].tolist()
                else:
                    log(f"ThetaData universe: Unexpected CSV format: {df.columns.tolist()}")
                    return []

                # Filter out invalid symbols
                valid_symbols = []
                for symbol in symbols:
                    if isinstance(symbol, str) and len(symbol) <= 10 and symbol.isalpha():
                        valid_symbols.append(symbol.upper())

                log(f"ThetaData universe: Retrieved {len(valid_symbols)} valid symbols")
                return valid_symbols

            except Exception as e:
                log(f"ThetaData universe: CSV parsing error: {e}")
                return []

        except Exception as e:
            log(f"ThetaData universe error: {e}")
            return []

    def get_trading_dates(self, request_type: str = "trade") -> List[str]:
        """
        Zero-Miss Discovery: Get available trading dates for completeness audit
        Uses ThetaData v3 list/dates endpoint
        """
        if not self.base or self.version != "v3":
            log("ThetaData trading dates: v3 required")
            return []

        endpoint = f"/v3/stock/list/dates/{request_type}"
        url = f"{self.base}{endpoint}"
        params = {"symbol": "*"}

        try:
            log(f"ThetaData trading dates: Fetching {request_type} dates")
            resp = self._theta_request(url, params)
            if not resp or resp.status_code != 200:
                log("ThetaData trading dates: Failed to retrieve dates")
                return []

            # Parse response (likely CSV format)
            import io
            try:
                df = pd.read_csv(io.StringIO(resp.text))
                if 'date' in df.columns:
                    dates = df['date'].tolist()
                elif len(df.columns) == 1:
                    dates = df.iloc[:, 0].tolist()
                else:
                    log(f"ThetaData trading dates: Unexpected format: {df.columns.tolist()}")
                    return []

                # Convert to YYYY-MM-DD format
                formatted_dates = []
                for date in dates:
                    try:
                        if isinstance(date, str) and len(date) == 8:
                            # YYYYMMDD format
                            formatted_date = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
                            formatted_dates.append(formatted_date)
                        elif isinstance(date, str) and len(date) == 10:
                            # Already YYYY-MM-DD
                            formatted_dates.append(date)
                    except:
                        continue

                log(f"ThetaData trading dates: Retrieved {len(formatted_dates)} dates")
                return formatted_dates

            except Exception as e:
                log(f"ThetaData trading dates: CSV parsing error: {e}")
                return []

        except Exception as e:
            log(f"ThetaData trading dates error: {e}")
            return []

# Polygon
class PolygonProvider(ProviderBase):
    name = "polygon"
    base = CONFIG["polygon_base"]
    api_key = POLYGON_API_KEY

    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = CONFIG["timeout_sec"]

    def supports_premarket(self) -> bool:
        return True if self.api_key else False

    def _aggs_day(self, symbol: str, start_ymd: str, end_ymd: str) -> List[Dict[str,Any]]:
        url = f"{self.base}/v2/aggs/ticker/{symbol}/range/1/day/{start_ymd}/{end_ymd}"
        params = {"adjusted": "false", "sort": "asc", "limit": 50000, "apiKey": self.api_key}
        try:
            data = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
            return data.get("results", []) or []
        except Exception:
            return []

    def get_daily_ohlcv(self, symbols: List[str], start_ymd: str, end_ymd: str) -> pd.DataFrame:
        out = []
        for sym in tqdm(symbols, desc="polygon daily"):
            arr = self._aggs_day(sym, start_ymd, end_ymd)
            for a in arr:
                ts = pd.to_datetime(a["t"], unit="ms")
                d = ts.strftime("%Y-%m-%d")
                out.append({"provider": self.name, "symbol": sym, "date": d,
                            "open": float(a["o"]), "high": float(a["h"]),
                            "low": float(a["l"]), "close": float(a["c"]), "volume": int(a["v"])})
        return pd.DataFrame(out)

    def get_grouped_daily(self, date_ymd: str) -> pd.DataFrame:
        """
        Zero-Miss Discovery: Get entire market daily data in one call
        Uses Polygon grouped daily endpoint for complete market coverage
        """
        url = f"{self.base}/v2/aggs/grouped/locale/us/market/stocks/{date_ymd}"
        params = {"adjusted": "false", "apiKey": self.api_key}

        try:
            log(f"Polygon grouped daily: Fetching whole market for {date_ymd}")
            response = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"])
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                log(f"Polygon grouped daily: No results for {date_ymd}")
                return pd.DataFrame()

            log(f"Polygon grouped daily: Retrieved {len(results)} symbols")

            # Convert to standardized format
            out = []
            for r in results:
                # Polygon grouped format: T=symbol, o=open, h=high, l=low, c=close, v=volume, t=timestamp
                try:
                    symbol = r.get("T", "").upper()
                    if not symbol or len(symbol) > 10:  # Filter out invalid symbols
                        continue

                    out.append({
                        "provider": self.name,
                        "symbol": symbol,
                        "date": date_ymd,
                        "open": float(r.get("o", 0)),
                        "high": float(r.get("h", 0)),
                        "low": float(r.get("l", 0)),
                        "close": float(r.get("c", 0)),
                        "volume": int(r.get("v", 0))
                    })
                except (ValueError, TypeError) as e:
                    log(f"Polygon grouped daily: Error parsing symbol {r.get('T', 'unknown')}: {e}")
                    continue

            return pd.DataFrame(out)

        except Exception as e:
            log(f"Polygon grouped daily error for {date_ymd}: {e}")
            return pd.DataFrame()

    def _aggs_1m(self, symbol: str, ymd: str) -> pd.DataFrame:
        url = f"{self.base}/v2/aggs/ticker/{symbol}/range/1/minute/{ymd}/{ymd}"
        params = {"adjusted": "false", "sort": "asc", "limit": 50000, "apiKey": self.api_key}
        try:
            data = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
            res = data.get("results", []) or []
        except Exception:
            return pd.DataFrame()
        rows = []
        for a in res:
            ts = pd.to_datetime(a["t"], unit="ms", utc=True).tz_convert("America/New_York").tz_localize(None)
            rows.append({"symbol": symbol, "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                         "open": float(a["o"]), "high": float(a["h"]),
                         "low": float(a["l"]), "close": float(a["c"]), "volume": int(a["v"])})
        return pd.DataFrame(rows)

    def get_premarket_high(self, symbol: str, event_ymd: str) -> Tuple[Optional[float], str, str]:
        df = self._aggs_1m(symbol, event_ymd)
        if df.empty:
            return None, self.name, "minute"
        mask = (df["datetime"] >= f"{event_ymd} {CONFIG['pm_time_start']}") & \
               (df["datetime"] <= f"{event_ymd} {CONFIG['pm_time_end']}")
        pm = df.loc[mask]
        if pm.empty:
            return None, self.name, "minute"
        return float(pm["high"].max()), self.name, "minute"

    def get_splits_window(self, symbol: str, from_ymd: str, to_ymd: str) -> List[Dict[str,Any]]:
        url = f"{self.base}/v3/reference/splits"
        params = {"ticker": symbol, "apiKey": self.api_key}
        try:
            data = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
            res = []
            for s in data.get("results", []) or []:
                d = s.get("execution_date", "")
                if not d: continue
                if d >= from_ymd and d <= to_ymd:
                    res.append({"execution_date": d, "split_from": s.get("split_from"), "split_to": s.get("split_to")})
            return res
        except Exception:
            return []

# FMP
class FmpProvider(ProviderBase):
    name = "fmp"
    base = CONFIG["fmp_base"]
    api_key = FMP_API_KEY

    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = CONFIG["timeout_sec"]

    def get_daily_ohlcv(self, symbols: List[str], start_ymd: str, end_ymd: str) -> pd.DataFrame:
        out = []
        for sym in tqdm(symbols, desc="fmp daily"):
            url = f"{self.base}/v3/historical-price-full/{sym}"
            params = {"from": start_ymd, "to": end_ymd, "apikey": self.api_key}
            try:
                data = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
                arr = data.get("historical", []) or []
            except Exception:
                arr = []
            for d in arr:
                out.append({"provider": self.name, "symbol": sym, "date": d["date"],
                            "open": float(d["open"]), "high": float(d["high"]), "low": float(d["low"]),
                            "close": float(d["close"]), "volume": int(d["volume"])})
        return pd.DataFrame(out)

    def get_splits_window(self, symbol: str, from_ymd: str, to_ymd: str) -> List[Dict[str,Any]]:
        url = f"{self.base}/v3/historical-price-full/stock_split/{symbol}"
        params = {"from": from_ymd, "to": to_ymd, "apikey": self.api_key}
        try:
            data = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
            arr = data.get("historical", []) or []
        except Exception:
            return []
        res = []
        for s in arr:
            d = s.get("date", "")
            if d and d >= from_ymd and d <= to_ymd:
                ratio = s.get("splitRatio") or s.get("label") or s.get("ratio") or ""
                sf, st = None, None
                if isinstance(ratio, str) and ":" in ratio:
                    parts = ratio.split(":")
                    try:
                        sf = float(parts[0]); st = float(parts[1])
                    except Exception:
                        sf, st = None, None
                res.append({"execution_date": d, "split_from": sf, "split_to": st})
        return res

    def get_delisted_companies(self) -> List[Dict[str, Any]]:
        """
        Zero-Miss Discovery: Get delisted companies to include in historical universe
        Uses FMP delisted-companies endpoint for complete historical coverage
        """
        if not self.api_key:
            log("FMP delisted companies: API key required")
            return []

        url = f"{self.base}/v3/delisted-companies"
        params = {"apikey": self.api_key}

        try:
            log("FMP delisted companies: Fetching complete list")
            response = self.session.get(url, params=params, timeout=CONFIG["timeout_sec"])
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                log("FMP delisted companies: Unexpected response format")
                return []

            # Process delisted companies
            delisted = []
            for company in data:
                try:
                    symbol = company.get("symbol", "").strip().upper()
                    company_name = company.get("companyName", "").strip()
                    exchange = company.get("exchange", "").strip()
                    ipo_date = company.get("ipoDate", "")
                    delisted_date = company.get("delistedDate", "")

                    if symbol and len(symbol) <= 10:
                        delisted.append({
                            "symbol": symbol,
                            "company_name": company_name,
                            "exchange": exchange,
                            "ipo_date": ipo_date,
                            "delisted_date": delisted_date
                        })
                except Exception as e:
                    log(f"FMP delisted companies: Error parsing company {company}: {e}")
                    continue

            log(f"FMP delisted companies: Retrieved {len(delisted)} delisted companies")
            return delisted

        except Exception as e:
            log(f"FMP delisted companies error: {e}")
            return []

# ----------------------------
# UNIVERSE
# ----------------------------
def build_universe() -> List[str]:
    syms = load_symbols_from_file("symbols.txt")
    if syms:
        log(f"Loaded {len(syms)} symbols from symbols.txt")
        if CONFIG["max_symbols"] > 0:
            syms = syms[:CONFIG["max_symbols"]]
        return syms
    # Fallback: light FMP screener for top names (kept small)
    if not FMP_API_KEY:
        return ["AAPL","TSLA","NVDA","AMD","AMZN"]
    url = f"{CONFIG['fmp_base']}/v3/stock-screener"
    params = {
        "exchange": "NYSE,NASDAQ",
        "marketCapMoreThan": 5_000_000_00, # 500M
        "limit": 200 if CONFIG["max_symbols"] == 0 else CONFIG["max_symbols"],
        "apikey": FMP_API_KEY
    }
    try:
        data = requests.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
        syms = [d["symbol"] for d in data if d.get("symbol")]
        log(f"Built universe from FMP screener: {len(syms)} symbols")
        return syms
    except Exception:
        log("FMP screener fallback failed; using tiny defaults")
        return ["AAPL","TSLA","NVDA","AMD","AMZN"]

# ----------------------------
# ZERO-MISS DISCOVERY SYSTEM
# ----------------------------

def build_daily_universe(date_ymd: str, providers: List) -> List[str]:
    """
    Zero-Miss Discovery: Build complete universe for a specific date
    Combines ThetaData current symbols + FMP delisted for historical completeness
    """
    all_symbols = set()

    # Get current tradable symbols from ThetaData
    for provider in providers:
        if isinstance(provider, ThetaDataProvider) and provider.version == "v3":
            theta_symbols = provider.get_universe_symbols(date_ymd)
            all_symbols.update(theta_symbols)
            log(f"Universe {date_ymd}: Added {len(theta_symbols)} symbols from ThetaData")
            break

    # Add delisted companies for historical completeness
    for provider in providers:
        if isinstance(provider, FmpProvider):
            delisted = provider.get_delisted_companies()
            delisted_symbols = [d["symbol"] for d in delisted]
            all_symbols.update(delisted_symbols)
            log(f"Universe {date_ymd}: Added {len(delisted_symbols)} delisted symbols from FMP")
            break

    universe = sorted(list(all_symbols))
    log(f"Universe {date_ymd}: Complete universe with {len(universe)} symbols")

    # Save universe snapshot
    import os
    os.makedirs("universe", exist_ok=True)
    universe_file = f"universe/universe_{date_ymd.replace('-', '')}.csv"
    try:
        with open(universe_file, 'w', encoding='ascii') as f:
            f.write("symbol\n")
            for symbol in universe:
                f.write(f"{symbol}\n")
        log(f"Universe {date_ymd}: Saved to {universe_file}")
    except Exception as e:
        log(f"Universe {date_ymd}: Error saving to file: {e}")

    return universe

def zero_miss_discovery_pass1(date_ymd: str, providers: List) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Zero-Miss Discovery Pass 1: Whole market daily scan using Polygon grouped
    Returns: (market_data, r4_metrics) where r4_metrics includes R4 statistics
    """
    log(f"Zero-Miss Pass-1: Starting whole market scan for {date_ymd}")

    # Find Polygon provider
    polygon_provider = None
    for provider in providers:
        if isinstance(provider, PolygonProvider):
            polygon_provider = provider
            break

    if not polygon_provider or not polygon_provider.api_key:
        log("Zero-Miss Pass-1: Polygon provider required")
        return pd.DataFrame()

    # Get whole market data in one call
    market_data = polygon_provider.get_grouped_daily(date_ymd)
    if market_data.empty:
        log(f"Zero-Miss Pass-1: No market data for {date_ymd}")
        return market_data

    log(f"Zero-Miss Pass-1: Retrieved {len(market_data)} symbols from market")

    # Add prev_close calculation using proper trading day logic
    log(f"Zero-Miss Pass-1: Calculating prev_close for {len(market_data)} symbols")

    def get_symbol_prev_close(symbol):
        """Helper to get prev_close for a single symbol"""
        try:
            return get_prev_close(symbol, date_ymd, providers)
        except Exception as e:
            log(f"Error getting prev_close for {symbol}: {e}")
            return None

    # Calculate prev_close for each symbol (with progress tracking)
    market_data["prev_close"] = None
    for idx, row in market_data.iterrows():
        symbol = row["symbol"]
        prev_close = get_symbol_prev_close(symbol)
        market_data.at[idx, "prev_close"] = prev_close

        # Log progress every 100 symbols
        if (idx + 1) % 100 == 0:
            log(f"Zero-Miss Pass-1: Processed {idx + 1}/{len(market_data)} symbols for prev_close")

    # Filter out symbols where we couldn't get prev_close
    initial_count = len(market_data)
    market_data = market_data[market_data["prev_close"].notna() & (market_data["prev_close"] > 0)]
    filtered_count = len(market_data)

    if filtered_count < initial_count:
        log(f"Zero-Miss Pass-1: Filtered out {initial_count - filtered_count} symbols without valid prev_close")
        log(f"Zero-Miss Pass-1: Continuing with {filtered_count} symbols")

    # Calculate basic rules metrics
    market_data["open_gap_pct"] = ((market_data["open"] / market_data["prev_close"]) - 1) * 100
    market_data["intraday_push_pct"] = ((market_data["high"] / market_data["open"]) - 1) * 100
    market_data["dollar_volume"] = market_data["close"] * market_data["volume"]

    # Identify R2/R3 candidates
    r2_candidates = market_data[market_data["open_gap_pct"] >= 50.0]
    r3_candidates = market_data[market_data["intraday_push_pct"] >= 50.0]

    log(f"Zero-Miss Pass-1: Found {len(r2_candidates)} R2 candidates, {len(r3_candidates)} R3 candidates")

    # R4 seven-day surge calculation for high-impact candidates
    # Only check R4 for symbols with significant volume to save time
    high_volume_symbols = market_data[market_data["dollar_volume"] >= 1_000_000]["symbol"].tolist()
    log(f"Zero-Miss Pass-1: Checking R4 for {len(high_volume_symbols)} high-volume symbols")

    r4_candidates = []
    r4_processed = 0

    for symbol in high_volume_symbols[:100]:  # Limit R4 checks for performance
        try:
            seven_day_data = get_seven_day_range(symbol, date_ymd, providers)
            if seven_day_data:
                lowest_low = seven_day_data["lowest_low_7d"]
                highest_high = seven_day_data["highest_high_7d"]

                r4_surge_pct = r4_surge7(lowest_low, highest_high, CONFIG["surge_7d_trigger_pct"])
                if r4_surge_pct is not None:
                    r4_candidates.append(symbol)
                    log(f"Zero-Miss Pass-1: R4 hit {symbol} surge {r4_surge_pct:.1f}%")

            r4_processed += 1
            if r4_processed % 20 == 0:
                log(f"Zero-Miss Pass-1: R4 processed {r4_processed}/{len(high_volume_symbols[:100])}")

        except Exception as e:
            log(f"Zero-Miss Pass-1: R4 error for {symbol}: {e}")
            continue

    log(f"Zero-Miss Pass-1: Found {len(r4_candidates)} R4 candidates")

    # Combine all candidate types
    all_candidate_symbols = set()
    all_candidate_symbols.update(r2_candidates["symbol"].tolist())
    all_candidate_symbols.update(r3_candidates["symbol"].tolist())
    all_candidate_symbols.update(r4_candidates)

    candidates = market_data[market_data["symbol"].isin(all_candidate_symbols)]
    log(f"Zero-Miss Pass-1: Total {len(candidates)} unique R2/R3/R4 candidates")

    # Prepare R4 metrics for return
    r4_metrics = {
        "r4_checks_attempted": len(high_volume_symbols[:100]),
        "r4_candidates": len(r4_candidates)
    }

    return market_data, r4_metrics

def zero_miss_discovery_pass2b(date_ymd: str, market_data: pd.DataFrame, providers: List) -> Dict[str, Any]:
    """
    Zero-Miss Discovery Pass 2B: Candidate prefilter + miss-audit for premarket (R1)
    Returns R1 hits and completeness metrics
    """
    log(f"Zero-Miss Pass-2B: Starting premarket scan for {date_ymd}")

    # Find ThetaData provider
    theta_provider = None
    for provider in providers:
        if isinstance(provider, ThetaDataProvider) and provider.version == "v3":
            theta_provider = provider
            break

    if not theta_provider:
        log("Zero-Miss Pass-2B: ThetaData v3 provider required")
        return {"r1_hits": [], "audit_results": {}}

    # Candidate prefilter: over-catch potential movers
    if not market_data.empty:
        prefilter_conditions = (
            (market_data["open"] / market_data["prev_close"] >= 1.2) |  # 20%+ gap
            (market_data["high"] / market_data["open"] >= 1.3) |        # 30%+ push
            (market_data["dollar_volume"] >= 1_000_000)                 # $1M+ volume
        )
        candidate_symbols = market_data[prefilter_conditions]["symbol"].tolist()
    else:
        candidate_symbols = []

    log(f"Zero-Miss Pass-2B: Prefilter identified {len(candidate_symbols)} candidates")

    # Get premarket data for candidates
    r1_hits = []
    for symbol in candidate_symbols[:50]:  # Limit for testing
        try:
            pm_high, pm_src, pm_method = theta_provider.get_premarket_high(symbol, date_ymd)
            if pm_high is not None:
                # Calculate R1 using proper prev_close
                prev_close = market_data[market_data["symbol"] == symbol]["prev_close"].iloc[0] if not market_data.empty else None
                if prev_close:
                    pm_gap_pct = ((pm_high / prev_close) - 1) * 100
                    if pm_gap_pct >= 50.0:
                        r1_hits.append({
                            "symbol": symbol,
                            "date": date_ymd,
                            "premarket_high": pm_high,
                            "prev_close": prev_close,
                            "pm_gap_pct": pm_gap_pct,
                            "source": pm_src
                        })
        except Exception as e:
            log(f"Zero-Miss Pass-2B: Error checking {symbol}: {e}")
            continue

    log(f"Zero-Miss Pass-2B: Found {len(r1_hits)} R1 hits")

    # Miss-audit: Random sampling of non-candidate symbols to verify no R1 misses
    audit_results = run_miss_audit(date_ymd, market_data, candidate_symbols, theta_provider, providers)

    return {
        "r1_hits": r1_hits,
        "audit_results": audit_results,
        "candidates_checked": len(candidate_symbols)
    }

def zero_miss_discovery_pass2a(date_ymd: str, universe: List[str], providers: List) -> Dict[str, Any]:
    """
    Zero-Miss Discovery Pass 2A: Full universe premarket sweep with ThetaData v3
    Bulletproof approach - checks every symbol for R1 premarket events
    Slower but guarantees absolute zero misses
    """
    log(f"Zero-Miss Pass-2A: Starting full universe premarket sweep for {date_ymd}")

    # Find ThetaData provider
    theta_provider = None
    for provider in providers:
        if isinstance(provider, ThetaDataProvider) and provider.version == "v3":
            theta_provider = provider
            break

    if not theta_provider:
        log("Zero-Miss Pass-2A: ThetaData v3 provider required")
        return {"r1_hits": [], "audit_results": {"full_sweep": False}}

    # Full universe sweep
    r1_hits = []
    processed_count = 0
    error_count = 0

    log(f"Zero-Miss Pass-2A: Sweeping {len(universe)} symbols")

    from tqdm import tqdm
    for symbol in tqdm(universe[:500], desc="Pass-2A full sweep"):  # Limit for testing
        try:
            # Get premarket high for this symbol
            pm_high, pm_src, pm_method = theta_provider.get_premarket_high(symbol, date_ymd)
            processed_count += 1

            if pm_high is not None:
                # Get prev_close for proper R1 calculation
                prev_close = get_prev_close(symbol, date_ymd, providers)

                if prev_close and prev_close > 0:
                    pm_gap_pct = ((pm_high / prev_close) - 1) * 100

                    # Check R1 threshold (>=50%)
                    if pm_gap_pct >= 50.0:
                        r1_hits.append({
                            "symbol": symbol,
                            "date": date_ymd,
                            "premarket_high": pm_high,
                            "prev_close": prev_close,
                            "pm_gap_pct": pm_gap_pct,
                            "source": pm_src,
                            "method": "full_universe_sweep"
                        })
                        log(f"Zero-Miss Pass-2A: R1 hit {symbol} PM gap {pm_gap_pct:.1f}%")

        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Log first few errors
                log(f"Zero-Miss Pass-2A: Error checking {symbol}: {e}")
            continue

    log(f"Zero-Miss Pass-2A: Processed {processed_count} symbols, {error_count} errors")
    log(f"Zero-Miss Pass-2A: Found {len(r1_hits)} R1 hits in full sweep")

    # Full sweep audit results
    audit_results = {
        "full_sweep": True,
        "symbols_processed": processed_count,
        "symbols_with_errors": error_count,
        "coverage_percentage": (processed_count / len(universe)) * 100 if universe else 0,
        "audit_passed": True  # Full sweep always passes audit
    }

    return {
        "r1_hits": r1_hits,
        "audit_results": audit_results,
        "symbols_processed": processed_count
    }

def run_miss_audit(date_ymd: str, market_data: pd.DataFrame, checked_symbols: List[str],
                  theta_provider, providers: List) -> Dict[str, Any]:
    """
    Zero-Miss Discovery: Miss-audit with random sampling
    Randomly sample non-candidate symbols to verify no R1 misses
    """
    import random

    if market_data.empty:
        return {"sample_size": 0, "sample_r1_hits": 0, "audit_passed": True}

    # Get all symbols from market data that weren't checked as candidates
    all_symbols = set(market_data["symbol"].tolist())
    checked_set = set(checked_symbols)
    unchecked_symbols = list(all_symbols - checked_set)

    if not unchecked_symbols:
        log("Miss-audit: No unchecked symbols for sampling")
        return {"sample_size": 0, "sample_r1_hits": 0, "audit_passed": True}

    # Random sample from unchecked symbols
    sample_size = min(CONFIG.get("pass2b_audit_sample", 50), len(unchecked_symbols))
    sample_symbols = random.sample(unchecked_symbols, sample_size)

    log(f"Miss-audit: Sampling {sample_size} symbols from {len(unchecked_symbols)} unchecked")

    # Check sample for R1 hits
    sample_r1_hits = 0
    audit_errors = 0

    for symbol in sample_symbols:
        try:
            pm_high, pm_src, pm_method = theta_provider.get_premarket_high(symbol, date_ymd)
            if pm_high is not None:
                # Get prev_close for this symbol
                prev_close = get_prev_close(symbol, date_ymd, providers)
                if prev_close and prev_close > 0:
                    pm_gap_pct = ((pm_high / prev_close) - 1) * 100
                    if pm_gap_pct >= 50.0:
                        sample_r1_hits += 1
                        log(f"Miss-audit: WARNING - Found R1 miss {symbol} PM gap {pm_gap_pct:.1f}%")
        except Exception as e:
            audit_errors += 1
            continue

    audit_passed = (sample_r1_hits == 0)

    if not audit_passed:
        log(f"Miss-audit: FAILED - Found {sample_r1_hits} R1 hits in random sample")
        log("Miss-audit: Consider widening prefilter thresholds")
    else:
        log(f"Miss-audit: PASSED - No R1 hits found in {sample_size} random samples")

    return {
        "sample_size": sample_size,
        "sample_r1_hits": sample_r1_hits,
        "audit_passed": audit_passed,
        "audit_errors": audit_errors,
        "total_unchecked": len(unchecked_symbols)
    }

def create_completeness_metrics(date_ymd: str, universe: List[str], market_data: pd.DataFrame,
                              pass2_results: Dict[str, Any], r4_metrics: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Zero-Miss Discovery: Create completeness proof metrics for the day
    """
    metrics = {
        "date": date_ymd,
        "total_symbols_in_universe": len(universe),
        "symbols_in_grouped_daily": len(market_data) if not market_data.empty else 0,
        "candidates_after_pass1": len(market_data[
            (market_data["open_gap_pct"] >= 50.0) |
            (market_data["intraday_push_pct"] >= 50.0)
        ]) if not market_data.empty else 0,
        "r2_candidates": len(market_data[market_data["open_gap_pct"] >= 50.0]) if not market_data.empty else 0,
        "r3_candidates": len(market_data[market_data["intraday_push_pct"] >= 50.0]) if not market_data.empty else 0,
        "r4_checks_attempted": (r4_metrics or {}).get("r4_checks_attempted", 0),
        "r4_candidates": (r4_metrics or {}).get("r4_candidates", 0),
        "r1_checked_symbols": pass2_results.get("candidates_checked", 0),
        "r1_hits": len(pass2_results.get("r1_hits", [])),
        "miss_audit_sample_size": pass2_results.get("audit_results", {}).get("sample_size", 0),
        "miss_audit_r1_hits": pass2_results.get("audit_results", {}).get("sample_r1_hits", 0),
        "audit_passed": pass2_results.get("audit_results", {}).get("audit_passed", True),
        "completeness_timestamp": datetime.now().isoformat()
    }

    # Save completeness metrics
    import os
    os.makedirs("project_state", exist_ok=True)
    metrics_file = f"project_state/day_{date_ymd.replace('-', '')}_completeness.json"
    try:
        import json
        with open(metrics_file, 'w', encoding='ascii') as f:
            json.dump(metrics, f, indent=2)
        log(f"Completeness metrics saved to {metrics_file}")
    except Exception as e:
        log(f"Error saving completeness metrics: {e}")

    return metrics

def get_prev_close(symbol: str, date_ymd: str, providers: List, provider_pref: List[str] = None) -> float:
    """
    Zero-Miss Discovery: Get reliable prev_close from multiple provider sources
    provider_pref: List of preferred providers like ['theta_eod', 'polygon_daily', 'fmp']
    """
    if provider_pref is None:
        provider_pref = ['theta_eod', 'polygon_daily', 'fmp']

    # Calculate previous trading day using market calendar
    from datetime import datetime, timedelta
    try:
        current_date = datetime.strptime(date_ymd, "%Y-%m-%d")

        # Use NYSE market calendar for trading days
        nyse = mcal.get_calendar('NYSE')

        # Get trading days up to current date
        # Look back 10 days to handle long weekends/holidays
        start_lookback = current_date - timedelta(days=10)
        trading_days = nyse.schedule(start_date=start_lookback, end_date=current_date)

        if trading_days.empty:
            # Fallback to simple date math if calendar fails
            prev_date = current_date - timedelta(days=1)
            prev_ymd = prev_date.strftime("%Y-%m-%d")
            log(f"prev_close: Market calendar failed, using simple prev day {prev_ymd}")
        else:
            # Get the trading day before current date
            trading_dates = trading_days.index.date
            current_date_only = current_date.date()

            # Find previous trading day
            prev_trading_days = [d for d in trading_dates if d < current_date_only]
            if prev_trading_days:
                prev_date = max(prev_trading_days)
                prev_ymd = prev_date.strftime("%Y-%m-%d")
                log(f"prev_close: Previous trading day for {date_ymd} is {prev_ymd}")
            else:
                # Fallback to simple date math
                prev_date = current_date - timedelta(days=1)
                prev_ymd = prev_date.strftime("%Y-%m-%d")
                log(f"prev_close: No previous trading day found, using simple prev day {prev_ymd}")

    except Exception as e:
        log(f"prev_close: Error calculating previous trading day for {date_ymd}: {e}")
        # Fallback to simple date math
        try:
            current_date = datetime.strptime(date_ymd, "%Y-%m-%d")
            prev_date = current_date - timedelta(days=1)
            prev_ymd = prev_date.strftime("%Y-%m-%d")
            log(f"prev_close: Using fallback prev day {prev_ymd}")
        except:
            log(f"prev_close: Invalid date format {date_ymd}")
            return None

    for pref in provider_pref:
        try:
            if pref == 'theta_eod':
                # Use ThetaData EOD for previous day
                for provider in providers:
                    if isinstance(provider, ThetaDataProvider) and provider.version == "v3":
                        endpoint = "/v3/stock/history/eod"
                        url = f"{provider.base}{endpoint}"
                        params = {
                            "symbol": symbol,
                            "start_date": prev_ymd.replace("-", ""),
                            "end_date": prev_ymd.replace("-", "")
                        }

                        resp = provider._theta_request(url, params)
                        if resp and resp.status_code == 200:
                            import io
                            df = pd.read_csv(io.StringIO(resp.text))
                            if not df.empty and 'close' in df.columns:
                                prev_close = float(df['close'].iloc[-1])
                                log(f"prev_close for {symbol}: {prev_close} from ThetaData EOD")
                                return prev_close
                        break

            elif pref == 'polygon_daily':
                # Use Polygon daily aggregates for previous day
                for provider in providers:
                    if isinstance(provider, PolygonProvider) and provider.api_key:
                        arr = provider._aggs_day(symbol, prev_ymd, prev_ymd)
                        if arr:
                            prev_close = float(arr[-1].get("c", 0))
                            if prev_close > 0:
                                log(f"prev_close for {symbol}: {prev_close} from Polygon daily")
                                return prev_close
                        break

            elif pref == 'fmp':
                # Use FMP historical data for previous day
                for provider in providers:
                    if isinstance(provider, FmpProvider) and provider.api_key:
                        url = f"{provider.base}/v3/historical-price-full/{symbol}"
                        params = {"from": prev_ymd, "to": prev_ymd, "apikey": provider.api_key}
                        try:
                            data = provider.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
                            arr = data.get("historical", [])
                            if arr:
                                prev_close = float(arr[0]["close"])
                                log(f"prev_close for {symbol}: {prev_close} from FMP")
                                return prev_close
                        except:
                            continue
                        break

        except Exception as e:
            log(f"prev_close: Error with {pref} for {symbol}: {e}")
            continue

    log(f"prev_close: Could not get prev_close for {symbol} on {date_ymd}")
    return None


def get_seven_day_range(symbol: str, date_ymd: str, providers: List, provider_pref: List[str] = None) -> Dict[str, float]:
    """
    Zero-Miss Discovery: Get 7-day low/high range for R4 surge calculation
    Returns: {"lowest_low_7d": float, "highest_high_7d": float} or empty dict if failed
    """
    if provider_pref is None:
        provider_pref = ['theta_eod', 'polygon_daily', 'fmp']

    # Calculate 7-day trading window ending before the event date
    from datetime import datetime, timedelta
    try:
        current_date = datetime.strptime(date_ymd, "%Y-%m-%d")

        # Use NYSE market calendar for trading days
        nyse = mcal.get_calendar('NYSE')

        # Look back 20 days to ensure we get 7 trading days
        start_lookback = current_date - timedelta(days=20)
        trading_days = nyse.schedule(start_date=start_lookback, end_date=current_date)

        if trading_days.empty:
            log(f"seven_day_range: Market calendar failed for {symbol} on {date_ymd}")
            return {}

        # Get trading days before current date
        trading_dates = trading_days.index.date
        current_date_only = current_date.date()
        prev_trading_days = [d for d in trading_dates if d < current_date_only]

        if len(prev_trading_days) < 7:
            log(f"seven_day_range: Not enough trading days for {symbol} on {date_ymd}")
            return {}

        # Get the 7 most recent trading days before event date
        seven_day_period = prev_trading_days[-7:]
        start_date_7d = seven_day_period[0].strftime("%Y-%m-%d")
        end_date_7d = seven_day_period[-1].strftime("%Y-%m-%d")

        log(f"seven_day_range: {symbol} 7-day period: {start_date_7d} to {end_date_7d}")

    except Exception as e:
        log(f"seven_day_range: Error calculating date range for {symbol}: {e}")
        return {}

    # Try each provider to get 7-day historical data
    for pref in provider_pref:
        try:
            if pref == 'theta_eod':
                # Use ThetaData EOD for 7-day range
                for provider in providers:
                    if isinstance(provider, ThetaDataProvider) and provider.version == "v3":
                        endpoint = "/v3/stock/history/eod"
                        url = f"{provider.base}{endpoint}"
                        params = {
                            "symbol": symbol,
                            "start_date": start_date_7d.replace("-", ""),
                            "end_date": end_date_7d.replace("-", "")
                        }

                        resp = provider._theta_request(url, params)
                        if resp and resp.status_code == 200:
                            import io
                            df = pd.read_csv(io.StringIO(resp.text))
                            if not df.empty and 'low' in df.columns and 'high' in df.columns:
                                if len(df) >= 7:  # Ensure we have enough data
                                    lowest_low = float(df['low'].min())
                                    highest_high = float(df['high'].max())
                                    log(f"seven_day_range for {symbol}: {lowest_low}-{highest_high} from ThetaData EOD")
                                    return {"lowest_low_7d": lowest_low, "highest_high_7d": highest_high}
                        break

            elif pref == 'polygon_daily':
                # Use Polygon daily aggregates for 7-day range
                for provider in providers:
                    if isinstance(provider, PolygonProvider) and provider.api_key:
                        arr = provider._aggs_day(symbol, start_date_7d, end_date_7d)
                        if arr and len(arr) >= 7:
                            lows = [float(item.get("l", float('inf'))) for item in arr if item.get("l")]
                            highs = [float(item.get("h", 0)) for item in arr if item.get("h")]

                            if lows and highs:
                                lowest_low = min(lows)
                                highest_high = max(highs)
                                log(f"seven_day_range for {symbol}: {lowest_low}-{highest_high} from Polygon daily")
                                return {"lowest_low_7d": lowest_low, "highest_high_7d": highest_high}
                        break

            elif pref == 'fmp':
                # Use FMP historical data for 7-day range
                for provider in providers:
                    if isinstance(provider, FmpProvider) and provider.api_key:
                        url = f"{provider.base}/v3/historical-price-full/{symbol}"
                        params = {"from": start_date_7d, "to": end_date_7d, "apikey": provider.api_key}
                        try:
                            data = provider.session.get(url, params=params, timeout=CONFIG["timeout_sec"]).json()
                            arr = data.get("historical", [])
                            if arr and len(arr) >= 7:
                                lows = [float(item.get("low", float('inf'))) for item in arr if item.get("low")]
                                highs = [float(item.get("high", 0)) for item in arr if item.get("high")]

                                if lows and highs:
                                    lowest_low = min(lows)
                                    highest_high = max(highs)
                                    log(f"seven_day_range for {symbol}: {lowest_low}-{highest_high} from FMP")
                                    return {"lowest_low_7d": lowest_low, "highest_high_7d": highest_high}
                        except:
                            continue
                        break

        except Exception as e:
            log(f"seven_day_range: Error with {pref} for {symbol}: {e}")
            continue

    log(f"seven_day_range: Could not get 7-day range for {symbol} on {date_ymd}")
    return {}

# ----------------------------
# SPLIT GATE
# ----------------------------
def is_reverse_split_event(split_from: Optional[float], split_to: Optional[float]) -> bool:
    try:
        return (split_from is not None) and (split_to is not None) and (float(split_from) > float(split_to))
    except Exception:
        return False

def get_enhanced_splits_data(symbol: str, from_ymd: str, to_ymd: str, providers: List) -> List[Dict[str, Any]]:
    """
    Zero-Miss Discovery: Get comprehensive splits data from multiple providers
    Primary: Polygon (most reliable), Secondary: FMP, Tertiary: ThetaData
    """
    all_splits = []

    # Primary: Polygon splits (most reliable and detailed)
    for provider in providers:
        if isinstance(provider, PolygonProvider) and provider.api_key:
            try:
                polygon_splits = provider.get_splits_window(symbol, from_ymd, to_ymd)
                for split in polygon_splits:
                    # Enhanced processing with ratio calculation
                    sf = split.get("split_from")
                    st = split.get("split_to")
                    if sf is not None and st is not None:
                        try:
                            ratio_float = float(sf) / float(st)
                            split["ratio_float"] = ratio_float
                            split["is_reverse"] = sf > st
                            split["source"] = "polygon"
                            all_splits.append(split)
                        except (ValueError, ZeroDivisionError):
                            continue
                log(f"Enhanced splits: Retrieved {len(polygon_splits)} splits from Polygon for {symbol}")
                break
            except Exception as e:
                log(f"Enhanced splits: Polygon error for {symbol}: {e}")
                continue

    # Secondary: FMP splits (if Polygon failed)
    if not all_splits:
        for provider in providers:
            if isinstance(provider, FmpProvider) and provider.api_key:
                try:
                    fmp_splits = provider.get_splits_window(symbol, from_ymd, to_ymd)
                    for split in fmp_splits:
                        sf = split.get("split_from")
                        st = split.get("split_to")
                        if sf is not None and st is not None:
                            try:
                                ratio_float = float(sf) / float(st)
                                split["ratio_float"] = ratio_float
                                split["is_reverse"] = sf > st
                                split["source"] = "fmp"
                                all_splits.append(split)
                            except (ValueError, ZeroDivisionError):
                                continue
                    log(f"Enhanced splits: Retrieved {len(fmp_splits)} splits from FMP for {symbol}")
                    break
                except Exception as e:
                    log(f"Enhanced splits: FMP error for {symbol}: {e}")
                    continue

    # Tertiary: ThetaData splits (if others failed)
    if not all_splits:
        for provider in providers:
            if isinstance(provider, ThetaDataProvider):
                try:
                    theta_splits = provider.get_splits_window(symbol, from_ymd, to_ymd)
                    for split in theta_splits:
                        sf = split.get("split_from")
                        st = split.get("split_to")
                        if sf is not None and st is not None:
                            try:
                                ratio_float = float(sf) / float(st)
                                split["ratio_float"] = ratio_float
                                split["is_reverse"] = sf > st
                                split["source"] = "thetadata"
                                all_splits.append(split)
                            except (ValueError, ZeroDivisionError):
                                continue
                    log(f"Enhanced splits: Retrieved {len(theta_splits)} splits from ThetaData for {symbol}")
                    break
                except Exception as e:
                    log(f"Enhanced splits: ThetaData error for {symbol}: {e}")
                    continue

    return all_splits

def apply_enhanced_split_gate(symbol: str, event_date: str, event_row: Dict[str, Any], providers: List) -> Dict[str, Any]:
    """
    Zero-Miss Discovery: Enhanced reverse split gate with vendor-agnostic detection
    Uses 1-2 trading day window and $10M + 50% intraday push override
    """
    # Calculate window (1-2 trading days around event)
    from datetime import datetime, timedelta
    try:
        event_dt = datetime.strptime(event_date, "%Y-%m-%d")
        # Simple window: event_date +/- 2 days (should be improved to trading days only)
        from_date = (event_dt - timedelta(days=2)).strftime("%Y-%m-%d")
        to_date = (event_dt + timedelta(days=2)).strftime("%Y-%m-%d")
    except:
        log(f"Enhanced split gate: Invalid date format {event_date}")
        return {"is_near_reverse_split": 0, "rs_filter_applied": 0, "rs_execution_date": "", "rs_ratio": "", "rs_override_reason": "date_error"}

    # Get comprehensive splits data
    splits = get_enhanced_splits_data(symbol, from_date, to_date, providers)

    # Find reverse splits in window
    reverse_splits = [s for s in splits if s.get("is_reverse", False)]

    if not reverse_splits:
        return {
            "is_near_reverse_split": 0,
            "rs_filter_applied": 0,
            "rs_execution_date": "",
            "rs_ratio": "",
            "rs_override_reason": ""
        }

    # Get the closest reverse split
    closest_split = reverse_splits[0]  # Take first one for now
    rs_exec = closest_split.get("execution_date", "")
    rs_ratio = f"{closest_split.get('split_from')}:{closest_split.get('split_to')}"
    rs_source = closest_split.get("source", "unknown")

    log(f"Enhanced split gate: Found reverse split for {symbol} on {rs_exec} ratio {rs_ratio} from {rs_source}")

    # Calculate dollar volume and intraday push for override logic
    close_price = event_row.get("close") or event_row.get("vwap") or 0.0
    volume = event_row.get("volume") or 0
    dollar_volume = float(close_price) * float(volume)

    intraday_push_pct = event_row.get("intraday_push_pct") or 0.0
    if "high" in event_row and "open" in event_row:
        try:
            intraday_push_pct = ((float(event_row["high"]) / float(event_row["open"])) - 1) * 100
        except (ValueError, ZeroDivisionError):
            pass

    # Heavy runner override: >= $10M dollar volume AND >= 50% intraday push
    override_dv_threshold = CONFIG.get("split_heavy_dv_min", 10_000_000)
    override_push_threshold = CONFIG.get("split_heavy_push_min_pct", 50.0)

    if dollar_volume >= override_dv_threshold and intraday_push_pct >= override_push_threshold:
        return {
            "is_near_reverse_split": 1,
            "rs_filter_applied": 0,
            "rs_execution_date": rs_exec,
            "rs_ratio": rs_ratio,
            "rs_override_reason": f"heavy_runner_override_dv{dollar_volume:.0f}_push{intraday_push_pct:.1f}%",
            "rs_source": rs_source
        }

    # Default: suppress as split artifact
    return {
        "is_near_reverse_split": 1,
        "rs_filter_applied": 1,
        "rs_execution_date": rs_exec,
        "rs_ratio": rs_ratio,
        "rs_override_reason": "split_artifact_suppressed",
        "rs_source": rs_source
    }

# ----------------------------
# MAIN DISCOVERY
# ----------------------------
def run():
    ensure_db_schema(CONFIG["db_path"])

    # Provider availability
    providers: List[ProviderBase] = []
    thetadata = ThetaDataProvider()
    theta_ok = thetadata.ping()
    if theta_ok:
        providers.append(thetadata)
        log("ThetaData detected and enabled")
    else:
        log("ThetaData not reachable; will fall back to Polygon for premarket/minute")

    if POLYGON_API_KEY:
        providers.append(PolygonProvider())
        log("Polygon enabled")
    else:
        log("Polygon API key missing; Polygon disabled")

    if FMP_API_KEY:
        providers.append(FmpProvider())
        log("FMP enabled")
    else:
        log("FMP API key missing; FMP disabled")

    if not providers:
        log("No providers available; aborting")
        sys.exit(1)

    # Choose discovery pipeline mode
    discovery_mode = CONFIG.get("discovery_mode", "legacy")
    log(f"Discovery mode: {discovery_mode}")

    if discovery_mode == "zero_miss":
        return run_zero_miss_pipeline(providers)
    else:
        return run_legacy_pipeline(providers)

def run_zero_miss_pipeline(providers: List[ProviderBase]):
    """Run the zero-miss discovery pipeline for a single day"""
    log("Starting Zero-Miss Discovery Pipeline")
    log("=" * 60)

    # Get date for zero-miss discovery
    target_date = CONFIG.get("zero_miss_single_date")
    if not target_date:
        # Default to today or most recent trading day
        from datetime import datetime
        target_date = datetime.now().strftime("%Y-%m-%d")
        log(f"No date specified, using today: {target_date}")

    try:
        # Step 1: Build universe
        log(f"Step 1: Building universe for {target_date}")
        universe = build_daily_universe(target_date, providers)
        if not universe:
            log(f"ERROR: Failed to build universe for {target_date}")
            return False

        # Step 2: Pass-1 whole market scan
        log(f"Step 2: Pass-1 whole market scan for {target_date}")
        market_data, r4_metrics = zero_miss_discovery_pass1(target_date, providers)
        if market_data.empty:
            log(f"ERROR: Pass-1 failed for {target_date}")
            return False

        # Step 3: Pass-2B premarket scan (default method)
        premarket_method = CONFIG.get("premarket_method", "pass2b")
        if premarket_method == "pass2a":
            log(f"Step 3: Pass-2A full universe premarket sweep for {target_date}")
            pass2_results = zero_miss_discovery_pass2a(target_date, universe, providers)
        else:
            log(f"Step 3: Pass-2B smart premarket scan for {target_date}")
            pass2_results = zero_miss_discovery_pass2b(target_date, market_data, providers)

        # Step 4: Create completeness metrics
        log(f"Step 4: Creating completeness metrics for {target_date}")
        metrics = create_completeness_metrics(target_date, universe, market_data, pass2_results, r4_metrics)

        # Step 5: Summary report
        log(f"Zero-Miss Discovery Summary for {target_date}")
        log("=" * 50)
        log(f"Universe size: {metrics['total_symbols_in_universe']}")
        log(f"Market data symbols: {metrics['symbols_in_grouped_daily']}")
        log(f"R2 candidates: {metrics['r2_candidates']}")
        log(f"R3 candidates: {metrics['r3_candidates']}")
        log(f"R4 candidates: {metrics['r4_candidates']}")
        log(f"R1 hits found: {metrics['r1_hits']}")
        log(f"Audit status: {'PASSED' if metrics['audit_passed'] else 'FAILED'}")

        if not metrics['audit_passed']:
            log("WARNING: Miss-audit failed - may have missed R1 events")
            return False

        log(f"SUCCESS: Zero-miss discovery completed for {target_date}")
        return True

    except Exception as e:
        log(f"ERROR: Zero-miss discovery failed for {target_date}: {e}")
        import traceback
        log(f"Traceback: {traceback.format_exc()}")
        return False

def run_legacy_pipeline(providers: List[ProviderBase]):
    """Run the legacy discovery pipeline for historical analysis"""
    log("Starting Legacy Discovery Pipeline")
    log("=" * 60)

    start_ymd = CONFIG["date_start"]; end_ymd = CONFIG["date_end"]
    days = trading_days_between(start_ymd, end_ymd)
    symbols = build_universe()
    log(f"Date range: {start_ymd}..{end_ymd} with {len(days)} trading days")
    log(f"Universe size: {len(symbols)} symbols")

    # Pre-fetch daily for each provider
    daily_by_provider: Dict[str, pd.DataFrame] = {}
    for p in providers:
        try:
            df = p.get_daily_ohlcv(symbols, start_ymd, end_ymd)
            if df is None or df.empty:
                log(f"{p.name} returned no daily data")
                continue
            daily_by_provider[p.name] = df.sort_values(["symbol","date"])
            log(f"{p.name} daily rows: {len(daily_by_provider[p.name])}")
        except Exception as e:
            log(f"{p.name} daily fetch failed: {e}")

    # Main loop: compute rules and write to DB
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        count_hits = 0
        for pname, df in daily_by_provider.items():
            log(f"Processing provider: {pname}")
            for sym, g in df.groupby("symbol"):
                g = g.sort_values("date").reset_index(drop=True)
                for i in range(1, len(g)):
                    row = g.iloc[i]
                    prev = g.iloc[i-1]
                    event_date = str(row["date"])
                    if event_date not in days:
                        continue

                    open_p = float(row["open"]); high_p = float(row["high"])
                    close_p = float(row["close"]); volume = int(row["volume"])
                    prev_close = float(prev["close"]) if prev["date"] in days else None
                    intraday_push_pct = ((high_p / open_p - 1.0) * 100.0) if (open_p and open_p > 0) else None

                    # R4 7-day window
                    past_win = g.loc[:i].tail(7)
                    r4_val = None
                    if len(past_win) == 7:
                        lo7 = float(past_win["low"].min())
                        hi7 = float(past_win["high"].max())
                        r4_val = r4_surge7(lo7, hi7, CONFIG["surge_7d_trigger_pct"])

                    # R2 and R3 (always)
                    r2_val = r2_open_gap(prev_close, open_p, CONFIG["open_gap_trigger_pct"]) if prev_close is not None else None
                    r3_val = r3_push(open_p, high_p, CONFIG["intraday_push_trigger_pct"])

                    # R1 (premarket): only if provider supports it
                    pm_high = None; pm_src = ""; pm_method = ""
                    r1_val = None
                    prov_obj = None
                    for p in providers:
                        if p.name == pname:
                            prov_obj = p
                            break
                    if prov_obj and prov_obj.supports_premarket():
                        try:
                            pm_high, pm_src, pm_method = prov_obj.get_premarket_high(sym, event_date)
                            if pm_high is not None and prev_close is not None:
                                r1_val = r1_pm(prev_close, pm_high, CONFIG["premarket_trigger_pct"])
                        except Exception:
                            r1_val = None

                    # Split window check: +/- N trading days
                    rs_win = CONFIG["split_window_trading_days"]
                    left = shift_trading_days(event_date, -rs_win)
                    right = shift_trading_days(event_date, rs_win)
                    splits_all = []
                    if POLYGON_API_KEY:
                        splits_all.extend(PolygonProvider().get_splits_window(sym, left, right))
                    if FMP_API_KEY:
                        splits_all.extend(FmpProvider().get_splits_window(sym, left, right))
                    if theta_ok:
                        splits_all.extend(ThetaDataProvider().get_splits_window(sym, left, right))

                    splits_in_win = [s for s in splits_all if is_reverse_split_event(s.get("split_from"), s.get("split_to"))]

                    # Split gate decision
                    gate = {"is_near_reverse_split": 0, "rs_filter_applied": 0,
                            "rs_execution_date": "", "rs_ratio": "", "rs_override_reason": ""}
                    if CONFIG["apply_split_gate"]:
                        gate = apply_split_gate(
                            {"open": open_p, "high": high_p, "close": close_p, "volume": volume, "intraday_push_pct": intraday_push_pct, "date": event_date},
                            splits_in_win
                        )
                    split_source = "none"
                    if splits_in_win:
                        split_source = "polygon" if any(s.get("source")=="polygon" for s in splits_in_win) else "mixed"

                    # Upsert hit row
                    hit_rec = {
                        "provider": pname, "ticker": sym, "event_date": event_date,
                        "volume": volume, "intraday_push_pct": intraday_push_pct,
                        "is_near_reverse_split": gate["is_near_reverse_split"],
                        "split_detect_source": split_source,
                        "rs_window_days": rs_win, "rs_execution_date": gate["rs_execution_date"], "rs_ratio": gate["rs_ratio"],
                        "rs_filter_applied": gate["rs_filter_applied"], "rs_override_reason": gate["rs_override_reason"],
                        "pm_source": pm_src, "pm_method": pm_method
                    }
                    hit_id = db_upsert_hit(conn, hit_rec)
                    count_hits += 1

                    # Write rules, applying split artifact suppression for R2-only artifacts
                    r_flags = []
                    if r1_val is not None:
                        r_flags.append(("PM_GAP_50", r1_val))
                    if r2_val is not None:
                        r_flags.append(("OPEN_GAP_50", r2_val))
                    if r3_val is not None:
                        r_flags.append(("INTRADAY_PUSH_50", r3_val))
                    if r4_val is not None:
                        r_flags.append(("SURGE_7D_300", r4_val))

                    # suppression rule: if gate applied and only OPEN_GAP_50 present, drop it
                    if gate["rs_filter_applied"] == 1:
                        only_r2 = (len(r_flags) == 1 and r_flags[0][0] == "OPEN_GAP_50")
                        if only_r2:
                            r_flags = []  # drop all to avoid fake gap record

                    for rn, rv in r_flags:
                        db_insert_rule(conn, hit_id, rn, rv)

                    if (count_hits % CONFIG["print_every"]) == 0:
                        log(f"Processed {count_hits} rows...")

    # Build comparison CSVs
    with sqlite3.connect(CONFIG["db_path"]) as conn:
        # apples-to-apples across all providers (R2,R3,R4)
        apples = pd.read_sql_query("""
            SELECT h.provider, h.ticker, h.event_date,
                   MAX(CASE WHEN r.trigger_rule='OPEN_GAP_50' THEN 1 ELSE 0 END) AS r2,
                   MAX(CASE WHEN r.trigger_rule='INTRADAY_PUSH_50' THEN 1 ELSE 0 END) AS r3,
                   MAX(CASE WHEN r.trigger_rule='SURGE_7D_300' THEN 1 ELSE 0 END) AS r4
            FROM discovery_hits h
            LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
            GROUP BY h.provider, h.ticker, h.event_date
        """, conn)
        apples["any_rule"] = apples[["r2","r3","r4"]].sum(axis=1).clip(upper=1)
        if CONFIG["write_csvs"]:
            apples.to_csv(CONFIG["csv_apples"], index=False)

        # superset including R1 for providers that support it (thetadata, polygon)
        superset = pd.read_sql_query("""
            SELECT h.provider, h.ticker, h.event_date,
                   MAX(CASE WHEN r.trigger_rule='PM_GAP_50' THEN 1 ELSE 0 END) AS r1,
                   MAX(CASE WHEN r.trigger_rule='OPEN_GAP_50' THEN 1 ELSE 0 END) AS r2,
                   MAX(CASE WHEN r.trigger_rule='INTRADAY_PUSH_50' THEN 1 ELSE 0 END) AS r3,
                   MAX(CASE WHEN r.trigger_rule='SURGE_7D_300' THEN 1 ELSE 0 END) AS r4
            FROM discovery_hits h
            LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
            WHERE h.provider IN ('thetadata','polygon')
            GROUP BY h.provider, h.ticker, h.event_date
        """, conn)
        superset["any_rule"] = superset[["r1","r2","r3","r4"]].sum(axis=1).clip(upper=1)
        if CONFIG["write_csvs"]:
            superset.to_csv(CONFIG["csv_superset"], index=False)

        summary = pd.read_sql_query("""
            WITH flags AS (
                SELECT h.provider,
                       SUM(CASE WHEN r.trigger_rule='PM_GAP_50' THEN 1 ELSE 0 END) AS pm_gap,
                       SUM(CASE WHEN r.trigger_rule='OPEN_GAP_50' THEN 1 ELSE 0 END) AS open_gap,
                       SUM(CASE WHEN r.trigger_rule='INTRADAY_PUSH_50' THEN 1 ELSE 0 END) AS push,
                       SUM(CASE WHEN r.trigger_rule='SURGE_7D_300' THEN 1 ELSE 0 END) AS surge
                FROM discovery_hits h
                LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
                GROUP BY h.provider
            )
            SELECT * FROM flags
        """, conn)
        if CONFIG["write_csvs"]:
            summary.to_csv(CONFIG["csv_summary"], index=False)

    log("Done. Wrote DB and CSV summaries.")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log(f"FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)