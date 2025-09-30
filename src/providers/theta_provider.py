#!/usr/bin/env python3
# ASCII ONLY

"""
ThetaData provider (v3 primary, v1 fallback) with tier-aware semaphores
and robust premarket-high retrieval for R1.

Public API:
    p = ThetaDataProvider()
    if p.ok():
        pm_high = p.get_premarket_high("AAPL", "2025-09-11")

Environment variables (optional):
    THETA_V3_URL=http://127.0.0.1:25503
    THETA_V1_URL=http://127.0.0.1:25510
    THETA_VENUE=""        # omit or set a specific venue; empty -> composite
    THETA_TIMEOUT_SEC=30
    THETA_RETRIES=3
    THETA_BACKOFF_BASE=0.75
    THETA_V3_MAX_OUTSTANDING=2   # STANDARD=2, PRO=4
    THETA_V1_MAX_OUTSTANDING=2   # treat legacy similarly
    PM_START=04:00:00
    PM_END=09:29:59
"""

from __future__ import annotations
import os
import time
import json
import threading
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load .env from project root (handle running from any directory)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

# Load configuration at module level
THETA_V3_URL = os.getenv("THETA_V3_URL", "http://127.0.0.1:25503")
THETA_V1_URL = os.getenv("THETA_V1_URL", "http://127.0.0.1:25510")
# Default to composite tape unless explicitly set
THETA_VENUE = os.getenv("THETA_VENUE", "utp_cta").strip()
VENUE_ORDER = ("utp_cta", "nqb")

def _venues_to_try(cfg: Optional[str]) -> List[str]:
    v = (cfg or "").strip().lower()
    if v in VENUE_ORDER:
        other = [x for x in VENUE_ORDER if x != v]
        return [v] + other
    return list(VENUE_ORDER)
THETA_TIMEOUT_SEC = int(os.getenv("THETA_TIMEOUT_SEC", "30"))
THETA_RETRIES = int(os.getenv("THETA_RETRIES", "3"))
THETA_BACKOFF_BASE = float(os.getenv("THETA_BACKOFF_BASE", "0.75"))
THETA_V3_MAX_OUTSTANDING = int(os.getenv("THETA_V3_MAX_OUTSTANDING", "2"))
THETA_V1_MAX_OUTSTANDING = int(os.getenv("THETA_V1_MAX_OUTSTANDING", "2"))
PM_START = os.getenv("PM_START", "04:00:00")
PM_END = os.getenv("PM_END", "09:29:59")
THETA_MAX_472_LOGS = int(os.getenv("THETA_MAX_472_LOGS", "3"))


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[theta] {ts} {msg}")


# Helper functions removed - using module-level variables instead


def _ymd_nodash(date_iso: str) -> str:
    # "YYYY-MM-DD" -> "YYYYMMDD"
    return date_iso.replace("-", "")


def _et_hms_to_ms(hms: str) -> str:
    """Convert HH:MM:SS to milliseconds-since-midnight for v1 API."""
    hh, mm, ss = (int(x) for x in hms.split(":"))
    return str(((hh * 60 + mm) * 60 + ss) * 1000)


class _BoundedSession:
    """
    Wraps a requests.Session with a semaphore to enforce per-terminal
    outstanding-request caps (tier-aware).
    """

    def __init__(self, max_outstanding: int, timeout: int) -> None:
        self.sem = threading.Semaphore(max_outstanding if max_outstanding > 0 else 1)
        self.sess = requests.Session()
        self.timeout = timeout

        # Reasonable connection pooling
        adapter = HTTPAdapter(
            pool_connections=max(8, max_outstanding * 2),
            pool_maxsize=max(16, max_outstanding * 4),
            max_retries=Retry(
                total=0,                # we do our own retry/backoff
                backoff_factor=0,
                respect_retry_after_header=True,
            ),
        )
        self.sess.mount("http://", adapter)
        self.sess.mount("https://", adapter)

    def get(self, url: str, params: Dict[str, Any]) -> requests.Response:
        # Use semaphore with timeout to prevent indefinite blocking
        acquired = self.sem.acquire(timeout=self.timeout)
        if not acquired:
            raise TimeoutError(f"Failed to acquire semaphore within {self.timeout}s")
        try:
            return self.sess.get(url, params=params, timeout=self.timeout)
        finally:
            self.sem.release()


class ThetaDataProvider:
    """
    Auto-detect Theta v3 (primary) then v1 (fallback). Provides:
    - get_premarket_high(symbol, date_iso)
    """

    name = "thetadata"

    def __init__(self) -> None:
        # Config
        self.v3_base = THETA_V3_URL
        self.v1_base = THETA_V1_URL
        self.venue = THETA_VENUE
        self.timeout_sec = THETA_TIMEOUT_SEC
        self.retries = THETA_RETRIES
        self.backoff_base = THETA_BACKOFF_BASE

        # Tier-aware semaphores (per terminal)
        self.v3_limit = THETA_V3_MAX_OUTSTANDING  # STANDARD default
        self.v1_limit = THETA_V1_MAX_OUTSTANDING

        self.v3 = _BoundedSession(self.v3_limit, self.timeout_sec)
        self.v1 = _BoundedSession(self.v1_limit, self.timeout_sec)
        try:
            _log(f"init v3_limit={self.v3_limit} v1_limit={self.v1_limit} timeout={self.timeout_sec}")
        except Exception:
            pass

        # Detection flags
        self.v3_ok = self._probe_v3()
        self.v1_ok = self._probe_v1()
        if self.v3_ok:
            _log("ThetaData v3 detected (primary)")
        elif self.v1_ok:
            _log("ThetaData v1 detected (fallback)")
        else:
            _log("ThetaData not detected; R1 will be skipped gracefully")

        # Premarket window
        self.pm_start = PM_START
        self.pm_end = PM_END
        # Simple per-date diagnostics counters
        # { 'YYYY-MM-DD': { 'v3_utp_cta': {'200':n, '204':m, '472':k, 'other':z}, ... } }
        self._pm_diag: Dict[str, Dict[str, Dict[str, int]]] = {}

    # ---------- Public ----------

    def ok(self) -> bool:
        """Provider detected."""
        return bool(self.v3_ok or self.v1_ok)

    def get_premarket_high(self, symbol: str, event_ymd: str) -> Optional[float]:
        """
        Return premarket high price for R1 rule processing.
        Try composite (utp_cta) first, then nqb, across v3 then v1.
        """
        for ven in _venues_to_try(self.venue):
            if self.v3_ok:
                v3 = self._premarket_high_v3(symbol, event_ymd, override_venue=ven)
                if v3 is not None:
                    return v3
            if self.v1_ok:
                v1 = self._premarket_high_v1(symbol, event_ymd, override_venue=ven)
                if v1 is not None:
                    return v1
        # Minute-bar fallback using v1 OHLC (premarket slice)
        try:
            pmh = self._premarket_high_v1_ohlc(symbol, event_ymd)
            return pmh
        except Exception:
            return None
    def get_premarket_high_with_meta(self, symbol: str, event_ymd: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        """
        Return (pm_high, source, venue_label) using deterministic order:
        v3 utp_cta -> v3 nqb -> v1 utp_cta -> v1 nqb -> v1_ohlc_1m fallback.
        source in { 'v3_trades','v1_trades','v1_ohlc_1m' }.
        venue_label in { 'utp_cta','nqb','rth_false' }.
        """
        for ven in _venues_to_try(self.venue):
            if self.v3_ok:
                v3 = self._premarket_high_v3(symbol, event_ymd, override_venue=ven)
                if v3 is not None:
                    return v3, 'v3_trades', ven
        for ven in _venues_to_try(self.venue):
            if self.v1_ok:
                v1 = self._premarket_high_v1(symbol, event_ymd, override_venue=ven)
                if v1 is not None:
                    return v1, 'v1_trades', ven
        pmh = self._premarket_high_v1_ohlc(symbol, event_ymd)
        if pmh is not None:
            return pmh, 'v1_ohlc_1m', 'rth_false'
        return None, None, None
        return None

    # ---------- Internals ----------

    def _probe_v3(self) -> bool:
        try:
            url = f"{self.v3_base}/v3/stock/history/trade"
            params = {"symbol": "SPY", "date": "2025-01-02", "start_time": "09:30:00", "end_time": "09:30:01", "format": "json"}
            r = self.v3.get(url, params)
            return r.status_code in (200, 204, 400, 422)
        except Exception:
            return False

    def _probe_v1(self) -> bool:
        try:
            url = f"{self.v1_base}/v2/hist/stock/trade"
            params = {"root": "SPY", "start_date": "20250102", "end_date": "20250102", "start_time": "09:30:00", "end_time": "09:30:01"}
            r = self.v1.get(url, params)
            return r.status_code in (200, 204, 400, 422)
        except Exception:
            return False

    def _premarket_high_v3(self, symbol: str, date_iso: str, override_venue: Optional[str] = None) -> Optional[float]:
        """
        v3 stock/history/trade with venue=nqb, JSON format, time-sliced premarket.
        """
        url = f"{self.v3_base}/v3/stock/history/trade"
        params = {
            "symbol": symbol,
            "date": date_iso,  # dashed format for v3
            "start_time": self.pm_start,
            "end_time": self.pm_end,
            "format": "json",
        }
        ven = (override_venue or self.venue or "").lower()
        if ven in ("utp_cta", "nqb"):
            params["venue"] = ven
        return self._request_trade_max_price(self.v3, url, params, v=f"v3/{ven or 'default'}")

    def _premarket_high_v1(self, symbol: str, date_iso: str, override_venue: Optional[str] = None) -> Optional[float]:
        """
        v1/v2 hist/stock/trade with venue=nqb, time-sliced premarket.
        v1 requires start_time/end_time as milliseconds-since-midnight.
        """
        url = f"{self.v1_base}/v2/hist/stock/trade"
        params = {
            "root": symbol,
            "start_date": _ymd_nodash(date_iso),
            "end_date": _ymd_nodash(date_iso),
            "start_time": _et_hms_to_ms(self.pm_start),
            "end_time": _et_hms_to_ms(self.pm_end),
            "use_csv": "false",
            "pretty_time": "true",
        }
        ven = (override_venue or self.venue or "").lower()
        if ven in ("utp_cta", "nqb"):
            params["venue"] = ven
        return self._request_trade_max_price(self.v1, url, params, v=f"v1/{ven or 'default'}")

    def _premarket_high_v1_ohlc(self, symbol: str, date_iso: str) -> Optional[float]:
        """
        Fallback via v1 minute OHLC (rth=false); returns max minute high in 04:00:00-09:30:00.
        """
        try:
            url = f"{self.v1_base}/v2/hist/stock/ohlc"
            params = {
                "root": symbol,
                "start_date": _ymd_nodash(date_iso),
                "end_date": _ymd_nodash(date_iso),
                "ivl": 60000,
                "rth": "false",
            }
            r = self.v1.get(url, params)
            if r.status_code != 200:
                return None
            js = r.json() or {}
            rows = js.get("response") or []
            if not rows:
                return None

            def in_pm(ms: int) -> bool:
                return 4*3600*1000 <= ms <= (9*3600+30*60)*1000

            pm_high: Optional[float] = None
            for rec in rows:
                if not isinstance(rec, list) or len(rec) < 3:
                    continue
                try:
                    ms = int(rec[0])
                    if in_pm(ms):
                        h = rec[2]
                        if h is not None:
                            h = float(h)
                            pm_high = h if pm_high is None else max(pm_high, h)
                except Exception:
                    continue
            return pm_high
        except Exception:
            return None

    def _request_trade_max_price(
        self,
        bounded: _BoundedSession,
        url: str,
        params: Dict[str, Any],
        v: str,
    ) -> Optional[float]:
        """
        Core GET with retry/backoff and robust JSON/NDJSON handling.
        Returns max trade price or None.
        """
        attempts = self.retries if self.retries > 0 else 1
        delay = self.backoff_base if self.backoff_base > 0 else 0.5

        for i in range(attempts):
            count_472 = None
            try:
                resp = bounded.get(url, params)
                code = resp.status_code
                # Diagnostics counters per date + venue
                try:
                    d = params.get("date") or params.get("start_date")
                    if d and isinstance(d, str) and len(d) in (8, 10):
                        date_iso = d if len(d) == 10 else f"{d[:4]}-{d[4:6]}-{d[6:]}"
                        label = v.replace("/", "_")
                        dd = self._pm_diag.setdefault(date_iso, {})
                        cc = dd.setdefault(label, {"200": 0, "204": 0, "472": 0, "other": 0})
                        if code == 200:
                            cc["200"] += 1
                        elif code == 204:
                            cc["204"] += 1
                        elif code == 472:
                            cc["472"] += 1
                            count_472 = cc["472"]
                        else:
                            cc["other"] += 1
                except Exception:
                    pass

                # Map specific Theta error codes to retry strategies:
                # 429 OS_LIMIT - brief backoff and retry
                # 570 LARGE_REQUEST - reduce range or split request
                # 571 SERVER_STARTING - exponential backoff
                # 474 DISCONNECTED - reconnect logic
                # 502/503/504 - standard HTTP server errors
                if code in (429, 474, 570, 571, 502, 503, 504):
                    if code == 570:
                        _log(f"{v} LARGE_REQUEST {code}: request too large, retry {i+1}/{attempts}")
                    elif code == 571:
                        _log(f"{v} SERVER_STARTING {code}: server starting, retry {i+1}/{attempts}")
                    elif code == 474:
                        _log(f"{v} DISCONNECTED {code}: reconnect needed, retry {i+1}/{attempts}")
                    elif code == 429:
                        _log(f"{v} OS_LIMIT {code}: rate limited, retry {i+1}/{attempts}")
                    else:
                        _log(f"{v} transient {code}: retry {i+1}/{attempts}")
                    time.sleep(delay * (2 ** i))
                    continue

                if code == 204:
                    # No data for the window/day
                    return None

                if code == 472:
                    # 472 = NO_DATA - expected for symbols without premarket activity on nqb venue
                    log_472 = True
                    if THETA_MAX_472_LOGS >= 0:
                        seen = count_472 if count_472 is not None else 0
                        if seen > THETA_MAX_472_LOGS:
                            log_472 = False
                    if log_472:
                        _log(f"{v} non-200 472: No data found for your request")
                    return None

                if code != 200:
                    _log(f"{v} non-200 {code}: {resp.text[:160]}")
                    return None

                # Try JSON first
                pm_high = self._parse_trade_max_json(resp, v=v)
                if pm_high is not None:
                    return pm_high

                # If not JSON array, try NDJSON line parsing
                pm_high = self._parse_trade_max_ndjson(resp.text)
                return pm_high

            except requests.RequestException as e:
                _log(f"{v} request error: {e}")
                time.sleep(delay * (2 ** i))
            except Exception as e:
                _log(f"{v} parse error: {e}")
                return None

        return None

    def flush_pm_diag(self, date_iso: str) -> None:
        """Write per-day diagnostics counters to project_state/artifacts/pm_diag_{date}.json"""
        try:
            diag = self._pm_diag.get(date_iso)
            if not diag:
                return
            out_dir = project_root / "project_state" / "artifacts"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"pm_diag_{date_iso}.json"
            with open(out_path, "w", encoding="ascii", errors="replace") as f:
                json.dump({"date": date_iso, **diag}, f)
        except Exception:
            pass

    @staticmethod
    def _parse_trade_max_json(resp: requests.Response, v: str) -> Optional[float]:
        """
        Handle both v3 (array of objects with 'price') and v1 (header/response).
        """
        try:
            data = resp.json()
        except Exception:
            return None

        # v3: array of objects OR dict with 'response'
        if isinstance(data, list):
            prices = [float(x.get("price")) for x in data if isinstance(x, dict) and "price" in x]
            return max(prices) if prices else None

        if isinstance(data, dict):
            # v3 format: { price: [230.96, 229.50, ...], timestamp: [...], ... }
            if "price" in data and isinstance(data["price"], list):
                prices = [float(p) for p in data["price"] if p is not None]
                return max(prices) if prices else None

            # v3 sometimes: { response: [ { price: ... }, ... ] }
            if "response" in data and isinstance(data["response"], list):
                prices = []
                for rec in data["response"]:
                    if isinstance(rec, dict) and "price" in rec:
                        prices.append(float(rec["price"]))
                    elif isinstance(rec, list):
                        # v1-style numeric row? fall through to v1 parser
                        pass
                if prices:
                    return max(prices)

            # v1: header + response (numeric arrays, price at index 9)
            if "header" in data and "response" in data and isinstance(data["response"], list):
                idx_price = 9  # per v1 trade format
                prices = []
                for rec in data["response"]:
                    if isinstance(rec, list) and len(rec) > idx_price and rec[idx_price] is not None:
                        try:
                            prices.append(float(rec[idx_price]))
                        except Exception:
                            pass
                return max(prices) if prices else None

        return None

    @staticmethod
    def _parse_trade_max_ndjson(body: str) -> Optional[float]:
        """
        NDJSON fallback: each line is a JSON object; pick max price.
        """
        prices: List[float] = []
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "price" in obj and obj["price"] is not None:
                    prices.append(float(obj["price"]))
            except Exception:
                continue
        return max(prices) if prices else None


    # ---------- Optional helpers ----------

    def get_daily_ohlc_range(self, symbol: str, start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
        """
        Provide daily OHLC for a symbol over [start_iso, end_iso] using Theta v1 minute bars.
        Returns a list of {date, open, high, low, close, volume}. Skips days with no data.
        """
        if not self.v1_ok:
            return []

        from datetime import date as _date, timedelta as _td

        def _daterange(a: str, b: str):
            s = _date.fromisoformat(a)
            e = _date.fromisoformat(b)
            d = s
            while d <= e:
                yield d.isoformat()
                d += _td(days=1)

        out: List[Dict[str, Any]] = []
        url = f"{self.v1_base}/v2/hist/stock/ohlc"
        for ymd in _daterange(start_iso, end_iso):
            params: Dict[str, Any] = {
                "root": symbol,
                "start_date": _ymd_nodash(ymd),
                "end_date": _ymd_nodash(ymd),
                "ivl": 60000,   # 1-minute
                "rth": "false"  # include premarket/afterhours
            }
            try:
                r = self.v1.get(url, params)
                if r.status_code != 200:
                    continue
                js = r.json() or {}
                rows = js.get("response") or []
                if not rows:
                    continue
                # rows: [ms_of_day, open, high, low, close, volume, count, date]
                day_open = None
                day_close = None
                day_high = None
                day_low = None
                vol_sum = 0.0
                for i, rec in enumerate(rows):
                    if not isinstance(rec, list) or len(rec) < 6:
                        continue
                    o = rec[1]
                    h = rec[2]
                    l = rec[3]
                    c = rec[4]
                    v = rec[5]
                    if day_open is None and o is not None:
                        day_open = float(o)
                    if c is not None:
                        day_close = float(c)
                    if h is not None:
                        day_high = float(h) if day_high is None else max(day_high, float(h))
                    if l is not None:
                        day_low = float(l) if day_low is None else min(day_low, float(l))
                    try:
                        vol_sum += float(v or 0)
                    except Exception:
                        pass
                if day_open is not None and day_close is not None and day_high is not None and day_low is not None:
                    out.append({
                        "date": ymd,
                        "open": day_open,
                        "high": day_high,
                        "low": day_low,
                        "close": day_close,
                        "volume": int(vol_sum)
                    })
            except Exception:
                # Skip day on error
                continue
        return out


# Backward compatibility alias
ThetaDataClient = ThetaDataProvider

