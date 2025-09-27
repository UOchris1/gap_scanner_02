#!/usr/bin/env python3
# ASCII ONLY

"""
ThetaData provider (v3 primary, v1 fallback) with tier-aware semaphores
and robust premarket-high retrieval for R1.

Public API:
    p = ThetaDataProvider()
    if p.ok():
        pm_high, source, method = p.get_premarket_high("AAPL", "2025-09-11")

Environment variables (optional):
    THETA_V3_URL=http://127.0.0.1:25503
    THETA_V1_URL=http://127.0.0.1:25510
    THETA_VENUE=nqb
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

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[theta] {ts} {msg}")


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v and isinstance(v, str) else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _ymd_nodash(date_iso: str) -> str:
    # "YYYY-MM-DD" -> "YYYYMMDD"
    return date_iso.replace("-", "")


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
        with self.sem:
            return self.sess.get(url, params=params, timeout=self.timeout)


class ThetaDataProvider:
    """
    Auto-detect Theta v3 (primary) then v1 (fallback). Provides:
    - get_premarket_high(symbol, date_iso)
    """

    name = "thetadata"

    def __init__(self) -> None:
        # Config
        self.v3_base = _env_str("THETA_V3_URL", "http://127.0.0.1:25503")
        self.v1_base = _env_str("THETA_V1_URL", "http://127.0.0.1:25510")
        self.venue = _env_str("THETA_VENUE", "nqb")
        self.timeout_sec = _env_int("THETA_TIMEOUT_SEC", 30)
        self.retries = _env_int("THETA_RETRIES", 3)
        self.backoff_base = _env_float("THETA_BACKOFF_BASE", 0.75)

        # Tier-aware semaphores (per terminal)
        self.v3_limit = _env_int("THETA_V3_MAX_OUTSTANDING", 2)  # STANDARD default
        self.v1_limit = _env_int("THETA_V1_MAX_OUTSTANDING", 2)

        self.v3 = _BoundedSession(self.v3_limit, self.timeout_sec)
        self.v1 = _BoundedSession(self.v1_limit, self.timeout_sec)

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
        self.pm_start = _env_str("PM_START", "04:00:00")
        self.pm_end = _env_str("PM_END", "09:29:59")

    # ---------- Public ----------

    def ok(self) -> bool:
        """Provider detected."""
        return bool(self.v3_ok or self.v1_ok)

    def get_premarket_high(self, symbol: str, date_iso: str) -> Tuple[Optional[float], str, str]:
        """
        Return (pm_high, provider_name, method_tag).
        Uses v3 trade history in [04:00:00, 09:29:59] ET and venue=nqb.
        Falls back to v1 trade history with same window.
        """
        # v3 primary
        if self.v3_ok:
            pmh = self._premarket_high_v3(symbol, date_iso)
            if pmh is not None:
                return pmh, self.name, "trade-v3"

        # v1 fallback
        if self.v1_ok:
            pmh = self._premarket_high_v1(symbol, date_iso)
            if pmh is not None:
                return pmh, self.name, "trade-v1"

        # Not available
        return None, self.name, "unavailable"

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

    def _premarket_high_v3(self, symbol: str, date_iso: str) -> Optional[float]:
        """
        v3 stock/history/trade with venue=nqb, JSON format, time-sliced premarket.
        """
        url = f"{self.v3_base}/v3/stock/history/trade"
        params = {
            "symbol": symbol,
            "date": date_iso,
            "start_time": self.pm_start,
            "end_time": self.pm_end,
            "venue": self.venue,
            "format": "json",
        }
        return self._request_trade_max_price(self.v3, url, params, v="v3")

    def _premarket_high_v1(self, symbol: str, date_iso: str) -> Optional[float]:
        """
        v1/v2 hist/stock/trade with venue=nqb, time-sliced premarket.
        """
        url = f"{self.v1_base}/v2/hist/stock/trade"
        params = {
            "root": symbol,
            "start_date": _ymd_nodash(date_iso),
            "end_date": _ymd_nodash(date_iso),
            "start_time": self.pm_start,
            "end_time": self.pm_end,
            "venue": self.venue,
            "use_csv": "false",
            "pretty_time": "true",
        }
        return self._request_trade_max_price(self.v1, url, params, v="v1")

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
            try:
                resp = bounded.get(url, params)
                code = resp.status_code

                # Transient / throttling / terminal-busy classes:
                # 429 (Too Many Requests), 570/571 (Terminal Busy/Locked),
                # 474 (Upstream venue error / request too large), 502/503/504
                if code in (429, 470, 471, 472, 473, 474, 570, 571, 502, 503, 504):
                    _log(f"{v} transient {code}: retry {i+1}/{attempts}")
                    time.sleep(delay * (2 ** i))
                    continue

                if code == 204:
                    # No data for the window/day
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


# Backward compatibility alias
ThetaDataClient = ThetaDataProvider