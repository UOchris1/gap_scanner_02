# -*- coding: ascii -*-
# Polygon provider: grouped-daily (whole market), single-day prev close, and splits.

import os
import time
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root (handle running from any directory)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

POLY_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BASE = "https://api.polygon.io"
POLYGON_TIMEOUT_SEC = int(os.getenv("POLYGON_TIMEOUT_SEC", "8"))
POLYGON_RETRIES = int(os.getenv("POLYGON_RETRIES", "2"))
POLYGON_BACKOFF = float(os.getenv("POLYGON_BACKOFF", "0.5"))

def grouped_daily(date_iso: str, adjusted: bool = False, include_otc: bool = False, timeout_sec: int = 45, max_retries: int = 3, backoff: float = 1.5) -> List[Dict]:
    """
    Deterministic Polygon grouped-daily fetch. Never loops forever.
    Returns [] on valid-but-empty days.
    Raises RuntimeError only after bounded retries on transport/HTTP errors.
    """
    import time

    if not POLY_KEY:
        raise RuntimeError("polygon_api_key_missing")

    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_iso}"
    params = {
        "adjusted": "true" if adjusted else "false",
        "include_otc": "true" if include_otc else "false",
        "apiKey": POLY_KEY,
    }

    s = requests.Session()
    attempt = 0
    while True:
        attempt += 1
        try:
            r = s.get(url, params=params, timeout=timeout_sec)
            if r.status_code == 200:
                data = r.json() or {}
                rows = data.get("results", []) or []

                # Convert to consistent format
                out = []
                for row in rows:
                    if row.get("T") and not row.get("otc"):
                        out.append({
                            "symbol": row["T"],
                            "open": float(row["o"]),
                            "high": float(row["h"]),
                            "low": float(row["l"]),
                            "close": float(row["c"]),
                            "volume": int(row["v"]),
                            "vwap": float(row.get("vw", row["c"]))  # Use VWAP or fallback to close
                        })

                # Return rows as-is (caller filters). Do NOT spin on empty.
                return out

            # Non-200: backoff and retry until cap
            if attempt >= max_retries:
                raise RuntimeError(f"polygon_grouped_daily_failed:{r.status_code}")
            time.sleep(backoff ** attempt)

        except Exception as e:
            if attempt >= max_retries:
                raise RuntimeError(f"polygon_grouped_daily_error:{type(e).__name__}") from e
            time.sleep(backoff ** attempt)

def prev_close(symbol: str, prev_date_iso: str) -> Optional[float]:
    """Single-symbol previous close fetch with bounded timeout and retries."""
    if not POLY_KEY:
        return None
    url = f"{BASE}/v2/aggs/ticker/{symbol}/prev"
    params = {"adjusted": "false", "apiKey": POLY_KEY}
    for attempt in range(POLYGON_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=POLYGON_TIMEOUT_SEC)
            if r.status_code != 200:
                return None
            data = r.json() or {}
            results = data.get("results") or []
            if not results:
                return None
            close = results[0].get("c")
            return float(close) if close is not None else None
        except requests.exceptions.ReadTimeout:
            if attempt < POLYGON_RETRIES:
                time.sleep(POLYGON_BACKOFF * (2 ** attempt))
                continue
            return None
        except Exception:
            if attempt < POLYGON_RETRIES:
                time.sleep(POLYGON_BACKOFF * (2 ** attempt))
                continue
            return None
    return None



def prev_close_bulk_map(prev_date_iso: str) -> Dict[str, float]:
    """Return a {symbol: close} map using Polygon grouped-daily for the prior day."""
    values: Dict[str, float] = {}
    if not POLY_KEY:
        return values
    try:
        rows = grouped_daily(prev_date_iso, adjusted=False, include_otc=False)
    except Exception:
        rows = []
    for row in rows or []:
        sym = row.get('symbol') or row.get('ticker') or row.get('T')
        close = row.get('close') or row.get('c')
        if sym and close is not None:
            try:
                values[sym] = float(close)
            except Exception:
                continue
    return values

def splits(symbol: str, start_date: str = None, end_date: str = None) -> List[Dict]:
    """
    Corporate action splits with optional date filtering for reverse split gating.
    Returns splits with execution_date, split_from, split_to, and calculated ratios.
    """
    if not POLY_KEY:
        return []

    url = f"{BASE}/v3/reference/splits"
    params = {"ticker": symbol, "apiKey": POLY_KEY}

    # Add date filtering if provided
    if start_date:
        params["execution_date.gte"] = start_date
    if end_date:
        params["execution_date.lte"] = end_date

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return []

        data = r.json() or {}
        splits_data = data.get("results", []) or []

        enhanced_splits = []
        for s in splits_data:
            split_from = s.get("split_from")
            split_to = s.get("split_to")

            # Calculate split characteristics
            is_reverse = False
            split_ratio = None

            try:
                if split_from and split_to:
                    from_val = float(split_from)
                    to_val = float(split_to)

                    if from_val > 0 and to_val > 0:
                        is_reverse = from_val > to_val  # Reverse split if from > to
                        split_ratio = from_val / to_val

                enhanced_splits.append({
                    "execution_date": s.get("execution_date", ""),
                    "split_from": split_from,
                    "split_to": split_to,
                    "is_reverse_split": is_reverse,
                    "split_ratio": split_ratio
                })

            except (ValueError, TypeError, ZeroDivisionError):
                # Still include the split even if we can't calculate the ratio
                enhanced_splits.append({
                    "execution_date": s.get("execution_date", ""),
                    "split_from": split_from,
                    "split_to": split_to,
                    "is_reverse_split": False,
                    "split_ratio": None
                })

        return enhanced_splits

    except Exception:
        return []

def get_daily_ohlc_range(symbol: str, start_date: str, end_date: str) -> List[Dict]:
    """
    Get daily OHLC data for a symbol over a date range.
    Used as backbone for R4 seven-day surge calculations.
    Returns list of dicts: date, open, high, low, close, volume
    """
    if not POLY_KEY:
        return []

    try:
        url = f"{BASE}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {
            "adjusted": "false",
            "sort": "asc",
            "limit": 5000,
            "apiKey": POLY_KEY
        }

        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return []

        data = r.json() or {}
        results = data.get("results", []) or []

        daily_data = []
        for bar in results:
            try:
                # Convert timestamp to date
                from datetime import datetime
                timestamp = bar.get("t", 0)
                dt = datetime.fromtimestamp(timestamp / 1000)
                date_str = dt.strftime("%Y-%m-%d")

                daily_data.append({
                    "date": date_str,
                    "open": float(bar.get("o", 0)),
                    "high": float(bar.get("h", 0)),
                    "low": float(bar.get("l", 0)),
                    "close": float(bar.get("c", 0)),
                    "volume": int(bar.get("v", 0))
                })
            except (ValueError, TypeError, KeyError):
                continue

        return daily_data

    except Exception:
        return []

def get_universe_symbols(include_delisted=True, max_pages=40, timeout_sec=20):
    """
    Robust Polygon v3/reference/tickers pager with strict timeouts and page caps.
    - Carries apiKey across next_url pages.
    - Caps pages to avoid infinite loops.
    - Returns a list of dicts compatible with core_universe_management.
    """
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        return []

    base = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "limit": 1000,
        "order": "asc",
        "sort": "ticker"
    }
    # If include_delisted is False, restrict to active only
    if not include_delisted:
        params["active"] = "true"

    sess = requests.Session()
    results = []
    pages = 0
    next_url = base

    while next_url and pages < max_pages:
        # next_url from Polygon may already contain a cursor; make sure apiKey is present
        if "apiKey=" in next_url:
            r = sess.get(next_url, timeout=timeout_sec)
        else:
            p = dict(params)
            p["apiKey"] = api_key
            r = sess.get(next_url, params=p, timeout=timeout_sec)

        r.raise_for_status()
        data = r.json() or {}
        for row in data.get("results", []) or []:
            results.append({
                "symbol": row.get("ticker", ""),
                "market": row.get("market", ""),
                "type": row.get("type", ""),
                "active": bool(row.get("active", True)),
                "primary_exchange": row.get("primary_exchange", ""),
                "delisted_utc": row.get("delisted_utc")
            })

        next_url = data.get("next_url")
        if next_url and "apiKey=" not in next_url:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={api_key}"

        pages += 1

    return results

def daily_symbol(date_iso: str, symbol: str, api_key: str) -> tuple[float|None, float|None]:
    """Return (volume, vw) for one symbol/day, unadjusted. None,None on failure."""
    import requests
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{date_iso}/{date_iso}"
    params = {"adjusted":"false", "apiKey": api_key}
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None, None
        res = r.json().get("results", [])
        if not res:
            return None, None
        v = res[0].get("v")
        vw = res[0].get("vw")
        return v, vw
    except Exception:
        return None, None

# --- Exchange normalization (MIC -> bucket) ---

_MIC_TO_EX = {
    "XNYS": "NYSE",
    "XASE": "AMEX",      # NYSE American (AMEX)
    "XNAS": "NASDAQ",    # NASDAQ - All Markets
    "XNGS": "NASDAQ",    # NASDAQ Global Select
    "XNMS": "NASDAQ",    # NASDAQ Global Market (NMS)
    "XNCM": "NASDAQ",    # NASDAQ Capital Market
}

def normalize_exchange(mic: Optional[str]) -> Optional[str]:
    if not mic:
        return None
    try:
        return _MIC_TO_EX.get(mic.upper())
    except Exception:
        return None

def get_exchange(symbol: str, date_iso: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """
    Return (primary_exchange_mic, normalized_bucket) for a symbol.
    If date_iso is provided, query as-of that date to handle historical transfers.
    """
    if not POLY_KEY:
        return None, None

    url = f"{BASE}/v3/reference/tickers/{symbol}"
    params = {"apiKey": POLY_KEY}
    if date_iso:
        params["date"] = date_iso

    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return None, None
        res = (r.json() or {}).get("results") or {}
        mic = res.get("primary_exchange")
        return mic, normalize_exchange(mic)
    except Exception:
        return None, None

def get_symbol_meta(symbol: str, date_iso: Optional[str] = None) -> Dict[str, Optional[str]]:
    """
    Return a dict of symbol metadata (as-of date if provided):
    - primary_exchange (MIC)
    - exchange (normalized bucket: NYSE/NASDAQ/AMEX or None)
    - security_type (Polygon 'type', e.g., CS, WARRANT, RIGHT, UNIT, ETF, ADRC, ADRP, ADRR, ADRW, GDR)
    - ticker_suffix (if detectable from Polygon fields; else None)
    """
    if not POLY_KEY:
        return {}
    url = f"{BASE}/v3/reference/tickers/{symbol}"
    params = {"apiKey": POLY_KEY}
    if date_iso:
        params["date"] = date_iso
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return {}
        res = (r.json() or {}).get("results") or {}
        mic = res.get("primary_exchange")
        ex = normalize_exchange(mic)
        sec_type = res.get("type")
        # Polygon does not always expose an explicit suffix; keep placeholder for future use
        ticker_suffix = None
        return {
            "primary_exchange": mic,
            "exchange": ex,
            "security_type": sec_type,
            "ticker_suffix": ticker_suffix,
        }
    except Exception:
        return {}
