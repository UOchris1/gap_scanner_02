# -*- coding: ascii -*-
# Fundamentals data provider using FMP and Polygon APIs

import os as _os
import requests as _requests
from datetime import datetime as _datetime
from typing import Optional as _Optional, Tuple as _Tuple, Dict as _Dict
from dotenv import load_dotenv as _load_dotenv
from pathlib import Path as _Path

# Load .env from project root (handle running from any directory)
_project_root = _Path(__file__).parent.parent.parent
_env_path = _project_root / ".env"
_load_dotenv(_env_path)

# Load API keys at module level
_FMP_API_KEY = _os.getenv("FMP_API_KEY", "").strip()
_POLYGON_API_KEY = _os.getenv("POLYGON_API_KEY", "").strip()

def _log(msg: str) -> None:
    print(f"[FUNDAMENTALS] {msg}")

class FundamentalsProvider:
    """Provider for historical float shares and market cap data."""

    def __init__(self):
        self.fmp_api_key = _FMP_API_KEY
        self.polygon_api_key = _POLYGON_API_KEY

        if not self.fmp_api_key:
            _log("WARNING: FMP_API_KEY not found in environment")
        if not self.polygon_api_key:
            _log("WARNING: POLYGON_API_KEY not found in environment")

    def get_historical_fundamentals(self, symbol: str, as_of_date: str) -> _Tuple[_Optional[float], _Optional[float], _Optional[float], str]:
        """
        Get historical float shares and market cap as of a specific date.
        Returns: (shares_outstanding, market_cap, float_shares, data_source)
        """
        # Try FMP first
        if self.fmp_api_key:
            result = self._get_fmp_fundamentals(symbol, as_of_date)
            if result[0] is not None or result[1] is not None:
                return result

        # Fallback to Polygon
        if self.polygon_api_key:
            result = self._get_polygon_fundamentals(symbol, as_of_date)
            if result[0] is not None or result[1] is not None:
                return result

        _log(f"No fundamental data found for {symbol} as of {as_of_date}")
        return None, None, None, "no_data"

    def _get_fmp_fundamentals(self, symbol: str, as_of_date: str) -> _Tuple[_Optional[float], _Optional[float], _Optional[float], str]:
        """Get fundamentals from Financial Modeling Prep API as of specific date."""
        try:
            # Use key-metrics endpoint with date filtering for as-of date accuracy
            url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}"
            params = {
                "limit": 5,  # Get multiple entries to find closest to as_of_date
                "apikey": self.fmp_api_key
            }

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                _log(f"FMP API error {response.status_code} for {symbol}")
                return None, None, None, "fmp_error"

            data = response.json()
            if not data:
                _log(f"FMP: No data for {symbol}")
                return None, None, None, "fmp_no_data"

            # Find entry closest to as_of_date (but not after)
            from datetime import datetime
            as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")

            best_entry = None
            best_diff = float('inf')

            for entry in data:
                entry_date_str = entry.get("date", "")
                if entry_date_str:
                    try:
                        entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d")
                        # Only consider entries on or before as_of_date
                        if entry_dt <= as_of_dt:
                            diff = (as_of_dt - entry_dt).days
                            if diff < best_diff:
                                best_diff = diff
                                best_entry = entry
                    except Exception:
                        continue

            # Fall back to most recent if no suitable entry found
            if best_entry is None:
                best_entry = data[0]

            market_cap = best_entry.get("marketCap")

            # FMP key-metrics doesn't have shares outstanding, but we have market cap
            # Prioritize as-of-date shares from enterprise values
            shares_outstanding = None

            # First try: get shares outstanding as-of-date from enterprise values
            shares_outstanding = self._get_fmp_shares_as_of(symbol, as_of_date)

            # Second try: get shares outstanding from profile endpoint
            if shares_outstanding is None:
                shares_outstanding = self._get_fmp_shares_from_profile(symbol)

            # If no shares from profile, calculate from market cap and as-of-date price
            if shares_outstanding is None and market_cap:
                # Try to get price as of the specific date
                price = self._get_fmp_price_as_of_date(symbol, as_of_date)
                if price and price > 0:
                    shares_outstanding = market_cap / price
                    _log(f"FMP: Calculated shares from market cap / as-of price: {shares_outstanding}")
                else:
                    # Fallback to current price if historical price not available
                    price = self._get_fmp_current_price(symbol)
                    if price and price > 0:
                        shares_outstanding = market_cap / price
                        _log(f"FMP: Calculated shares from market cap / current price (fallback): {shares_outstanding}")

            # Calculate float shares (use shares outstanding as approximation)
            float_shares = shares_outstanding

            _log(f"FMP: Retrieved data for {symbol} - Market Cap: {market_cap}, Shares: {shares_outstanding}")
            return shares_outstanding, market_cap, float_shares, "fmp"

        except Exception as e:
            _log(f"FMP API exception for {symbol}: {str(e)}")
            return None, None, None, "fmp_exception"

    def _get_fmp_shares_outstanding(self, symbol: str, as_of_date: str) -> _Optional[float]:
        """Get shares outstanding from FMP enterprise value endpoint."""
        try:
            url = "https://financialmodelingprep.com/api/v3/historical-enterprise-value"
            params = {
                "symbol": symbol,
                "from": as_of_date,
                "to": as_of_date,
                "apikey": self.fmp_api_key
            }

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            if not data:
                return None

            entry = data[0]
            return entry.get("numberOfShares")

        except Exception:
            return None

    def _get_fmp_shares_as_of(self, symbol: str, as_of_date: str) -> _Optional[float]:
        """Get shares outstanding as-of-date using FMP enterprise values endpoint."""
        return self._get_fmp_shares_outstanding(symbol, as_of_date)

    def _get_fmp_shares_from_profile(self, symbol: str) -> _Optional[float]:
        """Get shares outstanding from FMP company profile endpoint."""
        try:
            url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
            params = {"apikey": self.fmp_api_key}

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            if not data:
                return None

            profile = data[0]
            return profile.get("sharesOutstanding")

        except Exception:
            return None

    def _get_fmp_current_price(self, symbol: str) -> _Optional[float]:
        """Get current price from FMP profile endpoint."""
        try:
            url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}"
            params = {"apikey": self.fmp_api_key}

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            if not data:
                return None

            profile = data[0]
            return profile.get("price")

        except Exception:
            return None

    def _get_fmp_price_as_of_date(self, symbol: str, as_of_date: str) -> _Optional[float]:
        """Get historical price from FMP as of specific date."""
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
            params = {
                "from": as_of_date,
                "to": as_of_date,
                "apikey": self.fmp_api_key
            }

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            historical = data.get("historical", [])
            if not historical:
                return None

            # Get close price for the specific date
            day_data = historical[0]
            return day_data.get("close")

        except Exception:
            return None

    def _get_polygon_fundamentals(self, symbol: str, as_of_date: str) -> _Tuple[_Optional[float], _Optional[float], _Optional[float], str]:
        """Get fundamentals from Polygon API (fallback)."""
        try:
            # Get most recent financials
            url = f"https://api.polygon.io/vX/reference/financials"
            params = {
                "ticker": symbol,
                "limit": 1,
                "sort": "filing_date",
                "order": "desc",
                "apikey": self.polygon_api_key
            }

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                _log(f"Polygon API error {response.status_code} for {symbol}")
                return None, None, None, "polygon_error"

            data = response.json()
            results = data.get("results", [])

            if not results:
                _log(f"Polygon: No financials for {symbol}")
                return None, None, None, "polygon_no_data"

            # Use the most recent filing
            financials = results[0].get("financials", {})

            # Get shares outstanding from income statement (correct location)
            income_statement = financials.get("income_statement", {})

            # Try different possible field names for shares
            shares_outstanding = None

            # Check for diluted average shares (confirmed field)
            if "diluted_average_shares" in income_statement:
                shares_outstanding = income_statement["diluted_average_shares"].get("value")
            # Fallback to basic average shares
            elif "basic_average_shares" in income_statement:
                shares_outstanding = income_statement["basic_average_shares"].get("value")
            # Check weighted average shares
            elif "weighted_average_shares_outstanding" in income_statement:
                shares_outstanding = income_statement["weighted_average_shares_outstanding"].get("value")

            # Calculate market cap using ticker API if we have shares
            market_cap = None
            if shares_outstanding:
                market_cap = self._get_polygon_market_cap(symbol, as_of_date, shares_outstanding)

            # Use shares outstanding as float approximation
            float_shares = shares_outstanding

            _log(f"Polygon: Retrieved data for {symbol} - Market Cap: {market_cap}, Shares: {shares_outstanding}")
            return shares_outstanding, market_cap, float_shares, "polygon"

        except Exception as e:
            _log(f"Polygon API exception for {symbol}: {str(e)}")
            return None, None, None, "polygon_exception"

    def _get_polygon_market_cap(self, symbol: str, as_of_date: str, shares_outstanding: float) -> _Optional[float]:
        """Calculate market cap using Polygon ticker price data."""
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{as_of_date}/{as_of_date}"
            params = {"apikey": self.polygon_api_key}

            response = _requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            results = data.get("results", [])

            if not results:
                return None

            close_price = results[0].get("c")  # Close price
            if close_price and shares_outstanding:
                return close_price * shares_outstanding

            return None

        except Exception:
            return None

def get_fundamentals_for_hit(symbol: str, event_date: str) -> _Dict[str, any]:
    """
    Convenience function to get fundamentals for a discovery hit.
    Returns dict with all fundamental fields for database insertion.
    """
    provider = FundamentalsProvider()
    shares_outstanding, market_cap, float_shares, data_source = provider.get_historical_fundamentals(symbol, event_date)

    return {
        "shares_outstanding": shares_outstanding,
        "market_cap": market_cap,
        "float_shares": float_shares,
        "data_source": data_source
    }

def validate_fundamentals_around_split(symbol: str, event_date: str, split_exec_date: str) -> _Dict[str, any]:
    """
    Validate fundamentals before and after a split to detect split-adjusted vs non-adjusted data.
    Returns comparison data for validation.
    """
    provider = FundamentalsProvider()

    # Get fundamentals before split
    import datetime as dt
    split_date = dt.date.fromisoformat(split_exec_date)
    before_date = (split_date - dt.timedelta(days=1)).isoformat()
    after_date = (split_date + dt.timedelta(days=1)).isoformat()

    before_shares, before_mcap, before_float, before_source = provider.get_historical_fundamentals(symbol, before_date)
    after_shares, after_mcap, after_float, after_source = provider.get_historical_fundamentals(symbol, after_date)

    # Calculate ratios to detect split adjustments
    shares_ratio = None
    mcap_ratio = None
    float_ratio = None

    if before_shares and after_shares and before_shares > 0:
        shares_ratio = after_shares / before_shares
    if before_mcap and after_mcap and before_mcap > 0:
        mcap_ratio = after_mcap / before_mcap
    if before_float and after_float and before_float > 0:
        float_ratio = after_float / before_float

    return {
        "symbol": symbol,
        "split_date": split_exec_date,
        "before_date": before_date,
        "after_date": after_date,
        "before": {
            "shares": before_shares,
            "market_cap": before_mcap,
            "float": before_float,
            "source": before_source
        },
        "after": {
            "shares": after_shares,
            "market_cap": after_mcap,
            "float": after_float,
            "source": after_source
        },
        "ratios": {
            "shares_ratio": shares_ratio,
            "market_cap_ratio": mcap_ratio,
            "float_ratio": float_ratio
        },
        "validation_flags": {
            "shares_adjusted": shares_ratio and abs(shares_ratio - 1.0) > 0.1,  # >10% change suggests split adjustment
            "mcap_stable": mcap_ratio and abs(mcap_ratio - 1.0) < 0.1,  # Market cap should stay roughly same
            "data_available": bool(before_shares and after_shares)
        }
    }