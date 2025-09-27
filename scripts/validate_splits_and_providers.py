# -*- coding: ascii -*-
# Split context validation and provider comparison for zero-miss gap scanner

import sys
import os
import json
import sqlite3
import csv
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root (scripts folder is one level down from root)
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

# Load API keys at module level
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
FMP_API_KEY = os.getenv("FMP_API_KEY", "").strip()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.providers.polygon_provider import splits as polygon_splits, prev_close as polygon_prev_close
from src.providers.theta_provider import ThetaDataClient
from enhanced_db_schema import ensure_enhanced_db_schema

def analyze_split_context(symbol: str, analysis_date: str, window_days: int = 3) -> Dict[str, Any]:
    """Analyze split context around a discovery event"""
    try:
        # Get splits from Polygon
        splits_data = polygon_splits(symbol)

        if not splits_data:
            return {
                "symbol": symbol,
                "analysis_date": analysis_date,
                "has_splits": False,
                "reverse_split_detected": False,
                "split_context": None
            }

        # Filter splits near the analysis date
        analysis_dt = datetime.strptime(analysis_date, "%Y-%m-%d")
        window_start = analysis_dt - timedelta(days=window_days)
        window_end = analysis_dt + timedelta(days=window_days)

        relevant_splits = []
        for split in splits_data:
            if split.get("execution_date"):
                try:
                    split_dt = datetime.strptime(split["execution_date"], "%Y-%m-%d")
                    if window_start <= split_dt <= window_end:
                        relevant_splits.append(split)
                except ValueError:
                    continue

        if not relevant_splits:
            return {
                "symbol": symbol,
                "analysis_date": analysis_date,
                "has_splits": True,
                "total_splits": len(splits_data),
                "reverse_split_detected": False,
                "split_context": None
            }

        # Analyze splits for reverse split pattern
        split_analysis = []
        reverse_split_found = False

        for split in relevant_splits:
            split_from = split.get("split_from", 0)
            split_to = split.get("split_to", 0)

            try:
                split_from = float(split_from) if split_from else 0
                split_to = float(split_to) if split_to else 0

                is_reverse = split_from > split_to if split_from and split_to else False
                if is_reverse:
                    reverse_split_found = True

                split_ratio = split_from / split_to if split_to and split_to > 0 else 0

                split_analysis.append({
                    "execution_date": split["execution_date"],
                    "split_from": split_from,
                    "split_to": split_to,
                    "is_reverse_split": is_reverse,
                    "split_ratio": round(split_ratio, 4),
                    "days_from_analysis": (datetime.strptime(split["execution_date"], "%Y-%m-%d") - analysis_dt).days
                })

            except (ValueError, TypeError, ZeroDivisionError):
                continue

        return {
            "symbol": symbol,
            "analysis_date": analysis_date,
            "has_splits": True,
            "total_splits": len(splits_data),
            "relevant_splits": len(relevant_splits),
            "reverse_split_detected": reverse_split_found,
            "split_context": split_analysis
        }

    except Exception as e:
        return {
            "symbol": symbol,
            "analysis_date": analysis_date,
            "error": str(e),
            "has_splits": False,
            "reverse_split_detected": False
        }

def compare_provider_data(symbol: str, test_date: str) -> Dict[str, Any]:
    """Compare data across different providers for validation"""
    comparison_result = {
        "symbol": symbol,
        "test_date": test_date,
        "timestamp": datetime.now().isoformat(),
        "providers": {}
    }

    # Test ThetaData
    try:
        theta = ThetaDataClient()
        if theta.ok():
            theta_start = datetime.now()

            prev_close = theta.get_prev_close_eod(symbol, test_date)
            pm_high = theta.get_premarket_high(symbol, test_date)

            theta_duration = (datetime.now() - theta_start).total_seconds() * 1000

            comparison_result["providers"]["theta"] = {
                "available": True,
                "version": theta.version,
                "prev_close": prev_close,
                "premarket_high": pm_high,
                "response_time_ms": round(theta_duration, 2),
                "success": prev_close is not None or pm_high is not None
            }
        else:
            comparison_result["providers"]["theta"] = {
                "available": False,
                "error": "No ThetaData connection"
            }
    except Exception as e:
        comparison_result["providers"]["theta"] = {
            "available": False,
            "error": str(e)
        }

    # Test Polygon
    try:
        polygon_start = datetime.now()

        polygon_close = polygon_prev_close(symbol, test_date)

        polygon_duration = (datetime.now() - polygon_start).total_seconds() * 1000

        comparison_result["providers"]["polygon"] = {
            "available": bool(POLYGON_API_KEY),
            "prev_close": polygon_close,
            "response_time_ms": round(polygon_duration, 2),
            "success": polygon_close is not None
        }
    except Exception as e:
        comparison_result["providers"]["polygon"] = {
            "available": False,
            "error": str(e)
        }

    # Calculate discrepancies
    theta_close = comparison_result["providers"].get("theta", {}).get("prev_close")
    polygon_close = comparison_result["providers"].get("polygon", {}).get("prev_close")

    if theta_close and polygon_close:
        discrepancy = abs(theta_close - polygon_close)
        discrepancy_pct = (discrepancy / polygon_close) * 100 if polygon_close > 0 else 0

        comparison_result["cross_validation"] = {
            "prev_close_match": discrepancy < 0.01,  # 1 cent tolerance
            "discrepancy": round(discrepancy, 4),
            "discrepancy_pct": round(discrepancy_pct, 4)
        }
    else:
        comparison_result["cross_validation"] = {
            "prev_close_match": False,
            "reason": "Missing data from one or both providers"
        }

    return comparison_result

def validate_heavy_runner_override(db_path: str, date_range_start: str, date_range_end: str) -> Dict[str, Any]:
    """Validate heavy runner override logic for reverse split gating"""
    print(f"[HEAVY-RUNNER] Validating override logic from {date_range_start} to {date_range_end}")

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Find discoveries with high dollar volume and push
        query = """
            SELECT h.ticker, h.event_date, h.volume, h.intraday_push_pct, h.is_near_reverse_split,
                   dr.close, (dr.close * h.volume) as dollar_volume
            FROM discovery_hits h
            JOIN daily_raw dr ON h.ticker = dr.symbol AND h.event_date = dr.date
            WHERE h.event_date BETWEEN ? AND ?
            AND h.intraday_push_pct >= 50.0
            AND (dr.close * h.volume) >= 10000000
            ORDER BY dollar_volume DESC
        """

        cur.execute(query, (date_range_start, date_range_end))
        heavy_runners = cur.fetchall()

        conn.close()

        analysis_results = []

        for row in heavy_runners:
            ticker, event_date, volume, push_pct, is_near_split, close, dollar_volume = row

            # Analyze split context for this heavy runner
            split_analysis = analyze_split_context(ticker, event_date)

            # Check if override logic was applied correctly
            should_override = (
                dollar_volume >= 10_000_000 and
                push_pct >= 50.0 and
                split_analysis.get("reverse_split_detected", False)
            )

            analysis_results.append({
                "ticker": ticker,
                "event_date": event_date,
                "dollar_volume": dollar_volume,
                "intraday_push_pct": push_pct,
                "is_near_reverse_split": bool(is_near_split),
                "reverse_split_detected": split_analysis.get("reverse_split_detected", False),
                "should_override": should_override,
                "override_applied_correctly": bool(is_near_split) == should_override,
                "split_context": split_analysis.get("split_context", [])
            })

        # Calculate statistics
        total_heavy_runners = len(analysis_results)
        correct_overrides = sum(1 for r in analysis_results if r["override_applied_correctly"])
        override_accuracy = correct_overrides / total_heavy_runners if total_heavy_runners > 0 else 0

        return {
            "validation_type": "heavy_runner_override",
            "date_range": f"{date_range_start} to {date_range_end}",
            "total_heavy_runners": total_heavy_runners,
            "correct_overrides": correct_overrides,
            "override_accuracy": round(override_accuracy, 3),
            "heavy_runners": analysis_results
        }

    except Exception as e:
        return {
            "validation_type": "heavy_runner_override",
            "error": str(e),
            "success": False
        }

def run_30_day_split_analysis(db_path: str, end_date: str) -> Dict[str, Any]:
    """Run comprehensive 30-day split analysis"""
    print(f"[SPLIT-ANALYSIS] Running 30-day split analysis ending {end_date}")

    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Get all discoveries from the 30-day period
        query = """
            SELECT DISTINCT h.ticker, h.event_date
            FROM discovery_hits h
            WHERE h.event_date BETWEEN ? AND ?
            ORDER BY h.event_date DESC, h.ticker
        """

        cur.execute(query, (start_date, end_date))
        discoveries = cur.fetchall()

        conn.close()

        print(f"[SPLIT-ANALYSIS] Analyzing {len(discoveries)} discoveries for split context")

        split_analysis_results = []
        symbols_with_splits = 0
        symbols_with_reverse_splits = 0

        # Analyze each discovery for split context
        for ticker, event_date in discoveries:
            split_result = analyze_split_context(ticker, event_date)
            split_analysis_results.append(split_result)

            if split_result.get("has_splits"):
                symbols_with_splits += 1

            if split_result.get("reverse_split_detected"):
                symbols_with_reverse_splits += 1

        # Heavy runner validation
        heavy_runner_result = validate_heavy_runner_override(db_path, start_date, end_date)

        return {
            "analysis_type": "30_day_split_analysis",
            "period": f"{start_date} to {end_date}",
            "total_discoveries": len(discoveries),
            "symbols_with_splits": symbols_with_splits,
            "symbols_with_reverse_splits": symbols_with_reverse_splits,
            "split_rate": round(symbols_with_splits / len(discoveries) if discoveries else 0, 3),
            "reverse_split_rate": round(symbols_with_reverse_splits / len(discoveries) if discoveries else 0, 3),
            "heavy_runner_validation": heavy_runner_result,
            "detailed_results": split_analysis_results
        }

    except Exception as e:
        return {
            "analysis_type": "30_day_split_analysis",
            "error": str(e),
            "success": False
        }

def create_provider_comparison_table(db_path: str, symbols: List[str], test_date: str) -> None:
    """Create provider comparison table in database"""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Create provider_hits table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS provider_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                rule TEXT NOT NULL,
                provider TEXT NOT NULL,
                value REAL,
                response_time_ms REAL,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, symbol, rule, provider)
            )
        """)

        # Clear existing data for the test date
        cur.execute("DELETE FROM provider_hits WHERE date = ?", (test_date,))

        # Compare providers for each symbol
        for symbol in symbols:
            comparison = compare_provider_data(symbol, test_date)

            for provider_name, provider_data in comparison.get("providers", {}).items():
                if provider_data.get("available") and provider_data.get("success"):

                    # Insert prev_close data
                    if provider_data.get("prev_close") is not None:
                        cur.execute("""
                            INSERT OR REPLACE INTO provider_hits
                            (date, symbol, rule, provider, value, response_time_ms)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            test_date, symbol, "PREV_CLOSE", provider_name,
                            provider_data["prev_close"],
                            provider_data.get("response_time_ms")
                        ))

                    # Insert premarket_high data (ThetaData only)
                    if provider_name == "theta" and provider_data.get("premarket_high") is not None:
                        cur.execute("""
                            INSERT OR REPLACE INTO provider_hits
                            (date, symbol, rule, provider, value, response_time_ms)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            test_date, symbol, "PREMARKET_HIGH", provider_name,
                            provider_data["premarket_high"],
                            provider_data.get("response_time_ms")
                        ))

        conn.commit()
        conn.close()

        print(f"[PROVIDER-TABLE] Created provider comparison data for {len(symbols)} symbols on {test_date}")

    except Exception as e:
        print(f"[PROVIDER-TABLE] Error creating provider comparison table: {e}")

def main():
    """Main CLI entry point for split and provider validation"""
    parser = argparse.ArgumentParser(description="Split context validation and provider comparison")
    parser.add_argument("--test", choices=["splits", "providers", "heavy-runner", "all"],
                        default="all", help="Test type to run")
    parser.add_argument("--db", default="db/split_validation.db", help="Database path")
    parser.add_argument("--date", help="Test date (YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "GOOGL", "MSFT", "TSLA"],
                        help="Symbols for provider comparison")
    parser.add_argument("--days", type=int, default=30, help="Days for split analysis")

    args = parser.parse_args()

    if not args.date:
        args.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    ensure_enhanced_db_schema(args.db)

    if args.test in ["splits", "all"]:
        print("=== Split Context Analysis ===")
        end_date = args.date
        result = run_30_day_split_analysis(args.db, end_date)

        report_path = f"project_state/split_analysis_{end_date}.json"
        os.makedirs("project_state", exist_ok=True)

        with open(report_path, 'w', encoding='ascii', errors='replace') as f:
            json.dump(result, f, indent=2, default=str)

        print(f"Split analysis report saved to: {report_path}")

    if args.test in ["providers", "all"]:
        print("=== Provider Comparison ===")
        create_provider_comparison_table(args.db, args.symbols, args.date)

    if args.test in ["heavy-runner", "all"]:
        print("=== Heavy Runner Override Validation ===")
        start_date = (datetime.strptime(args.date, "%Y-%m-%d") - timedelta(days=args.days)).strftime("%Y-%m-%d")
        result = validate_heavy_runner_override(args.db, start_date, args.date)

        report_path = f"project_state/heavy_runner_validation_{args.date}.json"
        os.makedirs("project_state", exist_ok=True)

        with open(report_path, 'w', encoding='ascii', errors='replace') as f:
            json.dump(result, f, indent=2, default=str)

        print(f"Heavy runner validation saved to: {report_path}")

if __name__ == "__main__":
    main()