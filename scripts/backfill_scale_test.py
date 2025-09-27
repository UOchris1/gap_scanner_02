# -*- coding: ascii -*-
# 30-day backfill scale testing for zero-miss gap scanner

import sys
import os
import json
import sqlite3
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
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

from src.integration.cli_bridge import process_day_zero_miss
from enhanced_db_schema import ensure_enhanced_db_schema
from scripts.export_reports import export_all_reports

def get_trading_days(start_date: str, end_date: str) -> List[str]:
    """Generate list of trading days (excludes weekends and major holidays)"""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    # Major holidays to exclude (simplified list)
    holidays = [
        "2025-01-01",  # New Year's Day
        "2025-01-20",  # MLK Day
        "2025-02-17",  # Presidents' Day
        "2025-04-18",  # Good Friday
        "2025-05-26",  # Memorial Day
        "2025-07-04",  # Independence Day
        "2025-09-01",  # Labor Day
        "2025-11-27",  # Thanksgiving
        "2025-12-25",  # Christmas
    ]

    trading_days = []
    current = start

    while current <= end:
        # Exclude weekends (Monday=0, Sunday=6)
        if current.weekday() < 5:  # Monday to Friday
            if current.isoformat() not in holidays:
                trading_days.append(current.isoformat())
        current += timedelta(days=1)

    return trading_days

def check_database_performance(db_path: str) -> Dict[str, Any]:
    """Check database performance metrics and optimization status"""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Check WAL mode
        cur.execute("PRAGMA journal_mode")
        journal_mode = cur.fetchone()[0]

        # Check synchronous setting
        cur.execute("PRAGMA synchronous")
        synchronous = cur.fetchone()[0]

        # Check page size
        cur.execute("PRAGMA page_size")
        page_size = cur.fetchone()[0]

        # Get database size
        cur.execute("PRAGMA page_count")
        page_count = cur.fetchone()[0]
        db_size_mb = (page_count * page_size) / (1024 * 1024)

        # Count records in main tables
        cur.execute("SELECT COUNT(*) FROM daily_raw")
        daily_raw_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM discovery_hits")
        discovery_hits_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM completeness_log")
        completeness_count = cur.fetchone()[0]

        # Check indexes
        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
        index_count = cur.fetchone()[0]

        conn.close()

        return {
            "performance_check": "passed",
            "journal_mode": journal_mode,
            "synchronous": synchronous,
            "wal_enabled": journal_mode.upper() == "WAL",
            "sync_normal": synchronous == 1,
            "db_size_mb": round(db_size_mb, 2),
            "record_counts": {
                "daily_raw": daily_raw_count,
                "discovery_hits": discovery_hits_count,
                "completeness_log": completeness_count
            },
            "index_count": index_count,
            "optimized": journal_mode.upper() == "WAL" and synchronous == 1 and index_count >= 5
        }

    except Exception as e:
        return {
            "performance_check": "failed",
            "error": str(e)
        }

def apply_database_optimizations(db_path: str) -> bool:
    """Apply database optimizations before scale testing"""
    try:
        print("[DB-OPTIMIZE] Applying database optimizations...")

        # Use existing schema and index scripts
        ensure_enhanced_db_schema(db_path)

        from scripts.apply_db_indexes import apply
        apply(db_path)

        print("[DB-OPTIMIZE] Database optimizations applied successfully")
        return True

    except Exception as e:
        print(f"[DB-OPTIMIZE] Error applying optimizations: {e}")
        return False

def run_day_with_monitoring(day_iso: str, db_path: str, providers: Dict) -> Dict[str, Any]:
    """Run single day processing with performance monitoring"""
    start_time = time.time()

    # Get initial database size
    initial_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0

    try:
        # Run the discovery pipeline
        result = process_day_zero_miss(day_iso, db_path, providers)

        end_time = time.time()
        processing_time = end_time - start_time

        # Get final database size
        final_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        size_increase = final_size - initial_size

        # Add performance metrics to result
        result["performance"] = {
            "processing_time_seconds": round(processing_time, 2),
            "db_size_increase_bytes": size_increase,
            "db_size_increase_mb": round(size_increase / (1024 * 1024), 2),
            "initial_db_size_mb": round(initial_size / (1024 * 1024), 2),
            "final_db_size_mb": round(final_size / (1024 * 1024), 2)
        }

        return result

    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time

        return {
            "status": "error",
            "error": str(e),
            "performance": {
                "processing_time_seconds": round(processing_time, 2),
                "failed": True
            }
        }

def run_30_day_backfill_test(start_date: str, end_date: str, db_path: str) -> Dict[str, Any]:
    """Run comprehensive 30-day backfill test with performance monitoring"""
    print(f"=== 30-Day Backfill Scale Test: {start_date} to {end_date} ===")

    # Pre-test setup
    print("\n[SETUP] Preparing for scale test...")

    # Apply database optimizations
    if not apply_database_optimizations(db_path):
        return {
            "test_status": "failed",
            "reason": "database_optimization_failed"
        }

    # Check initial database performance
    initial_perf = check_database_performance(db_path)
    print(f"[SETUP] Database optimization status: {'OK' if initial_perf.get('optimized') else 'SUBOPTIMAL'}")

    if not initial_perf.get("optimized"):
        print("[WARNING] Database not fully optimized - performance may be degraded")

    # Get trading days
    trading_days = get_trading_days(start_date, end_date)
    print(f"[SETUP] Processing {len(trading_days)} trading days")

    # Check provider availability
    providers = {
        "polygon_ok": bool(POLYGON_API_KEY),
        "fmp_ok": bool(FMP_API_KEY)
    }

    print(f"[SETUP] Provider status: Polygon={'OK' if providers['polygon_ok'] else 'MISSING_KEY'}")

    if not providers["polygon_ok"]:
        return {
            "test_status": "failed",
            "reason": "no_polygon_api_key"
        }

    # Run backfill with monitoring
    test_start_time = time.time()
    daily_results = []
    performance_metrics = {
        "total_processing_time": 0,
        "total_db_growth_mb": 0,
        "avg_processing_time": 0,
        "successful_days": 0,
        "failed_days": 0,
        "audit_failures": 0,
        "total_discoveries": 0
    }

    print(f"\n[BACKFILL] Starting processing...")

    for i, day_iso in enumerate(trading_days, 1):
        print(f"[{i:2d}/{len(trading_days)}] Processing {day_iso}...")

        day_result = run_day_with_monitoring(day_iso, db_path, providers)
        daily_results.append(day_result)

        # Update performance metrics
        perf = day_result.get("performance", {})
        processing_time = perf.get("processing_time_seconds", 0)
        db_growth = perf.get("db_size_increase_mb", 0)

        performance_metrics["total_processing_time"] += processing_time
        performance_metrics["total_db_growth_mb"] += db_growth

        if day_result.get("status") == "ok":
            performance_metrics["successful_days"] += 1
            performance_metrics["total_discoveries"] += day_result.get("discoveries", 0)

            if day_result.get("audit_failed"):
                performance_metrics["audit_failures"] += 1
                print(f"[AUDIT-FAIL] {day_iso} failed audit")
        else:
            performance_metrics["failed_days"] += 1
            print(f"[ERROR] {day_iso} failed: {day_result.get('error', 'unknown')}")

        # Show progress
        print(f"    {processing_time:.1f}s, {db_growth:.1f}MB, {day_result.get('discoveries', 0)} discoveries")

    total_test_time = time.time() - test_start_time

    # Calculate final metrics
    if len(trading_days) > 0:
        performance_metrics["avg_processing_time"] = performance_metrics["total_processing_time"] / len(trading_days)
        performance_metrics["success_rate"] = performance_metrics["successful_days"] / len(trading_days)

    # Final database performance check
    final_perf = check_database_performance(db_path)

    # Generate test report
    test_report = {
        "test_type": "30_day_backfill_scale_test",
        "period": f"{start_date} to {end_date}",
        "timestamp": datetime.now().isoformat(),
        "test_duration_minutes": round(total_test_time / 60, 2),
        "trading_days_processed": len(trading_days),
        "performance_metrics": performance_metrics,
        "initial_db_performance": initial_perf,
        "final_db_performance": final_perf,
        "daily_results": daily_results,
        "test_status": "passed" if performance_metrics["success_rate"] >= 0.8 else "failed",
        "recommendations": []
    }

    # Add recommendations based on performance
    if performance_metrics["success_rate"] < 0.9:
        test_report["recommendations"].append("Success rate below 90% - investigate failed days")

    if performance_metrics["audit_failures"] > 0:
        test_report["recommendations"].append(f"{performance_metrics['audit_failures']} audit failures - review R1 candidate filtering")

    if performance_metrics["avg_processing_time"] > 60:
        test_report["recommendations"].append("Average processing time >60s - consider performance optimization")

    if performance_metrics["total_db_growth_mb"] > 1000:
        test_report["recommendations"].append("Database growth >1GB - monitor disk space for longer backtests")

    # Generate exports for the test period
    print(f"\n[EXPORT] Generating reports for test period...")
    try:
        export_files = export_all_reports(db_path, start_date, end_date, "project_state")
        test_report["export_files"] = export_files
    except Exception as e:
        print(f"[EXPORT-ERROR] Failed to generate exports: {e}")
        test_report["export_error"] = str(e)

    # Save test report
    report_file = f"project_state/backfill_scale_test_{start_date}_{end_date}.json"
    os.makedirs("project_state", exist_ok=True)

    with open(report_file, 'w', encoding='ascii', errors='replace') as f:
        json.dump(test_report, f, indent=2, default=str)

    # Print summary
    print(f"\n=== Backfill Scale Test Complete ===")
    print(f"Success rate: {performance_metrics['successful_days']}/{len(trading_days)} ({performance_metrics['success_rate']:.1%})")
    print(f"Total discoveries: {performance_metrics['total_discoveries']}")
    print(f"Audit failures: {performance_metrics['audit_failures']}")
    print(f"Average processing time: {performance_metrics['avg_processing_time']:.1f}s per day")
    print(f"Total database growth: {performance_metrics['total_db_growth_mb']:.1f}MB")
    print(f"Test status: {test_report['test_status'].upper()}")
    print(f"Detailed report: {report_file}")

    if test_report["recommendations"]:
        print("\nRecommendations:")
        for rec in test_report["recommendations"]:
            print(f"  - {rec}")

    return test_report

def main():
    """Main CLI entry point for 30-day backfill scale testing"""
    parser = argparse.ArgumentParser(description="30-day backfill scale testing")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--db", default="db/backfill_scale_test.db", help="Database path")
    parser.add_argument("--days", type=int, default=30, help="Number of trading days to test")

    args = parser.parse_args()

    # Default to last 30 trading days if not specified
    if not args.end:
        args.end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if not args.start:
        # Go back enough calendar days to get the requested trading days
        calendar_days_back = int(args.days * 1.4)  # ~40% overhead for weekends/holidays
        start_date = datetime.strptime(args.end, "%Y-%m-%d") - timedelta(days=calendar_days_back)
        args.start = start_date.strftime("%Y-%m-%d")

    print(f"30-day backfill scale test: {args.start} to {args.end}")

    result = run_30_day_backfill_test(args.start, args.end, args.db)

    # Exit with error if test failed
    exit_code = 0 if result.get("test_status") == "passed" else 1
    sys.exit(exit_code)

if __name__ == "__main__":
    main()