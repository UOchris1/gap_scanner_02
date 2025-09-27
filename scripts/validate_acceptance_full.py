#!/usr/bin/env python3
"""
Full acceptance validation script - comprehensive version
Recommended by plan3_suggestions.txt to keep alongside the minimal validate_acceptance.py

This script provides:
- Schema validation
- WAL mode verification
- Index analysis
- Provider health checks
- Rule distribution analysis
- Performance metrics
"""
import argparse
import sqlite3
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.integration.cli_bridge import validate_single_day
from src.providers.theta_provider import ThetaDataProvider


def check_database_health(db_path: str) -> Dict:
    """Comprehensive database health check"""
    print("[DB-HEALTH] Checking database configuration and indexes...")

    results = {"status": "ok", "issues": [], "metrics": {}}

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Check WAL mode
        cur.execute("PRAGMA journal_mode")
        journal_mode = cur.fetchone()[0]
        if journal_mode.upper() != "WAL":
            results["issues"].append(f"Journal mode is {journal_mode}, expected WAL")

        # Check synchronous mode
        cur.execute("PRAGMA synchronous")
        sync_mode = cur.fetchone()[0]
        results["metrics"]["synchronous_mode"] = sync_mode

        # Check required tables exist
        required_tables = ["daily_raw", "discovery_hits", "discovery_hit_rules", "completeness_log"]
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]

        for table in required_tables:
            if table not in tables:
                results["issues"].append(f"Missing required table: {table}")

        # Check indexes
        cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cur.fetchall()]
        results["metrics"]["index_count"] = len(indexes)

        # Performance check: analyze query plans
        if "daily_raw" in tables:
            cur.execute("EXPLAIN QUERY PLAN SELECT * FROM daily_raw WHERE date=? AND symbol=?", ("2025-01-01", "AAPL"))
            plan = cur.fetchall()
            uses_index = any("INDEX" in str(step) for step in plan)
            if not uses_index:
                results["issues"].append("daily_raw queries may not be using indexes efficiently")

        # Check database size and performance
        cur.execute("SELECT COUNT(*) FROM daily_raw")
        daily_raw_count = cur.fetchone()[0]
        results["metrics"]["daily_raw_total"] = daily_raw_count

        if "discovery_hits" in tables:
            cur.execute("SELECT COUNT(*) FROM discovery_hits")
            hits_count = cur.fetchone()[0]
            results["metrics"]["discovery_hits_total"] = hits_count

        conn.close()

    except Exception as e:
        results["status"] = "error"
        results["issues"].append(f"Database health check failed: {e}")

    return results


def check_provider_health() -> Dict:
    """Check provider connectivity and configuration"""
    print("[PROVIDER-HEALTH] Checking data provider connectivity...")

    results = {"theta": {"status": "unknown"}, "polygon": {"status": "unknown"}}

    # Test Theta connectivity
    try:
        theta = ThetaDataProvider()
        if theta.ok():
            results["theta"]["status"] = "connected"
            results["theta"]["v3_ok"] = theta.v3_ok
            results["theta"]["v1_ok"] = theta.v1_ok
            results["theta"]["v3_limit"] = theta.v3_limit
            results["theta"]["v1_limit"] = theta.v1_limit
        else:
            results["theta"]["status"] = "disconnected"
    except Exception as e:
        results["theta"]["status"] = "error"
        results["theta"]["error"] = str(e)

    # Test Polygon (basic import check)
    try:
        from src.providers.polygon_provider import grouped_daily
        results["polygon"]["status"] = "available"
    except Exception as e:
        results["polygon"]["status"] = "error"
        results["polygon"]["error"] = str(e)

    return results


def analyze_rule_distribution(db_path: str, date_iso: str) -> Dict:
    """Analyze rule distribution for the validation date"""
    print(f"[RULE-ANALYSIS] Analyzing rule distribution for {date_iso}...")

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Get rule breakdown
        cur.execute("""
            SELECT r.trigger_rule, COUNT(*) as count, AVG(r.rule_value) as avg_value
            FROM discovery_hit_rules r
            JOIN discovery_hits h ON r.hit_id = h.hit_id
            WHERE h.event_date = ?
            GROUP BY r.trigger_rule
            ORDER BY count DESC
        """, (date_iso,))

        rules = {}
        for rule, count, avg_val in cur.fetchall():
            rules[rule] = {"count": count, "avg_value": round(avg_val, 2) if avg_val else 0}

        # Get symbols with multiple rules
        cur.execute("""
            SELECT h.ticker, COUNT(DISTINCT r.trigger_rule) as rule_count
            FROM discovery_hits h
            JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
            WHERE h.event_date = ?
            GROUP BY h.ticker
            HAVING rule_count > 1
            ORDER BY rule_count DESC
        """, (date_iso,))

        multi_rule_symbols = cur.fetchall()

        conn.close()

        return {
            "rule_breakdown": rules,
            "multi_rule_symbols": len(multi_rule_symbols),
            "total_rule_applications": sum(r["count"] for r in rules.values())
        }

    except Exception as e:
        return {"error": f"Rule analysis failed: {e}"}


def main(date_iso: str, db_path: str):
    """Run comprehensive acceptance validation"""
    print("=" * 60)
    print("COMPREHENSIVE ACCEPTANCE VALIDATION")
    print("=" * 60)
    print(f"Date: {date_iso}")
    print(f"Database: {db_path}")
    print()

    overall_status = "PASS"

    # 1. Database health check
    db_health = check_database_health(db_path)
    if db_health["issues"]:
        print("[FAIL] DATABASE HEALTH ISSUES:")
        for issue in db_health["issues"]:
            print(f"   - {issue}")
        overall_status = "FAIL"
    else:
        print("[OK] Database health: OK")

    print(f"   Metrics: {db_health['metrics']}")
    print()

    # 2. Provider health check
    provider_health = check_provider_health()
    print("[PROVIDER] STATUS:")
    for provider, status in provider_health.items():
        print(f"   {provider}: {status['status']}")
        if status.get("error"):
            print(f"      Error: {status['error']}")
        if provider == "theta" and status.get("v3_limit"):
            print(f"      Concurrency: v3={status['v3_limit']}, v1={status['v1_limit']}")
    print()

    # 3. Run core validation
    print("[SEARCH] RUNNING CORE VALIDATION...")
    validation_result = validate_single_day(date_iso, db_path, {})

    if validation_result["validation_status"] != "passed":
        print("[FAIL] Core validation failed")
        print(f"   Reason: {validation_result.get('reason', 'unknown')}")
        overall_status = "FAIL"
    else:
        print("[OK] Core validation: PASSED")
        print(f"   Daily raw: {validation_result['daily_raw_count']:,} symbols")
        print(f"   Discovery hits: {validation_result['discovery_hits']:,}")
        print(f"   Audit failed: {validation_result['audit_failed']}")
    print()

    # 4. Rule distribution analysis
    rule_analysis = analyze_rule_distribution(db_path, date_iso)
    if "error" not in rule_analysis:
        print("[REPORT] RULE DISTRIBUTION:")
        for rule, stats in rule_analysis["rule_breakdown"].items():
            print(f"   {rule}: {stats['count']} hits (avg: {stats['avg_value']}%)")
        print(f"   Multi-rule symbols: {rule_analysis['multi_rule_symbols']}")
        print(f"   Total rule applications: {rule_analysis['total_rule_applications']}")
    else:
        print(f"[FAIL] Rule analysis failed: {rule_analysis['error']}")
    print()

    # 5. Performance metrics
    start_time = time.time()
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM daily_raw WHERE date = ?", (date_iso,))
        daily_count = cur.fetchone()[0]
        query_time = time.time() - start_time
        conn.close()

        print("[FAST] PERFORMANCE:")
        print(f"   Query time (daily_raw count): {query_time:.3f}s")
        print(f"   Database size: {os.path.getsize(db_path) / 1024 / 1024:.1f} MB")
    except Exception as e:
        print(f"[FAIL] Performance check failed: {e}")
    print()

    # Final result
    print("=" * 60)
    if overall_status == "PASS":
        print("[COMPLETE] COMPREHENSIVE VALIDATION: PASSED")
        print("System is ready for production use")
    else:
        print("[FAIL] COMPREHENSIVE VALIDATION: FAILED")
        print("Review issues above before production deployment")
    print("=" * 60)

    return 0 if overall_status == "PASS" else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Comprehensive acceptance validation")
    ap.add_argument("--date", required=True, help="Date to validate (YYYY-MM-DD)")
    ap.add_argument("--db", default="db/acceptance.db", help="Database path")
    args = ap.parse_args()

    exit_code = main(args.date, args.db)
    sys.exit(exit_code)