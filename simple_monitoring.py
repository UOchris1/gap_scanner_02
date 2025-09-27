#!/usr/bin/env python3
"""
Simple Production Monitoring for 30-Day Validation
Basic health checks and dashboard without email dependencies
"""
import os
import json
import sqlite3
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any

def check_database_health(db_path: str) -> Dict[str, Any]:
    """Check database connectivity and basic health"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check table existence
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        required_tables = ["discovery_hits", "discovery_hit_rules", "enhanced_audit_log", "diffs"]
        missing_tables = [table for table in required_tables if table not in tables]

        # Check recent data
        cursor.execute("SELECT COUNT(*) FROM discovery_hits")
        total_discoveries = cursor.fetchone()[0]

        conn.close()

        status = "critical" if missing_tables else "healthy"

        return {
            "status": status,
            "tables_present": len(tables),
            "missing_tables": missing_tables,
            "total_discoveries": total_discoveries
        }

    except Exception as e:
        return {"status": "critical", "error": str(e)}

def check_baseline_validation_health(db_path: str) -> Dict[str, Any]:
    """Check baseline validation status"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check for baseline-only hits (critical issue)
        cursor.execute("""
            SELECT SUM(baseline_only_count) as total_baseline_only
            FROM diffs
            WHERE date >= date('now', '-7 days')
        """)

        result = cursor.fetchone()
        baseline_only_hits = result[0] if result and result[0] else 0

        conn.close()

        status = "critical" if baseline_only_hits > 0 else "healthy"

        return {
            "status": status,
            "baseline_only_hits_7days": baseline_only_hits
        }

    except Exception as e:
        return {"status": "warning", "error": str(e)}

def check_audit_system_health(db_path: str) -> Dict[str, Any]:
    """Check statistical audit system"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check recent audit results
        cursor.execute("""
            SELECT
                COUNT(*) as total_audits,
                SUM(CASE WHEN audit_passed = 1 THEN 1 ELSE 0 END) as passed_audits
            FROM enhanced_audit_log
            WHERE date >= date('now', '-7 days')
        """)

        audit_result = cursor.fetchone()
        total_audits, passed_audits = audit_result if audit_result else (0, 0)

        conn.close()

        audit_success_rate = passed_audits / total_audits if total_audits > 0 else 1.0
        status = "critical" if audit_success_rate < 1.0 else "healthy"

        return {
            "status": status,
            "recent_audits": total_audits,
            "audit_success_rate": round(audit_success_rate, 4)
        }

    except Exception as e:
        return {"status": "warning", "error": str(e)}

def run_comprehensive_health_check(db_path: str) -> Dict[str, Any]:
    """Run all health checks"""
    print(f"Running health checks on database: {db_path}")
    print("=" * 60)

    health_status = {
        "timestamp": datetime.now().isoformat(),
        "database_health": check_database_health(db_path),
        "baseline_validation_health": check_baseline_validation_health(db_path),
        "audit_system_health": check_audit_system_health(db_path)
    }

    # Determine overall health
    statuses = [check["status"] for check in health_status.values() if isinstance(check, dict) and "status" in check]

    if "critical" in statuses:
        overall_health = "critical"
    elif "warning" in statuses:
        overall_health = "warning"
    else:
        overall_health = "healthy"

    health_status["overall_health"] = overall_health

    return health_status

def display_dashboard(health_status: Dict[str, Any]):
    """Display monitoring dashboard"""
    print("PRODUCTION MONITORING DASHBOARD")
    print("=" * 60)
    print(f"Overall Health: {health_status['overall_health'].upper()}")
    print(f"Last Updated: {health_status['timestamp']}")
    print()

    # Database Health
    db_health = health_status["database_health"]
    print("DATABASE HEALTH:")
    print(f"  Status: {db_health['status']}")
    print(f"  Tables Present: {db_health.get('tables_present', 0)}")
    print(f"  Total Discoveries: {db_health.get('total_discoveries', 0)}")
    if db_health.get("missing_tables"):
        print(f"  Missing Tables: {', '.join(db_health['missing_tables'])}")
    if db_health.get("error"):
        print(f"  Error: {db_health['error']}")
    print()

    # Baseline Validation Health
    baseline_health = health_status["baseline_validation_health"]
    print("BASELINE VALIDATION:")
    print(f"  Status: {baseline_health['status']}")
    print(f"  Baseline-Only Hits (7d): {baseline_health.get('baseline_only_hits_7days', 0)}")
    if baseline_health.get("error"):
        print(f"  Error: {baseline_health['error']}")
    print()

    # Audit System Health
    audit_health = health_status["audit_system_health"]
    print("AUDIT SYSTEM:")
    print(f"  Status: {audit_health['status']}")
    print(f"  Recent Audits: {audit_health.get('recent_audits', 0)}")
    print(f"  Success Rate: {audit_health.get('audit_success_rate', 0)}")
    if audit_health.get("error"):
        print(f"  Error: {audit_health['error']}")
    print()

    # Alerts
    print("ALERT STATUS:")
    if health_status["overall_health"] == "critical":
        print("  [CRITICAL] System requires immediate attention")
    elif health_status["overall_health"] == "warning":
        print("  [WARNING] System issues detected - review needed")
    else:
        print("  [OK] All systems operating normally")
    print()

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Simple Production Monitoring")
    parser.add_argument("--db", default="db/validation_30day.db", help="Database path")
    parser.add_argument("--dashboard", action="store_true", help="Show monitoring dashboard")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")

    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Error: Database not found at {args.db}")
        return 1

    # Run health checks
    health_status = run_comprehensive_health_check(args.db)

    if args.json:
        print(json.dumps(health_status, indent=2))
    elif args.dashboard:
        display_dashboard(health_status)
    else:
        print(f"Overall Health: {health_status['overall_health']}")

    # Exit code based on health
    if health_status["overall_health"] == "critical":
        return 2
    elif health_status["overall_health"] == "warning":
        return 1
    else:
        return 0

if __name__ == "__main__":
    exit(main())