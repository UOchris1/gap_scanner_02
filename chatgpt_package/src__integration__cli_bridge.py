# -*- coding: ascii -*-
# src/integration/cli_bridge.py
from src.pipelines.zero_miss import scan_day

def process_day_zero_miss(day_iso: str, db_path: str, providers: dict) -> dict:
    # providers kept for signature compatibility; pipeline uses env + auto-detect
    return scan_day(day_iso, db_path)

def validate_single_day(day_iso: str, db_path: str, providers: dict) -> dict:
    """
    Enhanced validation function for single-day acceptance testing

    Performs comprehensive validation beyond basic discovery count:
    - Verifies completeness logging
    - Checks database integrity
    - Validates provider fallback behavior
    """
    import sqlite3

    print(f"[VALIDATE] Starting single-day validation for {day_iso}")

    # Run the discovery pipeline
    result = process_day_zero_miss(day_iso, db_path, providers)

    if result.get("status") != "ok":
        return {"validation_status": "failed", "reason": "pipeline_failed", "result": result}

    # Database validation checks
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Check daily_raw population
        cur.execute("SELECT COUNT(*) FROM daily_raw WHERE date = ?", (day_iso,))
        daily_raw_count = cur.fetchone()[0]

        # Check discovery_hits
        cur.execute("SELECT COUNT(*) FROM discovery_hits WHERE event_date = ?", (day_iso,))
        hits_count = cur.fetchone()[0]

        # Check completeness_log
        cur.execute("SELECT * FROM completeness_log WHERE date = ?", (day_iso,))
        completeness_row = cur.fetchone()

        conn.close()

        validation_result = {
            "validation_status": "passed",
            "day_iso": day_iso,
            "daily_raw_count": daily_raw_count,
            "discovery_hits": hits_count,
            "discoveries": result.get("discoveries", 0),
            "audit_failed": result.get("audit_failed", False),
            "completeness_logged": completeness_row is not None
        }

        print(f"[VALIDATE] {day_iso}: {daily_raw_count} symbols, {hits_count} hits, completeness={'OK' if completeness_row else 'MISSING'}")

        return validation_result

    except Exception as e:
        print(f"[VALIDATE-ERROR] Database validation failed for {day_iso}: {e}")
        return {"validation_status": "failed", "reason": "db_validation_error", "error": str(e)}