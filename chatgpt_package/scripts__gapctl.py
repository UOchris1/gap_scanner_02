# -*- coding: ascii -*-
# Unified CLI for Gap Scanner pipeline

import argparse
import os
import sys
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv


def _project_root() -> Path:
    return Path(__file__).parent.parent


def _load_env() -> None:
    env_path = _project_root() / ".env"
    load_dotenv(env_path)
    # Ensure src/ is importable regardless of invocation path
    pr = str(_project_root())
    if pr not in sys.path:
        sys.path.insert(0, pr)


def cmd_scan_day(args) -> int:
    from src.integration.cli_bridge import process_day_zero_miss

    db_path = args.db
    date_iso = args.date
    print(f"[SCAN] date={date_iso} db={db_path}")
    res = process_day_zero_miss(date_iso, db_path, providers={})
    print(f"[SCAN] result: {res}")
    return 0 if res.get("status") == "ok" else 2


def _iter_dates(start_iso: str, end_iso: str):
    start = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end = datetime.strptime(end_iso, "%Y-%m-%d").date()
    cur = start
    while cur <= end:
        # Skip weekends (basic market-day filter)
        if cur.weekday() < 5:
            yield cur.isoformat()
        cur += timedelta(days=1)


def cmd_scan_range(args) -> int:
    from src.integration.cli_bridge import process_day_zero_miss

    db_path = args.db
    start = args.start
    end = args.end

    print(f"[SCAN-RANGE] {start}..{end} db={db_path}")
    ok = 0
    fail = 0
    for day in _iter_dates(start, end):
        print(f"[SCAN-RANGE] scanning {day}...")
        res = process_day_zero_miss(day, db_path, providers={})
        if res.get("status") == "ok":
            ok += 1
            print(f"[SCAN-RANGE] {day} ok")
        else:
            fail += 1
            print(f"[SCAN-RANGE] {day} FAILED: {res}")
    print(f"[SCAN-RANGE] done ok={ok} fail={fail}")
    return 0 if fail == 0 else 2


def cmd_export(args) -> int:
    import sqlite3
    from scripts.export_reports import export_hits, export_day_completeness

    db_path = args.db
    out_dir = args.out
    start = args.start
    end = args.end

    os.makedirs(out_dir, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        export_hits(conn, start, end, f"{out_dir}/discovery_hits_{start}_{end}.csv")
        export_day_completeness(conn, f"{out_dir}/day_completeness.csv")

    print(f"[EXPORT] wrote {out_dir}/discovery_hits_{start}_{end}.csv and {out_dir}/day_completeness.csv")
    return 0


def cmd_summary(args) -> int:
    db_path = args.db
    start = args.start
    end = args.end
    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    out_csv = os.path.join(out_dir, f"summary_{start}_{end}.csv")

    with sqlite3.connect(db_path) as conn, open(out_csv, "w", newline="") as f:
        cur = conn.cursor()
        # header
        f.write("date,daily_raw_symbols,hits,rule_rows\n")
        # iterate days in range present in daily_raw
        for (day,) in cur.execute(
            "SELECT DISTINCT date FROM daily_raw WHERE date BETWEEN ? AND ? ORDER BY date",
            (start, end),
        ):
            dr = cur.execute(
                "SELECT COUNT(DISTINCT symbol) FROM daily_raw WHERE date=?", (day,)
            ).fetchone()[0]
            h = cur.execute(
                "SELECT COUNT(*) FROM discovery_hits WHERE event_date=?", (day,)
            ).fetchone()[0]
            r = cur.execute(
                "SELECT COUNT(*) FROM discovery_hit_rules x JOIN discovery_hits y ON x.hit_id=y.hit_id WHERE y.event_date=?",
                (day,),
            ).fetchone()[0]
            f.write(f"{day},{dr},{h},{r}\n")

    print(f"[SUMMARY] wrote {out_csv}")
    return 0


def cmd_outcomes(args) -> int:
    from src.core.database_operations import recompute_next_day_outcomes_range
    import csv

    db_path = args.db
    start = args.start
    end = args.end
    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    n = recompute_next_day_outcomes_range(db_path, start, end)
    print(f"[OUTCOMES] upserted {n} rows")

    # Export a CSV for inspection
    import sqlite3
    out_csv = os.path.join(out_dir, f"next_day_outcomes_{start}_{end}.csv")
    with sqlite3.connect(db_path) as conn, open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "symbol", "close", "next_date", "next_close", "next_return_pct", "next_positive"])
        for row in conn.execute(
            """
            SELECT date, symbol, close, next_date, next_close, next_return_pct, next_positive
            FROM next_day_outcomes
            WHERE date BETWEEN ? AND ?
            ORDER BY date, symbol
            """,
            (start, end),
        ):
            w.writerow(row)

    print(f"[OUTCOMES] wrote {out_csv}")
    return 0

def cmd_validate(args) -> int:
    # Reuse the acceptance script
    from scripts.validate_acceptance import main as validate_main
    return int(validate_main(args.date, args.db) or 0)


def cmd_health(args) -> int:
    # Quick provider readiness snapshot
    polygon_key = os.getenv("POLYGON_API_KEY", "").strip()
    fmp_key = os.getenv("FMP_API_KEY", "").strip()

    theta_status = {
        "ok": False,
        "version": None,
        "base": None,
    }
    try:
        from src.providers.theta_provider import ThetaDataClient

        t = ThetaDataClient()
        theta_status["ok"] = bool(t.ok())
        theta_status["version"] = getattr(t, "version", None)
        theta_status["base"] = getattr(t, "v3_base", None) or getattr(t, "v1_base", None)
    except Exception as e:
        print(f"[HEALTH] Theta detection error: {e}")

    print("[HEALTH] Providers:")
    print(f"  Polygon API key present: {'YES' if polygon_key else 'NO'}")
    print(f"  FMP API key present: {'YES' if fmp_key else 'NO'}")
    print(
        f"  ThetaData: ok={theta_status['ok']} version={theta_status['version']} base={theta_status['base']}"
    )
    return 0


def _cmd_env_validate() -> int:
    from scripts.env_tools import validate_env
    report = validate_env(str(_project_root() / ".env"))
    print(f"[ENV] file: {report.get('path')}")
    print(f"[ENV] ascii_ok: {report.get('ascii_ok')}")
    print(f"[ENV] missing_required: {report.get('missing_required')}")
    print(f"[ENV] placeholders_found: {report.get('has_placeholders')} in {report.get('placeholder_keys')}")
    # Only list keys, never values
    print(f"[ENV] keys_present: {len(report.get('present_keys', []))}")
    return 0 if report.get("ok") else 3


def _cmd_env_format() -> int:
    from scripts.env_tools import format_env
    backup = format_env(str(_project_root() / ".env"))
    print(f"[ENV] normalized .env; backup written to {backup}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gapctl", description="Gap Scanner CLI")
    sp = p.add_subparsers(dest="cmd", required=True)

    # scan-day
    s = sp.add_parser("scan-day", help="Run zero-miss scan for a single day")
    s.add_argument("--date", required=True, help="YYYY-MM-DD")
    s.add_argument("--db", default="db/scanner.db")
    s.set_defaults(func=cmd_scan_day)

    # export
    e = sp.add_parser("export", help="Export CSVs for a date range")
    e.add_argument("--start", required=True, help="YYYY-MM-DD")
    e.add_argument("--end", required=True, help="YYYY-MM-DD")
    e.add_argument("--db", default="db/scanner.db")
    e.add_argument("--out", default="exports")
    e.set_defaults(func=cmd_export)

    # scan-range
    sr = sp.add_parser("scan-range", help="Run zero-miss scans for a date range")
    sr.add_argument("--start", required=True, help="YYYY-MM-DD")
    sr.add_argument("--end", required=True, help="YYYY-MM-DD")
    sr.add_argument("--db", default="db/scanner.db")
    sr.set_defaults(func=cmd_scan_range)

    # summary
    sm = sp.add_parser("summary", help="Summarize coverage and hits for a date range")
    sm.add_argument("--start", required=True, help="YYYY-MM-DD")
    sm.add_argument("--end", required=True, help="YYYY-MM-DD")
    sm.add_argument("--db", default="db/scanner.db")
    sm.add_argument("--out", default="exports")
    sm.set_defaults(func=cmd_summary)

    # outcomes
    oc = sp.add_parser("outcomes", help="Compute and export next-day outcomes (T+1) for a range")
    oc.add_argument("--start", required=True, help="YYYY-MM-DD")
    oc.add_argument("--end", required=True, help="YYYY-MM-DD")
    oc.add_argument("--db", default="db/scanner.db")
    oc.add_argument("--out", default="exports")
    oc.set_defaults(func=cmd_outcomes)

    # validate
    v = sp.add_parser("validate", help="Run acceptance gates for a single day")
    v.add_argument("--date", required=True, help="YYYY-MM-DD")
    v.add_argument("--db", default="db/acceptance.db")
    v.set_defaults(func=cmd_validate)

    # health
    h = sp.add_parser("health", help="Check provider readiness and keys")
    h.set_defaults(func=cmd_health)

    # env-validate
    ev = sp.add_parser("env-validate", help="Validate .env format and required keys")
    ev.set_defaults(func=lambda a: _cmd_env_validate())

    # env-format
    ef = sp.add_parser("env-format", help="Normalize .env layout (backup created)")
    ef.set_defaults(func=lambda a: _cmd_env_format())

    return p


def main() -> int:
    _load_env()
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
