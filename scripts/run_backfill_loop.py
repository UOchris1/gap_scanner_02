#!/usr/bin/env python3
"""Automated monthly backfill runner that uses gapctl commands."""
import argparse
import calendar
import json
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "scanner.db"
TASK_LOG = ROOT / "project_state" / "task_log.md"
STATE_PATH = ROOT / "project_state" / "current_state.json"
ARTIFACTS_DIR = ROOT / "project_state" / "artifacts"
DEFAULT_START_DATE = date(2022, 9, 1)
DEFAULT_TARGET_END = date(2025, 9, 20)
PHASE0_DATE = "2025-09-19"
ACCEPTANCE_ANCHORS = (7, 14, 21)


TARGET_END = DEFAULT_TARGET_END


def _fmt_cmd(parts):
    return " ".join(f'"{p}"' if " " in str(p) else str(p) for p in parts)


def run_gapctl(args, dry_run=False):
    cmd = [sys.executable, str(ROOT / "scripts" / "gapctl.py")] + list(args)
    print(f">> {_fmt_cmd(cmd)}", flush=True)
    if dry_run:
        return
    try:
        subprocess.run(cmd, cwd=str(ROOT), check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode)


def load_state():
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="ascii"))
    except Exception:
        return {}


def month_after(year, month):
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def month_end(day_start):
    last_day = calendar.monthrange(day_start.year, day_start.month)[1]
    cap = TARGET_END.day if (day_start.year == TARGET_END.year and day_start.month == TARGET_END.month) else last_day
    return date(day_start.year, day_start.month, min(last_day, cap))


def clamp_to_business_day(day_obj, window_start, window_end):
    if day_obj < window_start:
        day_obj = window_start
    if day_obj > window_end:
        day_obj = window_end
    if day_obj.weekday() < 5:
        return day_obj
    cur = day_obj
    while cur > window_start:
        cur -= timedelta(days=1)
        if cur.weekday() < 5:
            return cur
    cur = day_obj
    while cur < window_end:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            return cur
    return None


def pick_acceptance_days(range_start, range_end):
    chosen = []
    seen = set()
    for anchor in ACCEPTANCE_ANCHORS:
        target = max(range_start.day, anchor)
        target = min(target, range_end.day)
        candidate = clamp_to_business_day(date(range_start.year, range_start.month, target), range_start, range_end)
        if candidate and candidate not in seen:
            chosen.append(candidate)
            seen.add(candidate)
    cur = range_end
    while len(chosen) < 2 and cur >= range_start:
        if cur.weekday() < 5 and cur not in seen:
            chosen.append(cur)
            seen.add(cur)
        cur -= timedelta(days=1)
    chosen.sort()
    return [d.isoformat() for d in chosen]


def ensure_artifacts(day_iso):
    pm_diag = ARTIFACTS_DIR / f"pm_diag_{day_iso}.json"
    miss_audit = ARTIFACTS_DIR / f"miss_audit_filters_{day_iso}.json"
    missing = [p for p in (pm_diag, miss_audit) if not p.exists()]
    if missing:
        raise RuntimeError(f"Missing artifacts for {day_iso}: " + ", ".join(str(p) for p in missing))


def calc_month_stats(start_iso, end_iso):
    with sqlite3.connect(DB_PATH) as conn:
        days = conn.execute(
            "SELECT COUNT(DISTINCT date) FROM daily_raw WHERE date BETWEEN ? AND ?",
            (start_iso, end_iso),
        ).fetchone()[0]
        hits = conn.execute(
            "SELECT COUNT(*) FROM discovery_hits WHERE event_date BETWEEN ? AND ?",
            (start_iso, end_iso),
        ).fetchone()[0]
    return int(days or 0), int(hits or 0)


def append_task_log(month_start, days_scanned, hits, range_start, range_end):
    TASK_LOG.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    label = month_start.strftime("%Y-%m")
    entry = (
        f"[{timestamp}] backfill PASS: {label} "
        f"({days_scanned} days scanned, {hits} hits, window {range_start:%Y-%m-%d}->{range_end:%Y-%m-%d})"
    )
    with TASK_LOG.open("a", encoding="ascii") as handle:
        handle.write(entry + "\n")


def update_state(month_start, range_end):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_completed_month": month_start.strftime("%Y-%m"),
        "last_completed_day": range_end.isoformat(),
    }
    STATE_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="ascii")


def iter_months_to_run(start_date):
    current_month = date(start_date.year, start_date.month, 1)
    range_start = start_date
    while current_month <= TARGET_END:
        range_end = month_end(current_month)
        if range_start > range_end:
            current_month = month_after(current_month.year, current_month.month)
            range_start = max(range_start, current_month)
            continue
        yield current_month, range_start, range_end
        current_month = month_after(current_month.year, current_month.month)
        range_start = current_month
        if range_start > TARGET_END:
            break


def determine_start_date(user_start):
    state = load_state()
    start = user_start or DEFAULT_START_DATE
    last_day = state.get("last_completed_day")
    if last_day:
        try:
            candidate = date.fromisoformat(last_day) + timedelta(days=1)
            if candidate > start:
                start = candidate
        except Exception:
            pass
    elif state.get("last_completed_month"):
        try:
            year, month = map(int, state["last_completed_month"].split("-"))
            candidate = month_after(year, month)
            if candidate > start:
                start = candidate
        except Exception:
            pass
    if start > TARGET_END:
        return None
    return start


def phase0(db_path, dry_run=False, skip=False):
    if skip:
        print("-- Skipping Phase 0 sanity check per argument --")
        return
    print("=== Phase 0: validating 2025-09-19 ===")
    run_gapctl(["validate", "--date", PHASE0_DATE, "--db", str(db_path)], dry_run=dry_run)


def main():
    global TARGET_END
    parser = argparse.ArgumentParser(description="Run monthly scan-range/export/acceptance automatically")
    parser.add_argument("--start", help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Override target end date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--skip-phase0", action="store_true", help="Skip the initial 2025-09-19 validation gate")
    parser.add_argument("--max-months", type=int, default=None, help="Limit number of months to process")
    parser.add_argument("--dry-run", action="store_true", help="Print planned commands without executing or writing state")
    args = parser.parse_args()

    user_start = None
    if args.start:
        try:
            user_start = date.fromisoformat(args.start)
        except ValueError:
            raise SystemExit(f"Invalid --start date: {args.start}")
    if args.end:
        try:
            TARGET_END = date.fromisoformat(args.end)
        except ValueError:
            raise SystemExit(f"Invalid --end date: {args.end}")

    start_date = determine_start_date(user_start)
    if not start_date:
        print("No pending dates to process (state is already beyond target window).")
        return

    phase0(DB_PATH, dry_run=args.dry_run, skip=args.skip_phase0)

    months = list(iter_months_to_run(start_date))
    if args.max_months is not None:
        months = months[: args.max_months]
    print(
        "Planned months: "
        + [f"{m.strftime('%Y-%m')} ({rs:%Y-%m-%d}->{re:%Y-%m-%d})" for m, rs, re in months].__str__()
    )

    for month_start, range_start, range_end in months:
        month_label = month_start.strftime("%Y-%m")
        print(f"=== Processing {month_label} ({range_start} to {range_end}) ===")
        start_iso = range_start.isoformat()
        end_iso = range_end.isoformat()

        run_gapctl(["scan-range", "--start", start_iso, "--end", end_iso, "--db", str(DB_PATH)], dry_run=args.dry_run)
        run_gapctl(["export", "--start", start_iso, "--end", end_iso, "--db", str(DB_PATH), "--out", str(ROOT / "exports")], dry_run=args.dry_run)

        acceptance_days = pick_acceptance_days(range_start, range_end)
        print(f"Acceptance days: {acceptance_days}")
        for day_iso in acceptance_days:
            run_gapctl(["validate", "--date", day_iso, "--db", str(DB_PATH)], dry_run=args.dry_run)
            if not args.dry_run:
                ensure_artifacts(day_iso)

        if args.dry_run:
            continue

        days_scanned, hits = calc_month_stats(start_iso, end_iso)
        append_task_log(month_start, days_scanned, hits, range_start, range_end)
        update_state(month_start, range_end)
        print(f"Logged completion for {month_label}: {days_scanned} days, {hits} hits")

    print("All requested months processed. Stop-on-failure policy enforced.")


if __name__ == "__main__":
    main()
