# -*- coding: ascii -*-
"""
Automated daily catch-up enrichment runner.

Controls via config file so you can toggle on/off without changing the task:
  project_state/auto_enrich_config.json

Writes status to:
  project_state/auto_enrich_status.json
  project_state/artifacts/auto_enrich.log

Usage:
  python scripts/auto_enrich.py --status
  python scripts/auto_enrich.py --enable
  python scripts/auto_enrich.py --disable
  python scripts/auto_enrich.py --run-once
  python scripts/auto_enrich.py --loop   # long-running scheduler (use Task Scheduler/cron)
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, date
from pathlib import Path
import sqlite3
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "project_state" / "auto_enrich_config.json"
STATUS_PATH = PROJECT_ROOT / "project_state" / "auto_enrich_status.json"
ARTIFACTS_DIR = PROJECT_ROOT / "project_state" / "artifacts"
LOG_PATH = ARTIFACTS_DIR / "auto_enrich.log"


DEFAULT_CONFIG = {
    "enabled": False,
    "daily_time": "06:30",  # local time HH:MM
    "db_path": "db/scanner.db",
    "output_dir": "exports",
    "output_base": "discovery_hits",
    "lookback_days": 5,
    "compute_outcomes": False,
}


def _load_env():
    load_dotenv(PROJECT_ROOT / ".env")


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="ascii", errors="replace"))
    except Exception:
        return default


def _write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="ascii", errors="replace")


def _log(line: str):
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {line}\n"
    try:
        with open(LOG_PATH, "a", encoding="ascii", errors="replace") as f:
            f.write(msg)
    except Exception:
        pass


def _last_scanned_date(db_path: str):
    try:
        with sqlite3.connect(db_path) as c:
            row = c.execute("select max(date) from daily_raw").fetchone()
            return row[0]
    except Exception:
        return None


def _iter_days(start_iso: str, end_iso: str):
    s = datetime.strptime(start_iso, "%Y-%m-%d").date()
    e = datetime.strptime(end_iso, "%Y-%m-%d").date()
    d = s
    while d <= e:
        if d.weekday() < 5:
            yield d.isoformat()
        d += timedelta(days=1)


def _run_enrichment_range(db_path: str, start_iso: str, end_iso: str):
    from src.integration.cli_bridge import process_day_zero_miss
    ok, total = 0, 0
    for d in _iter_days(start_iso, end_iso):
        total += 1
        _log(f"Enrich {d} start")
        res = process_day_zero_miss(d, db_path, providers={})
        _log(f"Enrich {d} status={res.get('status')}")
        ok += 1 if res.get("status") == "ok" else 0
    return ok, total


def _export_range(db_path: str, start_iso: str, end_iso: str, output_dir: str, output_base: str):
    from scripts.export_reports import export_hits, export_day_completeness
    hits_out = Path(output_dir) / f"{output_base}_{start_iso}_{end_iso}.csv"
    with sqlite3.connect(db_path) as conn:
        export_hits(conn, start_iso, end_iso, str(hits_out))
        export_day_completeness(conn, str(Path(output_dir) / "day_completeness.csv"))
    return str(hits_out)


def _compute_outcomes(db_path: str, start_iso: str, end_iso: str, output_dir: str):
    from src.core.database_operations import recompute_next_day_outcomes_range
    import csv
    out_csv = Path(output_dir) / f"next_day_outcomes_{start_iso}_{end_iso}.csv"
    n = recompute_next_day_outcomes_range(db_path, start_iso, end_iso)
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
            (start_iso, end_iso),
        ):
            w.writerow(row)
    return str(out_csv), n


def _compute_next_run(now: datetime, hhmm: str) -> datetime:
    hh, mm = (int(x) for x in hhmm.split(":"))
    today_run = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if now <= today_run:
        return today_run
    return today_run + timedelta(days=1)


def _catchup_range(db_path: str, lookback_days: int) -> (str, str):
    today = date.today()
    last = _last_scanned_date(db_path)
    if last:
        start = datetime.strptime(last, "%Y-%m-%d").date()
    else:
        start = today - timedelta(days=max(lookback_days, 1))
    end = today
    return start.isoformat(), end.isoformat()


def _update_status(**kwargs):
    status = _read_json(STATUS_PATH, {})
    status.update(kwargs)
    _write_json(STATUS_PATH, status)


def cmd_status():
    cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
    st = _read_json(STATUS_PATH, {})
    print(json.dumps({"config": cfg, "status": st}, indent=2))
    return 0


def cmd_enable():
    cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
    cfg["enabled"] = True
    _write_json(CONFIG_PATH, cfg)
    print("enabled=true")
    return 0


def cmd_disable():
    cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
    cfg["enabled"] = False
    _write_json(CONFIG_PATH, cfg)
    print("enabled=false")
    return 0


def cmd_run_once():
    _load_env()
    cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
    db_path = cfg.get("db_path", DEFAULT_CONFIG["db_path"])
    output_dir = cfg.get("output_dir", DEFAULT_CONFIG["output_dir"])
    output_base = cfg.get("output_base", DEFAULT_CONFIG["output_base"])
    lookback_days = int(cfg.get("lookback_days", DEFAULT_CONFIG["lookback_days"]))
    compute_outcomes = bool(cfg.get("compute_outcomes", DEFAULT_CONFIG["compute_outcomes"]))
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    start_iso, end_iso = _catchup_range(db_path, lookback_days)
    _log(f"Run-once start range {start_iso}..{end_iso}")
    ok, total = _run_enrichment_range(db_path, start_iso, end_iso)
    hits_path = _export_range(db_path, start_iso, end_iso, output_dir, output_base)
    oc_rows = 0
    if compute_outcomes:
        _, oc_rows = _compute_outcomes(db_path, start_iso, end_iso, output_dir)
    _update_status(last_run=datetime.now().isoformat(), last_range=[start_iso, end_iso], last_ok=ok, last_total=total, last_hits=hits_path, last_outcomes_rows=oc_rows)
    _log(f"Run-once complete ok={ok}/{total}")
    return 0


def cmd_loop():
    _load_env()
    while True:
        cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
        enabled = bool(cfg.get("enabled", False))
        daily_time = cfg.get("daily_time", DEFAULT_CONFIG["daily_time"]) or DEFAULT_CONFIG["daily_time"]
        db_path = cfg.get("db_path", DEFAULT_CONFIG["db_path"])
        output_dir = cfg.get("output_dir", DEFAULT_CONFIG["output_dir"])
        output_base = cfg.get("output_base", DEFAULT_CONFIG["output_base"])
        lookback_days = int(cfg.get("lookback_days", DEFAULT_CONFIG["lookback_days"]))
        compute_outcomes = bool(cfg.get("compute_outcomes", DEFAULT_CONFIG["compute_outcomes"]))

        now = datetime.now()
        next_run = _compute_next_run(now, daily_time)
        _update_status(next_run=next_run.isoformat(), enabled=enabled)

        if not enabled:
            _log("Loop: disabled; sleeping 60s")
            time.sleep(60)
            continue

        # Sleep until next run time
        sleep_sec = max(1, int((next_run - now).total_seconds()))
        _log(f"Sleeping until {next_run.isoformat()} ({sleep_sec}s)")
        time.sleep(sleep_sec)

        # Run catch-up
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            start_iso, end_iso = _catchup_range(db_path, lookback_days)
            _log(f"Run start range {start_iso}..{end_iso}")
            ok, total = _run_enrichment_range(db_path, start_iso, end_iso)
            hits_path = _export_range(db_path, start_iso, end_iso, output_dir, output_base)
            oc_rows = 0
            if compute_outcomes:
                _, oc_rows = _compute_outcomes(db_path, start_iso, end_iso, output_dir)
            _update_status(last_run=datetime.now().isoformat(), last_range=[start_iso, end_iso], last_ok=ok, last_total=total, last_hits=hits_path, last_outcomes_rows=oc_rows)
            _log(f"Run complete ok={ok}/{total}")
        except Exception as e:
            _log(f"Run error: {e}")
            _update_status(last_error=str(e), last_run=datetime.now().isoformat())


def main(argv):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--enable", action="store_true")
    ap.add_argument("--disable", action="store_true")
    ap.add_argument("--run-once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args(argv)

    if args.status:
        return cmd_status()
    if args.enable:
        return cmd_enable()
    if args.disable:
        return cmd_disable()
    if args.run_once:
        return cmd_run_once()
    if args.loop:
        return cmd_loop()
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

