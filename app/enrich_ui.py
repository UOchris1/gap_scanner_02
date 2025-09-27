# -*- coding: ascii -*-
"""
Streamlit front end for Gap Scanner
- Select date mode (today, custom range, catch-up)
- Select database path
- Select default output folder and export file name pattern
- Start enrichment with per-day progress and live log tail
- Export CSVs and optional T+1 outcomes

Run:  streamlit run app/enrich_ui.py
"""

import os
import sys
import json
import time
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
import platform

import streamlit as st
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
ENV_PATH = PROJECT_ROOT / ".env"
STATE_DIR = PROJECT_ROOT / "project_state"
ARTIFACTS_DIR = STATE_DIR / "artifacts"
UI_SETTINGS = STATE_DIR / "ui_settings.json"


def _load_env():
    load_dotenv(ENV_PATH)


def _load_settings():
    import json
    if UI_SETTINGS.exists():
        try:
            return json.loads(UI_SETTINGS.read_text(encoding="ascii", errors="replace"))
        except Exception:
            return {}
    return {}


def _save_settings(settings):
    import json
    UI_SETTINGS.parent.mkdir(parents=True, exist_ok=True)
    UI_SETTINGS.write_text(json.dumps(settings, indent=2), encoding="ascii", errors="replace")


def _iter_days(start_iso: str, end_iso: str):
    s = datetime.strptime(start_iso, "%Y-%m-%d").date()
    e = datetime.strptime(end_iso, "%Y-%m-%d").date()
    d = s
    while d <= e:
        if d.weekday() < 5:
            yield d.isoformat()
        d += timedelta(days=1)


def _last_scanned_date(db_path: str):
    try:
        with sqlite3.connect(db_path) as c:
            row = c.execute("select max(date) from daily_raw").fetchone()
            return row[0]
    except Exception:
        return None


def _tail_log(day_iso: str, lines: int = 20):
    p = ARTIFACTS_DIR / f"scan_{day_iso}.log"
    if not p.exists():
        return "(no log yet)"
    try:
        txt = p.read_text(encoding="ascii", errors="replace").splitlines()
        return "\n".join(txt[-lines:])
    except Exception:
        return "(cannot read log)"


def _enrich_day(day_iso: str, db_path: str) -> dict:
    from src.integration.cli_bridge import process_day_zero_miss
    return process_day_zero_miss(day_iso, db_path, providers={})


def _export_range(db_path: str, start_iso: str, end_iso: str, hits_out: Path, completeness_out: Path):
    from scripts.export_reports import export_hits, export_day_completeness
    with sqlite3.connect(db_path) as conn:
        export_hits(conn, start_iso, end_iso, str(hits_out))
        export_day_completeness(conn, str(completeness_out))


def _compute_outcomes(db_path: str, start_iso: str, end_iso: str, out_csv: Path):
    from src.core.database_operations import recompute_next_day_outcomes_range
    import csv
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
    return n


def main():
    _load_env()
    st.set_page_config(page_title="Gap Scanner Enrichment", layout="wide")
    st.title("Gap Scanner ??? Enrichment Console")

    settings = _load_settings()

    # Controls
    with st.sidebar:
        st.header("Configuration")
        default_db = settings.get("db_path", "db/scanner.db")
        db_path = st.text_input("Database path", value=default_db)

        default_out = settings.get("output_dir", os.getenv("OUTPUT_DIR", "exports"))
        output_dir = st.text_input("Output folder", value=default_out, help="Folder for CSV outputs")
        out_name = st.text_input("Output base name", value=settings.get("output_base", "discovery_hits"))

        save_defaults = st.checkbox("Save as defaults", value=True)

        st.divider()
        st.caption("ThetaData plan: STANDARD (2 threads recommended)")

        st.divider()
        st.header("Automation")
        cfg_path = Path("project_state/auto_enrich_config.json")
        st.caption("Control is file-based so scheduled jobs can honor toggles.")
        # Load current config or defaults
        try:
            cfg = json.loads(cfg_path.read_text(encoding="ascii", errors="replace")) if cfg_path.exists() else {}
        except Exception:
            cfg = {}
        enabled = st.checkbox("Enable daily catch-up", value=bool(cfg.get("enabled", False)))
        daily_time = st.text_input("Run at (HH:MM)", value=cfg.get("daily_time", "06:30"))
        lookback = st.number_input("Lookback days (if no data)", min_value=1, max_value=30, value=int(cfg.get("lookback_days", 5)))
        outcomes = st.checkbox("Compute T+1 outcomes", value=bool(cfg.get("compute_outcomes", False)))
        if st.button("Save automation config"):
            new_cfg = {
                "enabled": enabled,
                "daily_time": daily_time,
                "db_path": db_path,
                "output_dir": output_dir,
                "output_base": out_name,
                "lookback_days": int(lookback),
                "compute_outcomes": bool(outcomes),
            }
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(json.dumps(new_cfg, indent=2), encoding="ascii", errors="replace")
            st.success("Automation config saved")
        st.caption("Run scheduler in background: python scripts/auto_enrich.py --loop")
        st.caption("One-time catch up now: python scripts/auto_enrich.py --run-once")

        st.divider()
        st.header("Diagnostics")
        st.caption("Load per-day R1 venue counters (from pm_diag_{date}.json)")
        diag_date = st.text_input("Diagnostics date (YYYY-MM-DD)", value="")
        if st.button("Load Venue Counters") and diag_date:
            try:
                diag_path = ARTIFACTS_DIR / f"pm_diag_{diag_date}.json"
                if diag_path.exists():
                    d = json.loads(diag_path.read_text(encoding="ascii", errors="replace"))
                    st.code(json.dumps(d, indent=2), language="json")
                    # Compute a simple health metric if counters present
                    try:
                        buckets = [v for v in d.values() if isinstance(v, dict)]
                        s200 = sum(int(v.get("200", 0)) for v in buckets)
                        s204 = sum(int(v.get("204", 0)) for v in buckets)
                        s472 = sum(int(v.get("472", 0)) for v in buckets)
                        denom = max(1, s200 + s204 + s472)
                        health = float(s200) / float(denom)
                        st.write(f"R1 health: {health:.3f} (200/(200+204+472))")
                    except Exception:
                        pass
                else:
                    st.warning(f"No diagnostics file: {diag_path}")
            except Exception:
                st.warning("Unable to read diagnostics.")

        st.divider()
        st.header("Shortcuts")
        colA, colB = st.columns(2)
        with colA:
            if st.button("Open Exports Folder"):
                try:
                    path = Path(output_dir).resolve()
                    path.mkdir(parents=True, exist_ok=True)
                    if platform.system() == "Windows":
                        os.startfile(str(path))
                    elif platform.system() == "Darwin":
                        os.system(f"open '{path}'")
                    else:
                        os.system(f"xdg-open '{path}'")
                except Exception:
                    pass
        with colB:
            if st.button("Open Artifacts Folder"):
                try:
                    path = ARTIFACTS_DIR.resolve()
                    path.mkdir(parents=True, exist_ok=True)
                    if platform.system() == "Windows":
                        os.startfile(str(path))
                    elif platform.system() == "Darwin":
                        os.system(f"open '{path}'")
                    else:
                        os.system(f"xdg-open '{path}'")
                except Exception:
                    pass

        # Create .bat helpers on Windows
        if platform.system() == "Windows":
            st.caption("Create Start/Stop scripts (.bat) for quick access")
            if st.button("Create/Update .bat Shortcuts"):
                auto_dir = PROJECT_ROOT / "automation_shortcuts"
                auto_dir.mkdir(parents=True, exist_ok=True)
                start_bat = auto_dir / "start_auto_enrich.bat"
                stop_bat = auto_dir / "stop_auto_enrich.bat"
                runonce_bat = auto_dir / "run_once_auto_enrich.bat"
                # Build content
                start_content = (
                    "@echo off\r\n"
                    "cd /d %~dp0..\r\n"
                    "python scripts\\auto_enrich.py --enable\r\n"
                    "start \"\" cmd /c \"python scripts\\auto_enrich.py --loop\"\r\n"
                )
                stop_content = (
                    "@echo off\r\n"
                    "cd /d %~dp0..\r\n"
                    "python scripts\\auto_enrich.py --disable\r\n"
                    "echo Disabled automation; close the running loop window if open.\r\n"
                )
                runonce_content = (
                    "@echo off\r\n"
                    "cd /d %~dp0..\r\n"
                    "python scripts\\auto_enrich.py --run-once\r\n"
                )
                start_bat.write_text(start_content, encoding="ascii", errors="replace")
                stop_bat.write_text(stop_content, encoding="ascii", errors="replace")
                runonce_bat.write_text(runonce_content, encoding="ascii", errors="replace")
                st.success(f"Shortcuts created in {auto_dir}")
                try:
                    os.startfile(str(auto_dir))
                except Exception:
                    pass

    mode = st.radio("Scan Mode", ["Today", "Custom Range", "Catch Up (last scanned ??? today)"])

    today = date.today()
    if mode == "Today":
        start_date = st.date_input("Date", value=today)
        end_date = start_date
    elif mode == "Custom Range":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start", value=today - timedelta(days=5))
        with col2:
            end_date = st.date_input("End", value=today)
    else:
        last = _last_scanned_date(db_path) or (today - timedelta(days=1)).isoformat()
        st.write(f"Last scanned in DB: {last}")
        start_date = datetime.strptime(last, "%Y-%m-%d").date()
        end_date = today

    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    run_enrich = st.button("Start Enrichment", type="primary")

    if run_enrich:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        days = list(_iter_days(start_iso, end_iso))
        total = len(days)
        prog = st.progress(0)
        status = st.empty()
        logbox = st.empty()

        ok = 0
        for i, d in enumerate(days, start=1):
            status.write(f"Processing {d} ({i}/{total})???")
            res = _enrich_day(d, db_path)
            ok += 1 if res.get("status") == "ok" else 0
            # show recent log lines
            logbox.code(_tail_log(d), language="text")
            prog.progress(int(i * 100 / total))
            # small pause so UI updates
            time.sleep(0.2)

        st.success(f"Completed {ok}/{total} days")

        # Exports
        hits_out = Path(output_dir) / f"{out_name}_{start_iso}_{end_iso}.csv"
        completeness_out = Path(output_dir) / "day_completeness.csv"
        _export_range(db_path, start_iso, end_iso, hits_out, completeness_out)
        st.write("Exports written:")
        st.write(f"- {hits_out}")
        st.write(f"- {completeness_out}")

        # Optional outcomes
        if st.checkbox("Also compute T+1 outcomes (next day positive)"):
            out_csv = Path(output_dir) / f"next_day_outcomes_{start_iso}_{end_iso}.csv"
            n = _compute_outcomes(db_path, start_iso, end_iso, out_csv)
            st.write(f"Outcomes rows upserted: {n}")
            st.write(f"- {out_csv}")

        if save_defaults:
            settings.update({
                "db_path": db_path,
                "output_dir": output_dir,
                "output_base": out_name,
            })
            _save_settings(settings)

    st.divider()
    st.caption("Tip: For unattended daily runs, use the Automation panel (config + shortcuts) or run 'scripts/auto_enrich.py --loop'.")
    # Show current scheduler status if present
    try:
        status_txt = (Path("project_state/auto_enrich_status.json").read_text(encoding="ascii", errors="replace") if Path("project_state/auto_enrich_status.json").exists() else "{}")
        st.caption("Automation status (from status file):")
        st.code(status_txt, language="json")
    except Exception:
        pass


if __name__ == "__main__":
    main()

