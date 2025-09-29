# -*- coding: ascii -*-
"""
Thin Streamlit UI that reuses the same integration shim and export helpers
as the CLI. Business logic stays in src/ and scripts/.

Run:  streamlit run app/scan_ui.py
"""

import os
import re
import sqlite3
import subprocess
import sys
import datetime as dt
from typing import List, Tuple, Optional, Callable

import streamlit as st

from src.integration.cli_bridge import process_day_zero_miss
from scripts.export_reports import export_hits


def _parse_multi_dates(raw_text: str) -> Tuple[List[dt.date], List[str]]:
    """Return parsed dates and any tokens that failed ISO parsing."""
    dates: List[dt.date] = []
    invalid: List[str] = []
    seen = set()
    for token in re.split(r"[,\n]+", raw_text or ""):
        token = token.strip()
        if not token:
            continue
        try:
            parsed = dt.date.fromisoformat(token)
        except ValueError:
            invalid.append(token)
            continue
        if parsed not in seen:
            dates.append(parsed)
            seen.add(parsed)
    dates.sort()
    return dates, invalid


def _open_directory(path: str) -> None:
    """Open folder in OS file explorer."""
    abs_path = os.path.abspath(path)
    if not os.path.isdir(abs_path):
        os.makedirs(abs_path, exist_ok=True)
    if sys.platform.startswith("win"):
        os.startfile(abs_path)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", abs_path])
    else:
        subprocess.Popen(["xdg-open", abs_path])


def _trigger_rerun() -> None:
    rerun: Optional[Callable[[], None]] = getattr(st, "rerun", None)
    if rerun is None:
        rerun = getattr(st, "experimental_rerun", None)
    if rerun:
        rerun()


st.set_page_config(page_title="Gap Scanner - Zero-Miss", layout="wide")
st.title("Gap Scanner - Zero-Miss")

today = dt.date.today()
dates_to_scan: List[dt.date] = []
range_error = ""

# Sidebar state defaults
if "export_dir" not in st.session_state:
    st.session_state.export_dir = "exports"
if "multi_dates" not in st.session_state:
    st.session_state.multi_dates = []  # list of ISO strings
if "show_folder_browser" not in st.session_state:
    st.session_state.show_folder_browser = False
if "fbrowse_path" not in st.session_state:
    st.session_state.fbrowse_path = ""
if "_fbrowse_refresh" not in st.session_state:
    st.session_state._fbrowse_refresh = False
if "_export_dir_refresh" not in st.session_state:
    st.session_state._export_dir_refresh = False

with st.sidebar:
    db_path = st.text_input("SQLite DB", "db/scanner.db")
    scan_mode = st.radio(
        "Scan mode",
        ("Single day", "Multiple days", "Date range"),
        index=0,
    )

    if scan_mode == "Single day":
        single_date = st.date_input("Scan date (ET)", today, key="scan_single")
        dates_to_scan = [single_date]
    elif scan_mode == "Multiple days":
        # Calendar picker with Add/Clear controls
        pick_date = st.date_input("Pick date to add", today, key="multi_pick")
        cols_md = st.columns([1, 1, 1])
        if cols_md[0].button("Add date", key="add_multi_date"):
            iso = pick_date.isoformat()
            if iso not in st.session_state.multi_dates:
                st.session_state.multi_dates.append(iso)
                st.session_state.multi_dates.sort()
                _trigger_rerun()
        if cols_md[1].button("Clear dates", key="clear_multi_dates"):
            st.session_state.multi_dates = []
            _trigger_rerun()
        # Optional: quick text paste for power users
        with st.expander("Paste dates (optional)"):
            paste_text = st.text_area(
                "YYYY-MM-DD, comma or newline separated", "", key="paste_multi", height=100
            )
            if st.button("Add pasted", key="add_pasted"):
                ds, bad = _parse_multi_dates(paste_text)
                for d in ds:
                    iso = d.isoformat()
                    if iso not in st.session_state.multi_dates:
                        st.session_state.multi_dates.append(iso)
                st.session_state.multi_dates.sort()
                if bad:
                    st.warning("Ignored invalid: " + ", ".join(bad))
                _trigger_rerun()
        # Consume selection
        dates_to_scan = [dt.date.fromisoformat(s) for s in st.session_state.multi_dates]
        if dates_to_scan:
            st.caption("Selected: " + ", ".join(d.isoformat() for d in dates_to_scan))
    else:
        range_value = st.date_input(
            "Scan range (ET)",
            (today, today),
            key="scan_range_input",
        )
        if isinstance(range_value, tuple):
            range_start, range_end = range_value
        else:
            range_start = range_end = range_value
        if range_start > range_end:
            range_error = "Range start must be on or before the end date."
            dates_to_scan = []
        else:
            span = (range_end - range_start).days
            dates_to_scan = [range_start + dt.timedelta(days=i) for i in range(span + 1)]

    export_start_default = dates_to_scan[0] if dates_to_scan else today
    export_end_default = dates_to_scan[-1] if dates_to_scan else today

    export_start = st.date_input("Export start", export_start_default, key="export_start")
    export_end = st.date_input("Export end", export_end_default, key="export_end")

    # Export folder controls ? avoid writing to widget key after creation
    export_dir_value = st.text_input(
        "Export folder",
        value=st.session_state.export_dir,
        key="export_dir_text",
        help="Type a folder path or use the buttons below.",
    ).strip()
    if export_dir_value and export_dir_value != st.session_state.export_dir:
        st.session_state.export_dir = export_dir_value

    cols = st.columns([1, 1])
    if cols[0].button("Select folder", key="select_folder_btn"):
        # Open a non-blocking inline browser (avoids hidden OS dialog + grey overlay)
        st.session_state.show_folder_browser = True
        st.session_state.fbrowse_path = st.session_state.export_dir or os.getcwd()
        st.session_state._fbrowse_refresh = True
        _trigger_rerun()

    # Inline folder browser
    if st.session_state.get("show_folder_browser"):
        with st.expander("Select export folder", expanded=True):
            cur = st.session_state.get("fbrowse_path") or st.session_state.export_dir or os.getcwd()
            cur = os.path.abspath(cur)
            # If a navigation action requested a refresh, clear the widget state
            if st.session_state.get("_fbrowse_refresh"):
                st.session_state.pop("fbrowse_path_input", None)
                st.session_state._fbrowse_refresh = False

            # Use a separate widget key; never assign to this key directly
            current = st.text_input("Current folder", value=cur, key="fbrowse_path_input")
            bcols = st.columns([1, 1, 1, 1])
            if bcols[0].button("Up one", key="fbrowse_up"):
                st.session_state.fbrowse_path = os.path.dirname(cur)
                st.session_state._fbrowse_refresh = True
                _trigger_rerun()
            try:
                entries = sorted([d for d in os.listdir(cur) if os.path.isdir(os.path.join(cur, d))])
            except Exception as exc:
                entries = []
                st.warning(f"Cannot list folder: {exc}")
            sel = st.selectbox("Subfolders", entries, index=0 if entries else None, key="fbrowse_select")
            if bcols[1].button("Open", key="fbrowse_open") and sel:
                st.session_state.fbrowse_path = os.path.join(cur, sel)
                st.session_state._fbrowse_refresh = True
                _trigger_rerun()
            if bcols[2].button("Use this", key="fbrowse_use"):
                # Apply exactly what the user sees in the text box
                use_path = os.path.abspath((current or cur).strip())
                st.session_state.export_dir = use_path
                st.session_state.show_folder_browser = False
                st.session_state._export_dir_refresh = True
                _trigger_rerun()
            if bcols[3].button("Close", key="fbrowse_close"):
                st.session_state.show_folder_browser = False
                _trigger_rerun()
    if cols[1].button("Open folder", key="open_folder_btn"):
        try:
            _open_directory(st.session_state.export_dir or "exports")
        except Exception as exc:  # pragma: no cover - OS dependent
            st.error(f"Unable to open folder: {exc}")

    export_filename = st.text_input(
        "Export filename (optional, .csv)",
        key="export_filename",
    ).strip()

    run_btn = st.button("Run Scan", key="run_button")
    export_btn = st.button("Export CSV", key="export_button")

if range_error:
    st.error(range_error)

if run_btn:
    if not dates_to_scan:
        st.warning("No scan dates selected.")
    else:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        results = []
        for date_obj in dates_to_scan:
            date_iso = date_obj.isoformat()
            try:
                result = process_day_zero_miss(date_iso, db_path, providers={})
            except Exception as exc:  # pragma: no cover - surfaced in UI
                st.error(f"Scan failed for {date_iso}: {exc}")
                continue
            results.append((date_iso, result))
        if results:
            st.success(f"Completed scans for {len(results)} day(s).")
            for date_iso, result in results:
                st.write(f"### Scan results for {date_iso}")
                st.json(result)

if export_btn:
    if export_start > export_end:
        st.error("Export start must be on or before export end.")
    else:
        export_dir_final = (st.session_state.export_dir or "exports").strip() or "exports"
        start_iso = export_start.isoformat()
        end_iso = export_end.isoformat()
        filename = export_filename or f"discovery_hits_{start_iso}_{end_iso}.csv"
        if not filename.lower().endswith(".csv"):
            filename += ".csv"
        os.makedirs(export_dir_final, exist_ok=True)
        target_path = os.path.join(export_dir_final, filename)
        with sqlite3.connect(db_path) as conn:
            export_hits(conn, start_iso, end_iso, target_path)
        st.success(f"Export saved to {target_path}")
        with open(target_path, "rb") as export_file:
            st.download_button(
                label="Download CSV",
                data=export_file.read(),
                file_name=filename,
                mime="text/csv",
                key=f"download_{start_iso}_{end_iso}",
            )
