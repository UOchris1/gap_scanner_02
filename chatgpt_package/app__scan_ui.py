# -*- coding: ascii -*-
"""
Thin Streamlit UI that reuses the same integration shim and export helpers
as the CLI. Business logic stays in src/ and scripts/.

Run:  streamlit run app/scan_ui.py
"""

import os
import sqlite3
import datetime as dt
import tempfile

import streamlit as st

from src.integration.cli_bridge import process_day_zero_miss
from scripts.export_reports import export_hits


st.set_page_config(page_title="Gap Scanner - Zero-Miss", layout="wide")
st.title("Gap Scanner - Zero-Miss")

with st.sidebar:
    date = st.date_input("Scan date (ET)", dt.date.today())
    db_path = st.text_input("SQLite DB", "db/scanner.db")
    start = st.date_input("Export start", date)
    end = st.date_input("Export end", date)
    run_btn = st.button("Run Scan")
    export_btn = st.button("Export CSV")

if run_btn:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    res = process_day_zero_miss(date.isoformat(), db_path, providers={})
    st.success("Scan complete")
    st.json(res)

if export_btn:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    with sqlite3.connect(db_path) as conn:
        export_hits(conn, start.isoformat(), end.isoformat(), tmp.name)
    st.success("Export ready")
    with open(tmp.name, "rb") as f:
        st.download_button(
            label="Download CSV",
            data=f,
            file_name=f"discovery_hits_{start.isoformat()}_{end.isoformat()}.csv",
            mime="text/csv",
        )

