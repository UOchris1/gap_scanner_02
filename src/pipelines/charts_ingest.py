# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import pandas as pd
from typing import Iterable
from src.core.chart_db import ChartDB
from src.core.chart_windows import intraday_1m_window, intraday_5m_window, daily_window
from src.providers.bars_provider import ThetaAgg, polygon_minute, polygon_daily

REQ_COLUMNS = [["symbol","ticker"], ["trigger_date","date","event_date"]]

def _flex_cols(df: pd.DataFrame) -> tuple[str,str]:
    """Return (symbol_col, date_col) matching available columns."""
    s = next((c for c in REQ_COLUMNS[0] if c in df.columns), None)
    d = next((c for c in REQ_COLUMNS[1] if c in df.columns), None)
    if not s or not d:
        raise ValueError(f"CSV must contain symbol/ticker and trigger_date/date columns. Found: {df.columns.tolist()}")
    return s, d

def ingest_from_csv(db_path: str, csv_path: str, limit: int | None = None) -> dict:
    """For each (symbol, trigger_date) row, persist 1m, 5m, 1d windows."""
    db = ChartDB(db_path)
    hits = pd.read_csv(csv_path)
    sym_col, date_col = _flex_cols(hits)
    hits = hits[[sym_col,date_col]].dropna().drop_duplicates()
    if limit: hits = hits.head(limit)

    theta = ThetaAgg()
    total = {"1m":0, "5m":0, "1d":0, "symbols": set(), "rows": len(hits)}

    for _, row in hits.iterrows():
        sym = str(row[sym_col]).upper().strip()
        tday = str(pd.to_datetime(row[date_col]).date())   # YYYY-MM-DD

        # 1m: T-1..T  (Theta -> 1m; fallback Polygon 1m)
        s1m,e1m = intraday_1m_window(tday)
        # Theta v3/v1 returns one day per call; fetch for T-1 & T
        for d in [pd.to_datetime(tday) - pd.Timedelta(days=1), pd.to_datetime(tday)]:
            df1 = theta.one_minute(sym, d.date().isoformat()) if theta.v3_ok or theta.v1_ok else pd.DataFrame()
            if df1.empty:
                df1 = polygon_minute(sym, d.date().isoformat(), d.date().isoformat(), mult=1)
            total["1m"] += db.upsert_df("1m", df1)

        # 5m: T-3..T+7  (prefer resample from 1m if present; else Polygon 5m per day)
        s5m,e5m = intraday_5m_window(tday, 3, 7)
        cur = pd.to_datetime(s5m).tz_convert("America/New_York").date()
        end = pd.to_datetime(e5m).tz_convert("America/New_York").date()
        while cur <= end:
            # try build from Theta 1m for that date
            d1 = theta.one_minute(sym, cur.isoformat()) if theta.v3_ok or theta.v1_ok else pd.DataFrame()
            if d1.empty:
                d5 = polygon_minute(sym, cur.isoformat(), cur.isoformat(), mult=5)
            else:
                d1["ts_utc"] = pd.to_datetime(d1["ts_utc"], utc=True)
                d1 = d1.set_index("ts_utc")
                o = d1["open"].resample("5min").first()
                h = d1["high"].resample("5min").max()
                l = d1["low"].resample("5min").min()
                c = d1["close"].resample("5min").last()
                v = d1["volume"].resample("5min").sum().fillna(0)
                d5 = pd.concat({"open":o,"high":h,"low":l,"close":c,"volume":v}, axis=1).dropna(how="all").reset_index()
                d5["symbol"]=sym; d5["provider"]="theta"; d5["adjusted"]=0
                d5["ts_utc"] = d5["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                d5 = d5[["symbol","ts_utc","open","high","low","close","volume","provider","adjusted"]]
            total["5m"] += db.upsert_df("5m", d5)
            cur = cur + pd.Timedelta(days=1)

        # 1d: T-2y..T+3m (Polygon daily unadjusted default)
        sd,ed = daily_window(tday, years_back=2, months_fwd=3)
        d1d = polygon_daily(sym, sd[:10], ed[:10], adjusted=False)
        total["1d"] += db.upsert_df("1d", d1d)

        total["symbols"].add(sym)

    total["symbols"]=len(total["symbols"])
    return {"status":"ok", **total}
