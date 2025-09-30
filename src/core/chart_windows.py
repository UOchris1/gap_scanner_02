# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from datetime import datetime, timedelta, timezone

NY = "America/New_York"

def iso(date_like) -> str:
    """Return YYYY-MM-DD for date or string."""
    return pd.to_datetime(date_like).date().isoformat()

def utc_end_of_day(d) -> str:
    end = pd.to_datetime(iso(d)) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    return end.tz_localize(NY).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

def utc_start_of_day(d) -> str:
    start = pd.to_datetime(iso(d))
    return start.tz_localize(NY).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

def daily_window(trigger_date: str, years_back=2, months_fwd=3) -> tuple[str,str]:
    t = pd.to_datetime(iso(trigger_date))
    start = (t - pd.DateOffset(years=years_back)).date().isoformat()
    end   = (t + pd.DateOffset(months=months_fwd)).date().isoformat()
    return utc_start_of_day(start), utc_end_of_day(end)

def intraday_1m_window(trigger_date: str) -> tuple[str,str]:
    """T-1 .. T (inclusive) in UTC ISO."""
    t = pd.to_datetime(iso(trigger_date))
    s = (t - pd.Timedelta(days=1)).date().isoformat()
    e = t.date().isoformat()
    return utc_start_of_day(s), utc_end_of_day(e)

def intraday_5m_window(trigger_date: str, days_back=3, days_fwd=7) -> tuple[str,str]:
    t = pd.to_datetime(iso(trigger_date))
    s = (t - pd.Timedelta(days=days_back)).date().isoformat()
    e = (t + pd.Timedelta(days=days_fwd)).date().isoformat()
    return utc_start_of_day(s), utc_end_of_day(e)
