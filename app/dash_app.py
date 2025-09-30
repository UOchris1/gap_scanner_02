# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, dcc, html
from plotly.subplots import make_subplots

from src.core.chart_db import ChartDB
from src.core.chart_windows import (
    NY,
    daily_window,
    intraday_1m_window,
    intraday_5m_window,
)
from src.providers.bars_provider import ThetaAgg, polygon_minute

print(f"[DASH] module file: {__file__}")
# ------------------------------------------------------------------------------
# App config
# ------------------------------------------------------------------------------
DB = os.getenv("GAP_DB_PATH", "db/scanner.db")
MAX_GRID = 12


# ------------------------------------------------------------------------------
# Time helpers & cosmetics
# ------------------------------------------------------------------------------
def _et_series(iso_utc: pd.Series) -> pd.Series:
    """UTC ISO -> tz-aware America/New_York for plotting."""
    return pd.to_datetime(iso_utc, utc=True).dt.tz_convert(NY)


def _rangebreaks_intraday():
    """Keep 04:00..20:00 visible, hide closed hours & weekends."""
    return [dict(bounds=["sat", "mon"]), dict(bounds=[20, 4], pattern="hour")]


def _apply_crosshair(fig: go.Figure):
    """TradingView-like crosshair (x-unified hover + spikes)."""
    fig.update_layout(hovermode="x unified", hoverdistance=1, spikedistance=1, uirevision="keep")
    fig.update_xaxes(
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikethickness=1,
        spikecolor="rgba(120,120,120,0.7)",
    )
    fig.update_yaxes(showspikes=True, spikethickness=1)


# ------------------------------------------------------------------------------
# Data guards: ensure OHLC/volume are usable for plotting
# ------------------------------------------------------------------------------
def _ensure_ohlc_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with safe OHLC/volume columns for candlesticks.

    - Guarantees presence of open/high/low/close/volume columns.
    - Fills missing open/high/low from close where needed.
    - Keeps ts_et/ts_utc untouched.
    - If close is entirely missing, returns an empty frame.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in d.columns:
            d[c] = np.nan
    # If we don't have any close values, nothing to plot safely
    if d["close"].isna().all():
        return pd.DataFrame()
    # Fill O/H/L from close where missing
    for c in ["open", "high", "low"]:
        if d[c].isna().all():
            d[c] = d["close"]
        else:
            d[c] = d[c].fillna(d["close"])
    d["volume"] = d["volume"].fillna(0)
    return d


def _add_premarket_shading(fig: go.Figure, df_et: pd.DataFrame, row=1, col=1, theme="dark"):
    if df_et.empty:
        return
    days = pd.to_datetime(df_et["ts_et"]).dt.normalize().unique()
    for d in days:
        pm0 = pd.to_datetime(d) + pd.Timedelta(hours=4)
        pm1 = pd.to_datetime(d) + pd.Timedelta(hours=9, minutes=30)
        fig.add_vrect(
            x0=pm0,
            x1=pm1,
            fillcolor="rgba(120,120,120,0.15)" if theme == "dark" else "rgba(50,50,50,0.08)",
            line_width=0,
            row=row,
            col=col,
        )


# ------------------------------------------------------------------------------
# QC: coverage & gap shading
# ------------------------------------------------------------------------------
def _expected_clock(day_et: pd.Timestamp, tf: str, start_hm: Tuple[int, int], end_hm: Tuple[int, int]):
    """Expected timestamps for a segment within the T-day (tz-aware ET)."""
    base = day_et.tz_localize(NY) if getattr(day_et, "tz", None) is None else day_et.tz_convert(NY)
    h0, m0 = start_hm
    h1, m1 = end_hm
    start = base.normalize() + pd.Timedelta(hours=h0, minutes=m0)
    end = base.normalize() + pd.Timedelta(hours=h1, minutes=m1)
    freq = "1min" if tf == "1m" else "5min"
    return pd.date_range(start, end, freq=freq)


def _qc_intraday(df_et: pd.DataFrame, tf: str):
    """
    Return coverage (expected vs got) and missing spans for:
      - premarket 04:00–09:30
      - RTH 09:30–16:00
      - post 16:00–20:00
    """
    if df_et.empty:
        return {"premkt": (0, 0, 0.0), "rth": (0, 0, 0.0), "post": (0, 0, 0.0), "gaps": []}

    day = pd.to_datetime(df_et["ts_et"]).dt.normalize().iloc[0]
    have = pd.Series(1, index=pd.DatetimeIndex(pd.to_datetime(df_et["ts_et"])))
    res, gaps = {}, []
    segments = {"premkt": ((4, 0), (9, 30)), "rth": ((9, 30), (16, 0)), "post": ((16, 0), (20, 0))}
    for k, (a, b) in segments.items():
        exp = _expected_clock(day, tf, a, b)
        if exp.empty:
            res[k] = (0, 0, 100.0)
            continue
        series = have.reindex(exp, fill_value=0)
        got = int(series.sum())
        total = len(exp)
        pct = (got / total) * 100.0 if total else 100.0
        miss = series == 0
        if miss.any():
            grp = (miss != miss.shift()).cumsum()
            for _, sub in miss.groupby(grp):
                if sub.all():
                    gaps.append((sub.index[0], sub.index[-1]))
        res[k] = (total, got, pct)
    return {"premkt": res["premkt"], "rth": res["rth"], "post": res["post"], "gaps": gaps}


def _y_range_tight(df: pd.DataFrame, pad=0.04):
    if df.empty:
        return None
    lo = float(np.nanmin([df["low"].min(), df["close"].min()]))
    hi = float(np.nanmax([df["high"].max(), df["close"].max()]))
    span = max(hi - lo, 1e-6)
    return [lo - pad * span, hi + pad * span]


# ------------------------------------------------------------------------------
# Indicators: MACD
# ------------------------------------------------------------------------------
def _macd(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return pd.DataFrame({"macd": macd, "signal": signal, "hist": hist}, index=df.index)


# ------------------------------------------------------------------------------
# Volume over price (same pane) figure builders
# ------------------------------------------------------------------------------
UP_VOL = "#26a69a"      # green for up bars
DOWN_VOL = "#ef5350"    # red for down bars


def _candles_with_overlay_volume(
    df_et: pd.DataFrame, title: str, show_macd: bool, theme="dark"
) -> go.Figure:
    """
    Row layout:
      - if MACD: 3 rows (row1 price, row2 volume, row3 MACD)
      - else: 2 rows (row1 price, row2 volume)
    Clean TradingView-style layout with separate volume subplot.
    """
    # Guard/sanitize data for plotting
    df_et = _ensure_ohlc_ready(df_et)
    if df_et.empty:
        return go.Figure()

    if show_macd:
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.6, 0.15, 0.25],
            specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": False}]]
        )
    else:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.75, 0.25],
            specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
        )

    # Price candles (row 1)
    fig.add_candlestick(
        x=df_et["ts_et"],
        open=df_et["open"],
        high=df_et["high"],
        low=df_et["low"],
        close=df_et["close"],
        name="Price",
        increasing_line_color=UP_VOL,
        decreasing_line_color=DOWN_VOL,
        row=1,
        col=1,
    )

    # Candlestick doesn't support hovertemplate, use hovertext instead
    hover_texts = [
        f"Time: {t.strftime('%b %d %Y %H:%M ET')}<br>"
        f"O {o:.4f}  H {h:.4f}<br>L {l:.4f}  C {c:.4f}"
        for t, o, h, l, c in zip(df_et["ts_et"], df_et["open"], df_et["high"], df_et["low"], df_et["close"])
    ]
    fig.data[-1].update(hovertext=hover_texts, hoverinfo="text")

    # Volume bars in separate subplot (row 2)
    vol_colors = [UP_VOL if c >= o else DOWN_VOL
                  for c, o in zip(df_et["close"], df_et["open"])]
    fig.add_bar(
        x=df_et["ts_et"],
        y=df_et["volume"],
        marker=dict(color=vol_colors, line=dict(width=0)),
        name="Volume",
        showlegend=False,
        row=2,
        col=1,
    )

    # Update volume y-axis
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    # MACD third row (optional)
    if show_macd:
        m = _macd(df_et.set_index("ts_et"))
        # Color histogram bars based on positive/negative
        hist_colors = [UP_VOL if val >= 0 else DOWN_VOL for val in m["hist"]]
        fig.add_bar(
            x=m.index,
            y=m["hist"],
            marker=dict(color=hist_colors, line=dict(width=0)),
            name="Histogram",
            showlegend=False,
            row=3,
            col=1
        )
        fig.add_scatter(
            x=m.index,
            y=m["macd"],
            mode="lines",
            name="MACD",
            line=dict(color="#2962FF", width=2),
            showlegend=False,
            row=3,
            col=1
        )
        fig.add_scatter(
            x=m.index,
            y=m["signal"],
            mode="lines",
            name="Signal",
            line=dict(color="#FF6D00", width=2),
            showlegend=False,
            row=3,
            col=1
        )
        fig.update_yaxes(title_text="MACD", showgrid=False, row=3, col=1)

    # Intraday cosmetics: rangebreaks for all subplots
    fig.update_xaxes(rangebreaks=_rangebreaks_intraday(), row=1, col=1)
    fig.update_xaxes(rangebreaks=_rangebreaks_intraday(), row=2, col=1)
    if show_macd:
        fig.update_xaxes(rangebreaks=_rangebreaks_intraday(), row=3, col=1)

    if theme == "dark":
        fig.update_layout(template="plotly_dark", paper_bgcolor="#131722", plot_bgcolor="#131722")
    else:
        fig.update_layout(template="plotly_white")

    # Tight Y and premarket shading
    yr = _y_range_tight(df_et)
    if yr:
        fig.update_yaxes(range=yr, row=1, col=1)
    _add_premarket_shading(fig, df_et, row=1, col=1, theme=theme)

    # Update y-axis label for price
    fig.update_yaxes(title_text="Price", row=1, col=1)

    # Crosshair & layout
    _apply_crosshair(fig)
    fig.update_layout(
        title=title,
        margin=dict(l=10, r=10, t=30, b=10),
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    return fig


def build_intraday_figure(df: pd.DataFrame, title: str, macd=False, theme="dark") -> go.Figure:
    if df.empty:
        return go.Figure()
    d = df.copy()
    d["ts_et"] = _et_series(d["ts_utc"])

    # QC (figure shows orange shading & ribbon)
    guess_tf = "1m" if (d["ts_et"].diff().dt.total_seconds().dropna().min() or 60) < 120 else "5m"
    qc = _qc_intraday(d, guess_tf)
    fig = _candles_with_overlay_volume(d, title, macd, theme)

    for a, b in qc["gaps"][:20]:  # cap shapes for perf
        fig.add_vrect(x0=a, x1=b, fillcolor="rgba(255,165,0,0.15)", line_width=0, row=1, col=1)

    pm_e, pm_g, pm_p = qc["premkt"]
    r_e, r_g, r_p = qc["rth"]
    label = f"Premkt {pm_p:.0f}% • RTH {r_p:.0f}%"
    fig.add_annotation(
        xref="paper", yref="paper", x=0, y=1.12, showarrow=False, text=label, font=dict(size=12, color="#888")
    )
    return fig


def build_daily_figure(df: pd.DataFrame, title: str, theme="dark") -> go.Figure:
    """Daily pane: price + volume in separate subplots (TradingView style)."""
    if df.empty:
        return go.Figure()
    d = df.copy()
    d["ts_et"] = _et_series(d["ts_utc"])
    d = _ensure_ohlc_ready(d)
    if d.empty:
        return go.Figure()

    # Create subplots: price + volume
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )

    # Price candlesticks
    fig.add_candlestick(
        x=d["ts_et"],
        open=d["open"],
        high=d["high"],
        low=d["low"],
        close=d["close"],
        name="Price",
        increasing_line_color=UP_VOL,
        decreasing_line_color=DOWN_VOL,
        row=1,
        col=1
    )

    # Candlestick doesn't support hovertemplate, use hovertext instead
    hover_texts = [
        f"Date: {t.strftime('%b %d %Y')}<br>"
        f"O {o:.4f}  H {h:.4f}<br>L {l:.4f}  C {c:.4f}"
        for t, o, h, l, c in zip(d["ts_et"], d["open"], d["high"], d["low"], d["close"])
    ]
    fig.data[-1].update(hovertext=hover_texts, hoverinfo="text")

    # Volume bars in separate subplot
    vol_colors = [UP_VOL if c >= o else DOWN_VOL
                  for c, o in zip(d["close"], d["open"])]
    fig.add_bar(
        x=d["ts_et"],
        y=d["volume"],
        marker=dict(color=vol_colors, line=dict(width=0)),
        name="Volume",
        showlegend=False,
        row=2,
        col=1
    )

    # Update axes
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    # Remove weekends from daily chart for cleaner view
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], row=1, col=1)
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], row=2, col=1)

    if theme == "dark":
        fig.update_layout(template="plotly_dark", paper_bgcolor="#131722", plot_bgcolor="#131722")
    else:
        fig.update_layout(template="plotly_white")

    _apply_crosshair(fig)

    yr = _y_range_tight(d)
    if yr:
        fig.update_yaxes(range=yr, row=1, col=1)

    fig.update_layout(
        title=title,
        margin=dict(l=10, r=10, t=30, b=10),
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    return fig


# ------------------------------------------------------------------------------
# Overlay (event‑day only) with gap breaks & 9:30 marker
# ------------------------------------------------------------------------------
def _event_day_bounds_et(d_str: str):
    d = pd.to_datetime(d_str).tz_localize(NY)
    start = d + pd.Timedelta(hours=4)
    end = d + pd.Timedelta(hours=20)
    return start, end


def _break_at_gaps(x: pd.Series, y: pd.Series, max_gap_min: int):
    xs, ys, last = [], [], None
    for t, v in zip(x, y):
        if last is not None:
            dt = (t - last).total_seconds() / 60.0
            if dt > max_gap_min + 0.1:
                xs.append(None)
                ys.append(None)
        xs.append(t)
        ys.append(v)
        last = t
    return xs, ys


def build_overlay_event_day(
    symbol: str, when_list: List[str], tf: str, db: ChartDB, normalize: bool, theme="dark"
) -> go.Figure:
    """Original single-day overlay for backward compatibility"""
    fig = go.Figure()
    for d0 in when_list:
        start_et, end_et = _event_day_bounds_et(d0)
        s_utc = start_et.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        e_utc = end_et.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        di = db.read(tf, symbol, s_utc, e_utc)
        if di.empty:
            continue
        di["t_et"] = _et_series(di["ts_utc"])
        di = di.sort_values("t_et")
        open_t = start_et.normalize() + pd.Timedelta(hours=9, minutes=30)
        idx930 = (di["t_et"] - open_t).abs().idxmin() if not di.empty else None
        y = di["close"].astype(float).copy()
        if normalize and idx930 is not None and pd.notna(y.loc[idx930]) and y.loc[idx930] != 0:
            y = (y / float(y.loc[idx930])) * 100.0
        xs, ys = _break_at_gaps(di["t_et"], y, max_gap_min=1 if tf == "1m" else 5)
        fig.add_scatter(x=xs, y=ys, mode="lines", name=d0)

    if when_list:
        open_t = _event_day_bounds_et(when_list[0])[0].normalize() + pd.Timedelta(hours=9, minutes=30)
        fig.add_vline(x=open_t, line_color="rgba(120,120,120,0.35)", line_dash="dot")

    if theme == "dark":
        fig.update_layout(template="plotly_dark", paper_bgcolor="black", plot_bgcolor="black")
    _apply_crosshair(fig)
    fig.update_layout(
        title=f"{symbol} overlay ({tf})" + (" — normalized %" if normalize else ""),
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h"),
    )
    return fig


def build_multi_day_overlay(
    symbol: str, when_list: List[str], tf: str, db: ChartDB,
    days_before: int, days_after: int, normalize: bool, align_to_hit: bool, theme="dark"
) -> go.Figure:
    """Enhanced overlay showing multiple days around each hit date.

    Args:
        symbol: Stock symbol
        when_list: List of hit dates to compare
        tf: Timeframe (1m or 5m)
        db: Chart database
        days_before: Days to show before hit date
        days_after: Days to show after hit date
        normalize: Normalize to percentage at 9:30 AM of hit day
        align_to_hit: If True, align all series to hit day's 9:30 AM
        theme: dark or light
    """
    fig = go.Figure()

    for d0 in when_list:
        # Calculate window around hit date
        hit_date = pd.to_datetime(d0)
        start_date = hit_date - pd.Timedelta(days=days_before)
        end_date = hit_date + pd.Timedelta(days=days_after)

        # Get data for entire window
        start_et = start_date.tz_localize(NY) + pd.Timedelta(hours=4)
        end_et = end_date.tz_localize(NY) + pd.Timedelta(hours=20)
        s_utc = start_et.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
        e_utc = end_et.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        di = db.read(tf, symbol, s_utc, e_utc)
        if di.empty:
            continue

        di["t_et"] = _et_series(di["ts_utc"])
        di = di.sort_values("t_et")

        # Find normalization point (9:30 AM on hit day)
        hit_open = hit_date.tz_localize(NY) + pd.Timedelta(hours=9, minutes=30)
        idx930 = (di["t_et"] - hit_open).abs().idxmin() if not di.empty else None

        # Prepare y values
        y = di["close"].astype(float).copy()
        if normalize and idx930 is not None and pd.notna(y.loc[idx930]) and y.loc[idx930] != 0:
            y = (y / float(y.loc[idx930])) * 100.0

        # Optionally align timeline to hit day
        if align_to_hit:
            # Shift timestamps so hit day 9:30 AM = 0
            di["relative_hours"] = (di["t_et"] - hit_open).dt.total_seconds() / 3600
            x_data = di["relative_hours"]
            label = f"{d0} (aligned)"
        else:
            x_data = di["t_et"]
            label = f"{d0} ±{days_before}/{days_after}d"

        # Break at gaps for cleaner visualization
        xs, ys = _break_at_gaps(x_data, y, max_gap_min=1 if tf == "1m" else 5)
        fig.add_scatter(x=xs, y=ys, mode="lines", name=label, opacity=0.8)

    # Add reference lines
    if align_to_hit:
        # Add vertical line at aligned 9:30 AM (hour 0)
        fig.add_vline(x=0, line_color="rgba(120,120,120,0.5)", line_dash="dot",
                     annotation_text="Hit Day 9:30")

        # Add lines for day boundaries
        for day_offset in range(-days_before, days_after + 1):
            if day_offset != 0:
                x_pos = day_offset * 24
                fig.add_vline(x=x_pos, line_color="rgba(80,80,80,0.2)", line_dash="dash")

        fig.update_xaxes(title="Hours relative to hit day 9:30 AM")
    else:
        # Add 9:30 AM line for first hit date as reference
        if when_list:
            hit_open = pd.to_datetime(when_list[0]).tz_localize(NY) + pd.Timedelta(hours=9, minutes=30)
            fig.add_vline(x=hit_open, line_color="rgba(120,120,120,0.35)", line_dash="dot")
        fig.update_xaxes(rangebreaks=_rangebreaks_intraday())

    if theme == "dark":
        fig.update_layout(template="plotly_dark", paper_bgcolor="black", plot_bgcolor="black")

    _apply_crosshair(fig)

    title_parts = [f"{symbol} Multi-Day Comparison ({tf})"]
    if normalize:
        title_parts.append("Normalized %")
    if align_to_hit:
        title_parts.append("Aligned to hit day")

    fig.update_layout(
        title=" — ".join(title_parts),
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )

    return fig


# ------------------------------------------------------------------------------
# Date inventory: prefer discovery_hits (hits‑only) with fallback
# ------------------------------------------------------------------------------
def _table_has_column(cx: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = cx.execute(f"PRAGMA table_info({table})").fetchall()
        names = {r[1] for r in rows}
        return col in names
    except sqlite3.Error:
        return False


def list_symbols_and_dates(db_path: str, hits_only: bool = True):
    def _bars_dates(cursor: sqlite3.Connection, symbol: str) -> List[str]:
        rows = cursor.execute(
            "SELECT DISTINCT substr(ts_utc,1,10) FROM bars_5m WHERE symbol=? ORDER BY 1",
            (symbol,),
        ).fetchall()
        return [r[0] for r in rows]

    with sqlite3.connect(db_path) as cx:
        syms = [r[0] for r in cx.execute("SELECT DISTINCT symbol FROM bars_5m ORDER BY 1").fetchall()]
        results: Dict[str, List[str]] = {}

        if hits_only:
            has_hits = cx.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='discovery_hits'"
            ).fetchone()
            if has_hits:
                date_col = None
                for candidate in ("event_date", "trigger_date", "date"):
                    if _table_has_column(cx, "discovery_hits", candidate):
                        date_col = candidate
                        break

                symbol_cols = [
                    col
                    for col in ("symbol", "ticker")
                    if _table_has_column(cx, "discovery_hits", col)
                ]

                if date_col and symbol_cols:
                    where_clause = " OR \n".join(f"{col}=?" for col in symbol_cols)
                    for s in syms:
                        params = tuple([s] * len(symbol_cols))
                        rows = cx.execute(
                            f"SELECT DISTINCT {date_col} FROM discovery_hits WHERE {where_clause} ORDER BY 1",
                            params,
                        ).fetchall()
                        dates = [r[0] for r in rows if r[0]]
                        results[s] = dates if dates else _bars_dates(cx, s)
                else:
                    for s in syms:
                        results[s] = _bars_dates(cx, s)
            else:
                for s in syms:
                    results[s] = _bars_dates(cx, s)
        else:
            for s in syms:
                results[s] = _bars_dates(cx, s)

        return syms, results


# ------------------------------------------------------------------------------
# Optional: auto‑fetch missing T‑day if coverage is low
# ------------------------------------------------------------------------------
def _autofill_intraday_if_needed(
    db: ChartDB, symbol: str, d0: str, tf: str, coverage_pct: float, threshold: float = 85.0
):
    """If coverage for T-day is low, pull 1m from Theta (or Polygon), upsert, and resample to 5m if needed."""
    if coverage_pct >= threshold:
        return
    th = ThetaAgg()
    df1 = th.one_minute(symbol, d0)
    if df1.empty:
        df1 = polygon_minute(symbol, d0, d0, mult=1)
    if not df1.empty:
        db.upsert_df("1m", df1)
        if tf == "5m":
            df1["ts_utc"] = pd.to_datetime(df1["ts_utc"], utc=True)
            d1 = df1.set_index("ts_utc")
            o = d1["open"].resample("5min").first()
            h = d1["high"].resample("5min").max()
            l = d1["low"].resample("5min").min()
            c = d1["close"].resample("5min").last()
            v = d1["volume"].resample("5min").sum().fillna(0)
            d5 = (
                pd.concat({"open": o, "high": h, "low": l, "close": c, "volume": v}, axis=1)
                .dropna(how="all")
                .reset_index()
            )
            d5["symbol"] = symbol
            d5["provider"] = "theta"
            d5["adjusted"] = 0
            d5["ts_utc"] = d5["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            db.upsert_df("5m", d5)


# ------------------------------------------------------------------------------
# Dash app
# ------------------------------------------------------------------------------
def make_app(db_path: str = DB):
    db = ChartDB(db_path)
    syms, date_map = list_symbols_and_dates(db_path, hits_only=True)

    app = Dash(__name__)
    app.title = "Gap Scanner Chart Viewer"

    app.layout = html.Div(
        [
            html.H3("Gap Scanner Chart Viewer"),
            html.Div(
                [
                    html.Label("Symbol"),
                    dcc.Dropdown(syms, id="sym"),

                    html.Label("Dates (select one or many)"),
                    dcc.Dropdown([], id="dates", multi=True),
                    dcc.Checklist(
                        id="hits_only",
                        options=[{"label": "Hits only", "value": "hits"}],
                        value=["hits"],
                    ),

                    html.Label("Mode"),
                    dcc.RadioItems(
                        options=[
                            {"label": "Single-hit (top=1m/5m toggle, bottom=daily)", "value": "single"},
                            {"label": "Overlay (single day per hit)", "value": "overlay"},
                            {"label": "Multi-Day Overlay (compare hit windows)", "value": "multi_overlay"},
                            {"label": "Grid (up to 12)", "value": "grid"},
                        ],
                        value="single",
                        id="mode",
                    ),

                    html.Label("Intraday TF"),
                    dcc.RadioItems(options=["1m", "5m"], value="5m", id="tf"),

                    html.Label("Days before/after hit"),
                    dcc.RangeSlider(
                        id="range_5m",
                        min=0,
                        max=15,
                        step=1,
                        value=[2, 7],
                        marks={0: '0', 2: '2', 5: '5', 7: '7', 10: '10', 15: '15'},
                        tooltip={"placement": "bottom", "always_visible": True},
                    ),

                    dcc.Checklist(
                        id="opts",
                        options=[
                            {"label": "MACD", "value": "macd"},
                            {"label": "Light Theme", "value": "light"},
                            {"label": "Auto‑fetch missing", "value": "autofill"},
                        ],
                        value=[],
                    ),

                    html.Label("Overlay Options"),
                    dcc.Checklist(
                        id="norm",
                        options=[
                            {"label": "Normalize %", "value": "norm"},
                            {"label": "Align to hit day", "value": "align"}
                        ],
                        value=["norm"],
                    ),

                    html.Label("Grid columns"),
                    dcc.Slider(
                        id="grid_cols",
                        min=2,
                        max=4,
                        step=1,
                        value=3,
                        marks=None,
                        tooltip={"placement": "bottom", "always_visible": True},
                    ),

                    html.Button("Load", id="load"),
                ],
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(6, minmax(200px, 1fr))",
                    "gap": "8px",
                    "alignItems": "end",
                    "marginBottom": "8px",
                },
            ),
            html.Div(id="info"),
            html.Div(id="charts"),
        ],
        style={"fontFamily": "system-ui, sans-serif"},
    )

    # Dynamic dates: hits only vs all cached
    @app.callback(Output("dates", "options"), Input("sym", "value"), Input("hits_only", "value"))
    def on_sym(sym, hits_only_val):
        if not sym:
            return []
        hits_only = "hits" in (hits_only_val or [])
        _, dm = list_symbols_and_dates(db_path, hits_only=hits_only)
        return [{"label": d, "value": d} for d in dm.get(sym, [])]

    # Main loader
    @app.callback(
        Output("info", "children"),
        Output("charts", "children"),
        Input("load", "n_clicks"),
        State("sym", "value"),
        State("dates", "value"),
        State("mode", "value"),
        State("tf", "value"),
        State("opts", "value"),
        State("norm", "value"),
        State("range_5m", "value"),
        State("grid_cols", "value"),
        prevent_initial_call=True,
    )
    def on_load(n, sym, dates, mode, tf, opts, norm, range_5m, grid_cols):
        theme = "light" if "light" in (opts or []) else "dark"
        macd = "macd" in (opts or [])
        autofill = "autofill" in (opts or [])
        if not sym:
            return ["Select a symbol.", []]
        sel = (dates or [])[:MAX_GRID]
        if not sel:
            return [f"No dates selected for {sym}.", []]

        before = int((range_5m or [3, 7])[0])
        after = int((range_5m or [3, 7])[1])

        def _read(tf_code, s, start_iso_utc, end_iso_utc):
            return db.read(tf_code, s, start_iso_utc, end_iso_utc)

        # Single-hit
        if mode == "single":
            d0 = sel[0]
            if tf == "1m":
                s, e = intraday_1m_window(d0)
            else:
                s, e = intraday_5m_window(d0, before, after)
            sd, ed = daily_window(d0)
            di = _read(tf, sym, s, e)

            # Fallback: if 5m window returned empty, resample from 1m for the T-day
            if di.empty and tf == "5m":
                s1, e1 = intraday_1m_window(d0)
                d1 = _read("1m", sym, s1, e1)
                if not d1.empty:
                    d1["ts_utc"] = pd.to_datetime(d1["ts_utc"], utc=True)
                    base = d1.set_index("ts_utc")
                    o = base["open"].resample("5min").first()
                    h = base["high"].resample("5min").max()
                    l = base["low"].resample("5min").min()
                    c = base["close"].resample("5min").last()
                    v = base["volume"].resample("5min").sum().fillna(0)
                    di = (
                        pd.concat({"open": o, "high": h, "low": l, "close": c, "volume": v}, axis=1)
                        .dropna(how="all")
                        .reset_index()
                    )
                    di["ts_utc"] = di["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Optional autofill for T-day if coverage is low
            if autofill:
                tmp = di.copy()
                if not tmp.empty:
                    tmp["ts_et"] = _et_series(tmp["ts_utc"])
                    qc = _qc_intraday(tmp, tf)
                    coverage = min(qc["premkt"][2], qc["rth"][2])
                else:
                    coverage = 0.0
                _autofill_intraday_if_needed(db, sym, d0, tf, coverage_pct=coverage, threshold=85.0)
                di = _read(tf, sym, s, e)

            dd = _read("1d", sym, sd, ed)

            fi = build_intraday_figure(di, f"{sym} {tf} — {d0}", macd=macd, theme=theme)
            fd = build_daily_figure(dd, f"{sym} Daily — context", theme=theme)

            info_text = f"{sym} — {d0} [{tf}] di_rows={len(di)} dd_rows={len(dd)} db={db_path}"
            return [
                info_text,
                html.Div(
                    [
                        dcc.Graph(figure=fi, style={"height": "60vh"}),
                        dcc.Graph(figure=fd, style={"height": "38vh", "marginTop": "8px"}),
                    ]
                ),
            ]

        # Original Overlay (event-day only)
        if mode == "overlay":
            fig = build_overlay_event_day(sym, sel, tf, db, normalize=("norm" in (norm or [])), theme=theme)
            return [f"Overlay {sym}: {len(sel)} date(s) — T-day clock", dcc.Graph(figure=fig, style={"height": "70vh"})]

        # Multi-Day Overlay (compare windows around hits)
        if mode == "multi_overlay":
            normalize = "norm" in (norm or [])
            align_to_hit = "align" in (norm or [])
            fig = build_multi_day_overlay(
                sym, sel, tf, db,
                days_before=before,
                days_after=after,
                normalize=normalize,
                align_to_hit=align_to_hit,
                theme=theme
            )
            info = f"Multi-Day Overlay {sym}: {len(sel)} hit(s) — {before} days before, {after} days after"
            return [info, dcc.Graph(figure=fig, style={"height": "75vh"})]

        # Grid
        if mode == "grid":
            tiles = []
            for d0 in sel:
                if tf == "1m":
                    s, e = intraday_1m_window(d0)
                else:
                    s, e = intraday_5m_window(d0, before, after)
                di = _read(tf, sym, s, e)
                fi = build_intraday_figure(di, f"{sym} {tf} — {d0}", macd=False, theme=theme)
                tiles.append(html.Div(dcc.Graph(figure=fi), style={"breakInside": "avoid"}))
            cols = int(grid_cols or 3)
            grid_style = {"columnCount": cols, "columnGap": "8px"}
            return [f"Grid {sym}: {len(tiles)} charts ({tf})", html.Div(tiles, style=grid_style)]

        return ["Unknown mode.", []]

    return app


def main(port: int = 8050):
    try:
        port = int(os.getenv("GAP_DASH_PORT", port))
    except (TypeError, ValueError):
        pass
    app = make_app(DB)
    if hasattr(app, "run"):
        app.run(debug=False, port=port, host="127.0.0.1")
    else:
        app.run_server(debug=False, port=port, host="127.0.0.1")


if __name__ == "__main__":
    main()
