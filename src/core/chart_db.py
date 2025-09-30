# -*- coding: utf-8 -*-
import os, sqlite3
from pathlib import Path
from typing import Iterable, Literal
import pandas as pd

TF = Literal["1m","5m","1d"]

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS bars_1m(
  symbol TEXT NOT NULL,
  ts_utc TEXT NOT NULL,        -- ISO UTC e.g. 2025-09-26T13:35:00Z
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  provider TEXT,               -- 'theta'|'polygon'|'fmp'
  adjusted INTEGER DEFAULT 0,  -- 0=raw intraday
  PRIMARY KEY(symbol, ts_utc)
);

CREATE TABLE IF NOT EXISTS bars_5m(
  symbol TEXT NOT NULL,
  ts_utc TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  provider TEXT, adjusted INTEGER DEFAULT 0,
  PRIMARY KEY(symbol, ts_utc)
);

CREATE TABLE IF NOT EXISTS bars_1d(
  symbol TEXT NOT NULL,
  ts_utc TEXT NOT NULL,        -- daily candle end in UTC
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  provider TEXT, adjusted INTEGER DEFAULT 1,
  PRIMARY KEY(symbol, ts_utc, adjusted)
);

CREATE TABLE IF NOT EXISTS splits(
  symbol TEXT NOT NULL,
  ex_date TEXT NOT NULL,       -- YYYY-MM-DD (calendar date)
  ratio REAL NOT NULL,         -- e.g. 0.1 for 1:10 reverse
  source TEXT,
  PRIMARY KEY(symbol, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_1m_sym_ts ON bars_1m(symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_5m_sym_ts ON bars_5m(symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_1d_sym_ts ON bars_1d(symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_split_sym ON splits(symbol, ex_date);
"""

class ChartDB:
    def __init__(self, db_path: str | Path = "db/scanner.db"):
        self.path = str(db_path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with sqlite3.connect(self.path) as cx:
            cx.executescript(SCHEMA)

    def upsert_df(self, tf: TF, df: pd.DataFrame) -> int:
        if df is None or df.empty: return 0
        tbl = {"1m":"bars_1m","5m":"bars_5m","1d":"bars_1d"}[tf]
        # ensure columns
        cols = ["symbol","ts_utc","open","high","low","close","volume","provider","adjusted"]
        for c in cols:
            if c not in df.columns: df[c] = None
        df = df[cols].copy()
        df = df.dropna(subset=['symbol', 'ts_utc']).drop_duplicates(subset=['symbol', 'ts_utc'], keep='last')
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        if not records:
            return 0
        placeholders = ','.join(['?'] * len(cols))
        columns = ','.join(cols)
        sql = f"INSERT OR REPLACE INTO {tbl} ({columns}) VALUES ({placeholders})"
        with sqlite3.connect(self.path) as cx:
            cx.executemany(sql, records)
        return len(records)

    def read(self, tf: TF, symbol: str, start_iso_utc: str, end_iso_utc: str) -> pd.DataFrame:
        tbl = {"1m":"bars_1m","5m":"bars_5m","1d":"bars_1d"}[tf]
        sql = f"SELECT * FROM {tbl} WHERE symbol=? AND ts_utc BETWEEN ? AND ? ORDER BY ts_utc"
        with sqlite3.connect(self.path) as cx:
            df = pd.read_sql(sql, cx, params=(symbol, start_iso_utc, end_iso_utc))
        return df

    def upsert_splits(self, rows: list[tuple[str,str,float,str]]) -> int:
        if not rows: return 0
        with sqlite3.connect(self.path) as cx:
            cx.executemany(
                "INSERT OR REPLACE INTO splits(symbol, ex_date, ratio, source) VALUES (?,?,?,?)",
                rows
            )
        return len(rows)
