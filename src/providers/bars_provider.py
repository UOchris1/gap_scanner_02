# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, threading, requests
from typing import Any, Dict, List
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt

POLY_KEY = (os.getenv("POLYGON_API_KEY") or "").strip()
POLY_BASE = "https://api.polygon.io"

THETA_V3 = os.getenv("THETA_V3_URL","http://127.0.0.1:25503")
THETA_V1 = os.getenv("THETA_V1_URL","http://127.0.0.1:25510")
THETA_VENUE = os.getenv("THETA_VENUE","nqb")
THETA_RETRIES = int(os.getenv("THETA_RETRIES","3"))
THETA_V3_MAX = int(os.getenv("THETA_V3_MAX_OUTSTANDING","2"))

def _to_iso_utc_ms(ms:int)->str:
    return pd.to_datetime(ms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%SZ")

class _Gate:
    def __init__(self, n:int=2): self.sem = threading.Semaphore(max(n,1))
    def get(self,*a,**k):
        with self.sem: return requests.get(*a,**k)

class ThetaAgg:
    """Trade->1m bars; resample to 5m; v3 primary then v1."""
    def __init__(self):
        self.v3 = _Gate(THETA_V3_MAX); self.v1 = _Gate(THETA_V3_MAX)
        self.v3_ok = self._probe(THETA_V3 + "/v3/stock/history/trade", {"symbol":"SPY","date":"2025-01-02","start_time":"09:30:00","end_time":"09:31:00","format":"json"})
        self.v1_ok = (not self.v3_ok) and self._probe(THETA_V1 + "/v2/hist/stock/trade", {"root":"SPY","start_date":"20250102","end_date":"20250102","start_time":"09:30:00","end_time":"09:31:00"})

    def _probe(self, url:str, params:dict)->bool:
        try: return self.v3.get(url, params=params, timeout=15).status_code in (200,204,400,422)
        except Exception: return False

    @retry(wait=wait_exponential(multiplier=0.75, min=0.75, max=6), stop=stop_after_attempt(THETA_RETRIES))
    def _fetch_v3(self, symbol:str, date_iso:str)->list[dict]:
        url = THETA_V3 + "/v3/stock/history/trade"
        p = {"symbol":symbol, "date":date_iso, "start_time":"04:00:00", "end_time":"20:00:00", "venue":THETA_VENUE, "format":"json"}
        r = self.v3.get(url, params=p, timeout=45); r.raise_for_status()
        data = r.json() or []
        out=[]; 
        for x in data:
            if "timestamp" in x and "price" in x:
                out.append({"ts":pd.to_datetime(x["timestamp"], utc=True), "price":float(x["price"]), "size":float(x.get("size",0))})
        return out

    @retry(wait=wait_exponential(multiplier=0.75, min=0.75, max=6), stop=stop_after_attempt(THETA_RETRIES))
    def _fetch_v1(self, symbol:str, date_iso:str)->list[dict]:
        url = THETA_V1 + "/v2/hist/stock/trade"
        p = {"root":symbol, "start_date":date_iso.replace("-",""), "end_date":date_iso.replace("-",""), "start_time":"04:00:00", "end_time":"20:00:00", "venue":THETA_VENUE}
        r = self.v1.get(url, params=p, timeout=45); r.raise_for_status()
        data = r.json() or {}
        out=[]; 
        for row in data.get("response", []) or []:
            # conservative indices (v1 schema varies); fallback if shorter
            ts = row[1] if len(row)>1 else None
            price = row[9] if len(row)>10 else None
            size = row[10] if len(row)>10 else 0
            if ts and price is not None:
                out.append({"ts":pd.to_datetime(ts, utc=True), "price":float(price), "size":float(size or 0)})
        return out

    def one_minute(self, symbol:str, date_iso:str)->pd.DataFrame:
        rows = []
        if self.v3_ok:
            try: rows = self._fetch_v3(symbol, date_iso)
            except Exception: rows=[]
        if not rows and self.v1_ok:
            try: rows = self._fetch_v1(symbol, date_iso)
            except Exception: rows=[]
        if not rows:
            return pd.DataFrame(columns=["symbol","ts_utc","open","high","low","close","volume","provider","adjusted"])
        df = pd.DataFrame(rows)
        df.set_index("ts", inplace=True)
        o = df["price"].resample("1min").first()
        h = df["price"].resample("1min").max()
        l = df["price"].resample("1min").min()
        c = df["price"].resample("1min").last()
        v = df["size"].resample("1min").sum().fillna(0)
        out = pd.concat({"open":o,"high":h,"low":l,"close":c,"volume":v}, axis=1).dropna(how="all")
        out.reset_index(inplace=True)
        out["symbol"]=symbol
        out["ts_utc"]=out["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        out["provider"]="theta"; out["adjusted"]=0
        return out[["symbol","ts_utc","open","high","low","close","volume","provider","adjusted"]]

def polygon_minute(symbol:str, start_ymd:str, end_ymd:str, mult:int)->pd.DataFrame:
    if not POLY_KEY: return pd.DataFrame()
    url = f"{POLY_BASE}/v2/aggs/ticker/{symbol}/range/{mult}/minute/{start_ymd}/{end_ymd}"
    r = requests.get(url, params={"adjusted":"false", "sort":"asc", "limit":50000, "apiKey":POLY_KEY}, timeout=45)
    if r.status_code != 200: return pd.DataFrame()
    res = (r.json() or {}).get("results") or []
    if not res: return pd.DataFrame()
    df = pd.DataFrame(res)
    out = pd.DataFrame({
        "symbol": symbol,
        "ts_utc": pd.to_datetime(df["t"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "open": df["o"], "high": df["h"], "low": df["l"], "close": df["c"], "volume": df["v"],
        "provider": "polygon", "adjusted": 0
    })
    return out

def polygon_daily(symbol:str, start_ymd:str, end_ymd:str, adjusted:bool=False)->pd.DataFrame:
    if not POLY_KEY: return pd.DataFrame()
    url = f"{POLY_BASE}/v2/aggs/ticker/{symbol}/range/1/day/{start_ymd}/{end_ymd}"
    r = requests.get(url, params={"adjusted": "true" if adjusted else "false", "sort":"asc","limit":50000,"apiKey":POLY_KEY}, timeout=45)
    if r.status_code != 200: return pd.DataFrame()
    res = (r.json() or {}).get("results") or []
    if not res: return pd.DataFrame()
    df = pd.DataFrame(res)
    out = pd.DataFrame({
        "symbol": symbol,
        "ts_utc": pd.to_datetime(df["t"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "open": df["o"], "high": df["h"], "low": df["l"], "close": df["c"], "volume": df["v"],
        "provider": "polygon", "adjusted": 1 if adjusted else 0
    })
    return out
