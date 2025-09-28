# -*- coding: ascii -*-
# ASCII only. Pure rule computations (no I/O).

from typing import Optional

def r1_pm(prev_close: Optional[float], pm_high: Optional[float], th: float = 50.0) -> Optional[float]:
    """R1 Premarket mover: ((premarket_high / prev_close) - 1) * 100 >= 50.0"""
    if prev_close and pm_high and prev_close > 0:
        pc = float(prev_close)
        pmh = float(pm_high)
        pct = (pmh / pc - 1.0) * 100.0
        return pct if pct >= th else None
    return None

def r2_open_gap(prev_close: Optional[float], open_price: Optional[float], th: float = 50.0) -> Optional[float]:
    """R2 Open gap: ((open / prev_close) - 1) * 100 >= 50.0"""
    if prev_close and open_price and prev_close > 0:
        pc = float(prev_close)
        o = float(open_price)
        pct = (o / pc - 1.0) * 100.0
        return pct if pct >= th else None
    return None

def r3_push(open_price: Optional[float], high_of_day: Optional[float], th: float = 50.0) -> Optional[float]:
    """R3 Intraday push: ((high / open) - 1) * 100 >= 50.0"""
    if open_price and high_of_day and open_price > 0:
        o = float(open_price)
        h = float(high_of_day)
        pct = (h / o - 1.0) * 100.0
        return pct if pct >= th else None
    return None

def r4_surge7(lowest_low_7d: Optional[float], highest_high_7d: Optional[float], th: float = 300.0) -> Optional[float]:
    """R4 7-day surge: ((highest_high_7d / lowest_low_7d) - 1) * 100 >= 300.0"""
    if lowest_low_7d and highest_high_7d and lowest_low_7d > 0:
        lo = float(lowest_low_7d)
        hi = float(highest_high_7d)
        pct = (hi / lo - 1.0) * 100.0
        return pct if pct >= th else None
    return None