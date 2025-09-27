# scripts/export_reports.py
# -*- coding: ascii -*-
# Enhanced CSV export with as-of-date fundamentals and split context tracking
#
# CSV FORMAT DOCUMENTATION:
# - market_cap_millions: Market cap in millions with 3 decimals (e.g., 0.087 = $87K nano-cap)
# - float_millions: Float shares in millions with 3 decimals (e.g., 0.011 = 11K shares)
# - shares_outstanding_millions: Shares outstanding in millions with 3 decimals
# - float_rotation: Daily volume / float (decimal, not percentage - values >1 are normal for viral nano-caps)
# - rs_exec_date: Reverse split execution date (YYYY-MM-DD) if within 3 days of event
# - rs_days_after: Days between event and split execution (can be negative)
# - rules_detail: Pipe-separated triggered rules (e.g., "PM_GAP_50:69.7|SURGE_7D_300:810.0")

import csv, argparse, sqlite3

def _fmt_pct(x):
    if x is None or x == "":
        return ""
    try:
        return f"{float(x):.1f}"
    except Exception:
        return ""

def _fmt_millions(x):
    if x is None or x == "":
        return ""
    try:
        return f"{(float(x)/1_000_000.0):.1f}"
    except Exception:
        return ""

def _fmt_millions_precise(x):
    if x is None or x == "":
        return ""
    try:
        return f"{(float(x)/1_000_000.0):.3f}"
    except Exception:
        return ""

def _build_rules_detail(rowmap):
    parts = []
    for key,label in [
        ("pm_gap_50","PM_GAP_50"),
        ("open_gap_50","OPEN_GAP_50"),
        ("intraday_push_50","INTRADAY_PUSH_50"),
        ("surge_7d_300","SURGE_7D_300")
    ]:
        val = rowmap.get(key)
        if val not in (None,""):
            try:
                parts.append(f"{label}:{float(val):.1f}")
            except Exception:
                parts.append(f"{label}:{val}")
    return "|".join(parts)

def export_hits(conn, start, end, path):
    cur = conn.cursor()
    # Wide rule pivot
    rp_cte = """
    WITH rule_pivot AS (
        SELECT hit_id,
            MAX(CASE WHEN trigger_rule='PM_GAP_50' THEN rule_value END) AS pm_gap_50,
            MAX(CASE WHEN trigger_rule='OPEN_GAP_50' THEN rule_value END) AS open_gap_50,
            MAX(CASE WHEN trigger_rule='INTRADAY_PUSH_50' THEN rule_value END) AS intraday_push_50,
            MAX(CASE WHEN trigger_rule='SURGE_7D_300' THEN rule_value END) AS surge_7d_300
        FROM discovery_hit_rules
        GROUP BY hit_id
    )
    """
    q = rp_cte + """
    SELECT
        d.hit_id, d.ticker, d.event_date,
        d.volume, d.intraday_push_pct, d.is_near_reverse_split,
        d.rs_exec_date, d.rs_days_after,
        rp.pm_gap_50, rp.open_gap_50, rp.intraday_push_50, rp.surge_7d_300,
        f.shares_outstanding, f.market_cap, f.float_shares,
        (dr.volume * COALESCE(dr.vwap, dr.close)) AS dollar_volume, f.data_source,
        d.exchange, d.pm_high_source, d.pm_high_venue,
        (SELECT GROUP_CONCAT(trigger_rule, '|') FROM discovery_hit_rules r WHERE r.hit_id=d.hit_id) AS rule_tags
    FROM discovery_hits d
    LEFT JOIN rule_pivot rp ON d.hit_id = rp.hit_id
    LEFT JOIN discovery_hit_fundamentals f ON d.hit_id = f.hit_id
    LEFT JOIN daily_raw dr ON dr.symbol = d.ticker AND dr.date = d.event_date
    WHERE d.event_date BETWEEN ? AND ?
    ORDER BY d.event_date, d.ticker
    """
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        # Final headers (keep legacy fields + provenance + tags)
        headers = [
            "hit_id","ticker","date",
            "volume","volume_millions","dollar_volume_millions",
            "intraday_push_pct",
            "near_rs","rs_exec_date","rs_days_after",
            "pm_gap_50","open_gap_50","intraday_push_50","surge_7d_300",
            "rules_detail",
            "shares_outstanding_millions","market_cap_millions","float_millions","dollar_volume","data_source",
            "float_rotation","exchange","pm_high_source","pm_high_venue","rule_tags"
        ]
        w.writerow(headers)
        for row in cur.execute(q, (start, end)):
            (hit_id, ticker, date_iso, volume, intraday_push_pct, near_rs, rs_exec_date, rs_days_after,
             pm_gap_50, open_gap_50, intraday_push_50, surge_7d_300,
             shares_outstanding, market_cap, float_shares, dollar_volume, data_source, exchange, pm_high_source, pm_high_venue, rule_tags) = row
            m = {
                "pm_gap_50": pm_gap_50,
                "open_gap_50": open_gap_50,
                "intraday_push_50": intraday_push_50,
                "surge_7d_300": surge_7d_300
            }
            rules_detail = _build_rules_detail(m)
            volume_millions = _fmt_millions(volume)
            dollar_vol_millions = _fmt_millions(dollar_volume)
            shares_millions = _fmt_millions_precise(shares_outstanding)
            market_cap_millions = _fmt_millions_precise(market_cap)
            float_millions = _fmt_millions_precise(float_shares)
            # float rotation (match CSV precision to keep acceptance checks aligned)
            float_rotation = ""
            try:
                shares_for_rotation = None
                if volume:
                    if float_millions not in ("", None):
                        try:
                            shares_for_rotation = float(float_millions) * 1_000_000.0
                        except Exception:
                            shares_for_rotation = None
                    if not shares_for_rotation and float_shares:
                        shares_for_rotation = float(float_shares)
                    if shares_for_rotation:
                        fr = float(volume) / shares_for_rotation
                        float_rotation = f"{fr:.2f}"
            except Exception:
                float_rotation = ""
            w.writerow([
                hit_id, ticker, date_iso,
                volume, volume_millions, dollar_vol_millions,
                _fmt_pct(intraday_push_pct),
                int(near_rs or 0), rs_exec_date or "", rs_days_after if rs_days_after is not None else "",
                _fmt_pct(pm_gap_50), _fmt_pct(open_gap_50), _fmt_pct(intraday_push_50), _fmt_pct(surge_7d_300),
                rules_detail,
                shares_millions, market_cap_millions, float_millions,
                dollar_volume if dollar_volume is not None else "",
                data_source or "",
                float_rotation, exchange or "",
                pm_high_source or "", pm_high_venue or "", rule_tags or ""
            ])

def export_day_completeness(conn, path):
    with open(path,"w",newline="") as f:
        w=csv.writer(f); w.writerow(["date","daily_raw","hits","rules"])
        for (d,) in conn.execute("select distinct date from daily_raw order by date"):
            dr = conn.execute("select count(*) from daily_raw where date=?", (d,)).fetchone()[0]
            h = conn.execute("select count(*) from discovery_hits where event_date=?", (d,)).fetchone()[0]
            r = conn.execute("select count(*) from discovery_hit_rules x join discovery_hits y on x.hit_id=y.hit_id where y.event_date=?", (d,)).fetchone()[0]
            w.writerow([d, dr, h, r])

def main(start, end, db, out_dir):
    conn = sqlite3.connect(db)
    export_hits(conn, start, end, f"{out_dir}/discovery_hits_{start}_{end}.csv")
    export_day_completeness(conn, f"{out_dir}/day_completeness.csv")
    conn.close()

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--db", default="db/scanner.db"); ap.add_argument("--out", default="out")
    a=ap.parse_args(); main(a.start, a.end, a.db, a.out)

