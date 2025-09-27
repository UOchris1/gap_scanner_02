# scripts/rolling_validation.py
import argparse, csv, os, sqlite3, datetime as dt
from src.integration.cli_bridge import process_day_zero_miss

def drange(a,b):
    s,e = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
    while s<=e:
        if s.weekday()<5: yield s.isoformat()
        s += dt.timedelta(days=1)

def audit_day(db_path, date_iso):
    with sqlite3.connect(db_path) as c:
        # quick audit: ensure top movers by high/prev_close were checked for R1
        q = """
        with prev as (select symbol, close from daily_raw where date=(select date(?,'-1 day')))
        select d.symbol, d.high, p.close
        from daily_raw d left join prev p on p.symbol=d.symbol
        where d.date=? and p.close>0
        order by (d.high/p.close) desc limit 150
        """
        top = c.execute(q,(date_iso,date_iso)).fetchall()
        checked = c.execute("select count(distinct ticker) from discovery_hits where event_date=?", (date_iso,)).fetchone()[0]
    return len(top), checked

def main(start,end,db_path,summary_csv):
    os.makedirs(os.path.dirname(summary_csv), exist_ok=True)
    rows=[]
    for d in drange(start,end):
        r = process_day_zero_miss(d, db_path, providers={})
        top, checked = audit_day(db_path, d)
        rows.append([d, r.get("status"), r.get("discoveries",0), top, checked])
    with open(summary_csv,"w",newline="") as f:
        w=csv.writer(f); w.writerow(["date","status","discoveries","audit_top","checked"]); w.writerows(rows)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--start",required=True); ap.add_argument("--end",required=True)
    ap.add_argument("--db",default="db/rolling.db"); ap.add_argument("--out",default="out/rolling_summary.csv")
    a=ap.parse_args(); main(a.start,a.end,a.db,a.out)