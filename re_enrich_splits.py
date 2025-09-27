# -*- coding: ascii -*-
# Re-enrich reverse split context data for existing discovery hits

import sqlite3
import datetime as dt
from src.providers.polygon_provider import splits as poly_splits

def re_enrich_splits(db_path: str, date_filter: str = None):
    print(f"Starting reverse split re-enrichment for {db_path}")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get discovery hits that need split context enrichment
        if date_filter:
            cur.execute("""
                SELECT hit_id, ticker, event_date
                FROM discovery_hits
                WHERE event_date = ? AND (rs_exec_date IS NULL OR rs_exec_date = '')
                ORDER BY event_date DESC
            """, (date_filter,))
        else:
            cur.execute("""
                SELECT hit_id, ticker, event_date
                FROM discovery_hits
                WHERE rs_exec_date IS NULL OR rs_exec_date = ''
                ORDER BY event_date DESC
                LIMIT 50
            """)

        hits = cur.fetchall()
        print(f"Found {len(hits)} hits to check for reverse splits")

        enriched_count = 0
        split_found_count = 0

        for hit_id, ticker, event_date in hits:
            try:
                print(f"Checking {ticker} on {event_date} for reverse splits...")

                # Check for reverse splits within 3 days of event
                event_dt = dt.date.fromisoformat(event_date)
                start_check = (event_dt - dt.timedelta(days=3)).isoformat()
                end_check = (event_dt + dt.timedelta(days=3)).isoformat()

                split_events = poly_splits(ticker) or []

                rs_exec_date = None
                rs_days_after = None

                # Find reverse splits within window
                for split_event in split_events:
                    try:
                        sf = float(split_event.get("split_from", 0))
                        st = float(split_event.get("split_to", 0))
                        exec_date = split_event.get("execution_date")

                        if sf > st and exec_date:  # Reverse split
                            exec_dt = dt.date.fromisoformat(exec_date)
                            days_diff = (event_dt - exec_dt).days

                            # Check if within 3 calendar days
                            if abs(days_diff) <= 3:
                                rs_exec_date = exec_date
                                rs_days_after = days_diff
                                split_found_count += 1
                                print(f"  Found reverse split: {sf}:{st} on {exec_date}, {days_diff} days from event")
                                break  # Use first/closest match
                    except Exception as e:
                        print(f"  Error parsing split event: {e}")
                        continue

                # Update the hit with split context (even if NULL to mark as checked)
                cur.execute("""
                    UPDATE discovery_hits
                    SET rs_exec_date = ?, rs_days_after = ?
                    WHERE hit_id = ?
                """, (rs_exec_date, rs_days_after, hit_id))

                enriched_count += 1

                if rs_exec_date:
                    print(f"  Split found: {rs_exec_date}, {rs_days_after} days after")
                else:
                    print(f"  No reverse splits found in window")

            except Exception as e:
                print(f"Error checking {ticker} on {event_date}: {e}")
                continue

        conn.commit()
        print(f"Completed split enrichment: {enriched_count} hits checked, {split_found_count} splits found")

if __name__ == "__main__":
    # Re-enrich for the recent date we exported
    re_enrich_splits("db/month_enriched.db", "2025-09-17")