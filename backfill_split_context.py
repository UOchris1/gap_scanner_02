# -*- coding: ascii -*-
# Backfill split context for existing discovery hits

import sqlite3
import datetime as dt
from src.providers.polygon_provider import splits as poly_splits

def backfill_split_context(db_path: str):
    print(f"Starting split context backfill for {db_path}")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get all discovery hits that need split context
        cur.execute("""
            SELECT hit_id, ticker, event_date
            FROM discovery_hits
            WHERE rs_exec_date IS NULL
            ORDER BY event_date DESC
        """)

        hits = cur.fetchall()
        print(f"Found {len(hits)} hits to enrich")

        enriched_count = 0
        for hit_id, ticker, event_date in hits:
            try:
                # Check for reverse splits within 3 days of event
                event_dt = dt.date.fromisoformat(event_date)
                start_check = (event_dt - dt.timedelta(days=3)).isoformat()
                end_check = (event_dt + dt.timedelta(days=3)).isoformat()

                split_events = poly_splits(ticker) or []

                # Find reverse splits within window
                rs_exec_date = None
                rs_days_after = None

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
                                break  # Use first/closest match
                    except Exception:
                        continue

                # Update the hit with split context
                cur.execute("""
                    UPDATE discovery_hits
                    SET rs_exec_date = ?, rs_days_after = ?
                    WHERE hit_id = ?
                """, (rs_exec_date, rs_days_after, hit_id))

                if rs_exec_date:
                    enriched_count += 1
                    print(f"Enriched {ticker} on {event_date}: split on {rs_exec_date}, {rs_days_after} days after")

            except Exception as e:
                print(f"Error enriching {ticker} on {event_date}: {e}")
                continue

        conn.commit()
        print(f"Completed enrichment: {enriched_count} hits enriched with split context")

if __name__ == "__main__":
    backfill_split_context("db/month_enriched.db")