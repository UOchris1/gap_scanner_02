# -*- coding: ascii -*-
# Re-enrich fundamentals data for existing discovery hits

import sqlite3
from src.providers.fundamentals_provider import get_fundamentals_for_hit
from src.core.database_operations import upsert_hit_fundamentals

def re_enrich_fundamentals(db_path: str, date_filter: str = None):
    print(f"Starting fundamentals re-enrichment for {db_path}")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Get discovery hits that need re-enrichment (those with no_data)
        if date_filter:
            cur.execute("""
                SELECT DISTINCT h.hit_id, h.ticker, h.event_date
                FROM discovery_hits h
                LEFT JOIN discovery_hit_fundamentals f ON h.hit_id = f.hit_id
                WHERE h.event_date = ? AND (f.data_source = 'no_data' OR f.data_source IS NULL)
                ORDER BY h.event_date DESC
            """, (date_filter,))
        else:
            cur.execute("""
                SELECT DISTINCT h.hit_id, h.ticker, h.event_date
                FROM discovery_hits h
                LEFT JOIN discovery_hit_fundamentals f ON h.hit_id = f.hit_id
                WHERE f.data_source = 'no_data' OR f.data_source IS NULL
                ORDER BY h.event_date DESC
                LIMIT 50
            """)

        hits = cur.fetchall()
        print(f"Found {len(hits)} hits to re-enrich")

        enriched_count = 0
        for hit_id, ticker, event_date in hits:
            try:
                print(f"Enriching {ticker} on {event_date}...")

                # Get fresh fundamentals data
                fundamentals = get_fundamentals_for_hit(ticker, event_date)

                # Calculate dollar volume from existing data
                cur.execute("SELECT volume FROM discovery_hits WHERE hit_id = ?", (hit_id,))
                volume_result = cur.fetchone()
                volume = volume_result[0] if volume_result else None

                # Get close price from daily_raw for dollar volume calculation
                cur.execute("""
                    SELECT close FROM daily_raw
                    WHERE symbol = ? AND date = ?
                    LIMIT 1
                """, (ticker, event_date))
                close_result = cur.fetchone()
                close_price = close_result[0] if close_result else None

                dollar_volume = None
                if volume and close_price:
                    dollar_volume = float(volume) * float(close_price)

                # Upsert the enriched fundamentals
                upsert_hit_fundamentals(
                    conn,
                    hit_id,
                    shares_outstanding=fundamentals.get("shares_outstanding"),
                    market_cap=fundamentals.get("market_cap"),
                    float_shares=fundamentals.get("float_shares"),
                    dollar_volume=dollar_volume,
                    data_source=fundamentals.get("data_source", "unknown")
                )

                if fundamentals.get("data_source") not in ["no_data", "unknown"]:
                    enriched_count += 1
                    print(f"  Success: {fundamentals.get('data_source')} - Market Cap: ${fundamentals.get('market_cap', 0):,.0f}")
                else:
                    print(f"  No data available for {ticker}")

            except Exception as e:
                print(f"Error enriching {ticker} on {event_date}: {e}")
                continue

        print(f"Completed re-enrichment: {enriched_count}/{len(hits)} hits successfully enriched")

if __name__ == "__main__":
    # Re-enrich for the recent date we exported
    re_enrich_fundamentals("db/month_enriched.db", "2025-09-17")