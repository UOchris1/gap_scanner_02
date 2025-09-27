#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ticker-Date CSV Export Script
Exports discovery hits and rules from the gap scanner database
"""
import sqlite3
import csv
import argparse
import os
from datetime import datetime

def export_ticker_date_pairs(db_path: str, output_path: str, date_filter: str = None):
    """
    Export ticker-date pairs with discovery rules to CSV

    Args:
        db_path: Path to SQLite database
        output_path: Path for output CSV file
        date_filter: Optional date filter (YYYY-MM-DD)
    """

    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return False

    # Create output directory if needed
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to join discovery_hits and discovery_hit_rules
    query = """
    SELECT
        h.ticker,
        h.event_date,
        h.volume,
        h.intraday_push_pct,
        h.is_near_reverse_split,
        r.trigger_rule,
        r.rule_value
    FROM discovery_hits h
    LEFT JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
    """

    params = []
    if date_filter:
        query += " WHERE h.event_date = ?"
        params.append(date_filter)

    query += " ORDER BY h.event_date, h.ticker, r.trigger_rule"

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            print(f"No data found in database {db_path}")
            if date_filter:
                print(f"Date filter: {date_filter}")
            return False

        # Write CSV file
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # Header
            writer.writerow([
                'ticker',
                'event_date',
                'volume',
                'intraday_push_pct',
                'is_near_reverse_split',
                'trigger_rule',
                'rule_value'
            ])

            # Data rows
            for row in rows:
                writer.writerow(row)

        print(f"[OK] Exported {len(rows)} records to {output_path}")

        # Summary statistics
        unique_tickers = len(set(row[0] for row in rows))
        unique_dates = len(set(row[1] for row in rows))
        rules_breakdown = {}
        for row in rows:
            rule = row[5] if row[5] else 'NO_RULE'
            rules_breakdown[rule] = rules_breakdown.get(rule, 0) + 1

        print(f"Summary:")
        print(f"   - Unique tickers: {unique_tickers}")
        print(f"   - Unique dates: {unique_dates}")
        print(f"   - Rules breakdown:")
        for rule, count in sorted(rules_breakdown.items()):
            print(f"     * {rule}: {count}")

        return True

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        conn.close()

def export_simple_ticker_date_pairs(db_path: str, output_path: str, date_filter: str = None):
    """
    Export simple ticker-date pairs (just ticker,date columns)

    Args:
        db_path: Path to SQLite database
        output_path: Path for output CSV file
        date_filter: Optional date filter (YYYY-MM-DD)
    """

    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return False

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = "SELECT DISTINCT ticker, event_date FROM discovery_hits"
    params = []
    if date_filter:
        query += " WHERE event_date = ?"
        params.append(date_filter)

    query += " ORDER BY event_date, ticker"

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            print(f"No data found in database {db_path}")
            return False

        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ticker', 'event_date'])
            writer.writerows(rows)

        print(f"[OK] Exported {len(rows)} ticker-date pairs to {output_path}")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export ticker-date pairs from gap scanner database')
    parser.add_argument('--db', required=True, help='Database path')
    parser.add_argument('--output', required=True, help='Output CSV path')
    parser.add_argument('--date', help='Filter by specific date (YYYY-MM-DD)')
    parser.add_argument('--simple', action='store_true', help='Export only ticker,date columns')

    args = parser.parse_args()

    print(f"Gap Scanner CSV Export")
    print(f"Database: {args.db}")
    print(f"Output: {args.output}")
    if args.date:
        print(f"Date filter: {args.date}")
    print(f"Mode: {'Simple' if args.simple else 'Full'}")
    print("-" * 50)

    if args.simple:
        success = export_simple_ticker_date_pairs(args.db, args.output, args.date)
    else:
        success = export_ticker_date_pairs(args.db, args.output, args.date)

    if success:
        print("[OK] Export completed successfully")
    else:
        print("[FAIL] Export failed")
        exit(1)