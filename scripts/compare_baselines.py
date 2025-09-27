#!/usr/bin/env python3
"""
Baseline Comparison Tool
Compares primary zero-miss discovery results with baseline scanner results.

Usage: python scripts/compare_baselines.py --date 2025-09-11 --db-path zero_miss.db --output-dir baseline_comparison/
"""
import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple

def load_primary_hits(db_path: str, date: str) -> List[Dict[str, Any]]:
    """Load primary discovery hits from database"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Join discovery_hits with discovery_hit_rules to get all rule data
        query = """
        SELECT
            h.ticker as symbol,
            h.event_date as date,
            r.trigger_rule as rule,
            r.rule_value as pct_value,
            h.volume,
            'primary' as source
        FROM discovery_hits h
        JOIN discovery_hit_rules r ON h.hit_id = r.hit_id
        WHERE h.event_date = ?
        """

        cursor = conn.execute(query, (date,))
        rows = cursor.fetchall()

        primary_hits = []
        for row in rows:
            primary_hits.append({
                "symbol": row["symbol"],
                "date": row["date"],
                "rule": row["rule"],
                "pct_value": row["pct_value"],
                "volume": row["volume"],
                "source": row["source"]
            })

        conn.close()
        print(f"Loaded {len(primary_hits)} primary hits for {date}")
        return primary_hits

    except Exception as e:
        print(f"Error loading primary hits: {e}")
        return []

def load_baseline_hits(baseline_files: List[str]) -> List[Dict[str, Any]]:
    """Load baseline hits from CSV files"""
    baseline_hits = []

    for file_path in baseline_files:
        if not os.path.exists(file_path):
            print(f"Warning: Baseline file not found: {file_path}")
            continue

        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Map event_type to rule format
                    rule = row.get("event_type", "")
                    if rule == "OPEN_GAP_50":
                        rule = "R2"
                    elif rule == "INTRADAY_PUSH_50":
                        rule = "R3"
                    else:
                        continue  # Skip unknown event types

                    baseline_hits.append({
                        "symbol": row["symbol"],
                        "date": row["date"],
                        "rule": rule,
                        "pct_value": float(row["pct_value"]) if row["pct_value"] else 0.0,
                        "volume": int(row["volume"]) if row["volume"] else 0,
                        "source": row.get("source", "baseline")
                    })

            print(f"Loaded {len(baseline_hits)} baseline hits from {file_path}")

        except Exception as e:
            print(f"Error loading baseline file {file_path}: {e}")

    return baseline_hits

def normalize_rule_mapping() -> Dict[str, str]:
    """Define mapping between primary and baseline rule names"""
    return {
        "R1": "R1",  # Premarket gap (baseline may not have this)
        "R2": "R2",  # Opening gap
        "R3": "R3",  # Intraday push
        "R4": "R4"   # 7-day surge (baseline may not have this)
    }

def create_comparison_key(hit: Dict[str, Any]) -> Tuple[str, str, str]:
    """Create comparison key for joining hits"""
    return (hit["symbol"], hit["date"], hit["rule"])

def compare_hits(primary_hits: List[Dict[str, Any]], baseline_hits: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Compare primary and baseline hits, return categorized results"""
    # Create sets of comparison keys
    primary_keys = {create_comparison_key(hit): hit for hit in primary_hits}
    baseline_keys = {create_comparison_key(hit): hit for hit in baseline_hits}

    primary_key_set = set(primary_keys.keys())
    baseline_key_set = set(baseline_keys.keys())

    # Calculate overlaps and differences
    overlap_keys = primary_key_set & baseline_key_set
    primary_only_keys = primary_key_set - baseline_key_set
    baseline_only_keys = baseline_key_set - primary_key_set

    return {
        "overlap": [primary_keys[key] for key in overlap_keys],
        "primary_only": [primary_keys[key] for key in primary_only_keys],
        "baseline_only": [baseline_keys[key] for key in baseline_only_keys]
    }

def write_comparison_csvs(comparison: Dict[str, List[Dict[str, Any]]], output_dir: str, date: str):
    """Write comparison results to CSV files"""
    os.makedirs(output_dir, exist_ok=True)

    for category, hits in comparison.items():
        output_file = os.path.join(output_dir, f"{category}_{date}.csv")

        with open(output_file, 'w', newline='') as f:
            if not hits:
                # Write empty file with header
                writer = csv.writer(f)
                writer.writerow(["symbol", "date", "rule", "pct_value", "volume", "source"])
            else:
                writer = csv.DictWriter(f, fieldnames=["symbol", "date", "rule", "pct_value", "volume", "source"])
                writer.writeheader()
                writer.writerows(hits)

        print(f"Wrote {len(hits)} {category} hits to {output_file}")

def generate_summary(comparison: Dict[str, List[Dict[str, Any]]], date: str) -> Dict[str, Any]:
    """Generate summary statistics"""
    summary = {
        "date": date,
        "overlap_count": len(comparison["overlap"]),
        "primary_only_count": len(comparison["primary_only"]),
        "baseline_only_count": len(comparison["baseline_only"]),
        "total_primary": len(comparison["overlap"]) + len(comparison["primary_only"]),
        "total_baseline": len(comparison["overlap"]) + len(comparison["baseline_only"]),
        "coverage_rate": 0.0,
        "baseline_only_symbols": [hit["symbol"] for hit in comparison["baseline_only"]]
    }

    # Calculate coverage rate (how much of baseline we captured)
    if summary["total_baseline"] > 0:
        summary["coverage_rate"] = summary["overlap_count"] / summary["total_baseline"]

    return summary

def write_summary_json(summary: Dict[str, Any], output_dir: str, date: str):
    """Write summary to JSON file"""
    summary_file = os.path.join(output_dir, f"summary_{date}.json")

    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"Wrote summary to {summary_file}")

def main():
    parser = argparse.ArgumentParser(description="Compare primary vs baseline discovery results")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--db-path", required=True, help="Path to primary discovery database")
    parser.add_argument("--baseline-gap", help="Path to baseline gap CSV file")
    parser.add_argument("--baseline-spike", help="Path to baseline spike CSV file")
    parser.add_argument("--output-dir", required=True, help="Output directory for comparison results")

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("ERROR: Date must be in YYYY-MM-DD format")
        sys.exit(1)

    # Load primary hits
    primary_hits = load_primary_hits(args.db_path, args.date)

    # Load baseline hits
    baseline_files = []
    if args.baseline_gap and os.path.exists(args.baseline_gap):
        baseline_files.append(args.baseline_gap)
    if args.baseline_spike and os.path.exists(args.baseline_spike):
        baseline_files.append(args.baseline_spike)

    if not baseline_files:
        print("Warning: No baseline files found")
        baseline_hits = []
    else:
        baseline_hits = load_baseline_hits(baseline_files)

    # Perform comparison
    comparison = compare_hits(primary_hits, baseline_hits)

    # Write results
    write_comparison_csvs(comparison, args.output_dir, args.date)

    # Generate and write summary
    summary = generate_summary(comparison, args.date)
    write_summary_json(summary, args.output_dir, args.date)

    # Print summary to console
    print("\n=== BASELINE COMPARISON SUMMARY ===")
    print(f"Date: {args.date}")
    print(f"Primary hits: {summary['total_primary']}")
    print(f"Baseline hits: {summary['total_baseline']}")
    print(f"Overlap: {summary['overlap_count']}")
    print(f"Primary only: {summary['primary_only_count']}")
    print(f"Baseline only: {summary['baseline_only_count']}")
    print(f"Coverage rate: {summary['coverage_rate']:.2%}")

    # Exit with error code if baseline-only hits exist (potential misses)
    if summary["baseline_only_count"] > 0:
        print(f"\nWARNING: {summary['baseline_only_count']} baseline-only hits detected!")
        print("Symbols with potential misses:", summary["baseline_only_symbols"])
        sys.exit(1)

    print("\nBaseline comparison PASSED - no potential misses detected")
    sys.exit(0)

if __name__ == "__main__":
    main()