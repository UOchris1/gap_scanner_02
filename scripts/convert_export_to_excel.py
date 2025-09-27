import argparse
from pathlib import Path
import pandas as pd

DEFAULT_COLUMN_ORDER = [
    "date",
    "ticker",
    "exchange",
    "volume",
    "volume_millions",
    "intraday_push_pct",
    "pm_gap_50",
    "open_gap_50",
    "intraday_push_50",
    "surge_7d_300",
    "near_rs",
    "rs_exec_date",
    "rs_days_after",
    "float_rotation",
    "shares_outstanding_millions",
    "float_millions",
    "market_cap_millions",
    "dollar_volume_millions",
    "dollar_volume",
    "data_source",
    "pm_high_source",
    "pm_high_venue",
    "rules_detail",
    "rule_tags",
    "hit_id"
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert discovery_hits CSV export to Excel workbooks")
    parser.add_argument("--csv", required=True, help="Path to discovery_hits CSV produced by export_reports")
    parser.add_argument("--pairs-out", required=True, help="Output Excel path for date/ticker pairs")
    parser.add_argument("--full-out", required=True, help="Output Excel path for full detail")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    if "date" not in df.columns:
        # legacy exports may call it 'event_date'
        if "event_date" in df.columns:
            df = df.rename(columns={"event_date": "date"})
        else:
            raise SystemExit("CSV missing 'date' column")

    # Date/Ticker pairs
    pairs = df[[col for col in ["date", "ticker"] if col in df.columns]].drop_duplicates()
    pairs = pairs.sort_values(by=[c for c in ["date", "ticker"] if c in pairs.columns])
    pairs.to_excel(args.pairs_out, index=False)

    # Full detail with logical ordering
    cols = [c for c in DEFAULT_COLUMN_ORDER if c in df.columns]
    remaining = [c for c in df.columns if c not in cols]
    ordered = df[cols + remaining]
    ordered.to_excel(args.full_out, index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
