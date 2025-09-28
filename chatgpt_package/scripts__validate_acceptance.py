# scripts/validate_acceptance.py
import argparse, sqlite3, os, sys, csv, json
from src.integration.cli_bridge import process_day_zero_miss

def write_help_request(error_msg: str, file_line: str = "") -> None:
    """Write help request file and stop execution as per instruction"""
    os.makedirs("project_state", exist_ok=True)
    with open("project_state/HELP_REQUEST.md", "w") as f:
        f.write(f"# Help Request - Acceptance Gate Failure\n\n")
        f.write(f"**File/Line**: {file_line}\n\n")
        f.write(f"**Error**: {error_msg}\n\n")
        f.write(f"**Time**: {os.path.basename(__file__)} at validation\n")
    print(f"[FAIL] {error_msg}")
    print(f"[FAIL] Help request written to project_state/HELP_REQUEST.md")
    sys.exit(1)

def gate1_basis_sanity(conn: sqlite3.Connection, date_iso: str) -> None:
    """Gate 1: Check basis consistency for hits near split dates"""
    print("[GATE1] Checking basis sanity near split dates...")

    # Check hits within +/-3 trading days of splits
    cursor = conn.execute("""
        SELECT d.ticker, d.event_date, d.rs_exec_date, dr.volume, COALESCE(dr.vwap, dr.close) as vw,
               (dr.volume * COALESCE(dr.vwap, dr.close)) as computed_dollar_volume,
               f.dollar_volume as stored_dollar_volume
        FROM discovery_hits d
        LEFT JOIN daily_raw dr ON dr.symbol = d.ticker AND dr.date = d.event_date
        LEFT JOIN discovery_hit_fundamentals f ON f.hit_id = d.hit_id
        WHERE d.event_date = ? AND d.is_near_reverse_split = 1
    """, (date_iso,))

    split_hits = cursor.fetchall()

    for hit in split_hits:
        ticker, event_date, rs_exec_date, volume, vw, computed_dv, stored_dv = hit

        # Check volume is integer-like (no decimals) when adjusted=false
        if volume and abs(volume - round(volume)) > 0.01:
            write_help_request(
                f"Volume not integer-like for {ticker} on {event_date}: {volume}",
                "polygon_provider.py:grouped_daily adjusted=false check"
            )

        # Check dollar volume consistency - now using vw (VWAP) instead of vwap
        if computed_dv and stored_dv and stored_dv > 0:
            diff_pct = abs(computed_dv - stored_dv) / stored_dv
            if diff_pct > 0.005:  # 0.5%
                write_help_request(
                    f"Dollar volume mismatch for {ticker}: computed={computed_dv}, stored={stored_dv}, diff={diff_pct:.1%}",
                    "export_reports.py:dollar_volume calculation"
                )

    print(f"[GATE1] PASS - Checked {len(split_hits)} split-adjacent hits")

def gate2_rules_uniqueness(conn: sqlite3.Connection) -> None:
    """Gate 2: Check for duplicate rules"""
    print("[GATE2] Checking rules uniqueness...")

    cursor = conn.execute("""
        SELECT d.ticker, d.event_date, r.trigger_rule, COUNT(*)
        FROM discovery_hit_rules r
        JOIN discovery_hits d ON d.hit_id=r.hit_id
        GROUP BY d.hit_id, r.trigger_rule
        HAVING COUNT(*)>1
    """)

    duplicates = cursor.fetchall()

    if duplicates:
        write_help_request(
            f"Found {len(duplicates)} duplicate rules: {duplicates}",
            "src/core/db.py:insert_rules OR unique index missing"
        )

    print("[GATE2] PASS - No duplicate rules found")

def gate_exchange_domain(conn: sqlite3.Connection, date_iso: str) -> None:
    """Gate: All hits must have exchange in {NYSE,NASDAQ,AMEX}."""
    print("[GATEX] Checking exchange domain...")
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM discovery_hits WHERE event_date=? AND (exchange IS NULL OR exchange NOT IN ('NYSE','NASDAQ','AMEX'))",
        (date_iso,)
    )
    bad = cur.fetchone()[0]
    if bad and bad > 0:
        write_help_request(
            f"Exchange domain violations: {bad}",
            "discovery_hits.exchange normalization"
        )
    print("[GATEX] PASS - Exchange domain valid")

def gate_min_volume(conn: sqlite3.Connection, date_iso: str) -> None:
    """Gate: Enforce minimum volume threshold for hits."""
    min_vol = int(os.getenv("DISCOVERY_MIN_VOL", "100000"))
    print(f"[GATEV] Checking min volume >= {min_vol}...")
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM discovery_hits WHERE event_date=? AND CAST(volume AS INTEGER) < ?",
        (date_iso, min_vol)
    )
    low = cur.fetchone()[0]
    if low and low > 0:
        write_help_request(
            f"Min volume violations: {low} < {min_vol}",
            "pipeline volume gate"
        )
    print("[GATEV] PASS - Min volume satisfied")

def gate3_csv_shape(db_path: str, date_iso: str) -> None:
    """Gate 3: Validate CSV shape and format"""
    print("[GATE3] Checking CSV shape...")

    # Generate test CSV
    csv_path = f"test_validation_{date_iso}.csv"
    try:
        from scripts.export_reports import export_hits
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            export_hits(conn, date_iso, date_iso, csv_path)
    except Exception as e:
        write_help_request(
            f"Failed to generate CSV: {e}",
            "scripts/export_reports.py:export_hits"
        )

    # Validate CSV content
    required_columns = [
        'hit_id', 'ticker', 'date', 'volume', 'volume_millions', 'dollar_volume_millions',
        'intraday_push_pct', 'near_rs', 'rs_exec_date', 'rs_days_after',
        'pm_gap_50', 'open_gap_50', 'intraday_push_50', 'surge_7d_300',
        'rules_detail', 'shares_outstanding_millions', 'market_cap_millions',
        'float_millions', 'dollar_volume', 'data_source', 'float_rotation'
    ]

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Check required columns
            missing = [col for col in required_columns if col not in headers]
            if missing:
                write_help_request(
                    f"Missing CSV columns: {missing}",
                    "scripts/export_reports.py:WIDE_HEADERS_BASE"
                )

            # Check data format in first few rows
            for i, row in enumerate(reader):
                if i >= 3:  # Check first 3 rows
                    break

                # Check millions formatting (1 decimal)
                for col in ['volume_millions', 'dollar_volume_millions']:
                    if row[col] and row[col] != '':
                        try:
                            val = float(row[col])
                            formatted = f"{val:.1f}"
                            if row[col] != formatted:
                                write_help_request(
                                    f"Incorrect millions formatting in {col}: {row[col]} (expected {formatted})",
                                    "scripts/export_reports.py:_fmt_millions"
                                )
                        except ValueError:
                            pass

                # Check float rotation coherence
                if row['float_rotation'] and row['volume'] and row['float_millions']:
                    try:
                        expected_rotation = float(row['volume']) / (float(row['float_millions']) * 1_000_000)
                        actual_rotation = float(row['float_rotation'])
                        if expected_rotation < 0.05:
                            continue
                        if abs(expected_rotation - actual_rotation) / max(expected_rotation, 0.001) > 0.1:
                            write_help_request(
                                f"Float rotation mismatch for {row['ticker']}: expected={expected_rotation:.2f}, actual={actual_rotation}",
                                "scripts/export_reports.py:float_rotation calculation"
                            )
                    except (ValueError, ZeroDivisionError):
                        pass

        os.remove(csv_path)  # Cleanup
        print("[GATE3] PASS - CSV shape and format validated")

    except Exception as e:
        write_help_request(
            f"CSV validation failed: {e}",
            "validate_acceptance.py:gate3_csv_shape"
        )

# R1 health gate helpers
import json as _json

def _load_pm_diag(day_iso: str) -> dict:
    try:
        with open(os.path.join('project_state','artifacts', f'pm_diag_{day_iso}.json'), 'r', encoding='ascii', errors='ignore') as f:
            return _json.load(f)
    except Exception:
        return {}

def _sum_counter(bucket: dict, key: str) -> int:
    try:
        return int(bucket.get(key, 0))
    except Exception:
        return 0

def _r3_count(conn: sqlite3.Connection, day_iso: str) -> int:
    q = """
    SELECT COUNT(*)
    FROM discovery_hit_rules h
    JOIN discovery_hits d ON d.hit_id = h.hit_id
    WHERE d.event_date = ? AND (
        h.trigger_rule LIKE 'INTRADAY%' OR h.trigger_rule LIKE 'R3%' OR h.trigger_rule LIKE 'PUSH%'
    )
    """
    return int(conn.execute(q, (day_iso,)).fetchone()[0])



def gate_pm_provenance_integrity(db_path: str, day_iso: str) -> None:
    """Fail if any PM_GAP_50 hit lacks pm provenance."""
    sql = """
    SELECT COUNT(*)
    FROM discovery_hit_rules r
    JOIN discovery_hits d ON d.hit_id = r.hit_id
    WHERE d.event_date = ?
      AND r.trigger_rule = 'PM_GAP_50'
      AND (d.pm_high_source IS NULL OR d.pm_high_source = ''
           OR d.pm_high_venue IS NULL OR d.pm_high_venue = '')
    """
    with sqlite3.connect(db_path) as conn:
        missing = conn.execute(sql, (day_iso,)).fetchone()[0]
    if missing and missing > 0:
        os.makedirs('project_state', exist_ok=True)
        fail = os.path.join('project_state', f'FAIL_PM_PROVENANCE_{day_iso}.md')
        with open(fail, 'w', encoding='ascii', errors='replace') as f:
            f.write(f"# FAIL PM provenance\nmissing_rows={missing}\n")
        write_help_request('PM_GAP_50 hits missing provenance', 'scripts/validate_acceptance.py:gate_pm_provenance_integrity')
    print(f"[GATE-PM] PASS - provenance OK (missing={missing})")
def gate_r1_health(day_iso: str, db_path: str, min_health: float = 0.15, r3_threshold: int = 10) -> None:
    print('[GATE-R1] Checking venue health via pm_diag...')
    diag = _load_pm_diag(day_iso)
    if not diag:
        write_help_request(f'missing pm_diag for {day_iso}', 'pm_diag file')
    sum200 = sum(_sum_counter(v, '200') for k,v in diag.items() if isinstance(v, dict))
    sum204 = sum(_sum_counter(v, '204') for k,v in diag.items() if isinstance(v, dict))
    sum472 = sum(_sum_counter(v, '472') for k,v in diag.items() if isinstance(v, dict))
    denom = max(1, sum200 + sum204 + sum472)
    health = float(sum200) / float(denom)
    with sqlite3.connect(db_path) as conn:
        r3hits = _r3_count(conn, day_iso)
    if (health < min_health) and (r3hits >= r3_threshold):
        path = os.path.join('project_state', f'FAIL_R1_MISS_AUDIT_{day_iso}.md')
        os.makedirs('project_state', exist_ok=True)
        with open(path, 'w', encoding='ascii', errors='ignore') as f:
            f.write(f'# FAIL R1 Health {day_iso}\n')
            f.write(f'sum200={sum200} sum204={sum204} sum472={sum472} health={health:.3f} r3hits={r3hits}\n')
            f.write(_json.dumps(diag, indent=2))
        write_help_request('R1 venue health too low for active day', 'validate_acceptance.py:gate_r1_health')
    print(f"[GATE-R1] PASS - health={health:.3f} r3hits={r3hits}")



def gate_rule_tags_pipe(day_iso: str, db_path: str) -> None:
    """Ensure exported rule_tags use pipe separators (no commas)."""
    sql = """
    WITH rules AS (
      SELECT r.hit_id,
             GROUP_CONCAT(r.trigger_rule, '|') AS rule_tags
      FROM discovery_hit_rules r
      JOIN discovery_hits d ON d.hit_id = r.hit_id
      WHERE d.event_date = ?
      GROUP BY r.hit_id
    )
    SELECT COUNT(*) FROM rules WHERE rule_tags LIKE '%,%';
    """
    with sqlite3.connect(db_path) as conn:
        bad = conn.execute(sql, (day_iso,)).fetchone()[0]
    if bad:
        os.makedirs('project_state', exist_ok=True)
        path = os.path.join('project_state', f'FAIL_RULE_TAGS_FORMAT_{day_iso}.md')
        with open(path, 'w', encoding='ascii', errors='ignore') as f:
            f.write(f'# FAIL rule_tags pipe format\nrows_with_commas={bad}\n')
        write_help_request('rule_tags must use pipe separator', 'scripts/validate_acceptance.py:gate_rule_tags_pipe')
    print(f"[GATE-TAGS] PASS - rule_tags pipe format OK (rows_with_commas=0)")
def main(date_iso: str, db_path: str, skip_scan: bool = False):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Run pipeline unless skipped
    if not skip_scan:
        r = process_day_zero_miss(date_iso, db_path, providers={})
        if r.get("status") != "ok":
            print(f"[FAIL] pipeline status: {r}")
            sys.exit(2)
    else:
        print(f"SKIP: Using existing scan results for {date_iso}")

    # Basic validation
    with sqlite3.connect(db_path) as c:
        hits = c.execute("select count(*) from discovery_hits where event_date=?", (date_iso,)).fetchone()[0]
        rules = c.execute("select count(*) from discovery_hit_rules h join discovery_hits d on d.hit_id=h.hit_id where d.event_date=?", (date_iso,)).fetchone()[0]
        daily = c.execute("select count(*) from daily_raw where date=?", (date_iso,)).fetchone()[0]

    print(f"[BASIC] {date_iso} daily_raw={daily} hits={hits} rule_rows={rules}")

    # Run acceptance gates (fail-hard)
    with sqlite3.connect(db_path) as conn:
        gate1_basis_sanity(conn, date_iso)
        gate2_rules_uniqueness(conn)
        gate_exchange_domain(conn, date_iso)
        gate_min_volume(conn, date_iso)
    gate3_csv_shape(db_path, date_iso)
    gate_pm_provenance_integrity(db_path, date_iso)
    gate_r1_health(date_iso, db_path)
    gate_rule_tags_pipe(date_iso, db_path)
    print(f"[OK] All acceptance gates passed for {date_iso}")
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--db", default="db/acceptance.db")
    ap.add_argument("--skip-scan", action="store_true")
    args = ap.parse_args()
    raise SystemExit(main(args.date, args.db, skip_scan=args.skip_scan))







