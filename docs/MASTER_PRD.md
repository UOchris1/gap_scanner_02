# MASTER PRD - GAP_SCANNER_01
Version: 2.0
Last Updated: 2025-09-19
Status: Operational with Zero-Miss pipeline; acceptance pending live keys

## Objectives
- Detect extreme movers using a T+1 pipeline with four rules (R1 to R4).
- Guarantee coverage (zero-miss posture) with deterministic universe and audit.
- Produce two canonical CSV outputs and persist normalized facts to SQLite.
- Enforce ASCII only, fix-in-place, and no rogue scripts as permanent policy.

## Scope
- In scope: providers, pipeline logic, reverse split gating, DB schema, exports,
  acceptance gates, monitoring artifacts, reliability and performance controls.
- Out of scope: dashboards, notebooks, ad hoc demo scripts, non-ASCII output.

## Non-Functional Requirements
- ASCII only across code, filenames, logs, and CSVs.
- Deterministic runs and reproducible outputs for a given date.
- Bounded network usage and concurrency per vendor limits.
- Idempotent DB schema evolution and safe reruns.

## Architecture
- Entry points
  - Primary: src/pipelines/zero_miss.scan_day (via src/integration/cli_bridge.process_day_zero_miss)
  - Validation: scripts/validate_acceptance.py (GATE1 to GATE3)
- Providers
  - ThetaData: v3 primary on 25503, v1 fallback on 25510; venue=nqb; premarket window 04:00:00 to 09:29:59 ET.
  - Polygon: grouped daily backbone (adjusted=false), prev close, splits, roster.
  - FMP: optional enterprise values for shares as-of date (future enhancement).
- Pipeline (per day)
  1) Universe: populate universe_day from Polygon v3 reference tickers, including delisted, then select XNAS/XNYS/XASE.
  2) Pass-1: grouped daily (adjusted=false). Compute R2 (open gap) and R3 (intraday push) candidates.
  3) Pass-2B: premarket checks on candidates using ThetaData; compute R1 and collect audit sample.
  4) R4: compute 7 day low/high window; apply rule.
  5) Reverse split gate: suppress artifacts within 1 trading day of reverse split unless heavy runner override (dollar_volume >= 10,000,000 and intraday_push_pct >= 50.0).
  6) Persist: discovery_hits, discovery_hit_rules, daily_raw, completeness_log.
  7) Completeness audit: record counts and audit results; generate day completeness CSV during exports.

## Rule Definitions (T+1)
- R1 Premarket mover: ((premarket_high / prev_close) - 1) * 100 >= 50.0
- R2 Open gap:        ((open / prev_close) - 1) * 100 >= 50.0
- R3 Intraday push:   ((high / open) - 1) * 100 >= 50.0
- R4 7-day surge:     ((highest_high_7d / lowest_low_7d) - 1) * 100 >= 300.0

## Reverse Split Gate
- Window: 1 trading day (allow up to 3 calendar days around the event to tolerate weekends).
- Keep hit if heavy runner override holds: dollar_volume >= 10,000,000 AND intraday_push_pct >= 50.0.
- Else mark as split artifact and suppress OPEN_GAP_50 only artifacts.

## Data Contracts
- Database (SQLite)
  - Tables: daily_raw(provider,date,symbol,open,high,low,close,volume,vwap),
    discovery_hits(hit_id,ticker,event_date,volume,intraday_push_pct,is_near_reverse_split,rs_exec_date,rs_days_after),
    discovery_hit_rules(hit_rule_id,hit_id,trigger_rule,rule_value),
    universe_day(date,symbol,active,delisted_utc,primary_exchange),
    completeness_log(date,total_universe,polygon_count,cand_pass1,r1_checked,r1_hits,miss_audit_sample,miss_audit_hits,audit_failed).
  - Unique index: discovery_hits(ticker,event_date). Optional unique index on discovery_hit_rules(hit_id,trigger_rule) to prevent duplicates.
  - Notional policy: dollar_volume should be computed as unadjusted volume * VWAP for that day; VWAP stored in daily_raw.vwap when available.
- CSV outputs (scripts/export_reports.py)
  - discovery_hits_{start}_{end}.csv
  - day_completeness.csv

## Configuration
- .env (project root)
  - Required: POLYGON_API_KEY
  - Optional: THETA_V3_URL, THETA_V1_URL, THETA_VENUE, THETA_V3_MAX_OUTSTANDING, THETA_V1_MAX_OUTSTANDING
  - Optional: FMP_API_KEY
- Defaults
  - Database path: db/scanner.db for production, temp paths for tests.
  - Concurrency: ThetaData bounded by plan (STANDARD=2 to 4, PRO=4+). Code defaults use safe values.

## Acceptance Criteria
- Imports succeed for core modules and providers without performing network calls.
- Rule functions return numeric values with correct thresholds.
- Validate day run (scripts/validate_acceptance.py) passes all gates for a target trading day when valid keys are present:
  - GATE1 basis sanity near split dates: dollar_volume coherence and integer-like volume when adjusted=false.
  - GATE2 rules uniqueness: no duplicates per (hit_id, trigger_rule) and unique index enforced.
  - GATE3 CSV shape: columns present, millions formatting, float rotation coherence.
- Two CSVs are produced with non-zero rows for the target day.
- Completeness metrics logged for the day.

## Monitoring and Artifacts
- project_state/artifacts/scan_{date}.log: stage breadcrumbs and timing.
- project_state/artifacts/hang_trace_{date}.txt: watchdog dump for hung threads.
- validation_results/validation_summary.json: summary rollup when available.

## Reliability and Performance
- ThetaData requests use bounded semaphores and retry/backoff for 429, 570, 571, 474.
- Polygon grouped daily uses bounded retries and never loops indefinitely.
- Universe is pinned per day for determinism; reuse when re-running.

## Known Constraints and Issues
- API keys required for live runs. Without POLYGON_API_KEY, universe population and grouped daily will fail by design.
- Some files are missing the ASCII header banner; see .github/scripts/validate_ascii.py for remediation.
- One defect fixed: src/providers/polygon_provider.py now uses POLY_KEY for apiKey parameter.

## Runbook
- Local day validation
  - python scripts/validate_acceptance.py --date YYYY-MM-DD --db db/acceptance.db
  - On failure, a help request is written to project_state/HELP_REQUEST.md per policy.
- Programmatic usage
  - from src.integration.cli_bridge import process_day_zero_miss
  - process_day_zero_miss("YYYY-MM-DD", "db/scanner.db", providers={})

## Future Enhancements
- Persist daily_raw.vw for all rows and compute dollar_volume as volume * vwap everywhere exports use it.
- Add unique index uq_hit_rule(hit_id, trigger_rule) and one-time cleanup for duplicates.
- Optional FMP enterprise values integration for historical shares as-of date.
