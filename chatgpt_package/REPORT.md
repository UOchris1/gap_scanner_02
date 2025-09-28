# Gap Scanner 02 – Setup Report and Rebuild Notes (ChatGPT Package)

This single document captures exactly what was needed to install, configure, and smoke-test the Gap Scanner 02 repo, plus the minimal guidance to rebuild it cleanly without any AI/agent. ASCII only. Keep this file in `chatgpt_package/` as the canonical handoff.

-------------------------------------------------------------------------------

1) Executive Summary

- One CLI entry: `scripts/gapctl.py` (scan-day, scan-range, export, summary, outcomes, validate, health, env-validate, env-format).
- One pipeline: `src/pipelines/zero_miss.py::scan_day()` orchestrates Polygon + rules (R2, R3, R1 via Theta premarket, R4) + completeness + reports.
- One integration shim: `src/integration/cli_bridge.py::process_day_zero_miss()` used by CLI (and UI if added).
- Data providers: Polygon (grouped daily backbone, prev close, splits, daily range); ThetaData (premarket R1); FMP (fundamentals).
- Outputs: SQLite DB under `db/`, CSV exports under `exports/`, run artifacts/logs under `project_state/`.

What worked in validation:
- Health check detected Polygon key and Theta v3 (local terminal), and FMP when provided.
- Universe prepopulation succeeded (v3/reference/tickers, page-capped), ~11,7k symbols/day.
- Scan range for 2025-09-22..2025-09-26 completed OK (5/5) with exports generated.

-------------------------------------------------------------------------------

2) Quick Start (No AI Required)

Prereqs
- Python 3.10 (recommended)
- Git
- ThetaData Terminal running locally if you want R1 premarket checks
  - v3 default `http://127.0.0.1:25503`, v1 default `http://127.0.0.1:25510`

Install
- Windows PowerShell:
  - `cd gap_scanner_02`
  - `py -3.10 -m venv .venv`
  - `.\\.venv\\Scripts\\Activate.ps1`
  - `pip install --upgrade pip`
  - `pip install -r requirements.txt`

Configure
- Create `.env` in repo root (NEVER commit keys):
  - `POLYGON_API_KEY=<your_polygon_key>`
  - `FMP_API_KEY=<optional_fmp_key>`
  - `THETA_V3_URL=http://127.0.0.1:25503`
  - `THETA_V1_URL=http://127.0.0.1:25510`
- Validate and normalize:
  - `python scripts/gapctl.py env-format`
  - `python scripts/gapctl.py env-validate`

Health Check
- `python scripts/gapctl.py health`
  - Confirms Polygon/FMP key presence, Theta detection.

Run a Scan
- Single day: `python scripts/gapctl.py scan-day --date 2025-09-26 --db db/scanner.db`
- Range: `python scripts/gapctl.py scan-range --start 2025-09-22 --end 2025-09-26 --db db/scanner.db`

Export CSVs
- `python scripts/gapctl.py export --start 2025-09-22 --end 2025-09-26 --db db/scanner.db --out exports`

Acceptance Validation (requires Theta running)
- `python scripts/gapctl.py validate --date 2025-09-26 --db db/acceptance.db`

Artifacts and Outputs
- Database: `db/scanner.db`
- Exports: `exports/discovery_hits_YYYY-MM-DD_YYYY-MM-DD.csv`, `exports/day_completeness.csv`, `exports/summary_*.csv`
- Logs/artifacts: `project_state/`

-------------------------------------------------------------------------------

3) Minimal File Map to Understand the System

- CLI: `scripts/gapctl.py` – single entry point for all operations.
- Shim: `src/integration/cli_bridge.py` – adapter that calls `scan_day()` for both CLI and UI.
- Pipeline: `src/pipelines/zero_miss.py` – orchestrates:
  - Polygon grouped daily backbone
  - R2 open gap, R3 intraday push, R1 premarket via Theta, R4 7‑day surge
  - Completeness logging, audit, provider overlap CSVs
- Core DB ops: `src/core/db.py`, `src/core/database_operations.py` (helpers, schema checks), `enhanced_db_schema.py` (base tables)
- Providers: `src/providers/polygon_provider.py`, `src/providers/theta_provider.py`, `src/providers/fundamentals_provider.py`
- Reports/Exports: `scripts/export_reports.py`
- Env tools: `scripts/env_tools.py` (env-format, env-validate)

Optional UI (current state)
- `app/enrich_ui.py` – working Streamlit UI. PRD suggests a single `app/streamlit_app.py`; you can alias or migrate to unify naming.

-------------------------------------------------------------------------------

4) Lessons Learned During Install + Run

- Keys and terminal matter:
  - Polygon key must be real; placeholders cause 401 on universe fetch.
  - Theta terminal not required to scan, but acceptance validation and R1 premarket rely on it. When not available, pipeline skips R1 gracefully.
  - FMP key improves fundamentals enrichment; missing key does not block scans.
- Universe paging:
  - `get_universe_symbols()` is paged and capped; it carried apiKey through `next_url` correctly; produced ~11.7k symbols/day.
- Completeness/Audit:
  - Coverage and audit CSVs are generated under `db/reports/` and overall day completeness aggregates into `exports/day_completeness.csv` via `gapctl export`.
- Known non-blocking warning:
  - During `ensure_schema_and_indexes()`, a SQL block attempted to enforce uniqueness and has a stray `try` inside a SQL string; logs: "[WARN] Could not enforce rule uniqueness: near 'try': syntax error". It’s harmless; future cleanup should fix the SQL string or move index creation to Python with separate `try/except`.
- Theta responses:
  - Occasional 472 "No data found" responses from v3 endpoints for specific symbols/venues are handled; R1 still proceeds across candidates.

-------------------------------------------------------------------------------

5) Rebuild Guidance (Clean, Portable, Single-Entry)

Required Inputs
- `.env` (ASCII only) with keys and Theta URLs as above.
- Python 3.10 venv and `pip install -r requirements.txt`.

Required Commands (order)
1) `python scripts/gapctl.py env-format`
2) `python scripts/gapctl.py env-validate`
3) `python scripts/gapctl.py health`
4) `python scripts/gapctl.py scan-day --date YYYY-MM-DD --db db/scanner.db` or `scan-range`
5) `python scripts/gapctl.py export --start YYYY-MM-DD --end YYYY-MM-DD --db db/scanner.db --out exports`
6) Optionally `python scripts/gapctl.py validate --date YYYY-MM-DD --db db/acceptance.db` (Theta required)

What to Include In the Repo (next rebuild)
- One CLI (`scripts/gapctl.py`) and one pipeline (`src/pipelines/zero_miss.py`) only.
- One Streamlit app file (`app/streamlit_app.py`) that calls `process_day_zero_miss()` and `export_reports`.
- `.env.example` with keys (placeholders, no secrets) and comments for required vs optional.
- `requirements.txt` – keep minimal; Python 3.10 baseline.
- `README.md` trimmed to only the Quick Start above; no legacy scripts mentioned.
- `db/`, `exports/`, `project_state/` created at runtime; keep them empty or `.gitkeep` if needed.

Pruning Strategy (ruthless but safe)
- Keep only what `gapctl` imports directly or via `scan_day()`.
- Move legacy/prototype scripts not referenced by `gapctl` to `examples/` or remove entirely if truly obsolete:
  - Examples to move/remove: `alpaca_*`, `polygon_spike_baseline.py`, `provider_qc_compare.py`, `run_discovery_compare.py`, operational runbooks, large baselines not called by CLI.
- Update `README.md` to reflect “one CLI, one UI” and remove all old paths (e.g., references to gap_scanner_01).
- Fix the minor SQL uniqueness-enforcement warning in `src/core/db.py` by removing the stray `try` inside SQL and wrapping the create-index calls in Python try/except.

-------------------------------------------------------------------------------

6) Acceptance Checklist (for future fresh machine)

- [ ] Python 3.10 installed; venv created; `pip install -r requirements.txt` succeeded.
- [ ] `.env` present with real `POLYGON_API_KEY`; Theta URLs set; optional `FMP_API_KEY` set.
- [ ] `python scripts/gapctl.py health` shows Polygon=YES, Theta ok (if running), FMP presence as desired.
- [ ] `python scripts/gapctl.py scan-day --date <known market day>` completes with status OK.
- [ ] `python scripts/gapctl.py export --start <start> --end <end>` writes CSVs to `exports/`.
- [ ] (Optional) `python scripts/gapctl.py validate --date <day>` passes when Theta is running.

-------------------------------------------------------------------------------

7) Troubleshooting (Fast Answers)

- 401 Unauthorized from Polygon during universe fetch:
  - Update `.env` with a real `POLYGON_API_KEY`; rerun `env-format`, `env-validate`, and `health`.
- Theta not detected or R1 skipped:
  - Ensure ThetaData Terminal is running locally at `THETA_V3_URL`. If not available, scans still run (R1 skipped), but acceptance may fail.
- Slow scans or timeouts:
  - Universe is pre-paged and capped; network or provider limits may still slow some requests. Re-run; the pipeline is bounded (no infinite loops).
- Non-blocking DB uniqueness warning:
  - Safe to ignore; fix in a cleanup pass by adjusting index creation code in `src/core/db.py`.

-------------------------------------------------------------------------------

8) Commands Used During This Smoke Test (for provenance)

- Venv + install (Windows):
  - `py -3.10 -m venv .venv`
  - `.\\.venv\\Scripts\\Activate.ps1`
  - `pip install --upgrade pip`
  - `pip install -r requirements.txt`
- Env checks:
  - `python scripts/gapctl.py env-format`
  - `python scripts/gapctl.py env-validate`
  - `python scripts/gapctl.py health`
- Optional universe prepopulation (faster range scans):
  - Internal call used during testing: `src.core.universe.bulk_populate_universe('db/scanner.db', '2025-09-22', '2025-09-26')`
- Range scan and exports:
  - `python scripts/gapctl.py scan-range --start 2025-09-22 --end 2025-09-26 --db db/scanner.db`
  - `python scripts/gapctl.py export --start 2025-09-22 --end 2025-09-26 --db db/scanner.db --out exports`

-------------------------------------------------------------------------------

9) Final Notes

- Keep this document up to date whenever dependencies, environment keys, or entry points change.
- Aim for the “one CLI, one UI” standard at all times; avoid reintroducing parallel scripts.
- For ruthless cleanup, prefer moving unused prototypes to `examples/` first, then removing once confirmed unnecessary.

