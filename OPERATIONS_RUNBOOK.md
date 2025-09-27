# Gap Scanner Operations Runbook (Updated 2025-09-22)

This runbook consolidates the scattered README/HELP notes into a single operational reference. It captures what we have done so far, lessons learned, and the exact steps for running or resuming the zero-miss pipeline and the 3-year backfill.

---

## 1. Scope & Current Status

- **Repository**: `C:\Users\socra\projects\gap_scanner_01`
- **Active Conda env**: `stat_project`
- **Data coverage**: database `db\scanner.db` contains validated data up to **2022-02-23**; a background backfill is running from **2022-02-24 through 2025-09-21**.
- **Backfill process**: launched 2025-09-22 02:17 with
  ```powershell
  $env:MISS_AUDIT_TOP_N='30';
  $env:MISS_AUDIT_THETA='false';
  $env:R1_THREAD_WORKERS='32';
  conda activate stat_project;
  python scripts\run_backfill_loop.py --start 2022-02-24 --end 2025-09-21
  ```
  Background PowerShell PID at launch: 48648 (check with `Get-Process -Id 48648`).
- **Next milestone**: fully backfill trading days through 2025-09-21, honoring stop-on-failure gates.

---

## 2. Environment & Key Settings

### 2.1 `.env`
Ensure the following overrides are present (values already committed locally):
```
THETA_V3_MAX_OUTSTANDING=4
THETA_V1_MAX_OUTSTANDING=4
THETA_TIMEOUT_SEC=10
PM_START=04:00:00
PM_END=09:30:00
EXCLUDE_DERIVATIVES=true
DISCOVERY_MIN_VOL=100000
```
API keys (Polygon, FMP, etc.) must remain populated; never commit `.env` to git.

### 2.2 Operational toggles
Set these **per shell** before running backfill or heavy scans:
```
$env:MISS_AUDIT_TOP_N = '30'      # caps post-scan miss audit sample
$env:MISS_AUDIT_THETA = 'false'   # skips Theta inside miss audit during bulk
$env:R1_THREAD_WORKERS = '32'     # ThreadPool size; semaphores still limit concurrency
```
These cut redundant Theta calls while keeping acceptance gates intact.

---

## 3. Core Scripts & Responsibilities

| Script | Purpose |
|--------|---------|
| `scripts/gapctl.py` | Single entry point for `scan-day`, `scan-range`, `export`, `validate`, `health`. Always run pipeline actions through this CLI. |
| `scripts/run_backfill_loop.py` | Automation wrapper that performs month-by-month `scan-range ? export ? validate` with stop-on-failure policy. Supports `--start/--end`, `--max-months`, `--skip-phase0`, and `--dry-run`. |
| `scripts/validate_acceptance.py` | Per-day acceptance gates (basis sanity, CSV shape, provenance, R1 health, etc.). |
| `scripts/export_reports.py` | Generates `discovery_hits_*.csv` and `day_completeness.csv`. Includes float-rotation precision fix. |
| `src/pipelines/zero_miss.py` | Main scan pipeline. R1 Theta stage now uses a thread pool (respecting semaphores) for better latency. |
| `project_state/current_state.json` | Tracks `last_completed_month` and `last_completed_day` for resume. |
| `project_state/task_log.md` | Append-only audit trail of monthly passes. |

---

## 4. Standard Operating Procedures

### 4.1 Phase 0 sanity check
Before any major run (unless automation already completed it this session):
```
conda activate stat_project
python scripts\gapctl.py validate --date 2025-09-19 --db db\scanner.db
```
Stop immediately if any gate fails; do **not** modify code. Investigate the generated `project_state\HELP_REQUEST.md`.

### 4.2 Manual scanning (single month example)
```
# Set env toggles
$env:MISS_AUDIT_TOP_N='30'
$env:MISS_AUDIT_THETA='false'
$env:R1_THREAD_WORKERS='32'
conda activate stat_project

# Scan and export
python scripts\gapctl.py scan-range --start 2023-03-01 --end 2023-03-31 --db db\scanner.db
python scripts\gapctl.py export --start 2023-03-01 --end 2023-03-31 --db db\scanner.db --out exports

# Acceptance spot checks (2-3 busy weekdays)
python scripts\gapctl.py validate --date 2023-03-07 --db db\scanner.db
python scripts\gapctl.py validate --date 2023-03-15 --db db\scanner.db
python scripts\gapctl.py validate --date 2023-03-27 --db db\scanner.db
```
Verify artifacts (`pm_diag_YYYY-MM-DD.json`, `miss_audit_filters_YYYY-MM-DD.json`) after each validated day.

### 4.3 Automation
Use `scripts/run_backfill_loop.py` for unattended operation. Key options:
```
python scripts\run_backfill_loop.py --start YYYY-MM-DD --end YYYY-MM-DD \
    [--skip-phase0] [--max-months N] [--dry-run]
```
The script prints the planned months and invokes `gapctl` for each stage. It updates `project_state/current_state.json` and `project_state/task_log.md` only after a successful month.

### 4.4 Monitoring & Control
- **Active process**: `Get-Process -Id <PID>` (current backfill: 48648).
- **Current day log**: `Get-ChildItem project_state\artifacts\scan_*.log | Sort LastWriteTime | Select -Last 1 | % { Get-Content $_.FullName -Tail 20 }`
- **Tail live**: `Get-Content project_state\artifacts\scan_YYYY-MM-DD.log -Wait`
- **Monthly progress**: `Get-Content project_state\task_log.md | Select-Object -Last 5`
- **Stop automation**: `Stop-Process -Id <PID>` (resume later; run_backfill_loop picks up from `current_state.json`).

---

## 5. Lessons Learned & Changes Implemented (2025-09-21 ? 2025-09-22)

1. **Theta R1 Bottleneck**
   - Original semaphore limits (2/2) plus sequential requests caused 6+ minute R1 phases.
   - Fixes:
     - `.env` now sets `THETA_V3_MAX_OUTSTANDING=4`, `THETA_V1_MAX_OUTSTANDING=4`, `THETA_TIMEOUT_SEC=10`.
     - `src/pipelines/zero_miss.py` now parallelizes R1 lookups via `ThreadPoolExecutor`; semaphores still cap to 4 in-flight per terminal.
     - `scripts/run_backfill_loop.py` logs actual concurrency at startup.

2. **Miss Audit Performance**
   - Audit was issuing ~150 extra Theta calls/day. Added env toggles in `src/core/completeness.py`:
     - `MISS_AUDIT_TOP_N` to cap audit list (default 150, we use 30 during backfills).
     - `MISS_AUDIT_THETA` to disable Theta checks during bulk runs (set `false`).

3. **Float Rotation Acceptance Fail**
   - Exporter now derives float rotation from `float_millions` when available to maintain precision (`scripts/export_reports.py`).

4. **Automation Improvements**
   - Added start/end overrides and partial-month handling to `scripts/run_backfill_loop.py`.
   - State now records both last completed month and day for finer resume control.

5. **R1 Diagnostics**
   - Theta provider logs `[theta] init v3_limit=4 v1_limit=4 timeout=10` at startup for easy verification.

---

## 6. Artifacts & Directories

- `project_state/artifacts/scan_YYYY-MM-DD.log` ? per-day pipeline log (append-only).
- `project_state/artifacts/pm_diag_YYYY-MM-DD.json` ? R1 health diagnostics used by acceptance.
- `project_state/artifacts/miss_audit_filters_YYYY-MM-DD.json` ? audit transparency summary.
- `project_state/task_log.md` ? monthly completion append log.
- `project_state/current_state.json` ? resume pointer (now includes `last_completed_day`).
- `exports/` ? CSV outputs (`discovery_hits_*.csv`, `day_completeness.csv`).
- `db/reports/` ? per-day provider overlap and completeness reports.

---

## 7. Troubleshooting Checklist

1. **Theta slow again**
   - Confirm `.env` concurrency values (4/4) and look for `[theta] init ...` log line.
   - Ensure `R1_THREAD_WORKERS` env is set (>=32) before launching scans.

2. **Miss audit taking too long**
   - Verify `MISS_AUDIT_THETA=false` during bulk runs. Re-enable (`true`) for small-scale or production validation as needed.

3. **Acceptance Failure**
   - Check generated FAIL/HELP artifacts in `project_state`. Do not rerun until root cause is addressed.

4. **Automation halted**
   - Review last log in `project_state/task_log.md` and the most recent `scan_*.log`. If process exited due to failure, fix the issue and rerun `run_backfill_loop.py` (it resumes from `current_state.json`).

5. **Process resume**
   - To resume after manual stop, re-export the env toggles and run:
     ```powershell
     $env:MISS_AUDIT_TOP_N='30'
     $env:MISS_AUDIT_THETA='false'
     $env:R1_THREAD_WORKERS='32'
     conda activate stat_project
     python scripts\run_backfill_loop.py --start 2022-02-24 --end 2025-09-21
     ```
     (Start date can be omitted; script reads `current_state.json`.)

---

## 8. Contact & Future Work

- **Next checks**: monitor monthly entries as automation progresses; ensure acceptance gates stay green.
- **Enhancements under consideration**:
  1. Caching premarket highs for miss audit to allow full audit with reduced Theta load.
  2. Suppressing repeated Theta ?472? log spam.
  3. Addressing reverse-split basis correction noted in earlier reports.

Keep this runbook updated whenever major configuration or pipeline changes occur.
