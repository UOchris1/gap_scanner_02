# -*- coding: ascii -*-
# Quick Start: Clone and Run (Frictionless)

This project is configured to run cleanly on a fresh machine with a few simple steps.

Requirements
- Python 3.10+
- Node/gh not required
- ThetaData Terminal (optional, local HTTP; STANDARD plan OK)

Steps
1) Clone and install
   - git clone <repo>
   - cd gap_scanner_01
   - python -m venv .venv && .venv\\Scripts\\activate  (Windows)
   - pip install -r requirements.txt

2) Configure environment
   - Copy: .env.example -> .env
   - Fill keys in .env (POLYGON_API_KEY required; FMP_API_KEY optional)
   - Keep .env private (already gitignored)

3) Smoke checks
   - python scripts/gapctl.py env-validate
   - python scripts/gapctl.py health

4) Run a day (acceptance gate)
   - python scripts/gapctl.py validate --date 2025-09-12 --db db/acceptance.db

5) Enrich and export (example: a week)
   - python scripts/gapctl.py scan-range --start 2025-06-09 --end 2025-06-13 --db db/scanner.db
   - python scripts/gapctl.py export --start 2025-06-09 --end 2025-06-13 --db db/scanner.db --out exports
   - python scripts/gapctl.py summary --start 2025-06-09 --end 2025-06-13 --db db/scanner.db --out exports

6) Optional T+1 outcomes (next-day positive)
   - python scripts/gapctl.py outcomes --start 2025-06-09 --end 2025-06-13 --db db/scanner.db --out exports

Notes
- Database is created automatically with idempotent schema migrations.
- No legacy tables are created (e.g., polygon_prev). Backbone is daily_raw.
- Exports are regenerated on demand; CSVs are gitignored by default.

