# -*- coding: ascii -*-
# Automation Guide

This project supports file-based automation with clear on/off toggles and visible status.

Key files
- Config: project_state/auto_enrich_config.json
- Status: project_state/auto_enrich_status.json
- Logs: project_state/artifacts/auto_enrich.log

CLI
- Show status: python scripts/auto_enrich.py --status
- Enable: python scripts/auto_enrich.py --enable
- Disable: python scripts/auto_enrich.py --disable
- Run once (catch-up now): python scripts/auto_enrich.py --run-once
- Background loop (scheduler): python scripts/auto_enrich.py --loop

Config fields (auto_enrich_config.json)
- enabled (bool): turn daily catch-up on/off
- daily_time (HH:MM local): time to start each day
- db_path: database path (e.g., db/scanner.db)
- output_dir: folder for CSV exports (e.g., exports)
- output_base: CSV base name (e.g., discovery_hits)
- lookback_days: when DB is empty, how many days back to seed
- compute_outcomes: also compute T+1 outcomes CSV

Windows Task Scheduler (example)
1) Create a basic task: Daily at 06:30
2) Action: Start a program
   - Program/script: C:\Windows\System32\cmd.exe
   - Add arguments: /c cd /d C:\path\to\gap_scanner_01 && C:\Python313\python.exe scripts\auto_enrich.py --loop
3) Run whether user is logged on or not; stop task if it runs longer than 1 day

Linux/macOS cron (example)
`
# Every day at 06:30
30 6 * * * cd /path/to/gap_scanner_01 && /usr/bin/python3 scripts/auto_enrich.py --loop >> project_state/artifacts/auto_enrich.log 2>&1
`

Streamlit UI
- Use pp/enrich_ui.py to edit automation config (toggle, time, lookback) and to inspect status in the sidebar.
