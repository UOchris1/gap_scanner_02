# Execute 10-Day Validation - ChatGPT Expected Workflow

**Following ChatGPT Handoff Report Instructions**

## Status: Ready to Execute

### ✅ Prerequisites Completed
1. Fixed Alpaca baseline scanner (working)
2. Database indexing applied (working)
3. Polygon roster export (working)
4. Provider comparison tools (working)

### Commands to Execute (Per ChatGPT Handoff)

#### 1. Database Setup
```bash
python -m scripts.apply_db_indexes db/validation_10day.db
```

#### 2. Export Polygon Roster (ChatGPT Expected)
```bash
python scripts/export_polygon_roster.py 2025-09-11 roster_2025-09-11.csv
```

#### 3. Run Discovery Engine (ChatGPT Expected)
**Note**: ChatGPT expected `run_10day_validation.py` but we have enhanced framework
```bash
python run_zero_miss_phase_b.py scan --start 2025-08-29 --end 2025-09-12 --db db/validation_10day.db
```

#### 4. Run Alpaca Baseline (Fixed and Working)
```bash
python quick_baseline_test.py
```

#### 5. Compare Providers (Working)
```bash
python -m scripts.compare_baseline out/discovery_*.csv out/baseline_*.csv out/validation_compare
```

### Expected Results (Per ChatGPT)
- R2/R3 hits on most days
- Statistical audit passes (miss rate <1%)
- Alpaca baseline comparison shows agreement
- Processing time <10 minutes per day

### Critical Success Criteria
- **Pass**: Baseline-only hits = 0
- **Pass**: Audit confidence >95%
- **Pass**: R2 or R3 hits detected on trading days
- **Fail**: Any baseline-only hits > 0

## Next: Execute This Workflow

This implements the exact validation workflow ChatGPT expected, using the enhanced tools Claude built.