# Baseline Validation Runbook

## Overview

This runbook implements **Phase A** of universe_04.txt: establishing baseline cross-checkers before the main 3-year production run. The baseline validation system compares our zero-miss discovery pipeline against simpler Alpaca and Polygon scanners to detect potential systematic misses.

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Primary       │    │   Baseline       │    │   Comparison    │
│   Discovery     │───▶│   Scanners       │───▶│   & Audit       │
│   (Zero-Miss)   │    │   (Alpaca/Poly)  │    │   (Rule of 3)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   SQLite DB     │    │   Baseline CSV   │    │   Diff Reports  │
│   discovery_*   │    │   Files          │    │   & Summary     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## File Structure

```
gap_scanner_01/
├── run_baseline_validation.py          # Main CLI orchestrator
├── scripts/
│   ├── alpaca_gap_baseline.py          # Alpaca-based gap scanner (R2)
│   ├── polygon_spike_baseline.py       # Polygon-based spike scanner (R3)
│   ├── compare_baselines.py            # Baseline comparison engine
│   └── export_polygon_roster.py        # Polygon roster export
├── enhanced_audit.py                   # Rule of three audit system
├── enhanced_db_schema.py               # Database with baseline tracking
├── run_zero_miss.py                    # Core zero-miss discovery
└── BASELINE_VALIDATION_RUNBOOK.md      # This file
```

## Prerequisites

### Environment Variables
```bash
export POLYGON_API_KEY="your_polygon_key"
export ALPACA_API_KEY="your_alpaca_key"
export ALPACA_SECRET_KEY="your_alpaca_secret"
export THETA_TERMINAL_HOST="127.0.0.1"  # or localhost
```

### Data Providers
- **ThetaData v3**: Required for premarket R1 scanning and prev_close retrieval
- **Polygon**: Required for grouped daily market sweeps and baseline spike scanning
- **Alpaca**: Required for baseline gap scanning (alternative data source)

### Dependencies
```bash
pip install requests sqlite3 pandas numpy
```

## CLI Commands

### Single Day Validation
```bash
# Validate a specific trading day
python run_baseline_validation.py --date 2025-09-11 --db validation.db

# With custom output directory
python run_baseline_validation.py --date 2025-09-11 --db validation.db --output results/
```

### 10-Day Validation Window
```bash
# Standard 10-day validation per universe_03.txt
python run_baseline_validation.py --start 2025-08-29 --end 2025-09-12 --db validation.db

# Custom date range
python run_baseline_validation.py --start 2025-09-01 --end 2025-09-15 --db validation.db
```

### Individual Component Testing
```bash
# Test Alpaca gap baseline only
python scripts/alpaca_gap_baseline.py --date 2025-09-11 --min_gap 50.0 --output baseline_gaps.csv

# Test Polygon spike baseline only
python scripts/polygon_spike_baseline.py --date 2025-09-11 --min_spike 50.0 --output baseline_spikes.csv

# Test comparison only (requires existing primary and baseline data)
python scripts/compare_baselines.py --date 2025-09-11 --db-path validation.db --baseline-gap baseline_gaps.csv --baseline-spike baseline_spikes.csv --output-dir comparison/
```

## Validation Process

### Step 1: Primary Discovery
1. **Universe Pinning**: Get deterministic daily symbol list from ThetaData v3
2. **Pass-1 Market Sweep**: Polygon grouped daily for entire market (R2/R3 candidates)
3. **Pass-2 Premarket Scan**: ThetaData v3 premarket OHLC for R1 gaps
4. **Enhanced Audit**: Rule of three statistical validation (n≥300 for 1% miss rate bound)

### Step 2: Baseline Scanners
1. **Alpaca Gap Scanner**: Detects R2 opening gaps ≥50% using Alpaca bars API
2. **Polygon Spike Scanner**: Detects R3 intraday pushes ≥50% using Polygon grouped daily

### Step 3: Baseline Comparison
1. **Set Intersection**: Find overlaps, primary-only, and baseline-only hits
2. **Coverage Analysis**: Calculate coverage rates and identify potential misses
3. **Pass/Fail Determination**: Exit code 1 if any baseline-only hits detected

## Database Schema

### Enhanced Tables
```sql
-- Baseline hits from simple scanners
CREATE TABLE baseline_hits (
    baseline_id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rule TEXT NOT NULL,            -- 'R2' or 'R3'
    pct_value REAL,
    source TEXT NOT NULL,          -- 'alpaca_baseline' or 'polygon_baseline'
    volume INTEGER,
    prev_close REAL,
    open_price REAL,
    high REAL
);

-- Baseline comparison results
CREATE TABLE diffs (
    diff_id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,
    rule TEXT NOT NULL,
    primary_only_count INTEGER,
    baseline_only_count INTEGER,   -- KEY METRIC: Should be 0
    overlap_count INTEGER,
    coverage_rate REAL
);

-- Enhanced audit with rule of three
CREATE TABLE enhanced_audit_log (
    audit_id INTEGER PRIMARY KEY,
    date TEXT NOT NULL,
    required_sample_size INTEGER,  -- n≥300 for 1% bound
    actual_sample_size INTEGER,
    observed_misses INTEGER,       -- Should be 0
    miss_rate_bound REAL,          -- ≤1% target
    audit_passed INTEGER
);
```

## Success Criteria

### Per Day
- ✅ **R2 or R3 candidates found**: At least one gap or spike event detected
- ✅ **Enhanced audit passes**: Miss rate bound ≤1% with 95% confidence
- ✅ **Baseline comparison passes**: Zero baseline-only hits detected
- ✅ **No processing errors**: All API calls and calculations succeed

### Per 10-Day Window
- ✅ **All days pass individual criteria**
- ✅ **Aggregate coverage rate ≥95%**: Overall baseline coverage acceptable
- ✅ **Statistical confidence maintained**: Audit bounds consistently met

## Troubleshooting

### Common Issues

#### 1. Baseline-Only Hits Detected
```
WARNING: 3 baseline-only hits detected!
Symbols with potential misses: ['ABCD', 'EFGH', 'IJKL']
```

**Diagnosis**: Primary discovery may have missed legitimate events.

**Resolution**:
1. Check candidate prefiltering logic in Pass-1
2. Verify ThetaData v3 premarket OHLC responses
3. Review prev_close retrieval for affected symbols
4. Consider widening Pass-1 prefilter (e.g., `high/prev_close >= 1.1`)

#### 2. Enhanced Audit Failure
```
AUDIT FAILURE - Sample size insufficient for target miss rate bound
Miss rate bound: 3.2% (target: 1.0%)
```

**Diagnosis**: Sample size too small for statistical guarantee.

**Resolution**:
1. Increase `target_miss_rate` parameter (e.g., 0.02 for 2%)
2. Use larger sample size if exchange roster permits
3. Check if exchange roster is unusually small for the date

#### 3. ThetaData API Errors
```
ERROR: ThetaData v3 endpoint failed: 429 OS_LIMIT
```

**Diagnosis**: Exceeded concurrency limits or rate limits.

**Resolution**:
1. Reduce `THETA_MAX` concurrency in run_zero_miss.py
2. Add backoff delays between requests
3. Check ThetaData terminal status with `scripts/probe_theta.py`

#### 4. Empty Baseline Results
```
Found 0 baseline events
```

**Diagnosis**: Baseline scanners found no qualifying events.

**Resolution**:
1. Verify date is a trading day (not holiday/weekend)
2. Check API credentials and data availability
3. Lower thresholds temporarily for testing (e.g., 30% instead of 50%)
4. Verify market had sufficient volatility on the date

### Debugging Commands

```bash
# Check database contents
sqlite3 validation.db "SELECT COUNT(*) FROM discovery_hits WHERE event_date='2025-09-11';"
sqlite3 validation.db "SELECT COUNT(*) FROM baseline_hits WHERE date='2025-09-11';"
sqlite3 validation.db "SELECT * FROM diffs WHERE date='2025-09-11';"

# Verify ThetaData health
python scripts/probe_theta.py

# Test individual components
python scripts/alpaca_gap_baseline.py --date 2025-09-11 --min_gap 30.0 --output test_gaps.csv
python scripts/polygon_spike_baseline.py --date 2025-09-11 --min_spike 30.0 --output test_spikes.csv
```

## Performance Expectations

### API Call Volumes (per day)
- **ThetaData v3**: ~500-1000 calls (candidates + audit sample)
- **Polygon**: 1 grouped daily call + 1 splits call per symbol
- **Alpaca**: ~100-500 calls (NASDAQ subset for baseline)

### Processing Times
- **Single day validation**: 5-15 minutes
- **10-day validation**: 1-3 hours
- **Database operations**: <1 minute per day

### Resource Usage
- **Memory**: ~100-500 MB peak
- **Disk**: ~10-50 MB per day (SQLite + CSV files)
- **Network**: ~10-100 MB per day (API responses)

## Configuration Tuning

### Rule of Three Parameters
```python
# For 1% miss rate bound with 95% confidence
target_miss_rate = 0.01  # requires n≥300

# For 0.5% miss rate bound with 95% confidence
target_miss_rate = 0.005  # requires n≥600

# For 99% confidence level
confidence = 0.99  # increases required sample size
```

### Baseline Scanner Thresholds
```python
# Standard production settings
min_gap = 50.0      # 50% opening gap for R2
min_spike = 50.0    # 50% intraday push for R3
min_volume = 1_000_000  # 1M volume filter

# Sensitive testing settings
min_gap = 30.0      # Lower threshold for testing
min_volume = 500_000  # Lower volume for small-cap inclusion
```

### Concurrency Limits
```python
# Conservative (for stability)
THETA_MAX = 2      # 2 concurrent ThetaData requests

# Aggressive (for speed)
THETA_MAX = 4      # 4 concurrent ThetaData requests (max for Standard tier)
```

## Next Steps

After successful baseline validation:

1. **Proceed to Phase B**: 3-year backfill with zero-miss guarantees
2. **Production deployment**: Wire baseline comparator into daily runs
3. **Monitoring setup**: Alert on baseline-only hits or audit failures
4. **Performance optimization**: Optimize for 3-year processing efficiency

## Support

For issues or questions:
1. Check this runbook first
2. Review universe_04.txt PRD
3. Examine validation logs and database contents
4. Test individual components in isolation