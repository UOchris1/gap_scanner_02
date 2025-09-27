# Production Runbook - Zero-Miss Gap Scanner

## Quick Reference

### Emergency Contacts
- **System Owner**: Gap Scanner Production Team
- **Database**: SQLite production_3year.db
- **Monitoring**: Real-time dashboard at monitoring_config.json

### Critical Commands
```bash
# Production deployment
python deploy_production_system.py --environment production

# Single day validation
python run_baseline_validation.py --date 2025-09-16 --db db/production.db

# 10-day validation window
python run_baseline_validation.py --start 2025-08-29 --end 2025-09-12 --db db/validation.db

# 3-year backfill
python run_zero_miss_phase_b.py scan --db db/production_3year.db --resume

# System health check
python production_monitoring_alerting.py --health-check

# Performance optimization
python production_db_optimization.py --db db/production_3year.db --optimize
```

## System Architecture

### Core Components
1. **Discovery Engine**: `run_baseline_validation.py` (Phase A) + `run_zero_miss_phase_b.py` (Phase B)
2. **Database Layer**: Enhanced SQLite with production indexes
3. **Monitoring System**: Real-time alerts and SLA tracking
4. **Export System**: Monthly CSV generation and 3-year reports
5. **Quality Gates**: Baseline cross-validation + statistical audits

### Data Flow
```
Polygon API → Primary Discovery → Baseline Comparison → Audit Validation → Export Reports
     ↓              ↓                    ↓                   ↓               ↓
  Universe     R1/R2/R3/R4         Overlap Analysis    Rule of Three    Monthly CSV
  Building    Event Detection      Zero Tolerance      95% Confidence   Roll-up Reports
```

## Operating Procedures

### Daily Operations

#### 1. Single Day Processing
```bash
# Process yesterday's data
python run_baseline_validation.py --date $(date -d "yesterday" +%Y-%m-%d) --db db/production.db

# Check results
sqlite3 db/production.db "SELECT * FROM diffs WHERE baseline_only_count > 0;"

# Expected result: No rows (zero baseline-only hits)
```

#### 2. Health Monitoring
```bash
# Check system health
python -c "
from production_monitoring_alerting import ProductionMonitor
monitor = ProductionMonitor('db/production.db')
health = monitor.health_checker.run_comprehensive_health_check()
print(f'Overall Health: {health[\"overall_health\"]}')
"

# Check SLA compliance
python -c "
from production_monitoring_alerting import SLATracker
tracker = SLATracker('db/production.db')
sla = tracker.check_current_sla_status()
print(f'SLA Compliance: {sla[\"overall_sla_compliance\"]}')
"
```

### Weekly Operations

#### 1. Performance Review
```bash
# Generate performance trends
python reporting_export_system.py --weekly-report

# Check processing times
sqlite3 db/production.db "
SELECT date, processing_time_seconds/60.0 as minutes
FROM qc_daily_summary
WHERE date >= date('now', '-7 days')
ORDER BY date;
"
```

#### 2. Database Maintenance
```bash
# Optimize database
python production_db_optimization.py --db db/production.db --analyze

# Backup database
cp db/production.db backups/production_$(date +%Y%m%d).db
```

### Monthly Operations

#### 1. CSV Export Generation
```bash
# Generate monthly exports
python -c "
from reporting_export_system import ReportingSystem
reporting = ReportingSystem('db/production.db')
result = reporting.generate_monthly_csv_exports(2025, 9)
print(f'Exports generated: {len(result[\"export_files\"])}')
"
```

#### 2. Performance Analysis
```bash
# Monthly statistics
sqlite3 db/production.db "
SELECT
  strftime('%Y-%m', date) as month,
  COUNT(*) as trading_days,
  SUM(r1_hits + r2_hits + r3_hits + r4_hits) as total_discoveries,
  AVG(processing_time_seconds)/60.0 as avg_minutes,
  SUM(CASE WHEN baseline_only_count = 0 THEN 1 ELSE 0 END) as perfect_days
FROM qc_daily_summary
WHERE date >= date('now', '-30 days')
GROUP BY strftime('%Y-%m', date);
"
```

## Troubleshooting Guide

### Critical Issues

#### 🚨 CRITICAL: Baseline-Only Hits Detected
**Symptom**: `baseline_only_count > 0` in diffs table
**Impact**: Potential missed discoveries
**Action**:
1. **IMMEDIATE**: Stop all processing
2. Investigate baseline-only symbols:
```bash
sqlite3 db/production.db "
SELECT symbol, date, event_type, pct_value
FROM baseline_hits bh
WHERE NOT EXISTS (
  SELECT 1 FROM discovery_hits dh
  WHERE dh.ticker = bh.symbol AND dh.event_date = bh.date
);
"
```
3. Review discovery rules and thresholds
4. Re-run with wider prefilter if needed
5. Root cause analysis before resuming

#### 🚨 CRITICAL: Statistical Audit Failures
**Symptom**: `audit_passed = 0` in enhanced_audit_log
**Impact**: Statistical validation compromised
**Action**:
1. Check audit sample size: `SELECT sample_size FROM enhanced_audit_log WHERE audit_passed = 0;`
2. Verify rule of three calculations
3. Review miss rate bounds
4. Increase sample size if needed
5. Re-audit before proceeding

### Warning Issues

#### ⚠️ WARNING: Processing Time SLA Breach
**Symptom**: Processing time > 15 minutes per day
**Impact**: Performance degradation
**Action**:
1. Check database indexes: `python production_db_optimization.py --analyze`
2. Review API concurrency settings
3. Monitor system resources
4. Consider parallel processing optimization

#### ⚠️ WARNING: API Errors
**Symptom**: HTTP errors or timeouts
**Impact**: Data quality issues
**Action**:
1. Check API keys and endpoints
2. Review rate limiting
3. Implement retry logic
4. Consider provider failover

### Common Issues

#### Database Locked
```bash
# Check for long-running connections
lsof db/production.db

# Force unlock (use with caution)
sqlite3 db/production.db ".backup backup.db"
mv backup.db db/production.db
```

#### Missing Environment Variables
```bash
# Required variables
export POLYGON_API_KEY="your_key_here"
export THETA_V3_URL="https://api.thetadata.net"

# Verify
python -c "import os; print('POLYGON_API_KEY:', bool(os.environ.get('POLYGON_API_KEY')))"
```

#### Disk Space Issues
```bash
# Check disk usage
df -h .

# Clean old backups (keep last 30 days)
find backups/ -name "*.db" -mtime +30 -delete

# Vacuum database
sqlite3 db/production.db "VACUUM;"
```

## Performance Monitoring

### Key Metrics
- **Processing Time**: ≤15 minutes per day (SLA)
- **Baseline Success Rate**: 100% (zero baseline-only hits)
- **Audit Success Rate**: 100% (all audits pass)
- **API Error Rate**: ≤5%

### Monitoring Commands
```bash
# Real-time monitoring
python production_monitoring_alerting.py --start-monitoring

# Dashboard data
python -c "
from production_monitoring_alerting import ProductionMonitor
monitor = ProductionMonitor('db/production.db')
dashboard = monitor.generate_monitoring_dashboard()
print('System Status:', dashboard['system_status']['overall_health'])
"

# Performance trends
sqlite3 db/production.db "
SELECT date,
       processing_time_seconds/60.0 as minutes,
       r1_hits + r2_hits + r3_hits + r4_hits as discoveries,
       baseline_only_count,
       audit_passed
FROM qc_daily_summary
ORDER BY date DESC
LIMIT 10;
"
```

## Disaster Recovery

### Database Backup
```bash
# Daily backup
sqlite3 db/production.db ".backup backups/production_$(date +%Y%m%d_%H%M%S).db"

# Restore from backup
cp backups/production_YYYYMMDD_HHMMSS.db db/production.db

# Verify integrity
sqlite3 db/production.db "PRAGMA integrity_check;"
```

### Configuration Recovery
```bash
# Backup configuration
tar -czf config_backup_$(date +%Y%m%d).tar.gz *.json *.py

# Critical files to backup:
# - monitoring_config.json
# - deployment_config_production.json
# - All Python scripts
```

### Emergency Contacts
1. Check system logs: `tail -f logs/production_gap_scanner.log`
2. Review monitoring alerts
3. Contact system administrator
4. Document incident in deployment log

## Quality Assurance

### Validation Checklist
- [ ] Zero baseline-only hits in last 7 days
- [ ] All statistical audits passing
- [ ] Processing times within SLA
- [ ] API error rates acceptable
- [ ] Database integrity verified
- [ ] Monitoring system active
- [ ] Recent backups available

### Monthly QA Report
```bash
# Generate QA metrics
sqlite3 db/production.db "
SELECT
  'Last 30 Days QA Report' as report,
  COUNT(*) as total_days,
  SUM(CASE WHEN baseline_only_count = 0 THEN 1 ELSE 0 END) as perfect_baseline_days,
  SUM(CASE WHEN audit_passed = 1 THEN 1 ELSE 0 END) as audit_pass_days,
  AVG(processing_time_seconds)/60.0 as avg_processing_minutes,
  SUM(r1_hits + r2_hits + r3_hits + r4_hits) as total_discoveries
FROM qc_daily_summary
WHERE date >= date('now', '-30 days');
"
```

---

## Emergency Procedures

### System Down
1. Check system health
2. Review recent logs
3. Restart monitoring
4. Validate database integrity
5. Resume processing from checkpoint

### Data Corruption
1. Stop all processing immediately
2. Restore from latest backup
3. Verify data integrity
4. Re-run affected date range
5. Update monitoring alerts

Remember: **Zero tolerance for baseline-only hits**. Any detection requires immediate investigation and resolution before continuing operations.