# Gap Scanner 02

Single-entry gap scanner with one CLI, one pipeline, and an optional thin Streamlit UI. Clean, portable, and driven by `.env`.

## Security

- Never commit `.env` or real keys
- Use environment variables for secrets
- `scripts/env_tools.py` provides `env-format` and `env-validate`

## Quick Start

Prereqs: Python 3.10, Git, optional ThetaData Terminal (for R1 premarket)

```bash
git clone <repository-url>
cd gap_scanner_02
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

copy .env.example .env   # then edit with your keys
python -m scripts.gapctl env-format
python -m scripts.gapctl env-validate
python -m scripts.gapctl health

# run a day or range
python -m scripts.gapctl scan-day --date 2025-09-26 --db db/scanner.db
python -m scripts.gapctl scan-range --start 2025-09-22 --end 2025-09-26 --db db/scanner.db

# export
python -m scripts.gapctl export --start 2025-09-22 --end 2025-09-26 --db db/scanner.db --out exports

# acceptance (Theta Terminal recommended)
python -m scripts.gapctl validate --date 2025-09-26 --db db/acceptance.db
```

## Streamlit (optional)

```bash
streamlit run app/scan_ui.py
```

## Entrypoints

- CLI: `scripts/gapctl.py`
- Shim: `src/integration/cli_bridge.py`
- Pipeline: `src/pipelines/zero_miss.py`
- Providers: `src/providers/*`
- Exports: `scripts/export_reports.py`

## Environment (.env)

- `POLYGON_API_KEY` (required)
- `FMP_API_KEY` (optional)
- `THETA_V3_URL` / `THETA_V1_URL` (optional, defaults to localhost)

See `.env.example` for a template.

## ???? API Providers

### Polygon.io
- **Purpose**: Primary market data source
- **Endpoints**: Grouped daily, splits, tickers
- **Rate Limits**: 5 requests/minute (free tier)
- **Documentation**: [polygon.io/docs](https://polygon.io/docs)

### Alpaca
- **Purpose**: Baseline validation and paper trading
- **Endpoints**: Assets, bars, historical data
- **Rate Limits**: 200 requests/minute
- **Documentation**: [alpaca.markets/docs](https://alpaca.markets/docs)

### Financial Modeling Prep (FMP)
- **Purpose**: Fundamental data and delisted companies
- **Endpoints**: Historical data, company profiles
- **Rate Limits**: 300 requests/day (free tier)
- **Documentation**: [financialmodelingprep.com/api](https://financialmodelingprep.com/developer/docs)

### ThetaData
- **Purpose**: Real-time market data
- **Endpoints**: Options, equities, historical data
- **Rate Limits**: Local terminal (no API limits)
- **Documentation**: [thetadata.net/docs](https://http-docs.thetadata.net/)

## ??????? Development

### Code Structure

- **Modular Design**: Separate providers, pipelines, and core functionality
- **Environment Variables**: All configuration through `.env`
- **Error Handling**: Comprehensive logging and error recovery
- **Testing**: Validation scripts and baseline comparisons

### Adding New Providers

1. Create provider class in `src/providers/`
2. Implement standard interface methods
3. Add environment variables to `.env.example`
4. Update configuration documentation

### Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Make changes with proper security practices
4. Test thoroughly with validation scripts
5. Submit pull request

## ???? Troubleshooting

### Common Issues

| Problem | Solution |
|---------|----------|
| **"API key not found"** | Check `.env` file exists and has correct keys |
| **"ThetaData connection failed"** | Ensure ThetaData Terminal is running |
| **"Database locked"** | Close other processes using the database |
| **"Rate limit exceeded"** | Wait and retry, check API usage |

### Debug Commands

```bash
# Check environment variables
python -c "import os; print('POLYGON_API_KEY' in os.environ)"

# Test API connections
python scripts/validate_splits_and_providers.py

# Check database schema
python -c "from src.core.db import *; check_db_schema('db/test.db')"

# Validate ThetaData connection
python -c "from src.providers.theta_provider import *; test_connection()"
```

### Log Analysis

```bash
# View recent logs
tail -f logs/gap_scanner.log

# Search for errors
grep "ERROR" logs/gap_scanner.log

# Monitor API calls
grep "API" logs/gap_scanner.log
```

## ???? Performance Monitoring

### Key Metrics

- **Gap Detection Rate**: Percentage of gaps found vs expected
- **API Response Time**: Average response time per provider
- **Data Quality Score**: Completeness and accuracy metrics
- **System Uptime**: Pipeline reliability metrics

### Performance Reports

```bash
# Generate performance report
python scripts/export_reports.py --performance

# Database optimization
python production_db_optimization.py

# System health check
python validate_production.py
```

## ???? License

This project is for educational and research purposes. Ensure compliance with all API provider terms of service.

## ???? Support

- **Documentation**: See inline code comments and docstrings
- **Issues**: Use the issue tracker for bug reports
- **API Support**: Contact respective API providers for API-specific issues

---

**?????? SECURITY REMINDER**: Always keep your API keys secure and never commit them to version control. Regularly rotate keys and monitor usage for security.

## Quick Start (Frictionless Clone)

Prefer the single CLI and .env-only setup for a clean run on any machine:

1) Copy .env.example -> .env and add POLYGON_API_KEY (and optionally FMP_API_KEY).
2) Install deps: pip install -r requirements.txt
3) Health: python scripts/gapctl.py health
4) Acceptance (known green day): python scripts/gapctl.py validate --date 2025-09-12 --db db/acceptance.db
5) Enrich a range: python scripts/gapctl.py scan-range --start 2025-06-09 --end 2025-06-13 --db db/scanner.db
6) Export CSVs: python scripts/gapctl.py export --start 2025-06-09 --end 2025-06-13 --db db/scanner.db --out exports

The single entrypoint is scripts/gapctl.py; legacy root-level runners are not required.

