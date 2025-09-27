# Gap Scanner Project

A comprehensive market gap detection and analysis system that identifies and validates stock price gaps using multiple data sources and advanced filtering rules.

## ???? SECURITY FIRST

**CRITICAL SECURITY REQUIREMENTS:**
- NEVER commit `.env` files to version control
- NEVER share API keys in code, comments, or documentation
- ALWAYS use environment variables for sensitive configuration
- Regularly rotate your API keys
- Monitor API usage for unusual activity

## ???? Table of Contents

- [Overview](#overview)
- [Security Setup](#security-setup)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [API Providers](#api-providers)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## ???? Overview

The Gap Scanner Project is a sophisticated market analysis tool that:

- **Detects Price Gaps**: Identifies significant price gaps (???50%) in stock market data
- **Multi-Source Validation**: Uses Polygon.io, ThetaData, Alpaca, and FMP for comprehensive coverage
- **Advanced Filtering**: Applies volume, market cap, and split-detection rules
- **Zero-Miss Pipeline**: Ensures comprehensive gap detection with minimal false negatives
- **Performance Monitoring**: Tracks system performance and data quality metrics

### Key Features

- Real-time gap detection and historical analysis
- Multi-provider data aggregation and validation
- Comprehensive split detection and adjustment
- Performance benchmarking and validation
- Automated reporting and export capabilities

## ???? Security Setup

### 1. Environment Configuration

1. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your actual API keys:**
   ```bash
   # NEVER use these placeholder values in production
   POLYGON_API_KEY="your_actual_polygon_key_here"
   ALPACA_API_KEY="your_actual_alpaca_key_here"
   ALPACA_SECRET_KEY="your_actual_alpaca_secret_here"
   FMP_API_KEY="your_actual_fmp_key_here"
   ```

3. **Verify `.env` is gitignored:**
   ```bash
   git check-ignore .env
   # Should return: .env
   ```

### 2. API Key Security Guidelines

| Provider | Security Level | Notes |
|----------|---------------|-------|
| **Polygon.io** | HIGH | Rate limited, read-only recommended |
| **Alpaca** | CRITICAL | Use paper trading keys for development |
| **ThetaData** | MEDIUM | Local terminal required |
| **FMP** | MEDIUM | Rate limited, monitor usage |

### 3. Security Checklist

- [ ] `.env` file is never committed to git
- [ ] All API keys use environment variables
- [ ] Production vs development keys are separated
- [ ] API rate limits are configured
- [ ] Key rotation schedule is established
- [ ] Access logging is enabled

## ???? Installation

### Prerequisites

- Python 3.8+ (3.10+ recommended)
- ThetaData Terminal (for real-time data)
- Git

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd gap_scanner_01
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\\Scripts\\activate
# macOS/Linux:
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your API keys (NEVER commit this file)
# See Configuration section for details
```

### Step 5: Verify Installation

```bash
# Run quick baseline test
python quick_baseline_test.py

# Run system validation
python run_baseline_validation.py
```

## ?????? Configuration

### Required API Keys

| Provider | Purpose | Get Key From | Required |
|----------|---------|--------------|----------|
| **Polygon.io** | Market data | [polygon.io](https://polygon.io/) | ??? Yes |
| **Alpaca** | Baseline validation | [alpaca.markets](https://alpaca.markets/) | ??? Yes |
| **FMP** | Fundamental data | [financialmodelingprep.com](https://financialmodelingprep.com/) | ??? Yes |
| **ThetaData** | Real-time data | Local terminal | ?????? Optional |

### Environment Variables

Edit your `.env` file with these required settings:

```bash
# Core API Keys (REQUIRED)
POLYGON_API_KEY="your_polygon_key"
ALPACA_API_KEY="your_alpaca_key"
ALPACA_SECRET_KEY="your_alpaca_secret"
FMP_API_KEY="your_fmp_key"

# ThetaData Configuration (OPTIONAL)
THETA_V3_URL=http://127.0.0.1:25503
THETA_V1_URL=http://127.0.0.1:25510

# Application Settings
LOG_LEVEL=INFO
OUTPUT_DIR=data/outputs
```

### ThetaData Terminal Setup

1. Download ThetaData Terminal from [thetadata.net](https://www.thetadata.net/)
2. Install and run the terminal application
3. Verify connection: Terminal should be accessible at `localhost:25503`

## ???? Usage

### Quick Start

```bash
# 1. Run baseline validation
python run_baseline_validation.py

# 2. Run 10-day validation
python run_10day_validation.py

# 3. Run zero-miss pipeline
python run_zero_miss.py --date 2024-01-15

# 4. Generate reports
python scripts/export_reports.py
```

### Main Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `run_zero_miss.py` | Core gap detection | `python run_zero_miss.py --date YYYY-MM-DD` |
| `run_baseline_validation.py` | System validation | `python run_baseline_validation.py` |
| `run_10day_validation.py` | Multi-day analysis | `python run_10day_validation.py` |
| `scripts/export_reports.py` | Generate reports | `python scripts/export_reports.py` |

### Advanced Usage

```bash
# Run with specific database
python run_zero_miss.py --date 2024-01-15 --db custom.db

# Export specific date range
python scripts/export_reports.py --start 2024-01-01 --end 2024-01-31

# Validate with custom thresholds
python run_baseline_validation.py --threshold 0.01
```

## ???? Project Structure

```
gap_scanner_01/
????????? ???? README.md                    # This file
????????? ???? .env.example                 # Environment template
????????? ???? .gitignore                   # Git ignore patterns
????????? ???? requirements.txt             # Python dependencies
?????????
????????? ???? Main Scripts
????????? run_zero_miss.py               # Core gap detection pipeline
????????? run_baseline_validation.py     # System validation
????????? run_10day_validation.py        # Multi-day analysis
?????????
????????? ???? src/                        # Source code
???   ????????? core/                      # Core functionality
???   ???   ????????? db.py                  # Database operations
???   ???   ????????? rules.py               # Gap detection rules
???   ???   ????????? database_operations.py # Enhanced DB ops
???   ????????? providers/                 # Data providers
???   ???   ????????? polygon_provider.py    # Polygon.io integration
???   ???   ????????? theta_provider.py      # ThetaData integration
???   ???   ????????? fundamentals_provider.py # FMP integration
???   ????????? pipelines/                 # Data pipelines
???       ????????? zero_miss.py           # Zero-miss pipeline
?????????
????????? ???? scripts/                    # Utility scripts
???   ????????? export_reports.py          # Report generation
???   ????????? alpaca_baseline.py         # Alpaca validation
???   ????????? validate_splits_and_providers.py # Data validation
?????????
????????? ???? db/                         # Database files
????????? ???? out/                        # Output files
????????? ???? logs/                       # Log files
```

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

