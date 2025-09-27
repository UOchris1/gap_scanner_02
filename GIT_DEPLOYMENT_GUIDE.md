# Git Deployment Guide - Gap Scanner

## Overview
This guide ensures seamless deployment from the first machine to your second machine using Git. The project has been optimized for clean Git operations and zero-issue cloning.

## ✅ Pre-Push Checklist (First Machine)

### 1. Verify Clean State
```bash
# Run validation to ensure everything is ready
python validate_production.py

# Check current status
python claude_wrapper.py
```

### 2. Git Repository Setup
```bash
# Initialize repository (if not already done)
git init

# Add all cleaned files
git add .

# Commit with descriptive message
git commit -m "Production-ready gap scanner with ThetaData v3 integration

- Complete ThetaData v3 implementation with CSV parsing
- Semaphore-based concurrency control (4 requests)
- Persistent Claude Code rule system
- ASCII-only enforcement throughout
- Comprehensive validation and deployment automation
- Archive system for development artifacts
- One-click deployment for second machine

Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>"

# Add remote repository (replace with your repository URL)
git remote add origin https://github.com/yourusername/gap_scanner_01.git

# Push to repository
git push -u origin master
```

## 🚀 Second Machine Deployment

### 1. Clone Repository
```bash
# Clone to desired location
git clone https://github.com/yourusername/gap_scanner_01.git
cd gap_scanner_01
```

### 2. One-Click Setup
```bash
# Run the automated deployment script
deploy.bat
```

### 3. Manual Setup (if needed)
```bash
# Activate conda environment (if using)
conda activate stat_project

# Install dependencies
pip install -r requirements.txt

# Validate installation
python validate_production.py

# Test ThetaData integration
python test_theta_v3.py

# Start development environment
start_claude_code.bat
```

## 📋 What Gets Deployed

### Core Production Files
- `run_discovery_compare.py` - Main gap scanner
- `test_theta_v3.py` - ThetaData v3 integration tests
- `optimize_theta_config.py` - Terminal configuration optimizer
- `claude_wrapper.py` - Context injection system
- `validate_production.py` - Health check system
- `symbols.txt` - Target symbols for scanning

### Configuration & Rules
- `.claude_rules` - Persistent development rules
- `docs/MASTER_PRD.md` - Project requirements
- `project_state/` - State tracking system
- `.vscode/` - IDE configuration
- `.env.template` - API configuration template

### Deployment Tools
- `deploy.bat` - One-click setup script
- `start_claude_code.bat` - Development environment launcher
- `requirements.txt` - Dependency specification
- `.gitignore` - Git ignore rules

### Development Archives
- `archive/development_tools/` - Historical development scripts
- `archive/testing_data/` - Test databases and artifacts

## 🔧 Environment Configuration

### Required API Keys (.env file)
```bash
# Edit .env file with your API keys
POLYGON_API_KEY=your_polygon_api_key_here
FMP_API_KEY=your_fmp_api_key_here
# ThetaData uses local terminal - no key needed
```

### ThetaData Terminal Setup
1. Install ThetaData Terminal on second machine
2. Ensure it's running on default ports:
   - v3: port 25503
   - v1.8.6: port 25510 (fallback)
3. Run terminal config optimizer: `python optimize_theta_config.py`
4. Restart ThetaData Terminal after optimization

## ✅ Validation Checklist

### After Clone
- [ ] Repository cloned successfully
- [ ] All core files present
- [ ] `deploy.bat` executed without errors
- [ ] `python validate_production.py` passes
- [ ] Dependencies installed correctly

### Before Production Use
- [ ] `.env` file configured with API keys
- [ ] ThetaData Terminal running and optimized
- [ ] `python test_theta_v3.py` passes
- [ ] Context injection working: `python claude_wrapper.py`
- [ ] VS Code opens correctly: `start_claude_code.bat`

## 🛡️ Quality Guarantees

### ASCII Compliance
- All Python files validated as ASCII-only
- Pre-commit hooks prevent unicode commits
- VS Code configured to highlight unicode characters

### Performance
- ThetaData v3 integration: 0.86s per symbol-day
- Semaphore-controlled concurrency: 4 requests max
- Optimized terminal configuration included

### Development Workflow
- Persistent rule injection on every Claude Code session
- State tracking across development sessions
- Automated context loading and PRD reference

## 🚨 Troubleshooting

### Common Issues
1. **ThetaData connection fails**
   - Ensure ThetaData Terminal is running
   - Check ports 25503 (v3) and 25510 (v1.8.6)
   - Run `python optimize_theta_config.py`

2. **Dependency installation fails**
   - Activate conda environment: `conda activate stat_project`
   - Update pip: `python -m pip install --upgrade pip`
   - Install manually: `pip install pandas requests tqdm toml`

3. **Unicode errors**
   - Run ASCII validation: `python validate_production.py`
   - Check VS Code settings for unicode highlighting
   - Use context injection: `python claude_wrapper.py`

### Support Commands
```bash
# Full validation
python validate_production.py

# Context injection
python claude_wrapper.py

# ThetaData testing
python test_theta_v3.py

# Project state check
type project_state\current_state.json
```

---

**This deployment system ensures zero-issue setup on your second machine with full preservation of the optimized development environment.**