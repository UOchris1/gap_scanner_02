#!/usr/bin/env python3
# -*- coding: ascii -*-
# Validate repository file structure compliance

import os
import sys
import glob

def check_root_python_files():
    """Check for prohibited Python files in root directory"""
    prohibited_patterns = [
        'test_*.py',
        'demo_*.py',
        'PROBLEM_SCRIPT_*.py'
    ]

    errors = []

    for pattern in prohibited_patterns:
        matches = glob.glob(pattern)
        if matches:
            errors.extend([f"Prohibited file in root: {f}" for f in matches])

    # Check for any Python files in root except allowed ones
    allowed_root_files = [
        'run_zero_miss_phase_b.py',
        'enhanced_db_schema.py'
    ]

    root_python_files = glob.glob('*.py')
    for py_file in root_python_files:
        if py_file not in allowed_root_files:
            errors.append(f"Unauthorized Python file in root: {py_file}")

    return errors

def check_data_artifacts():
    """Check for data artifacts that shouldn't be committed"""
    prohibited_extensions = ['*.db', '*.sqlite', '*.csv', '*.parquet', '*.xlsx', '*.zip', '*.jar']
    errors = []

    for pattern in prohibited_extensions:
        matches = glob.glob(pattern)
        if matches:
            errors.extend([f"Data artifact in root: {f}" for f in matches])

    return errors

def check_required_directories():
    """Check that required directories exist with proper structure"""
    required_dirs = [
        'src',
        'src/core',
        'src/providers',
        'src/pipelines',
        'src/integration',
        'scripts',
        'project_state',
        '.claude',
        '.claude/commands',
        '.claude/hooks'
    ]

    missing_dirs = []

    for directory in required_dirs:
        if not os.path.exists(directory):
            missing_dirs.append(f"Missing required directory: {directory}")

    return missing_dirs

def check_gitignore_compliance():
    """Check that .gitignore properly blocks artifacts"""
    if not os.path.exists('.gitignore'):
        return ["Missing .gitignore file"]

    with open('.gitignore', 'r') as f:
        gitignore_content = f.read()

    required_patterns = [
        '*.db',
        '*.csv',
        '*.jar',
        'attic/',
        'out/',
        'reports/',
        'universe/',
        'logs/'
    ]

    missing_patterns = []

    for pattern in required_patterns:
        if pattern not in gitignore_content:
            missing_patterns.append(f"Missing .gitignore pattern: {pattern}")

    return missing_patterns

def check_src_package_structure():
    """Validate src/ package structure"""
    errors = []

    # Check for __init__.py files
    required_init_files = [
        'src/__init__.py',
        'src/core/__init__.py',
        'src/providers/__init__.py',
        'src/pipelines/__init__.py',
        'src/integration/__init__.py'
    ]

    for init_file in required_init_files:
        if not os.path.exists(init_file):
            errors.append(f"Missing package __init__.py: {init_file}")

    # Check for required core files
    required_files = [
        'src/core/rules.py',
        'src/core/db.py',
        'src/providers/theta_provider.py',
        'src/providers/polygon_provider.py',
        'src/pipelines/zero_miss.py',
        'src/integration/cli_bridge.py'
    ]

    for required_file in required_files:
        if not os.path.exists(required_file):
            errors.append(f"Missing required source file: {required_file}")

    return errors

def main():
    """Run all structure validation checks"""
    print("Validating repository structure...")

    all_errors = []

    # Run all checks
    all_errors.extend(check_root_python_files())
    all_errors.extend(check_data_artifacts())
    all_errors.extend(check_required_directories())
    all_errors.extend(check_gitignore_compliance())
    all_errors.extend(check_src_package_structure())

    if all_errors:
        print("\nRepository structure validation FAILED:")
        for error in all_errors:
            print(f"  {error}")
        return 1
    else:
        print("Repository structure validation PASSED")
        return 0

if __name__ == "__main__":
    sys.exit(main())