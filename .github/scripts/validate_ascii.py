#!/usr/bin/env python3
# -*- coding: ascii -*-
# Validate ASCII encoding across all Python files

import os
import sys
import glob

def check_ascii_file(file_path):
    """Check if file contains only ASCII characters"""
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            content = f.read()
        return True, "ASCII encoding verified"
    except UnicodeDecodeError as e:
        return False, f"Non-ASCII characters found: {e}"

def validate_ascii_header(file_path):
    """Check for ASCII encoding header in Python files"""
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            first_line = f.readline().strip()

        if first_line == "# -*- coding: ascii -*-":
            return True, "ASCII header present"
        else:
            return False, f"Missing or incorrect ASCII header: {first_line}"
    except Exception as e:
        return False, f"Header check error: {e}"

def main():
    """Validate ASCII encoding for all Python files"""
    python_files = []

    # Find all Python files in src/ and scripts/
    for directory in ['src', 'scripts']:
        if os.path.exists(directory):
            pattern = os.path.join(directory, '**', '*.py')
            python_files.extend(glob.glob(pattern, recursive=True))

    if not python_files:
        print("No Python files found to validate")
        return 0

    errors = []

    for file_path in python_files:
        print(f"Checking {file_path}...")

        # Check ASCII encoding
        ascii_ok, ascii_msg = check_ascii_file(file_path)
        if not ascii_ok:
            errors.append(f"{file_path}: {ascii_msg}")
            continue

        # Check ASCII header
        header_ok, header_msg = validate_ascii_header(file_path)
        if not header_ok:
            errors.append(f"{file_path}: {header_msg}")

    if errors:
        print("\nASCII validation FAILED:")
        for error in errors:
            print(f"  {error}")
        return 1
    else:
        print(f"\nASCII validation PASSED for {len(python_files)} files")
        return 0

if __name__ == "__main__":
    sys.exit(main())