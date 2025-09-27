# -*- coding: ascii -*-
# Check behavioral control rules
"""
Behavioral Control System Validation
"""
from pathlib import Path
from datetime import datetime
import json

print("=" * 60)
print("BEHAVIORAL CONTROL SYSTEM VALIDATION")
print("=" * 60)

RULES = """
1. FILE ORGANIZATION RULES:
   - NEVER create test_*.py in root directory
   - ALL test outputs go to project_state/artifacts/
   - ALL validation scripts go to scripts/ folder
   - NO duplicate functionality files
   - If file exists, FIX IT, don't create new one

2. TROUBLESHOOTING RULES:
   - If error occurs: DEBUG the existing script
   - DO NOT create workaround script
   - DO NOT switch to fake/mock data
   - FIX the actual problem
   - Maximum 3 attempts before requesting help

3. HELP REQUEST PROTOCOL:
   - After 3 failed attempts, STOP
   - Create: project_state/HELP_REQUEST.md
   - Include: Error details, attempts made, specific question
   - Print: "EXTERNAL HELP NEEDED - See HELP_REQUEST.md"
   - WAIT for response before continuing

4. TASKMASTER MCP AI RULE:
   - USE TodoWrite tool for ALL tasks and subtasks
   - CONSTANTLY refer to and update task list
   - NEVER work without active task tracking
   - Mark tasks in_progress BEFORE starting work
   - Mark tasks completed IMMEDIATELY after finishing
   - Break complex work into tracked subtasks
"""

print(RULES)

# CHECK: Required files exist
required_files = [
    'scripts/create_help_request.py',
    'project_state/attempt_counter.json',
    'config/behavioral_control.yaml',
    'project_state/artifacts/.gitkeep'
]

print("\nFILE VALIDATION:")
for file_path in required_files:
    if Path(file_path).exists():
        print(f"[OK] {file_path}")
    else:
        print(f"[MISSING] {file_path}")

# CHECK: Attempt counter
attempt_file = Path('project_state/attempt_counter.json')
if attempt_file.exists():
    try:
        attempts = json.loads(attempt_file.read_text())
        print(f"\nAttempt counter: {attempts.get('count', 0)}")
        if attempts.get('count', 0) >= 3:
            print("\n" + "!" * 60)
            print("STOP: 3 ATTEMPTS REACHED - CREATE HELP REQUEST")
            print("!" * 60)
    except Exception as e:
        print(f"\nWarning: Could not read attempt counter: {e}")

print("\nREMINDER: Fix problems in place, don't create new files")
print("TASKMASTER REMINDER: Use TodoWrite for ALL task tracking")
print("=" * 60)