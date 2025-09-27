# -*- coding: ascii -*-
"""
Creates standardized help request for external assistance
"""
from pathlib import Path
from datetime import datetime
import json

def create_help_request(error_type, error_details, attempts_made):
    """Generate help request for ChatGPT/external PM"""

    help_request = f"""# EXTERNAL HELP REQUEST
Generated: {datetime.now().isoformat()}
Project: gap_scanner_01
Requester: Claude Code

## PROBLEM SUMMARY
Error Type: {error_type}
Failed After: {len(attempts_made)} attempts

## ERROR DETAILS
```
{error_details}
```

## ATTEMPTS MADE
{chr(10).join([f"{i+1}. {attempt}" for i, attempt in enumerate(attempts_made)])}

## SPECIFIC QUESTIONS
1. [Add specific question here]
2. [Add what needs clarification]

## CONTEXT FILES
- Main script: run_discovery_compare.py
- Error location: [line/function]
- Related config: config/default.yaml

## PROJECT STATE
Check: project_state/current_state.json
Logs: project_state/artifacts/

---
TO RESPOND: Edit this file with solution and save as HELP_RESPONSE.md
"""

    # Write request
    request_path = Path('project_state/HELP_REQUEST.md')
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(help_request, encoding='ascii')

    # Update attempt counter
    counter_path = Path('project_state/attempt_counter.json')
    counter_path.parent.mkdir(parents=True, exist_ok=True)
    counter = {'count': len(attempts_made), 'timestamp': datetime.now().isoformat()}
    counter_path.write_text(json.dumps(counter, indent=2), encoding='ascii')

    print("=" * 60)
    print("HELP REQUEST CREATED")
    print("=" * 60)
    print(f"File: {request_path}")
    print("Waiting for external assistance...")
    print("=" * 60)

    return str(request_path)