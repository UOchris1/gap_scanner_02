#!/usr/bin/env python3
"""
Security Audit Script for Gap Scanner Project
Performs comprehensive security checks before GitHub push
"""

import os
import re
import sys
import glob
from pathlib import Path
from typing import List, Dict, Any


class SecurityAuditor:
    """Comprehensive security auditing for the gap scanner project"""

    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root).resolve()
        self.issues = []
        self.warnings = []

    def log_issue(self, level: str, category: str, file_path: str, line: int, message: str):
        """Log a security issue"""
        self.issues.append({
            'level': level,
            'category': category,
            'file': str(file_path),
            'line': line,
            'message': message
        })

    def log_warning(self, category: str, message: str):
        """Log a security warning"""
        self.warnings.append({
            'category': category,
            'message': message
        })

    def scan_for_secrets(self) -> None:
        """Scan all Python files for potential secrets"""
        print("[SCAN] Scanning for hardcoded secrets...")

        secret_patterns = [
            (r'api_key\s*=\s*["\'][^"\']+["\']', 'Potential hardcoded API key'),
            (r'secret\s*=\s*["\'][^"\']+["\']', 'Potential hardcoded secret'),
            (r'password\s*=\s*["\'][^"\']+["\']', 'Potential hardcoded password'),
            (r'token\s*=\s*["\'][^"\']+["\']', 'Potential hardcoded token'),
            (r'key\s*=\s*["\'][A-Za-z0-9+/]{20,}["\']', 'Potential base64 encoded key'),
            (r'["\'][A-Za-z0-9]{32,}["\']', 'Potential long alphanumeric secret'),
            (r'sk-[A-Za-z0-9]{48}', 'OpenAI API key pattern'),
            (r'xox[baprs]-[A-Za-z0-9-]{10,}', 'Slack token pattern'),
            (r'AKIA[0-9A-Z]{16}', 'AWS access key pattern'),
        ]

        for py_file in self.project_root.rglob("*.py"):
            if "venv" in str(py_file) or "__pycache__" in str(py_file):
                continue

            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        # Skip environment variable usage (this is good)
                        if 'os.environ.get(' in line or 'os.getenv(' in line:
                            continue

                        # Skip obvious test/mock patterns
                        if any(test_pattern in line.upper() for test_pattern in [
                            'MOCK_', 'TEST_', '_FOR_TESTING', 'FAKE_', 'DUMMY_',
                            'PLACEHOLDER_', 'EXAMPLE_'
                        ]):
                            continue

                        for pattern, description in secret_patterns:
                            if re.search(pattern, line, re.IGNORECASE):
                                self.log_issue('HIGH', 'secrets', py_file, line_num,
                                             f"{description}: {line.strip()}")
            except Exception as e:
                self.log_warning('file_access', f"Could not read {py_file}: {e}")

    def check_env_files(self) -> None:
        """Check environment files for security issues"""
        print("[SCAN] Checking environment files...")

        # Check for .env files with real secrets
        env_files = list(self.project_root.glob(".env*"))
        for env_file in env_files:
            if env_file.name == ".env.example":
                continue

            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue

                        # Check for placeholder values (good)
                        if 'your_' in line.lower() and '_here' in line.lower():
                            continue

                        # Check for potentially real API keys
                        if '=' in line:
                            key, value = line.split('=', 1)
                            value = value.strip().strip('"\'')

                            # Flag suspiciously long or complex values
                            if len(value) > 20 and not value.startswith('http'):
                                if re.match(r'^[A-Za-z0-9+/=]{20,}$', value):
                                    self.log_issue('CRITICAL', 'env_secrets', env_file, line_num,
                                                 f"Potential real API key in .env: {key}")
            except Exception as e:
                self.log_warning('file_access', f"Could not read {env_file}: {e}")

    def check_gitignore(self) -> None:
        """Verify .gitignore is comprehensive"""
        print("[SCAN] Checking .gitignore configuration...")

        gitignore_path = self.project_root / ".gitignore"
        if not gitignore_path.exists():
            self.log_issue('HIGH', 'gitignore', gitignore_path, 0,
                         ".gitignore file is missing")
            return

        required_patterns = [
            '.env',
            '*.key',
            '*secret*',
            '*password*',
            '*credential*',
        ]

        try:
            with open(gitignore_path, 'r', encoding='utf-8') as f:
                gitignore_content = f.read()

            for pattern in required_patterns:
                if pattern not in gitignore_content:
                    self.log_issue('MEDIUM', 'gitignore', gitignore_path, 0,
                                 f"Missing gitignore pattern: {pattern}")
        except Exception as e:
            self.log_warning('file_access', f"Could not read .gitignore: {e}")

    def check_git_history(self) -> None:
        """Check if sensitive files might be in git history"""
        print("[SCAN] Checking git configuration...")

        # Check if .env is tracked
        try:
            import subprocess
            result = subprocess.run(['git', 'ls-files', '.env'],
                                  capture_output=True, text=True, cwd=self.project_root)
            if '.env' in result.stdout:
                self.log_issue('CRITICAL', 'git_tracking', '.env', 0,
                             ".env file is tracked by git - REMOVE IMMEDIATELY")
        except Exception:
            self.log_warning('git', "Could not check git status")

    def check_file_permissions(self) -> None:
        """Check file permissions for sensitive files"""
        print("[SCAN] Checking file permissions...")

        sensitive_files = ['.env', '.env.local', '.env.production']
        for filename in sensitive_files:
            file_path = self.project_root / filename
            if file_path.exists():
                try:
                    # On Unix systems, check if file is readable by others
                    if hasattr(os, 'stat'):
                        import stat
                        file_stat = file_path.stat()
                        if file_stat.st_mode & stat.S_IROTH:
                            self.log_issue('MEDIUM', 'permissions', file_path, 0,
                                         f"{filename} is readable by others")
                except Exception as e:
                    self.log_warning('permissions', f"Could not check permissions for {filename}: {e}")

    def check_requirements(self) -> None:
        """Check requirements.txt for security"""
        print("[SCAN] Checking requirements.txt...")

        req_file = self.project_root / "requirements.txt"
        if not req_file.exists():
            self.log_warning('dependencies', "requirements.txt is missing")
            return

        try:
            with open(req_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for python-dotenv
            if 'python-dotenv' not in content:
                self.log_issue('MEDIUM', 'dependencies', req_file, 0,
                             "python-dotenv not found in requirements.txt")
        except Exception as e:
            self.log_warning('file_access', f"Could not read requirements.txt: {e}")

    def run_full_audit(self) -> Dict[str, Any]:
        """Run complete security audit"""
        print("[AUDIT] Starting comprehensive security audit...")
        print(f"Project root: {self.project_root}")
        print()

        # Run all security checks
        self.scan_for_secrets()
        self.check_env_files()
        self.check_gitignore()
        self.check_git_history()
        self.check_file_permissions()
        self.check_requirements()

        return self.generate_report()

    def generate_report(self) -> Dict[str, Any]:
        """Generate security audit report"""
        print("\n" + "="*80)
        print("[REPORT] SECURITY AUDIT REPORT")
        print("="*80)

        # Count issues by level
        critical_count = len([i for i in self.issues if i['level'] == 'CRITICAL'])
        high_count = len([i for i in self.issues if i['level'] == 'HIGH'])
        medium_count = len([i for i in self.issues if i['level'] == 'MEDIUM'])

        print(f"Summary:")
        print(f"   CRITICAL issues: {critical_count}")
        print(f"   HIGH issues:     {high_count}")
        print(f"   MEDIUM issues:   {medium_count}")
        print(f"   Warnings:        {len(self.warnings)}")
        print()

        # Show critical and high issues
        for issue in self.issues:
            if issue['level'] in ['CRITICAL', 'HIGH']:
                icon = "[CRITICAL]" if issue['level'] == 'CRITICAL' else "[HIGH]"
                print(f"{icon} {issue['level']}: {issue['category']}")
                print(f"   File: {issue['file']}:{issue['line']}")
                print(f"   Issue: {issue['message']}")
                print()

        # Show warnings
        if self.warnings:
            print("[WARNING] Warnings:")
            for warning in self.warnings:
                print(f"   {warning['category']}: {warning['message']}")
            print()

        # Security recommendations
        print("[RECOMMENDATIONS] Security Recommendations:")
        if critical_count > 0:
            print("   [X] DO NOT PUSH TO GITHUB until critical issues are resolved")
        else:
            print("   [OK] No critical security issues found")

        print("   Best practices:")
        print("      - Never commit .env files")
        print("      - Use environment variables for all secrets")
        print("      - Rotate API keys regularly")
        print("      - Monitor API usage for anomalies")
        print("      - Use read-only API keys when possible")
        print()

        # Overall assessment
        if critical_count == 0 and high_count == 0:
            print("[STATUS] SECURITY STATUS: SAFE TO PUSH")
        elif critical_count > 0:
            print("[STATUS] SECURITY STATUS: CRITICAL - DO NOT PUSH")
        else:
            print("[STATUS] SECURITY STATUS: REVIEW REQUIRED")

        return {
            'status': 'safe' if critical_count == 0 and high_count == 0 else 'unsafe',
            'critical_count': critical_count,
            'high_count': high_count,
            'medium_count': medium_count,
            'warning_count': len(self.warnings),
            'issues': self.issues,
            'warnings': self.warnings
        }


def main():
    """Main entry point"""
    print("[SECURITY] Gap Scanner Security Audit")
    print("This tool checks for security issues before GitHub push")
    print()

    auditor = SecurityAuditor()
    report = auditor.run_full_audit()

    # Exit with error code if security issues found
    if report['critical_count'] > 0:
        sys.exit(1)
    elif report['high_count'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()