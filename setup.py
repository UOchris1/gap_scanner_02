#!/usr/bin/env python3
"""
Secure Setup Script for Gap Scanner Project
Helps users configure the project with proper security practices
"""

import os
import sys
import shutil
from pathlib import Path
from typing import Dict, List


class SecureSetup:
    """Secure setup assistant for Gap Scanner project"""

    def __init__(self):
        self.project_root = Path(__file__).parent
        self.setup_steps = []
        self.warnings = []

    def check_prerequisites(self) -> bool:
        """Check if system prerequisites are met"""
        print("[SETUP] Checking prerequisites...")

        # Check Python version
        if sys.version_info < (3, 8):
            print("ERROR: Python 3.8+ required. Current version:", sys.version)
            return False

        # Check if git is available
        if not shutil.which("git"):
            print("WARNING: Git not found. Version control features may not work.")
            self.warnings.append("Git not available")

        print("[OK] Prerequisites check completed")
        return True

    def setup_environment(self) -> bool:
        """Set up environment configuration"""
        print("[SETUP] Setting up environment configuration...")

        env_path = self.project_root / ".env"
        env_example_path = self.project_root / ".env.example"

        # Check if .env.example exists
        if not env_example_path.exists():
            print("ERROR: .env.example template not found")
            return False

        # Check if .env already exists
        if env_path.exists():
            response = input(".env file already exists. Overwrite? (y/N): ")
            if response.lower() != 'y':
                print("Skipping environment setup")
                return True

        # Copy template to .env
        try:
            shutil.copy2(env_example_path, env_path)
            print(f"[OK] Created .env from template")
            print(f"[ACTION REQUIRED] Edit {env_path} with your actual API keys")
            print("[SECURITY] NEVER commit the .env file to version control")
        except Exception as e:
            print(f"ERROR: Could not create .env file: {e}")
            return False

        return True

    def verify_gitignore(self) -> bool:
        """Verify .gitignore is properly configured"""
        print("[SETUP] Verifying .gitignore configuration...")

        gitignore_path = self.project_root / ".gitignore"
        if not gitignore_path.exists():
            print("ERROR: .gitignore file not found")
            return False

        try:
            with open(gitignore_path, 'r', encoding='utf-8') as f:
                gitignore_content = f.read()

            # Check for essential security patterns
            essential_patterns = ['.env', '*.key', '*secret*']
            missing_patterns = []

            for pattern in essential_patterns:
                if pattern not in gitignore_content:
                    missing_patterns.append(pattern)

            if missing_patterns:
                print(f"WARNING: Missing gitignore patterns: {missing_patterns}")
                self.warnings.append(f"Gitignore missing patterns: {missing_patterns}")
            else:
                print("[OK] .gitignore properly configured")

        except Exception as e:
            print(f"ERROR: Could not read .gitignore: {e}")
            return False

        return True

    def install_dependencies(self) -> bool:
        """Guide user through dependency installation"""
        print("[SETUP] Dependency installation guide...")

        requirements_path = self.project_root / "requirements.txt"
        if not requirements_path.exists():
            print("ERROR: requirements.txt not found")
            return False

        print("To install dependencies, run:")
        print("  pip install -r requirements.txt")
        print("")
        print("For development, also consider installing:")
        print("  pip install pytest flake8 black")
        print("")

        return True

    def security_checklist(self) -> None:
        """Display security checklist for user"""
        print("[SECURITY] Security Checklist:")
        print("")
        print("Before using this project:")
        print("  [1] Get API keys from required providers:")
        print("      - Polygon.io: https://polygon.io/")
        print("      - Alpaca: https://alpaca.markets/")
        print("      - FMP: https://financialmodelingprep.com/")
        print("")
        print("  [2] Update .env file with your actual API keys")
        print("  [3] NEVER commit .env file to version control")
        print("  [4] Use paper trading keys for development")
        print("  [5] Rotate API keys regularly")
        print("  [6] Monitor API usage for anomalies")
        print("")
        print("Optional (for enhanced features):")
        print("  [7] Install ThetaData Terminal for real-time data")
        print("  [8] Set up additional API keys for AI features")
        print("")

    def verification_commands(self) -> None:
        """Show commands to verify setup"""
        print("[VERIFY] Verification Commands:")
        print("")
        print("Test your setup with these commands:")
        print("  python security_audit.py          # Run security audit")
        print("  python quick_baseline_test.py     # Test basic functionality")
        print("  python run_baseline_validation.py # Full system validation")
        print("")

    def run_setup(self) -> bool:
        """Run complete setup process"""
        print("="*80)
        print("[SETUP] Gap Scanner Secure Setup")
        print("="*80)
        print("")

        # Run setup steps
        if not self.check_prerequisites():
            return False

        if not self.setup_environment():
            return False

        if not self.verify_gitignore():
            return False

        if not self.install_dependencies():
            return False

        # Display guidance
        self.security_checklist()
        self.verification_commands()

        # Show warnings if any
        if self.warnings:
            print("[WARNING] Setup completed with warnings:")
            for warning in self.warnings:
                print(f"  - {warning}")
            print("")

        print("[SUCCESS] Setup completed!")
        print("[NEXT STEPS]")
        print("1. Edit .env file with your API keys")
        print("2. Run: pip install -r requirements.txt")
        print("3. Run: python security_audit.py")
        print("4. Run: python quick_baseline_test.py")
        print("")
        print("[SECURITY] Remember: NEVER commit .env files to version control")

        return True


def main():
    """Main setup entry point"""
    setup = SecureSetup()
    success = setup.run_setup()

    if not success:
        print("Setup failed. Please check errors above.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()