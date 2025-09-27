#!/usr/bin/env python3
"""
ThetaData Terminal Configuration Optimizer
Updates the terminal's config.toml to optimize request queue settings
"""

import os
import shutil
import toml
from datetime import datetime

def optimize_theta_terminal_config():
    """Optimize ThetaData terminal configuration for better performance"""
    config_path = r"C:\Users\socra\projects\theta_data_project\ThetaTerminal\config.toml"

    if not os.path.exists(config_path):
        print(f"ThetaData config file not found at: {config_path}")
        return False

    print(f"Found ThetaData config file: {config_path}")

    try:
        # Create backup
        backup_path = f"{config_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(config_path, backup_path)
        print(f"Created backup: {backup_path}")

        # Load current config
        with open(config_path, 'r') as f:
            config = toml.load(f)

        print("Current configuration:")
        print(toml.dumps(config))

        # Check for existing request queue settings
        needs_update = False

        # Look for server section
        if 'server' not in config:
            config['server'] = {}
            needs_update = True

        # Check request queue size
        current_queue_size = config['server'].get('request_queue_size', 16)  # Default is 16

        if current_queue_size <= 16:
            # Increase to 64 for better performance with our 4 concurrent requests
            config['server']['request_queue_size'] = 64
            needs_update = True
            print(f"Updating request_queue_size from {current_queue_size} to 64")
        else:
            print(f"Request queue size already optimized: {current_queue_size}")

        # Add other potential optimizations
        optimizations = {
            'connection_timeout': 30,        # Connection timeout in seconds
            'read_timeout': 30,              # Read timeout in seconds
            'max_connections': 10,           # Max concurrent connections
            'enable_compression': True       # Enable response compression
        }

        for key, value in optimizations.items():
            if key not in config['server']:
                config['server'][key] = value
                needs_update = True
                print(f"Added optimization: {key} = {value}")

        if needs_update:
            # Write updated config
            with open(config_path, 'w') as f:
                toml.dump(config, f)

            print("\nUpdated configuration:")
            print(toml.dumps(config))
            print("\n" + "="*50)
            print("IMPORTANT: Please restart ThetaData Terminal to apply changes!")
            print("="*50)
            return True
        else:
            print("No configuration updates needed.")
            return True

    except Exception as e:
        print(f"Error optimizing config: {e}")
        return False

def check_theta_performance_settings():
    """Check and report current ThetaData performance settings"""
    print("ThetaData Performance Configuration Summary:")
    print("=" * 50)

    # Check current gap scanner config
    from run_discovery_compare import CONFIG

    print(f"Gap Scanner ThetaData Settings:")
    print(f"  Outstanding requests: {CONFIG['theta_outstanding']}")
    print(f"  Retry attempts: {CONFIG['theta_retry_total']}")
    print(f"  Backoff factor: {CONFIG['theta_backoff']}")
    print(f"  Timeout: {CONFIG['timeout_sec']}s")

    # Recommendations based on subscription tier
    print(f"\nRecommendations by Subscription Tier:")
    print(f"  STANDARD: theta_outstanding=4, request_queue_size=64")
    print(f"  PRO: theta_outstanding=8, request_queue_size=128")
    print(f"  Current setting: theta_outstanding={CONFIG['theta_outstanding']}")

    if CONFIG['theta_outstanding'] == 4:
        print(f"  -> Configuration optimized for STANDARD tier")
    elif CONFIG['theta_outstanding'] == 8:
        print(f"  -> Configuration optimized for PRO tier")
    else:
        print(f"  -> Consider updating to 4 (STANDARD) or 8 (PRO)")

if __name__ == "__main__":
    print("ThetaData Terminal Configuration Optimizer")
    print("=" * 50)

    # Optimize terminal config
    success = optimize_theta_terminal_config()

    print("\n")

    # Show performance settings
    check_theta_performance_settings()

    if success:
        print(f"\nConfiguration optimization completed successfully!")
    else:
        print(f"\nConfiguration optimization failed - check logs above.")