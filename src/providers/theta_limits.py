# src/providers/theta_limits.py
import os, threading
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root (handle running from any directory)
project_root = Path(__file__).parent.parent.parent
env_path = project_root / ".env"
load_dotenv(env_path)

# Defaults to STANDARD
V3_MAX = int(os.getenv("THETA_V3_MAX_OUTSTANDING", "2"))  # STANDARD=2, PRO=4
V1_MAX = int(os.getenv("THETA_V1_MAX_OUTSTANDING", "2"))  # legacy terminal; treat similarly

sem_v3 = threading.Semaphore(V3_MAX)
sem_v1 = threading.Semaphore(V1_MAX)