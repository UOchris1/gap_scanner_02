# -*- coding: ascii -*-
"""
Env file validation and formatting helpers.
Preserves values, never prints secrets, enforces ASCII-only and consistent layout.
"""
import os
from typing import Dict, List, Tuple
from pathlib import Path
from datetime import datetime


ENV_REQUIRED = ["POLYGON_API_KEY"]
ENV_RECOMMENDED = [
    "FMP_API_KEY",
    "THETA_V3_URL",
    "THETA_V1_URL",
    "THETA_V3_MAX_OUTSTANDING",
    "THETA_V1_MAX_OUTSTANDING",
]

PLACEHOLDER_MARKERS = (
    "your_polygon_api_key_here",
    "changeme",
    "REPLACE_ME",
    "INSERT_KEY",
)


def _is_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return True
    except Exception:
        return False


def _parse_env(path: Path) -> Tuple[Dict[str, str], List[str]]:
    kv: Dict[str, str] = {}
    raw: List[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw.append(line)
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" in s:
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            kv[k] = v
    return kv, raw


def validate_env(path: str) -> Dict[str, object]:
    p = Path(path)
    if not p.exists():
        return {"ok": False, "error": ".env not found", "path": str(p)}

    kv, raw = _parse_env(p)

    ascii_ok = all(_is_ascii(line) for line in raw)
    duplicates = []  # simplified: last-wins in parser; duplicates not tracked by this basic parse

    missing = [k for k in ENV_REQUIRED if k not in kv]
    placeholders = []
    for k, v in kv.items():
        v_l = v.lower()
        if any(m in v_l for m in PLACEHOLDER_MARKERS):
            placeholders.append(k)

    result = {
        "ok": ascii_ok and not missing,
        "path": str(p),
        "ascii_ok": ascii_ok,
        "missing_required": missing,
        "has_placeholders": bool(placeholders),
        "placeholder_keys": placeholders,
        "present_keys": sorted(list(kv.keys())),
    }
    return result


def format_env(path: str) -> str:
    """
    Normalize .env layout in-place. Preserves values; creates a timestamped backup.
    Returns backup file path.
    """
    p = Path(path)
    kv, _ = _parse_env(p)

    # Fill sensible defaults for Theta concurrency if blank (improves robustness)
    if kv.get("THETA_V3_MAX_OUTSTANDING", "").strip() == "":
        kv["THETA_V3_MAX_OUTSTANDING"] = "2"
    if kv.get("THETA_V1_MAX_OUTSTANDING", "").strip() == "":
        kv["THETA_V1_MAX_OUTSTANDING"] = "2"
    # Provide default local URLs if blank
    if kv.get("THETA_V3_URL", "").strip() == "":
        kv["THETA_V3_URL"] = "http://127.0.0.1:25503"
    if kv.get("THETA_V1_URL", "").strip() == "":
        kv["THETA_V1_URL"] = "http://127.0.0.1:25510"

    backup = p.with_suffix(".bak." + datetime.now().strftime("%Y%m%d_%H%M%S"))
    p.replace(backup)

    lines: List[str] = []
    lines.append("# -*- coding: ascii -*-")
    lines.append("# Gap Scanner .env - managed format; values preserved")
    lines.append("")

    def add_block(title: str, keys: List[str]):
        lines.append(f"# {title}")
        for k in keys:
            v = kv.get(k, "")
            lines.append(f"{k}={v}")
        lines.append("")

    add_block("Core APIs", ["POLYGON_API_KEY", "FMP_API_KEY"])
    add_block(
        "ThetaData Terminal (local HTTP; no API key)",
        [
            "THETA_V3_URL",
            "THETA_V1_URL",
            "THETA_V3_MAX_OUTSTANDING",
            "THETA_V1_MAX_OUTSTANDING",
        ],
    )

    # Other known optional keys (only if present)
    optional_keys = [
        "BRAVE_API_KEY",
        "ALPACA_API_KEY",
        "ALPACA_SECRET_KEY",
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "LOG_LEVEL",
        "OUTPUT_DIR",
        "CHART_DPI",
        "CHART_WIDTH",
        "CHART_HEIGHT",
        "YAHOO_USER_AGENT",
    ]
    present_optionals = [k for k in optional_keys if k in kv]
    if present_optionals:
        add_block("Optional Extras", present_optionals)

    # Any other keys preserved under Other Keys
    known = set(["POLYGON_API_KEY", "FMP_API_KEY"]) | set(ENV_RECOMMENDED) | set(optional_keys)
    others = [k for k in kv.keys() if k not in known]
    if others:
        add_block("Other Keys", sorted(others))

    text = "\n".join(lines) + "\n"
    # Ensure ASCII; replace non-ascii just in case
    p.write_text(text, encoding="ascii", errors="replace")
    return str(backup)


