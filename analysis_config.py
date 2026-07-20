"""Small shared helpers for reading analysis configuration."""

import os


def env_or_default(name: str, default: str) -> str:
    """Return a stripped environment value, or ``default`` when it is blank."""
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def get_beacon_rate() -> float:
    """Read BEACON_RATE while preserving the scripts' existing fallback."""
    try:
        return float(os.getenv("BEACON_RATE"))
    except ValueError:
        print("Invalid float value in BEACON_RATE env variable.")
        return 0.1024
