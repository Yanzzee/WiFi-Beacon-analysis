"""Small shared helpers for reading analysis configuration."""

import os


def env_or_default(name: str, default: str) -> str:
    """Return a stripped environment value, or ``default`` when it is blank."""
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default
