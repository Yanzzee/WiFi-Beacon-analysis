"""Optional project-level AP enablement declaration, not radio-band inference."""
from __future__ import annotations

import pandas as pd

SIX_GHZ_COLUMN = "6GHz_Enabled"


def optional_enabled(value):
    if value is None or value is pd.NA or pd.isna(value) or str(value).strip().lower() in {"", "auto"}:
        return None
    text = str(value).strip().lower()
    if text in {"true", "yes", "1", "1.0", "on"}:
        return True
    if text in {"false", "no", "0", "0.0", "off"}:
        return False
    raise ValueError(f"{SIX_GHZ_COLUMN} must be true/false, yes/no, 1/0, or blank/auto; got {value!r}")


def validate_enablement_column(frame):
    result = frame.copy()
    if SIX_GHZ_COLUMN in result:
        result[SIX_GHZ_COLUMN] = pd.array(result[SIX_GHZ_COLUMN].map(optional_enabled), dtype="boolean")
    return result
