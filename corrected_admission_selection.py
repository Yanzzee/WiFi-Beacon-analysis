"""ADR-0013 admission-capacity selection, independent of CU availability."""
from __future__ import annotations

from typing import Any

import pandas as pd


ADMISSION_MAXIMUM_RAW = 31_250
ADMISSION_METRIC_ID = "beacon.metric/radio-admission-capacity/v1"
RADIO_DETAIL_SCHEMA_ID = "beacon.schema/radio-bin-detail/v2"
ADMISSION_BIN_COLUMNS = (
    "admission_metric_id", "admission_state", "qbss_admission_capacity",
    "admission_source_ssid", "admission_source_bssid",
    "admission_source_selection_method", "admission_source_observation_time",
    "valid_admission_candidate_count", "invalid_admission_candidate_count",
    "admission_disagreement", "admission_source_matches_cu",
)


def admission_raw(value: Any) -> int | None:
    """Preserve advertised integers, including uninterpretable uint16 values."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        raise ValueError("admission capacity must be an integer")
    number = int(value)
    if number != value or not 0 <= number <= 65_535:
        raise ValueError("advertised admission capacity must be a uint16 integer")
    return number


def empty_admission(state: str) -> dict[str, Any]:
    return {
        "admission_metric_id": ADMISSION_METRIC_ID,
        "admission_state": state,
        "qbss_admission_capacity": None,
        "admission_source_ssid": None,
        "admission_source_bssid": None,
        "admission_source_selection_method": None,
        "admission_source_observation_time": None,
        "valid_admission_candidate_count": 0,
        "invalid_admission_candidate_count": 0,
        "admission_disagreement": False,
        "admission_source_matches_cu": False,
    }


def preferred_admission_source(
    ordered: pd.DataFrame, primary_ssid: str | None,
) -> tuple[str | None, str | None]:
    valid = ordered.loc[
        ordered["qbss_admission_capacity"].between(0, ADMISSION_MAXIMUM_RAW)
    ]
    primary = valid.loc[valid["ssid"].eq(primary_ssid)] if primary_ssid else valid.iloc[:0]
    if not primary.empty:
        return str(primary.iloc[0]["bssid"]), "primary_ssid"
    if not valid.empty:
        return str(valid.iloc[0]["bssid"]), "first_valid_session_bssid"
    return None, None


def resolve_admission(
    candidates: list[Any], cu_source: Any | None,
    preferred: tuple[str | None, str | None],
) -> dict[str, Any]:
    result = empty_admission("missing_capacity" if candidates else "no_beacon")
    first = preferred_candidate = cu_candidate = None
    distinct: set[int] = set()
    for candidate in candidates:
        value = candidate.qbss_admission_capacity
        if value is None or pd.isna(value):
            continue
        value = int(value)
        if not 0 <= value <= ADMISSION_MAXIMUM_RAW:
            result["invalid_admission_candidate_count"] += 1
            continue
        result["valid_admission_candidate_count"] += 1
        distinct.add(value)
        if first is None:
            first = candidate
        if str(candidate.bssid) == preferred[0]:
            preferred_candidate = candidate
        if candidate is cu_source:
            cu_candidate = candidate
    chosen = cu_candidate or preferred_candidate or first
    if chosen is None:
        if result["invalid_admission_candidate_count"]:
            result["admission_state"] = "invalid_capacity"
        return result
    result.update({
        "admission_state": "valid",
        "qbss_admission_capacity": int(chosen.qbss_admission_capacity),
        "admission_source_ssid": chosen.ssid,
        "admission_source_bssid": str(chosen.bssid),
        "admission_source_selection_method": (
            "cu_source" if cu_candidate is not None else preferred[1]
            if preferred_candidate is not None else "fallback_member_bssid"
        ),
        "admission_source_observation_time": chosen.observation_time,
        "admission_disagreement": len(distinct) > 1,
        "admission_source_matches_cu": cu_candidate is not None,
    })
    return result
