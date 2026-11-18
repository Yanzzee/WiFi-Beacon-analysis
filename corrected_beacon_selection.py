"""Corrected per-BSSID class timing and representative selection.

This module implements the accepted PR-0021/PR-0022 timing boundary without
wiring it into the legacy analysis scripts. Observation time admits rows to a
half-open class and controls ordering and bin placement. Native TSF is used
only for diagnostics and conservative recognition of duplicate capture views.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


TU_US = 1_024
DEFAULT_BEACON_INTERVAL_TU = 100
TIMING_POLICY_ID = "beacon.timing/observation-time/v1"
MAX_DUPLICATE_SEPARATION_US = 102_400
TSF_DIAGNOSTIC_FIELDS = (
    "tsf_clock_jump_count", "tsf_max_step_error_us", "tsf_offset_range_us",
    "tsf_anomaly_source_capture", "tsf_anomaly_source_frame",
    "tsf_anomaly_observation_time", "tsf_anomaly_value_us",
)


def capture_families(frame: pd.DataFrame) -> pd.Series:
    profiles = frame.get("capture_profile", pd.Series("", index=frame.index)).astype("string").fillna("")
    family = pd.Series("unknown", index=frame.index, dtype="string")
    family.loc[profiles.eq("native_80211")] = "native"
    family.loc[profiles.str.startswith("aruba_erm")] = "erm"
    return family


@dataclass(frozen=True)
class BeaconSelectionResult:
    """Logical results from one class-session selection operation."""

    observations: pd.DataFrame
    selected: pd.DataFrame
    excluded: pd.DataFrame
    timing: pd.DataFrame


def _as_utc_timestamp(value: Any, *, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def build_class_bins(
    class_start: Any,
    class_end: Any,
    bin_width_us: int,
) -> pd.DataFrame:
    """Build fixed, half-open bins including an exact-width terminal partial."""

    start = _as_utc_timestamp(class_start, name="class_start")
    end = _as_utc_timestamp(class_end, name="class_end")
    if end <= start:
        raise ValueError("class_end must be after class_start")
    if not isinstance(bin_width_us, int) or isinstance(bin_width_us, bool):
        raise ValueError("bin_width_us must be a positive integer")
    if bin_width_us <= 0:
        raise ValueError("bin_width_us must be positive")

    width = pd.Timedelta(microseconds=bin_width_us)
    rows: list[dict[str, Any]] = []
    bin_start = start
    bin_number = 0
    while bin_start < end:
        bin_end = min(bin_start + width, end)
        duration_us = (bin_end.value - bin_start.value) // 1_000
        rows.append(
            {
                "bin_number": bin_number,
                "bin_start": bin_start,
                "bin_end": bin_end,
                "bin_duration_us": int(duration_us),
                "width_status": (
                    "complete"
                    if duration_us == bin_width_us
                    else "partial"
                ),
            }
        )
        bin_start = bin_end
        bin_number += 1

    return pd.DataFrame(rows)


def _parse_time_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    values = frame[column].replace("", pd.NA)
    return pd.to_datetime(values, errors="coerce", utc=True, format="mixed")


def preferred_observation_times(observations: pd.DataFrame) -> pd.Series:
    """Return accepted Aruba-first, frame-time-fallback observation times."""

    if not isinstance(observations, pd.DataFrame):
        raise TypeError("observations must be a pandas DataFrame")
    aruba_time = _parse_time_column(observations, "aruba_erm_time")
    aruba_time = aruba_time.mask(capture_families(observations).eq("native"))
    frame_time = _parse_time_column(observations, "frame_time_epoch")
    return aruba_time.where(aruba_time.notna(), frame_time)


def _parse_uint64(value: Any) -> Any:
    if value is None or value is pd.NA or pd.isna(value) or value == "":
        return pd.NA
    if isinstance(value, bool):
        return pd.NA
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return pd.NA
    if parsed < 0 or parsed > (2**64 - 1):
        return pd.NA
    if isinstance(value, float) and not value.is_integer():
        return pd.NA
    if isinstance(value, str) and str(parsed) != value.strip():
        return pd.NA
    return parsed


def _string_sort_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series("", index=frame.index, dtype="string")
    return frame[column].fillna("").astype("string")


def _add_sort_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    max_sort_value = 2**63 - 1
    source_rank = pd.to_numeric(
        result.get("source_rank", pd.Series(pd.NA, index=result.index)),
        errors="coerce",
    )
    result["_source_rank_sort"] = source_rank.fillna(max_sort_value).astype(
        "int64"
    )
    result["_observation_source_sort"] = result[
        "observation_time_source"
    ].map({"aruba_erm": 0, "frame_time_epoch": 1}).fillna(2).astype("int64")
    result["_source_capture_sort"] = _string_sort_column(
        result, "source_capture"
    )
    source_frame = pd.to_numeric(
        result.get("source_frame", pd.Series(pd.NA, index=result.index)),
        errors="coerce",
    )
    result["_source_frame_number_sort"] = source_frame.fillna(
        max_sort_value
    ).astype("int64")
    result["_source_frame_text_sort"] = _string_sort_column(
        result, "source_frame"
    )
    input_order = pd.to_numeric(
        result.get("input_order", pd.Series(pd.NA, index=result.index)),
        errors="coerce",
    )
    fallback_order = pd.Series(range(len(result)), index=result.index)
    result["_input_order_sort"] = input_order.where(
        input_order.notna(), fallback_order
    ).astype("int64")
    return result


def _source_precedence_columns(*, include_observation_time: bool) -> list[str]:
    columns = [
        "_observation_source_sort",
        "_source_rank_sort",
    ]
    if include_observation_time:
        columns.append("observation_time")
    columns.extend(
        [
            "_source_capture_sort",
            "_source_frame_number_sort",
            "_source_frame_text_sort",
            "_input_order_sort",
        ]
    )
    return columns


def _append_exclusion(
    exclusions: list[pd.DataFrame],
    rows: pd.DataFrame,
    reason: str,
) -> None:
    if rows.empty:
        return
    excluded = rows.copy()
    excluded["exclusion_reason"] = reason
    exclusions.append(excluded)


def _interval_us_for_group(
    group: pd.DataFrame,
    default_beacon_interval_tu: int,
) -> int:
    if "beacon_interval_tu" in group:
        intervals = pd.to_numeric(
            group["beacon_interval_tu"], errors="coerce"
        ).dropna()
        intervals = intervals.loc[intervals > 0]
        if not intervals.empty:
            return int(intervals.max()) * TU_US
    return default_beacon_interval_tu * TU_US


def _diagnose_native_tsf(
    group: pd.DataFrame,
    *,
    default_beacon_interval_tu: int,
) -> dict[str, Any]:
    result = dict.fromkeys(TSF_DIAGNOSTIC_FIELDS)
    result["tsf_clock_jump_count"] = 0
    tsf = group["tsf_us"]
    if tsf.isna().any():
        return {**result, "tsf_status": "missing"}
    if len(group) < 2:
        return {**result, "tsf_status": "insufficient_progression"}
    if tsf.nunique() == 1:
        return {**result, "tsf_status": "frozen"}

    interval_us = _interval_us_for_group(group, default_beacon_interval_tu)
    observed_order = group.sort_values(
        [
            "observation_time",
            *_source_precedence_columns(include_observation_time=False),
        ],
        kind="stable",
    )
    values = [int(value) for value in observed_order["tsf_us"]]
    observed_us = (observed_order["observation_time"].astype("int64") // 1_000).tolist()
    # Relative integer differences avoid absolute TSF epoch mapping and uint64
    # subtraction overflow. One beacon interval is a conservative trust bound,
    # not a capture-clock correction or a claim about timestamp accuracy.
    residual = [(value - values[0]) - (observed - observed_us[0])
                for value, observed in zip(values, observed_us)]
    errors = [abs(current - previous) for previous, current in zip(residual, residual[1:])]
    result.update(tsf_clock_jump_count=sum(error > interval_us for error in errors),
                  tsf_max_step_error_us=max(errors), tsf_offset_range_us=max(residual) - min(residual))
    if any(current - previous < -interval_us for previous, current in zip(values, values[1:])):
        status = "reversing"
    elif result["tsf_clock_jump_count"]:
        status = "discontinuous"
    elif result["tsf_offset_range_us"] > interval_us:
        status = "time_mismatch"
    else:
        status = "usable_advancing"
    if status != "usable_advancing":
        position = errors.index(max(errors)) + 1
        row = observed_order.iloc[position]
        result.update(tsf_anomaly_source_capture=row.get("source_capture"),
                      tsf_anomaly_source_frame=str(row.get("source_frame", "")),
                      tsf_anomaly_observation_time=row.observation_time,
                      tsf_anomaly_value_us=str(int(row.tsf_us)))
    return {**result, "tsf_status": status}


def _unusable_reason(status: str) -> str:
    return f"tsf_unusable_{status}"


def _deduplicate_usable_tsf(
    group: pd.DataFrame,
    exclusions: list[pd.DataFrame],
) -> pd.DataFrame:
    candidates = group.loc[group.tsf_us.duplicated(keep=False)]
    content = [field for field in ("ssid", "qbss_cu_raw", "qbss_station_count",
                                   "qbss_admission_capacity", "payload_id") if field in group]
    evidence = [field for field in content if field != "ssid"]
    if candidates.empty or "source_capture" not in group or not evidence:
        return group
    kept = group.copy()
    dropped = []
    for _, views in candidates.groupby("tsf_us", sort=False):
        # Do not collapse repeated timestamps within a capture or conflicting
        # payloads. Matching retained fields corroborate a trusted native TSF.
        if (views.source_capture.isna().any() or views.source_capture.nunique() != len(views)
                or len(views[content].drop_duplicates()) != 1
                or not views[evidence].notna().any().any()
                or (views.observation_time.max() - views.observation_time.min()).value > MAX_DUPLICATE_SEPARATION_US * 1000):
            continue
        ranked = views.sort_values(_source_precedence_columns(include_observation_time=True), kind="stable")
        winner = ranked.iloc[0]
        kept.loc[ranked.index[0], "duplicate_views_collapsed"] = len(ranked) - 1
        losers = ranked.iloc[1:].copy()
        losers["duplicate_of_source_capture"] = winner.source_capture
        losers["duplicate_of_source_frame"] = winner.get("source_frame")
        _append_exclusion(exclusions, losers, "duplicate_view")
        dropped.extend(losers.index)
    return kept.drop(index=dropped)


def _add_sequence_diagnostics(
    group: pd.DataFrame,
    *,
    tsf_usable: bool,
    default_beacon_interval_tu: int,
) -> pd.DataFrame:
    coordinate = "observation_time"
    ordered = group.sort_values(
        [coordinate, *_source_precedence_columns(include_observation_time=False)],
        kind="stable",
    ).copy()
    ordered["sequence_order"] = pd.array(range(len(ordered)), dtype="Int64")
    ordered["skipped_intervals_before"] = pd.array(
        [pd.NA] * len(ordered), dtype="Int64"
    )

    if tsf_usable and len(ordered) > 1:
        interval_us = _interval_us_for_group(
            ordered, default_beacon_interval_tu
        )
        tsf_values = [int(value) for value in ordered["tsf_us"]]
        skipped: list[Any] = [pd.NA]
        for previous, current in zip(tsf_values, tsf_values[1:]):
            difference = current - previous
            if difference > interval_us and difference % interval_us == 0:
                skipped.append((difference // interval_us) - 1)
            else:
                skipped.append(0)
        ordered["skipped_intervals_before"] = pd.array(
            skipped, dtype="Int64"
        )
    return ordered


def _assign_bins(
    observations: pd.DataFrame,
    *,
    bins: pd.DataFrame,
    class_start: pd.Timestamp,
    bin_width_us: int,
) -> pd.DataFrame:
    result = observations.copy()
    placement = result["placement_time"]
    result["bin_number"] = (
        (placement.astype("int64") - class_start.value)
        // (bin_width_us * 1_000)
    ).astype("int64")

    bin_lookup = bins.set_index("bin_number")
    for column in [
        "bin_start",
        "bin_end",
        "bin_duration_us",
        "width_status",
    ]:
        result[column] = result["bin_number"].map(bin_lookup[column])
    return result


def _select_first_per_bin(observations: pd.DataFrame) -> pd.DataFrame:
    result = observations.copy()
    result["is_selected"] = False
    ranked = result.sort_values(
        [
            "bssid",
            "bin_number",
            "observation_time",
            *_source_precedence_columns(include_observation_time=False),
        ],
        kind="stable",
    )
    winner_indices = ranked.groupby(
        ["bssid", "bin_number"], sort=False
    ).head(1).index
    result.loc[winner_indices, "is_selected"] = True
    return result


def _drop_internal_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop(
        columns=[column for column in frame if column.startswith("_")],
        errors="ignore",
    )


def select_bssid_session_observations(
    observations: pd.DataFrame,
    *,
    class_session_id: str,
    class_start: Any,
    class_end: Any,
    bin_width_us: int,
    default_beacon_interval_tu: int = DEFAULT_BEACON_INTERVAL_TU,
) -> BeaconSelectionResult:
    """Apply the accepted corrected timing and selection rules to one class."""

    start = _as_utc_timestamp(class_start, name="class_start")
    end = _as_utc_timestamp(class_end, name="class_end")
    bins = build_class_bins(start, end, bin_width_us)
    if default_beacon_interval_tu <= 0:
        raise ValueError("default_beacon_interval_tu must be positive")
    if "bssid" not in observations:
        raise ValueError("observations must include bssid")

    frame = observations.copy(deep=True)
    frame["_capture_family"] = capture_families(frame)
    frame["observation_time"] = pd.NaT
    aruba_time = _parse_time_column(frame, "aruba_erm_time")
    aruba_time = aruba_time.mask(frame["_capture_family"].eq("native"))
    frame_time = _parse_time_column(frame, "frame_time_epoch")
    aruba_raw_present = (
        frame.get("aruba_erm_time", pd.Series("", index=frame.index))
        .fillna("")
        .astype("string")
        .str.strip()
        .ne("")
    )

    frame["observation_time"] = aruba_time.where(
        aruba_time.notna(), frame_time
    )
    frame["observation_time_source"] = pd.Series(
        pd.NA, index=frame.index, dtype="string"
    )
    frame.loc[aruba_time.notna(), "observation_time_source"] = "aruba_erm"
    frame.loc[
        aruba_time.isna() & frame_time.notna(), "observation_time_source"
    ] = "frame_time_epoch"
    frame["observation_time_diagnostic"] = pd.Series(
        pd.NA, index=frame.index, dtype="string"
    )
    frame.loc[
        aruba_raw_present & aruba_time.isna() & frame["_capture_family"].ne("native"),
        "observation_time_diagnostic",
    ] = "aruba_erm_time_malformed"
    frame["tsf_us"] = pd.Series(
        pd.array(
            [
                _parse_uint64(value)
                for value in frame.get(
                    "tsf_us", pd.Series(pd.NA, index=frame.index)
                )
            ],
            dtype="UInt64",
        ),
        index=frame.index,
    )
    frame["duplicate_views_collapsed"] = 0
    frame = _add_sort_columns(frame)

    exclusions: list[pd.DataFrame] = []
    missing_time = frame["observation_time"].isna()
    _append_exclusion(
        exclusions,
        frame.loc[missing_time],
        "source_timestamp_unavailable",
    )

    has_time = frame.loc[~missing_time].copy()
    outside_observation = (has_time["observation_time"] < start) | (
        has_time["observation_time"] >= end
    )
    _append_exclusion(
        exclusions,
        has_time.loc[outside_observation],
        "outside_class_observation_time",
    )
    admitted = has_time.loc[~outside_observation].copy()
    admitted["class_session_id"] = class_session_id

    retained_groups: list[pd.DataFrame] = []
    timing_rows: list[dict[str, Any]] = []
    for (bssid, family), group in admitted.groupby(["bssid", "_capture_family"], sort=False, dropna=False):
        diagnostic = (_diagnose_native_tsf(group, default_beacon_interval_tu=default_beacon_interval_tu)
                      if family == "native" else {"tsf_status": "not_used_for_erm" if family == "erm" else "unknown_capture_profile"})
        status = diagnostic["tsf_status"]
        usable = status == "usable_advancing"
        timing_method = "observation_time"
        reason = pd.NA if usable else _unusable_reason(status)

        current = group.copy()
        if usable:
            current = _deduplicate_usable_tsf(current, exclusions)
        current["tsf_origin"] = pd.NaT
        current["tsf_derived_time"] = pd.NaT
        current["placement_time"] = current["observation_time"]

        current["timing_method"] = timing_method
        current["tsf_status"] = status
        current["unusable_tsf_reason"] = reason
        current = _add_sequence_diagnostics(
            current,
            tsf_usable=usable,
            default_beacon_interval_tu=default_beacon_interval_tu,
        )
        retained_groups.append(current)
        timing_rows.append(
            {
                "class_session_id": class_session_id,
                "bssid": bssid,
                "capture_family": family,
                "timing_method": timing_method,
                "tsf_status": status,
                "unusable_tsf_reason": reason,
                "tsf_origin": pd.NaT,
                **diagnostic,
            }
        )

    if retained_groups:
        retained = pd.concat(retained_groups, ignore_index=False)
        retained = _assign_bins(
            retained,
            bins=bins,
            class_start=start,
            bin_width_us=bin_width_us,
        )
        retained = _select_first_per_bin(retained)
        retained = retained.sort_values("_input_order_sort", kind="stable")
    else:
        retained = admitted.copy()
        retained["is_selected"] = pd.Series(dtype="bool")

    selected = retained.loc[retained["is_selected"]].copy()
    if exclusions:
        excluded = pd.concat(exclusions, ignore_index=False).sort_values(
            "_input_order_sort", kind="stable"
        )
    else:
        excluded = frame.iloc[0:0].copy()
        excluded["exclusion_reason"] = pd.Series(dtype="string")

    timing = pd.DataFrame(timing_rows)
    return BeaconSelectionResult(
        observations=_drop_internal_columns(retained).reset_index(drop=True),
        selected=_drop_internal_columns(selected).reset_index(drop=True),
        excluded=_drop_internal_columns(excluded).reset_index(drop=True),
        timing=timing.reset_index(drop=True),
    )
