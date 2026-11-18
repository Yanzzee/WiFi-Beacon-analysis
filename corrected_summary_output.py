"""ADR-0004 corrected radio/class and classroom/class summaries.

The calculation consumes the complete expected class-bin grid, the stable
project identity map, and the already selected per-BSSID/bin observations.
It does not synthesize observations or calculate station-count rollups.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import groupby
import os
from pathlib import Path
import re
import uuid
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from corrected_admission_selection import (
    ADMISSION_BIN_COLUMNS, admission_raw, empty_admission,
    preferred_admission_source, resolve_admission,
)


RESULT_SCHEMA_ID = "beacon.schema/analysis-results/v1"
CORRECTED_METRIC_ID = "beacon.metric/corrected-cu/v2"
RADIO_BIN_METRIC_ID = "beacon.metric/corrected-radio-bin/v1"
RADIO_CLASS_SUMMARY_FILENAME = "radio_class_summary.parquet"
CLASSROOM_CLASS_SUMMARY_FILENAME = "classroom_class_summary.parquet"

HIGH_CU_MINIMUM_RAW = 128
VERY_HIGH_CU_MINIMUM_RAW = 192

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_REQUIRED_BIN_COLUMNS = {
    "class_session_id",
    "classroom",
    "bin_number",
    "bin_start",
    "bin_end",
    "bin_duration_us",
}
_REQUIRED_IDENTITY_COLUMNS = {
    "bssid",
    "radio_group_id",
    "classroom",
    "band",
}
_REQUIRED_SELECTED_COLUMNS = {
    "class_session_id",
    "classroom",
    "bin_number",
    "bssid",
    "radio_group_id",
    "observation_time",
    "observation_time_source",
    "source_capture",
    "source_frame",
    "qbss_cu_raw",
}

RADIO_BIN_CU_COLUMNS = (
    "run_id",
    "schema_id",
    "metric_id",
    "class_session_id",
    "classroom",
    "radio_group_id",
    "ap_id",
    "band",
    "channel",
    "bin_number",
    "bin_start",
    "bin_end",
    "bin_duration_us",
    "width_status",
    "cu_state",
    "qbss_cu_raw",
    "source_ssid",
    "source_bssid",
    "source_selection_method",
    "source_observation_time",
    "observed_member_bssid_count",
    "valid_cu_candidate_count",
    *ADMISSION_BIN_COLUMNS,
)

RADIO_CLASS_SUMMARY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("class_session_id", pa.string(), nullable=False),
        pa.field("classroom", pa.string(), nullable=False),
        pa.field("class_start", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("class_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("radio_group_id", pa.string(), nullable=False),
        pa.field("band", pa.string()),
        pa.field("channel", pa.int32()),
        pa.field("member_bssids", pa.list_(pa.string()), nullable=False),
        pa.field("preferred_bssid", pa.string()),
        pa.field("class_duration_us", pa.int64(), nullable=False),
        pa.field("valid_cu_duration_us", pa.int64(), nullable=False),
        pa.field("missing_cu_duration_us", pa.int64(), nullable=False),
        pa.field("no_beacon_duration_us", pa.int64(), nullable=False),
        pa.field("cu_coverage", pa.float64(), nullable=False),
        pa.field("average_cu_raw", pa.float64()),
        pa.field("average_cu_ratio", pa.float64()),
        pa.field(
            "valid_cu_raw_counts",
            pa.list_(pa.uint64(), 256),
            nullable=False,
        ),
        pa.field("high_duration_us", pa.int64()),
        pa.field("high_possible_connected_streak_us", pa.int64()),
        pa.field("high_actual_observed_streak_us", pa.int64()),
        pa.field("very_high_duration_us", pa.int64()),
        pa.field("very_high_possible_connected_streak_us", pa.int64()),
        pa.field("very_high_actual_observed_streak_us", pa.int64()),
    ]
)

CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("class_session_id", pa.string(), nullable=False),
        pa.field("classroom", pa.string(), nullable=False),
        pa.field("class_start", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("class_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("class_duration_us", pa.int64(), nullable=False),
        pa.field("radio_count_2_4_ghz", pa.int32(), nullable=False),
        pa.field("radio_count_5_ghz", pa.int32(), nullable=False),
        pa.field("radio_count_6_ghz", pa.int32(), nullable=False),
        pa.field("radio_count_unknown", pa.int32(), nullable=False),
        pa.field("radio_count_total", pa.int32(), nullable=False),
        pa.field("high_possible_connected_max_us", pa.int64()),
        pa.field(
            "high_possible_connected_responsible_radio_ids",
            pa.list_(pa.string()),
        ),
        pa.field("high_actual_observed_max_us", pa.int64()),
        pa.field(
            "high_actual_observed_responsible_radio_ids",
            pa.list_(pa.string()),
        ),
        pa.field("very_high_possible_connected_max_us", pa.int64()),
        pa.field(
            "very_high_possible_connected_responsible_radio_ids",
            pa.list_(pa.string()),
        ),
        pa.field("very_high_actual_observed_max_us", pa.int64()),
        pa.field(
            "very_high_actual_observed_responsible_radio_ids",
            pa.list_(pa.string()),
        ),
    ]
)


@dataclass(frozen=True)
class CorrectedSummaryTables:
    radio_class: pd.DataFrame
    classroom_class: pd.DataFrame
    radio_bin_cu: pd.DataFrame | None = None


@dataclass(frozen=True)
class WrittenCorrectedSummaries:
    radio_class_path: Path
    classroom_class_path: Path
    radio_class_rows: int
    classroom_class_rows: int


def _missing_columns(frame: pd.DataFrame, required: set[str]) -> list[str]:
    return sorted(required.difference(frame.columns))


def _required_text(value: Any, *, name: str) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        raise ValueError(f"{name} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def _band_text(value: Any) -> str | None:
    text = _optional_text(value)
    if text in {"2.4", "5", "6"}:
        return text
    if text in {"5.0", "6.0"}:
        return text[:-2]
    return text


def _integer(
    value: Any,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    if number < minimum or number > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return number


def _valid_cu(value: Any) -> int | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value) or str(value).strip() == "":
            return None
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return number if 0 <= number <= 255 else None


def _utc_timestamp(value: Any, *, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _integer_values(value: Any) -> list[int]:
    if value is None or value is pd.NA:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        values: Iterable[Any] = re.split(r"[;,]", value)
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = [value]
    result: set[int] = set()
    for item in values:
        if item is None or str(item).strip() == "":
            continue
        result.add(_integer(item, name="channel", minimum=0, maximum=2**31 - 1))
    return sorted(result)


def _normalize_bins(class_bins: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(class_bins, pd.DataFrame):
        raise TypeError("class_bins must be a pandas DataFrame")
    missing = _missing_columns(class_bins, _REQUIRED_BIN_COLUMNS)
    if missing:
        raise ValueError("class bins are missing columns: " + ", ".join(missing))

    rows: list[dict[str, Any]] = []
    for record in class_bins.to_dict(orient="records"):
        start = _utc_timestamp(record["bin_start"], name="bin_start")
        end = _utc_timestamp(record["bin_end"], name="bin_end")
        duration = _integer(
            record["bin_duration_us"],
            name="bin_duration_us",
            minimum=1,
            maximum=2**63 - 1,
        )
        elapsed_ns = end.value - start.value
        measured = elapsed_ns // 1_000
        if end <= start or elapsed_ns % 1_000 or measured != duration:
            raise ValueError("bin duration must exactly match bin_start/bin_end")
        rows.append(
            {
                "class_session_id": _required_text(
                    record["class_session_id"], name="class_session_id"
                ),
                "classroom": _required_text(record["classroom"], name="classroom"),
                "bin_number": _integer(
                    record["bin_number"],
                    name="bin_number",
                    minimum=0,
                    maximum=2**63 - 1,
                ),
                "bin_start": start,
                "bin_end": end,
                "bin_duration_us": duration,
                "width_status": _optional_text(record.get("width_status")),
            }
        )
    result = pd.DataFrame(
        rows,
        columns=[*sorted(_REQUIRED_BIN_COLUMNS), "width_status"],
    )
    if result.empty:
        return result
    key = ["class_session_id", "classroom", "bin_number"]
    if result.duplicated(key).any():
        raise ValueError("class bins require unique session/classroom/bin keys")
    for (_session, _room), group in result.groupby(
        ["class_session_id", "classroom"], sort=False
    ):
        ordered = group.sort_values("bin_number", kind="stable")
        if ordered["bin_number"].tolist() != list(range(len(ordered))):
            raise ValueError("class bin numbers must be contiguous and start at zero")
        starts = ordered["bin_start"].tolist()
        ends = ordered["bin_end"].tolist()
        if starts[1:] != ends[:-1]:
            raise ValueError("class bins must be contiguous")
        maximum_duration = int(ordered["bin_duration_us"].max())
        missing_width = ordered["width_status"].isna()
        result.loc[ordered.index[missing_width], "width_status"] = ordered.loc[
            missing_width, "bin_duration_us"
        ].map(lambda value: "complete" if value == maximum_duration else "partial")
    return result.sort_values(key, kind="stable").reset_index(drop=True)


def _identity_channel_values(record: dict[str, Any]) -> list[int]:
    if "observed_channels" in record:
        return _integer_values(record["observed_channels"])
    if "channels" in record:
        return _integer_values(record["channels"])
    return []


def _normalize_identity(identity_map: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(identity_map, pd.DataFrame):
        raise TypeError("identity_map must be a pandas DataFrame")
    missing = _missing_columns(identity_map, _REQUIRED_IDENTITY_COLUMNS)
    if missing:
        raise ValueError("identity map is missing columns: " + ", ".join(missing))
    rows = []
    for record in identity_map.to_dict(orient="records"):
        rows.append(
            {
                "bssid": _required_text(record["bssid"], name="bssid"),
                "radio_group_id": _required_text(
                    record["radio_group_id"], name="radio_group_id"
                ),
                "classroom": _required_text(record["classroom"], name="classroom"),
                "band": _band_text(record["band"]),
                "ap_id": _optional_text(record.get("ap_id")),
                "channels": _identity_channel_values(record),
            }
        )
    result = pd.DataFrame(
        rows,
        columns=[
            "bssid",
            "radio_group_id",
            "classroom",
            "band",
            "ap_id",
            "channels",
        ],
    )
    if result["bssid"].duplicated().any():
        raise ValueError("identity map requires one row per BSSID")
    for radio_group_id, group in result.groupby("radio_group_id", sort=False):
        if group["classroom"].nunique(dropna=False) != 1:
            raise ValueError(f"radio {radio_group_id} maps to multiple classrooms")
        if group["band"].nunique(dropna=False) != 1:
            raise ValueError(f"radio {radio_group_id} maps to multiple bands")
    return result


def _normalize_selected(selected: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(selected, pd.DataFrame):
        raise TypeError("selected_observations must be a pandas DataFrame")
    missing = _missing_columns(selected, _REQUIRED_SELECTED_COLUMNS)
    if missing:
        raise ValueError(
            "selected observations are missing columns: " + ", ".join(missing)
        )
    rows = []
    for index, record in enumerate(selected.to_dict(orient="records")):
        source_frame = _required_text(record["source_frame"], name="source_frame")
        try:
            source_frame_number = int(source_frame)
        except ValueError:
            source_frame_number = 2**63 - 1
        rows.append(
            {
                "class_session_id": _required_text(
                    record["class_session_id"], name="class_session_id"
                ),
                "classroom": _required_text(record["classroom"], name="classroom"),
                "bin_number": _integer(
                    record["bin_number"],
                    name="bin_number",
                    minimum=0,
                    maximum=2**63 - 1,
                ),
                "bssid": _required_text(record["bssid"], name="bssid"),
                "ssid": _optional_text(record.get("ssid")),
                "radio_group_id": _required_text(
                    record["radio_group_id"], name="radio_group_id"
                ),
                "observation_time": _utc_timestamp(
                    record["observation_time"], name="observation_time"
                ),
                "observation_time_source": _required_text(
                    record["observation_time_source"],
                    name="observation_time_source",
                ),
                "source_capture": _required_text(
                    record["source_capture"], name="source_capture"
                ),
                "source_frame": source_frame,
                "source_frame_number": source_frame_number,
                "source_rank": _source_rank(record.get("source_rank")),
                "qbss_cu_raw": _valid_cu(record["qbss_cu_raw"]),
                "qbss_admission_capacity": admission_raw(record.get("qbss_admission_capacity")),
                "input_order": index,
                "channel": _valid_channel(record.get("channel")),
            }
        )
    columns = [
        "class_session_id",
        "classroom",
        "bin_number",
        "bssid",
        "ssid",
        "radio_group_id",
        "observation_time",
        "observation_time_source",
        "source_capture",
        "source_frame",
        "source_frame_number",
        "source_rank",
        "qbss_cu_raw",
        "qbss_admission_capacity",
        "input_order",
        "channel",
        "source_order",
    ]
    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    key = ["class_session_id", "bssid", "bin_number"]
    if result.duplicated(key).any():
        raise ValueError("selected observations require one row per BSSID/bin")
    result["source_order"] = (
        result["observation_time_source"]
        .map({"aruba_erm": 0, "frame_time_epoch": 1})
        .fillna(2)
        .astype("int64")
    )
    return result


def _valid_channel(value: Any) -> int | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value) or str(value).strip() == "":
            return None
        return _integer(value, name="channel", minimum=0, maximum=2**31 - 1)
    except ValueError:
        return None


def _source_rank(value: Any) -> int:
    if value is None or value is pd.NA:
        return 2**63 - 1
    try:
        if pd.isna(value) or str(value).strip() == "":
            return 2**63 - 1
        return _integer(value, name="source_rank", minimum=0, maximum=2**63 - 1)
    except ValueError:
        return 2**63 - 1


_OBSERVATION_ORDER = [
    "bin_number",
    "source_order",
    "source_rank",
    "observation_time",
    "source_capture",
    "source_frame_number",
    "source_frame",
    "bssid",
    "input_order",
]


def _threshold_metrics(
    states: list[tuple[str, int | None, int]],
    *,
    minimum_raw: int,
    has_valid_cu: bool,
) -> tuple[int | None, int | None, int | None]:
    if not has_valid_cu:
        return None, None, None
    total = 0
    possible_current = 0
    possible_max = 0
    actual_current = 0
    actual_max = 0
    for state, cu_raw, duration in states:
        is_high = state == "valid" and cu_raw is not None and cu_raw >= minimum_raw
        if is_high:
            total += duration
            possible_current += duration
            possible_max = max(possible_max, possible_current)
            actual_current += duration
            actual_max = max(actual_max, actual_current)
        elif state == "valid":
            possible_current = 0
            actual_current = 0
        else:
            actual_current = 0
    return total, possible_max, actual_max


def _single_or_none(values: Iterable[int]) -> int | None:
    unique = sorted(set(values))
    return unique[0] if len(unique) == 1 else None


def _single_text(values: Iterable[Any]) -> str | None:
    unique = sorted(
        {
            text
            for value in values
            if (text := _optional_text(value)) is not None
        }
    )
    return unique[0] if len(unique) == 1 else None


def _radio_row(
    *,
    run_id: str,
    schema_id: str,
    metric_id: str,
    session_id: str,
    classroom: str,
    bins: pd.DataFrame,
    identity: pd.DataFrame,
    observations: pd.DataFrame,
    radio_bin_rows: list[dict[str, Any]],
    primary_ssid: str | None,
) -> dict[str, Any]:
    radio_group_id = str(identity.iloc[0]["radio_group_id"])
    member_bssids = sorted(identity["bssid"].tolist())
    ordered_observations = observations.sort_values(
        _OBSERVATION_ORDER, kind="stable"
    )
    observation_groups = iter(
        groupby(
            ordered_observations.itertuples(index=False),
            key=lambda row: int(row.bin_number),
        )
    )
    next_observation_group = next(observation_groups, None)
    valid_observations = ordered_observations.loc[
        ordered_observations["qbss_cu_raw"].notna()
    ]
    primary_valid = (
        valid_observations.loc[valid_observations["ssid"].eq(primary_ssid)]
        if primary_ssid is not None
        else valid_observations.iloc[0:0]
    )
    if not primary_valid.empty:
        preferred_bssid = str(primary_valid.iloc[0]["bssid"])
        preferred_method = "primary_ssid"
    elif not valid_observations.empty:
        preferred_bssid = str(valid_observations.iloc[0]["bssid"])
        preferred_method = "first_valid_session_bssid"
    else:
        preferred_bssid = None
        preferred_method = None
    admission_preferred = preferred_admission_source(ordered_observations, primary_ssid)
    states: list[tuple[str, int | None, int]] = []
    weighted_total = 0
    valid_cu_raw_counts = [0] * 256
    valid_duration = 0
    missing_duration = 0
    no_beacon_duration = 0
    observed_channels: set[int] = {
        value for values in identity["channels"] for value in values
    }

    for bin_row in bins.itertuples(index=False):
        duration = int(bin_row.bin_duration_us)
        bin_number = int(bin_row.bin_number)
        if (
            next_observation_group is None
            or next_observation_group[0] != bin_number
        ):
            states.append(("no_beacon", None, duration))
            no_beacon_duration += duration
            radio_bin_rows.append(
                {
                    "run_id": run_id,
                    "schema_id": schema_id,
                    "metric_id": RADIO_BIN_METRIC_ID,
                    "class_session_id": session_id,
                    "classroom": classroom,
                    "radio_group_id": radio_group_id,
                    "ap_id": _single_text(identity["ap_id"]),
                    "band": identity.iloc[0]["band"],
                    "channel": _single_or_none(observed_channels),
                    "bin_number": bin_number,
                    "bin_start": bin_row.bin_start,
                    "bin_end": bin_row.bin_end,
                    "bin_duration_us": duration,
                    "width_status": bin_row.width_status,
                    "cu_state": "no_beacon",
                    "qbss_cu_raw": None,
                    "source_ssid": None,
                    "source_bssid": None,
                    "source_selection_method": None,
                    "source_observation_time": None,
                    "observed_member_bssid_count": 0,
                    "valid_cu_candidate_count": 0,
                    **empty_admission("no_beacon"),
                }
            )
            continue

        first_usable = None
        preferred_usable = None
        observed_count = 0
        valid_count = 0
        candidates = list(next_observation_group[1])
        for candidate in candidates:
            observed_count += 1
            if candidate.channel is not None and not pd.isna(candidate.channel):
                observed_channels.add(int(candidate.channel))
            if candidate.qbss_cu_raw is None or pd.isna(candidate.qbss_cu_raw):
                continue
            valid_count += 1
            if first_usable is None:
                first_usable = candidate
            if (
                preferred_usable is None
                and str(candidate.bssid) == preferred_bssid
            ):
                preferred_usable = candidate
        next_observation_group = next(observation_groups, None)

        chosen = preferred_usable or first_usable
        admission = resolve_admission(candidates, chosen, admission_preferred)

        if first_usable is None:
            states.append(("missing_cu", None, duration))
            missing_duration += duration
            radio_bin_rows.append(
                {
                    "run_id": run_id,
                    "schema_id": schema_id,
                    "metric_id": RADIO_BIN_METRIC_ID,
                    "class_session_id": session_id,
                    "classroom": classroom,
                    "radio_group_id": radio_group_id,
                    "ap_id": _single_text(identity["ap_id"]),
                    "band": identity.iloc[0]["band"],
                    "channel": _single_or_none(observed_channels),
                    "bin_number": bin_number,
                    "bin_start": bin_row.bin_start,
                    "bin_end": bin_row.bin_end,
                    "bin_duration_us": duration,
                    "width_status": bin_row.width_status,
                    "cu_state": "missing_cu",
                    "qbss_cu_raw": None,
                    "source_ssid": None,
                    "source_bssid": None,
                    "source_selection_method": None,
                    "source_observation_time": None,
                    "observed_member_bssid_count": observed_count,
                    "valid_cu_candidate_count": 0,
                    **admission,
                }
            )
            continue
        cu_raw = int(chosen.qbss_cu_raw)
        states.append(("valid", cu_raw, duration))
        valid_duration += duration
        weighted_total += cu_raw * duration
        valid_cu_raw_counts[cu_raw] += 1
        radio_bin_rows.append(
            {
                "run_id": run_id,
                "schema_id": schema_id,
                "metric_id": RADIO_BIN_METRIC_ID,
                "class_session_id": session_id,
                "classroom": classroom,
                "radio_group_id": radio_group_id,
                "ap_id": _single_text(identity["ap_id"]),
                "band": identity.iloc[0]["band"],
                "channel": (
                    int(chosen.channel)
                    if chosen.channel is not None and not pd.isna(chosen.channel)
                    else _single_or_none(observed_channels)
                ),
                "bin_number": bin_number,
                "bin_start": bin_row.bin_start,
                "bin_end": bin_row.bin_end,
                "bin_duration_us": duration,
                "width_status": bin_row.width_status,
                "cu_state": "valid",
                "qbss_cu_raw": cu_raw,
                "source_ssid": chosen.ssid,
                "source_bssid": str(chosen.bssid),
                "source_selection_method": (
                    preferred_method
                    if preferred_usable is not None
                    else "fallback_member_bssid"
                ),
                "source_observation_time": chosen.observation_time,
                "observed_member_bssid_count": observed_count,
                "valid_cu_candidate_count": valid_count,
                **admission,
            }
        )

    if next_observation_group is not None:
        raise AssertionError("radio observations were not consumed by class bins")

    class_duration = int(bins["bin_duration_us"].sum())
    if valid_duration + missing_duration + no_beacon_duration != class_duration:
        raise AssertionError("radio durations do not partition the class")
    average_raw = weighted_total / valid_duration if valid_duration else None
    high = _threshold_metrics(
        states, minimum_raw=HIGH_CU_MINIMUM_RAW, has_valid_cu=bool(valid_duration)
    )
    very_high = _threshold_metrics(
        states,
        minimum_raw=VERY_HIGH_CU_MINIMUM_RAW,
        has_valid_cu=bool(valid_duration),
    )
    return {
        "run_id": run_id,
        "schema_id": schema_id,
        "metric_id": metric_id,
        "class_session_id": session_id,
        "classroom": classroom,
        "class_start": bins["bin_start"].min(),
        "class_end": bins["bin_end"].max(),
        "radio_group_id": radio_group_id,
        "band": identity.iloc[0]["band"],
        "channel": _single_or_none(observed_channels),
        "member_bssids": member_bssids,
        "preferred_bssid": preferred_bssid,
        "class_duration_us": class_duration,
        "valid_cu_duration_us": valid_duration,
        "missing_cu_duration_us": missing_duration,
        "no_beacon_duration_us": no_beacon_duration,
        "cu_coverage": valid_duration / class_duration,
        "average_cu_raw": average_raw,
        "average_cu_ratio": None if average_raw is None else average_raw / 255.0,
        "valid_cu_raw_counts": valid_cu_raw_counts,
        "high_duration_us": high[0],
        "high_possible_connected_streak_us": high[1],
        "high_actual_observed_streak_us": high[2],
        "very_high_duration_us": very_high[0],
        "very_high_possible_connected_streak_us": very_high[1],
        "very_high_actual_observed_streak_us": very_high[2],
    }


def _maximum_with_radios(
    radio_rows: list[dict[str, Any]], field: str
) -> tuple[int | None, list[str] | None]:
    eligible = [row for row in radio_rows if row[field] is not None]
    if not eligible:
        return None, None
    maximum = max(int(row[field]) for row in eligible)
    radios = sorted(
        str(row["radio_group_id"])
        for row in eligible
        if int(row[field]) == maximum
    )
    return maximum, radios


def _band_counts(radio_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"2.4": 0, "5": 0, "6": 0, "unknown": 0}
    for row in radio_rows:
        band = row["band"]
        key = band if band in {"2.4", "5", "6"} else "unknown"
        counts[key] += 1
    return counts


def build_corrected_summaries(
    class_bins: pd.DataFrame,
    identity_map: pd.DataFrame,
    selected_observations: pd.DataFrame,
    *,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
    primary_ssid: str | None = None,
) -> CorrectedSummaryTables:
    """Calculate the two accepted ADR-0004 logical summary tables."""

    if not _SAFE_RUN_ID.fullmatch(str(run_id)):
        raise ValueError("run_id must be one safe path segment")
    primary_ssid = _optional_text(primary_ssid)
    bins = _normalize_bins(class_bins)
    identity = _normalize_identity(identity_map)
    selected = _normalize_selected(selected_observations)

    identity_by_bssid = identity.set_index("bssid")
    bin_keys = set(
        bins[["class_session_id", "classroom", "bin_number"]]
        .itertuples(index=False, name=None)
    )
    for row in selected.itertuples(index=False):
        if row.bssid not in identity_by_bssid.index:
            raise ValueError(f"selected BSSID is absent from identity map: {row.bssid}")
        mapped = identity_by_bssid.loc[row.bssid]
        if row.radio_group_id != mapped["radio_group_id"]:
            raise ValueError(f"selected BSSID has conflicting radio identity: {row.bssid}")
        if row.classroom != mapped["classroom"]:
            raise ValueError(f"selected BSSID has conflicting classroom: {row.bssid}")
        selected_bin_key = (
            row.class_session_id,
            row.classroom,
            row.bin_number,
        )
        if selected_bin_key not in bin_keys:
            raise ValueError(
                "selected observation references an unknown class bin: "
                f"session={row.class_session_id!r}, "
                f"classroom={row.classroom!r}, bin_number={row.bin_number!r}, "
                f"bssid={row.bssid!r}"
            )

    radio_rows: list[dict[str, Any]] = []
    classroom_rows: list[dict[str, Any]] = []
    radio_bin_rows: list[dict[str, Any]] = []
    for (session_id, classroom), session_bins in bins.groupby(
        ["class_session_id", "classroom"], sort=True
    ):
        room_identity = identity.loc[identity["classroom"].eq(classroom)]
        session_selected = selected.loc[
            selected["class_session_id"].eq(session_id)
            & selected["classroom"].eq(classroom)
        ]
        session_radio_rows: list[dict[str, Any]] = []
        for _radio_id, radio_identity in room_identity.groupby(
            "radio_group_id", sort=True
        ):
            members = set(radio_identity["bssid"])
            observations = session_selected.loc[
                session_selected["bssid"].isin(members)
            ]
            row = _radio_row(
                run_id=str(run_id),
                schema_id=str(schema_id),
                metric_id=str(metric_id),
                session_id=str(session_id),
                classroom=str(classroom),
                bins=session_bins,
                identity=radio_identity,
                observations=observations,
                radio_bin_rows=radio_bin_rows,
                primary_ssid=primary_ssid,
            )
            radio_rows.append(row)
            session_radio_rows.append(row)

        counts = _band_counts(session_radio_rows)
        high_possible = _maximum_with_radios(
            session_radio_rows, "high_possible_connected_streak_us"
        )
        high_actual = _maximum_with_radios(
            session_radio_rows, "high_actual_observed_streak_us"
        )
        very_high_possible = _maximum_with_radios(
            session_radio_rows, "very_high_possible_connected_streak_us"
        )
        very_high_actual = _maximum_with_radios(
            session_radio_rows, "very_high_actual_observed_streak_us"
        )
        classroom_rows.append(
            {
                "run_id": str(run_id),
                "schema_id": str(schema_id),
                "metric_id": str(metric_id),
                "class_session_id": str(session_id),
                "classroom": str(classroom),
                "class_start": session_bins["bin_start"].min(),
                "class_end": session_bins["bin_end"].max(),
                "class_duration_us": int(session_bins["bin_duration_us"].sum()),
                "radio_count_2_4_ghz": counts["2.4"],
                "radio_count_5_ghz": counts["5"],
                "radio_count_6_ghz": counts["6"],
                "radio_count_unknown": counts["unknown"],
                "radio_count_total": len(session_radio_rows),
                "high_possible_connected_max_us": high_possible[0],
                "high_possible_connected_responsible_radio_ids": high_possible[1],
                "high_actual_observed_max_us": high_actual[0],
                "high_actual_observed_responsible_radio_ids": high_actual[1],
                "very_high_possible_connected_max_us": very_high_possible[0],
                "very_high_possible_connected_responsible_radio_ids": (
                    very_high_possible[1]
                ),
                "very_high_actual_observed_max_us": very_high_actual[0],
                "very_high_actual_observed_responsible_radio_ids": very_high_actual[1],
            }
        )

    return CorrectedSummaryTables(
        radio_class=pd.DataFrame(
            radio_rows, columns=RADIO_CLASS_SUMMARY_ARROW_SCHEMA.names
        ),
        classroom_class=pd.DataFrame(
            classroom_rows, columns=CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA.names
        ),
        radio_bin_cu=pd.DataFrame(radio_bin_rows, columns=RADIO_BIN_CU_COLUMNS),
    )


def _arrow_table(frame: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    def arrow_value(value: Any) -> Any:
        if value is None or value is pd.NA or value is pd.NaT:
            return None
        try:
            missing = pd.isna(value)
        except (TypeError, ValueError):
            return value
        if hasattr(missing, "__len__"):
            return value
        return None if bool(missing) else value

    rows = [
        {name: arrow_value(value) for name, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]
    return pa.Table.from_pylist(rows, schema=schema)


def write_corrected_summaries(
    class_bins: pd.DataFrame,
    identity_map: pd.DataFrame,
    selected_observations: pd.DataFrame,
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> WrittenCorrectedSummaries:
    """Write both corrected summary tables without replacing existing files."""

    summaries = build_corrected_summaries(
        class_bins,
        identity_map,
        selected_observations,
        run_id=run_id,
        schema_id=schema_id,
        metric_id=metric_id,
    )
    return write_corrected_summary_tables(
        summaries,
        output_root=output_root,
        run_directory=run_directory,
        run_id=run_id,
    )


def write_corrected_summary_tables(
    summaries: CorrectedSummaryTables,
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
    run_id: str,
) -> WrittenCorrectedSummaries:
    """Write already-calculated bounded summary tables."""

    if not isinstance(summaries, CorrectedSummaryTables):
        raise TypeError("summaries must be CorrectedSummaryTables")
    if not _SAFE_RUN_ID.fullmatch(str(run_id)):
        raise ValueError("run_id must be one safe path segment")
    if (output_root is None) == (run_directory is None):
        raise ValueError("provide exactly one of output_root or run_directory")
    destination = (
        Path(run_directory)
        if run_directory is not None
        else Path(output_root) / run_id
    )
    missing_radio = _missing_columns(
        summaries.radio_class,
        set(RADIO_CLASS_SUMMARY_ARROW_SCHEMA.names),
    )
    missing_classroom = _missing_columns(
        summaries.classroom_class,
        set(CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA.names),
    )
    if missing_radio or missing_classroom:
        details = []
        if missing_radio:
            details.append("radio: " + ", ".join(missing_radio))
        if missing_classroom:
            details.append("classroom: " + ", ".join(missing_classroom))
        raise ValueError("summary tables are missing columns; " + "; ".join(details))
    if not summaries.radio_class.empty:
        duplicate_radio = summaries.radio_class.duplicated(
            subset=["class_session_id", "radio_group_id"], keep=False
        )
        if duplicate_radio.any():
            raise ValueError("radio summary requires one row per class/radio")
    if not summaries.classroom_class.empty:
        duplicate_classroom = summaries.classroom_class.duplicated(
            subset=["class_session_id", "classroom"], keep=False
        )
        if duplicate_classroom.any():
            raise ValueError(
                "classroom summary requires one row per class/classroom"
            )

    destination.mkdir(parents=True, exist_ok=True)
    radio_path = destination / RADIO_CLASS_SUMMARY_FILENAME
    classroom_path = destination / CLASSROOM_CLASS_SUMMARY_FILENAME
    for path in (radio_path, classroom_path):
        if path.exists():
            raise FileExistsError(f"summary artifact already exists: {path}")

    radio_temporary = radio_path.with_name(
        f".{radio_path.name}.{uuid.uuid4().hex}.tmp"
    )
    classroom_temporary = classroom_path.with_name(
        f".{classroom_path.name}.{uuid.uuid4().hex}.tmp"
    )
    radio_published = False
    try:
        pq.write_table(
            _arrow_table(summaries.radio_class, RADIO_CLASS_SUMMARY_ARROW_SCHEMA),
            radio_temporary,
        )
        pq.write_table(
            _arrow_table(
                summaries.classroom_class,
                CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
            ),
            classroom_temporary,
        )
        os.replace(radio_temporary, radio_path)
        radio_published = True
        os.replace(classroom_temporary, classroom_path)
    except BaseException:
        radio_temporary.unlink(missing_ok=True)
        classroom_temporary.unlink(missing_ok=True)
        if radio_published:
            radio_path.unlink(missing_ok=True)
        raise
    return WrittenCorrectedSummaries(
        radio_class_path=radio_path,
        classroom_class_path=classroom_path,
        radio_class_rows=len(summaries.radio_class),
        classroom_class_rows=len(summaries.classroom_class),
    )
