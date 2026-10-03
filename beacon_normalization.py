"""Shared normalization of decoded beacon evidence into selection columns."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any

import pandas as pd

from band_channel import attach_band_channel_evidence


def column_or_index(frame: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
        if frame.index.name == name:
            return pd.Series(frame.index, index=frame.index, name=name)
    return pd.Series(pd.NA, index=frame.index)


def normalized_text(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    return text.mask(text.eq("") | text.eq("<MISSING>"), pd.NA)


def _normalized_mac(values: pd.Series) -> pd.Series:
    return normalized_text(values).str.lower()


def _utc_time(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce", utc=True, format="mixed")


def _epoch_seconds(values: pd.Series) -> pd.Series:
    instants = []
    for value in values.array:
        if value is None or pd.isna(value) or str(value).strip() == "":
            instants.append(pd.NaT)
            continue
        try:
            seconds = Decimal(str(value))
            if not seconds.is_finite():
                raise InvalidOperation
            nanoseconds = int(
                (seconds * Decimal(1_000_000_000)).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
            instants.append(pd.Timestamp(nanoseconds, unit="ns", tz="UTC"))
        except (InvalidOperation, ValueError, OverflowError):
            instants.append(pd.NaT)
    return pd.Series(
        instants,
        index=values.index,
        dtype="datetime64[ns, UTC]",
    )


def _nullable_integer(values: pd.Series, dtype: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.where(numeric >= 0).astype(dtype)


def _with_default(
    existing: pd.Series,
    supplied: Any,
    *,
    dtype: str,
) -> pd.Series:
    if supplied is None:
        return existing.astype(dtype)
    replacement = pd.Series(supplied, index=existing.index, dtype=dtype)
    return existing.astype(dtype).fillna(replacement)


def normalize_beacon_evidence(
    records: pd.DataFrame,
    *,
    ap_name_fields: tuple[str, ...],
    source_capture: str | None = None,
    source_rank: int | None = None,
) -> pd.DataFrame:
    """Normalize vendor evidence without performing selection or I/O."""

    if not isinstance(records, pd.DataFrame):
        raise TypeError("records must be a pandas DataFrame")
    if source_rank is not None and (
        not isinstance(source_rank, int)
        or isinstance(source_rank, bool)
        or source_rank < 0
    ):
        raise ValueError("source_rank must be a non-negative integer")
    if "wlan.bssid" not in records and "bssid" not in records:
        raise ValueError("beacon records must include wlan.bssid")

    result = records.copy(deep=True)
    result["bssid"] = _normalized_mac(
        column_or_index(result, "wlan.bssid", "bssid")
    )
    result["transmitter_address"] = _normalized_mac(
        column_or_index(result, "wlan.ta", "transmitter_address")
    )
    result["ssid"] = normalized_text(
        column_or_index(result, "wlan.ssid", "ssid")
    )
    result["ap_name"] = normalized_text(
        column_or_index(result, "ap_name", *ap_name_fields)
    )

    canonical_aruba = column_or_index(result, "aruba_erm_time")
    raw_aruba = column_or_index(result, "aruba_erm.time")
    result["aruba_erm_time"] = _utc_time(canonical_aruba).fillna(
        _utc_time(raw_aruba)
    )

    canonical_frame = column_or_index(result, "frame_time_epoch")
    raw_frame = column_or_index(result, "frame.time_epoch")
    result["frame_time_epoch"] = _utc_time(canonical_frame).fillna(
        _epoch_seconds(raw_frame)
    )
    result["tsf_us"] = _nullable_integer(
        column_or_index(result, "tsf_us", "wlan.fixed.timestamp"),
        "UInt64",
    )
    result["beacon_interval_tu"] = _nullable_integer(
        column_or_index(result, "beacon_interval_tu", "wlan.fixed.beacon"),
        "UInt32",
    )
    result["qbss_cu_raw"] = _nullable_integer(
        column_or_index(result, "wlan.qbss.cu", "qbss_cu_raw"),
        "Int64",
    )
    result["qbss_station_count"] = _nullable_integer(
        column_or_index(
            result,
            "wlan.qbss.scount",
            "qbss_station_count",
        ),
        "Int64",
    )
    result["qbss_admission_capacity"] = _nullable_integer(
        column_or_index(
            result,
            "wlan.qbss.adc",
            "qbss_admission_capacity",
        ),
        "Int64",
    )
    result = attach_band_channel_evidence(result)

    capture = normalized_text(column_or_index(result, "source_capture"))
    result["source_capture"] = _with_default(
        capture,
        source_capture,
        dtype="string",
    )
    ranks = _nullable_integer(column_or_index(result, "source_rank"), "Int64")
    result["source_rank"] = _with_default(
        ranks,
        0 if source_rank is None else source_rank,
        dtype="Int64",
    )
    result["source_frame"] = _nullable_integer(
        column_or_index(result, "source_frame", "frame.number"),
        "Int64",
    )

    if "input_order" not in result:
        result["input_order"] = pd.array(range(len(result)), dtype="Int64")
    return result
