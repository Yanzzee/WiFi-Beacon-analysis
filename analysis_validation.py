"""Shared input validation for analysis stages."""

import pandas as pd


REQUIRED_GROUPED_AP_COLUMNS = (
    "wlan.ta",
    "wlan.vs.aruba.ap_name",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
)


def _grouped_timestamp_series(ap_df):
    if "aruba_erm.time" in ap_df.columns:
        return pd.Series(ap_df["aruba_erm.time"])
    if ap_df.index.name == "aruba_erm.time":
        return pd.Series(ap_df.index)
    return None


def validate_grouped_ap_timestamps(ap_df, file_path, ap_name):
    timestamps = _grouped_timestamp_series(ap_df)
    if timestamps is None:
        return

    if int(timestamps.notna().sum()) == 0:
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} has no non-null "
            "aruba_erm.time values."
        )

    if not pd.api.types.is_datetime64_any_dtype(timestamps):
        parsed = pd.to_datetime(timestamps, errors="coerce", utc=True)
        invalid_count = int((timestamps.notna() & parsed.isna()).sum())
        invalid_detail = (
            f" Found {invalid_count} non-null timestamp values that pandas could not parse."
            if invalid_count
            else ""
        )
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} must store "
            "aruba_erm.time as a timezone-aware datetime dtype before "
            "script analysis. Mixed string/input timestamp normalization remains "
            "deferred to a dedicated normalization change."
            f"{invalid_detail}"
        )

    if getattr(timestamps.dt, "tz", None) is None:
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} has timezone-naive "
            "aruba_erm.time values. Run or repair grouped-data cleanup so "
            "timestamps are timezone-aware before script analysis."
        )


def validate_grouped_ap_schema(ap_df, file_path, ap_name):
    missing_columns = [
        column for column in REQUIRED_GROUPED_AP_COLUMNS
        if column not in ap_df.columns
    ]
    if "aruba_erm.time" not in ap_df.columns and ap_df.index.name != "aruba_erm.time":
        missing_columns.insert(0, "aruba_erm.time")

    if not missing_columns:
        validate_grouped_ap_timestamps(ap_df, file_path, ap_name)
        return

    missing_text = ", ".join(missing_columns)
    message = (
        f"Grouped AP data for {ap_name} at {file_path} is missing required "
        f"analysis columns: {missing_text}. The current analysis pipeline "
        "require Aruba-shaped grouped data with aruba_erm.time and "
        "wlan.vs.aruba.ap_name."
    )
    if "frame.time_epoch" in ap_df.columns or ap_df.index.name == "frame.time_epoch":
        message += (
            " Detected native --no-erm conversion output with frame.time_epoch. "
            "Native/non-Aruba captures are conversion-only for now and need "
            "a timestamp plus AP-name mapping/adaptation before script analysis."
        )
    raise ValueError(message)
