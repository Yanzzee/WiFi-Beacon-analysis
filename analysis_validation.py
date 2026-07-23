"""Shared input validation for analysis stages."""

import glob
import os

import pandas as pd


def validate_radio_inference_platform(ap_platform):
    configured_platform = (ap_platform or "").strip().lower()
    if configured_platform != "aruba":
        configured_display = ap_platform if ap_platform else "<unset>"
        raise ValueError(
            "analyze_data.py radio inference is supported only when "
            "AP_PLATFORM=aruba. The current algorithm groups adjacent transmitter "
            "MAC addresses using Aruba-specific allocation assumptions; channel- "
            "or vendor-neutral radio inference is deferred until a dedicated "
            f"analysis change. Configured AP_PLATFORM: {configured_display}."
        )


def validate_no_existing_analysis_outputs(sampled_folder, data_folder):
    existing_outputs = []

    sampled_files = glob.glob(
        os.path.join(sampled_folder, "*_class_beacons.parquet")
    )
    existing_outputs.extend(sampled_files)

    for output_file in [
        os.path.join(data_folder, "all_class_beacons.parquet"),
        os.path.join(data_folder, "radio_summary.parquet"),
    ]:
        if os.path.exists(output_file):
            existing_outputs.append(output_file)

    if existing_outputs:
        output_list = "\n".join(f"- {path}" for path in sorted(existing_outputs))
        raise FileExistsError(
            "Existing analyze_data outputs were found. "
            "Use a fresh workspace or remove these generated files before rerunning:\n"
            f"{output_list}"
        )


def validate_grouped_files_for_sample_aps(
    sample_aps_df,
    grouped_data_folder,
    sample_classes_df=None,
):
    if "Hostname" not in sample_aps_df.columns:
        raise ValueError(
            "Sample AP metadata must include a Hostname column for grouped AP lookup."
        )

    scoped_sample_aps_df = sample_aps_df
    if (
        sample_classes_df is not None
        and "Building-Room" in sample_aps_df.columns
        and "location" in sample_classes_df.columns
    ):
        sampled_locations = set(
            sample_classes_df["location"].dropna().astype("string").str.strip()
        )
        scoped_sample_aps_df = sample_aps_df[
            sample_aps_df["Building-Room"].astype("string").str.strip().isin(sampled_locations)
        ]

    hostnames = scoped_sample_aps_df["Hostname"].dropna().astype("string").str.strip()
    expected_paths = []
    for hostname in sorted(hostname for hostname in hostnames.unique().tolist() if hostname):
        expected_path = os.path.join(grouped_data_folder, f"{hostname}.parquet")
        if not os.path.exists(expected_path):
            expected_paths.append(expected_path)

    if expected_paths:
        expected_list = "\n".join(f"- {path}" for path in expected_paths)
        raise FileNotFoundError(
            "Missing grouped AP parquet files for sample AP Hostname values. "
            "analyze_data.py currently loads grouped data by Sample/sample_aps.parquet "
            "Hostname. BSSID fallback files cannot be reliably associated with all "
            "SSIDs on the same radio or all radios on the same AP without vendor AP-name "
            "metadata. Aruba AP-name beacons are supported now; broader vendor mapping "
            "is deferred. Expected files:\n"
            f"{expected_list}"
        )


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
