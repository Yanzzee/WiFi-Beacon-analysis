"""Dataframe preparation helpers for radio-summary analysis."""

import pandas as pd


def sort_radio_summary(radio_summary_df):
    return radio_summary_df.sort_values(by="total_beacons")


def prepare_radio_summary_frames(radio_summary_df, log_and_print):
    # Compute class duration in seconds, add as a new column
    radio_summary_df["class_duration"] = (
        radio_summary_df["end_time"] - radio_summary_df["start_time"]
    ).dt.total_seconds()

    filtered_radio_summary_df = radio_summary_df[
        (radio_summary_df["total_beacons"] >= 300)
        & (radio_summary_df["class_duration"] > 0)
    ].copy()

    # Avoid division by zero or missing SSID counts downstream.
    filtered_radio_summary_df["ssid_count"] = pd.to_numeric(
        filtered_radio_summary_df["ssid_count"],
        errors="coerce",
    )
    valid_ssid_count_mask = filtered_radio_summary_df["ssid_count"] > 0
    excluded_ssid_count_rows = (
        len(filtered_radio_summary_df) - valid_ssid_count_mask.sum()
    )
    if excluded_ssid_count_rows:
        log_and_print(
            f"Excluding {excluded_ssid_count_rows} rows with missing or "
            "non-positive ssid_count."
        )
    filtered_radio_summary_df = filtered_radio_summary_df[
        valid_ssid_count_mask
    ].copy()

    filtered_radio_summary_df["beacons_per_second"] = (
        filtered_radio_summary_df["total_beacons"]
        / filtered_radio_summary_df["class_duration"]
    )

    df_radio_1 = filtered_radio_summary_df[
        filtered_radio_summary_df["radio_count"] == 1
    ]
    df_radio_2 = filtered_radio_summary_df[
        filtered_radio_summary_df["radio_count"] == 2
    ]
    combined_df = filtered_radio_summary_df[
        filtered_radio_summary_df["radio_count"].isin([1, 2])
    ]

    return (
        filtered_radio_summary_df,
        df_radio_1,
        df_radio_2,
        combined_df,
    )
