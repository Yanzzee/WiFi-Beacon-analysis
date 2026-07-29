"""Dataframe preparation helpers for radio-summary analysis."""

import pandas as pd

from analysis_statistics import compute_pdf_cdf


def sort_radio_summary(radio_summary_df):
    return radio_summary_df.sort_values(by="total_beacons")


def report_radio_summary_statistics(radio_summary_df, log_and_print):
    log_and_print(
        "rows in the radio_summary_df represent a single 5GHz radio per "
        "class session"
    )
    log_and_print(f"total number of rows: {len(radio_summary_df)}")
    log_and_print(
        f"median number of beacons: "
        f"{radio_summary_df['total_beacons'].median()}"
    )
    log_and_print(
        f"median row of high channel utilization duration: "
        f"{radio_summary_df['longest_duration_high_cu'].median()}"
    )
    log_and_print(
        f"(channel utilization is in percentage: "
        f"{radio_summary_df['highest_cu'].max()})"
    )
    log_and_print(
        f"number of rows with total beacons less than 200: "
        f"{(radio_summary_df['total_beacons'] < 200).sum()}"
    )
    log_and_print(
        f"number of rows with some very high channel use (>75%): "
        f"{(radio_summary_df['highest_cu'] > 75).sum()}"
    )
    log_and_print(
        f"number of rows with some high channel use (>50%): "
        f"{(radio_summary_df['highest_cu'] > 50).sum()}"
    )
    log_and_print(
        f"median number of devices per radio (peak per class session): "
        f"{radio_summary_df['high_scount'].median()}"
    )
    log_and_print(
        f"average number of devices per radio (peak per class session): "
        f"{radio_summary_df['high_scount'].mean():.2f}"
    )

    correlation = radio_summary_df["high_scount"].corr(
        radio_summary_df["median_cu"]
    )
    log_and_print(
        "The correlation between high_scount and median channel "
        f"utilization is: {correlation}"
    )
    return correlation


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


def normalize_beacon_rate_series(df_radio_1, df_radio_2, combined_df):
    radio_1_beacon_rate = (
        df_radio_1["beacons_per_second"] / df_radio_1["ssid_count"]
    ).dropna()
    radio_2_beacon_rate = (
        df_radio_2["beacons_per_second"] / df_radio_2["ssid_count"]
    ).dropna()
    combined_beacon_rate = (
        combined_df["beacons_per_second"] / combined_df["ssid_count"]
    ).dropna()
    return (
        radio_1_beacon_rate,
        radio_2_beacon_rate,
        combined_beacon_rate,
    )


def compute_beacon_rate_distributions(
    radio_1_beacon_rate,
    radio_2_beacon_rate,
    combined_beacon_rate,
):
    return (
        compute_pdf_cdf(radio_1_beacon_rate, bins=50),
        compute_pdf_cdf(radio_2_beacon_rate, bins=50),
        compute_pdf_cdf(combined_beacon_rate, bins=50),
    )


def print_beacon_rate_null_counts(
    radio_1_beacon_rate,
    radio_2_beacon_rate,
):
    print(
        "Radio 1 - Count:",
        radio_1_beacon_rate.count(),
        "NaNs:",
        radio_1_beacon_rate.isna().sum(),
    )
    print(
        "Radio 2 - Count:",
        radio_2_beacon_rate.count(),
        "NaNs:",
        radio_2_beacon_rate.isna().sum(),
    )
