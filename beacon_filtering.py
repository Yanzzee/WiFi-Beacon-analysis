"""Time-based beacon filtering utilities."""

import pandas as pd


def filter_by_time(df, beacon_rate):
    df = df.sort_values("aruba_erm.time")
    keep_rows = [df.iloc[0]]

    last_time = df.iloc[0]["aruba_erm.time"]
    for _, row in df.iloc[1:].iterrows():
        if (
            row["aruba_erm.time"] - last_time
        ).total_seconds() >= (0.95 * beacon_rate):
            keep_rows.append(row)
            last_time = row["aruba_erm.time"]

    return pd.DataFrame(keep_rows)


def remove_duplicate_beacons(ap_df, beacon_rate):
    # Keep the existing groupby().apply() behavior, including its current
    # pandas deprecation warning.
    filtered_ap_df = (
        ap_df.groupby(
            "wlan.ta",
            group_keys=False,
            observed=True,
        )
        .apply(
            lambda group: filter_by_time(
                group.reset_index(),
                beacon_rate,
            )
        )
        .reset_index(drop=True)
    )

    string_columns = [
        "wlan.ta",
        "wlan.vs.aruba.ap_name",
        "wlan.ssid",
    ]
    filtered_ap_df[string_columns] = filtered_ap_df[
        string_columns
    ].astype("string")

    return filtered_ap_df
