"""Metric calculation helpers for class-session analysis."""

import pandas as pd


CU_THRESHOLDS = {"75": 191, "50": 127}


def calculate_class_session_metrics(
    session,
    all_class_beacons_processed_df,
    beacon_window_sec,
    radio_count_ssid,
):
    start = session["start_time"]
    end = session["end_time"]
    location = session["location"]

    mask = (
        (all_class_beacons_processed_df["aruba_erm.time"] >= start)
        & (all_class_beacons_processed_df["aruba_erm.time"] <= end)
        & (all_class_beacons_processed_df["location"] == location)
    )
    filtered = all_class_beacons_processed_df[mask].copy()
    filtered.sort_values("aruba_erm.time", inplace=True)

    number_beacons = len(filtered)

    if number_beacons > 0:
        max_scount = filtered["room_scount"].max()
        cu_values = filtered["wlan.qbss.cu"].dropna()
        max_cu = cu_values.max()
        avg_cu = cu_values.mean()
        median_cu = cu_values.median()

        # Time binning for duration + streaks
        time_range = pd.date_range(
            start,
            end,
            freq=f"{beacon_window_sec}s",
        )
        duration = {"75": 0, "50": 0}
        current_streak = {"75": 0, "50": 0}
        max_streak = {"75": 0, "50": 0}

        for i in range(len(time_range) - 1):
            t_start = time_range[i]
            t_end = time_range[i + 1]

            bin_slice = filtered[
                (filtered["aruba_erm.time"] >= t_start)
                & (filtered["aruba_erm.time"] < t_end)
            ]

            if not bin_slice.empty:
                for level, threshold in CU_THRESHOLDS.items():
                    if (bin_slice["wlan.qbss.cu"] > threshold).any():
                        duration[level] += beacon_window_sec
                        current_streak[level] += beacon_window_sec
                        max_streak[level] = max(
                            max_streak[level],
                            current_streak[level],
                        )
                    else:
                        current_streak[level] = 0

        # Scount sums for each threshold based on unique wlan.ta
        sum_scount = {}
        for level, threshold in CU_THRESHOLDS.items():
            cu_filtered = filtered[filtered["wlan.qbss.cu"] > threshold]
            # Drop duplicate wlan.ta, keep first (could use .max() if preferred)
            unique_scount = cu_filtered.drop_duplicates("wlan.ta")
            sum_scount[level] = unique_scount["wlan.qbss.scount"].sum()
        ssid_radios = filtered[
            filtered["wlan.ssid"] == radio_count_ssid
        ]["wlan.ta"].nunique()
    else:
        max_scount = max_cu = avg_cu = median_cu = 0
        duration = {"75": 0, "50": 0}
        max_streak = {"75": 0, "50": 0}
        sum_scount = {"75": 0, "50": 0}
        ssid_radios = 0

    return {
        "max_stations": max_scount,
        "stations_enrolled": (
            max_scount / session["enrolled"] if session["enrolled"] else 0
        ),
        "max_cu": max_cu,
        "average_cu": avg_cu,
        "median_cu": median_cu,
        "number_beacons": number_beacons,
        "duration_75": duration["75"],
        "duration_50": duration["50"],
        "max_streak_75": max_streak["75"],
        "max_streak_50": max_streak["50"],
        "sum_scount_75": sum_scount["75"],
        "sum_scount_50": sum_scount["50"],
        "number_radios": ssid_radios,
    }
