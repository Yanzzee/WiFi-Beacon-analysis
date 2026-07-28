"""Metric calculation helpers for class-session analysis."""

import pandas as pd
from tqdm import tqdm


CU_THRESHOLDS = {"75": 191, "50": 127}
METRIC_COLUMNS = (
    "max_stations",
    "stations_enrolled",
    "max_cu",
    "average_cu",
    "median_cu",
    "number_beacons",
    "duration_75",
    "duration_50",
    "max_streak_75",
    "max_streak_50",
    "sum_scount_75",
    "sum_scount_50",
    "number_radios",
)


def initialize_class_sessions(
    class_sessions_df,
    sample_aps_df,
    sample_classrooms_df,
):
    class_sessions_df["group"] = pd.Series(dtype="string")

    class_sessions_df["number_aps"] = pd.Series(dtype="Int64")
    class_sessions_df["number_radios"] = pd.Series(dtype="Int64")
    class_sessions_df["number_beacons"] = pd.Series(dtype="Int64")
    class_sessions_df["max_stations"] = pd.Series(dtype="Int64")
    class_sessions_df["max_cu"] = pd.Series(dtype="Int64")
    class_sessions_df["average_cu"] = pd.Series(dtype="Float64")
    class_sessions_df["median_cu"] = pd.Series(dtype="Float64")
    class_sessions_df["stations_enrolled"] = pd.Series(dtype="Float64")

    class_sessions_df["class_duration"] = (
        class_sessions_df["end_time"] - class_sessions_df["start_time"]
    ).dt.total_seconds().astype("Float64")

    class_sessions_df["duration_75"] = pd.Series(dtype="Float64")
    class_sessions_df["duration_50"] = pd.Series(dtype="Float64")
    class_sessions_df["max_streak_75"] = pd.Series(dtype="Float64")
    class_sessions_df["max_streak_50"] = pd.Series(dtype="Float64")
    class_sessions_df["sum_scount_75"] = pd.Series(dtype="Float64")
    class_sessions_df["sum_scount_50"] = pd.Series(dtype="Float64")

    aps_count = sample_aps_df["Building-Room"].value_counts()
    class_sessions_df["number_aps"] = (
        class_sessions_df["location"].map(aps_count).astype("Int64")
    )

    location_to_group = sample_classrooms_df.set_index("Location")["Group"]
    class_sessions_df["group"] = (
        class_sessions_df["location"].map(location_to_group).astype("string")
    )
    return class_sessions_df


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


def calculate_all_class_session_metrics(
    class_sessions_df,
    all_class_beacons_processed_df,
    beacon_window_sec,
    radio_count_ssid,
):
    print("Calculating metrics for each class session...")
    sessions = tqdm(
        class_sessions_df.iterrows(),
        total=len(class_sessions_df),
    )
    for idx, session in sessions:
        metrics = calculate_class_session_metrics(
            session,
            all_class_beacons_processed_df,
            beacon_window_sec,
            radio_count_ssid,
        )
        for column in METRIC_COLUMNS:
            class_sessions_df.at[idx, column] = metrics[column]
    return class_sessions_df


def report_class_session_aggregates(class_sessions_df, log_and_print):
    total_sessions = len(class_sessions_df)
    sessions_over_50 = (class_sessions_df["duration_50"] > 0).sum()
    sessions_over_75 = (class_sessions_df["duration_75"] > 0).sum()
    percent_over_50 = (sessions_over_50 / total_sessions) * 100
    percent_over_75 = (sessions_over_75 / total_sessions) * 100

    log_and_print(
        "Percentage of class sessions with at least 1s CU > 50%: "
        f"{percent_over_50:.2f}%"
    )
    log_and_print(
        "Percentage of class sessions with at least 1s CU > 75%: "
        f"{percent_over_75:.2f}%"
    )

    total_duration_50 = class_sessions_df["duration_50"].sum()
    total_duration_75 = class_sessions_df["duration_75"].sum()
    total_duration_all = class_sessions_df["class_duration"].sum()
    total_scount_50 = class_sessions_df["sum_scount_50"].sum()
    total_scount_75 = class_sessions_df["sum_scount_75"].sum()
    total_scount_all = class_sessions_df["max_stations"].sum()

    log_and_print(
        f"Total duration all classes: {total_duration_all:.2f} seconds"
    )
    log_and_print(
        f"Total class duration with CU > 50%: "
        f"{total_duration_50:.2f} seconds "
        f"({total_duration_50 / total_duration_all * 100:.2f}%)"
    )
    log_and_print(
        f"Total class duration with CU > 75%: "
        f"{total_duration_75:.2f} seconds "
        f"({total_duration_75 / total_duration_all * 100:.2f}%)"
    )
    log_and_print(f"Total stations in all classes: {int(total_scount_all)}")
    log_and_print(
        f"Total stations affected (CU > 50%): {int(total_scount_50)} "
        f"({total_scount_50 / total_scount_all * 100:.2f}%)"
    )
    log_and_print(
        f"Total stations affected (CU > 75%): {int(total_scount_75)} "
        f"({total_scount_75 / total_scount_all * 100:.2f}%)"
    )

    mean_streak_50 = class_sessions_df["max_streak_50"].mean()
    median_streak_50 = class_sessions_df["max_streak_50"].median()
    mean_streak_75 = class_sessions_df["max_streak_75"].mean()
    median_streak_75 = class_sessions_df["max_streak_75"].median()

    log_and_print("Duration of longest continuous channel utilization")
    log_and_print(
        f"CU > 50% - Mean Streak: {mean_streak_50:.2f} sec, "
        f"Median Streak: {median_streak_50:.2f} sec"
    )
    log_and_print(
        f"CU > 75% - Mean Streak: {mean_streak_75:.2f} sec, "
        f"Median Streak: {median_streak_75:.2f} sec"
    )


def report_average_devices_per_enrolled(
    class_sessions_df,
    log_and_print,
):
    total_max_stations = class_sessions_df["max_stations"].sum()
    total_enrolled = class_sessions_df["enrolled"].sum()
    if total_enrolled == 0:
        average_devices_per_enrolled = 0
    else:
        average_devices_per_enrolled = (
            total_max_stations / total_enrolled
        )
    log_and_print(
        f"Average devices per enrolled: "
        f"{average_devices_per_enrolled:.2f}"
    )
    return average_devices_per_enrolled
