"""Core orchestration helpers for the primary analysis stage."""

import pandas as pd
from tqdm import tqdm


RADIO_SUMMARY_COLUMNS = {
    "subject": str,
    "location": str,
    "AP_hostname": str,
    "radio_number": int,
    "start_time": "datetime64[ns]",
    "end_time": "datetime64[ns]",
    "capacity": int,
    "enrolled": int,
    "AP_count": int,
    "radio_count": int,
    "bssid_count": int,
    "ssid_count": int,
    "total_beacons": int,
    "high_cu_beacons": int,
    "percent_high_cu": float,
    "highest_cu": int,
    "median_cu": int,
    "lowest_cu": int,
    "high_scount": int,
    "median_scount": int,
    "low_scount": int,
    "longest_duration_high_cu": float,
    "graph_filename": str,
}


def create_radio_summary_dataframe():
    return pd.DataFrame(
        {
            column: pd.Series(dtype=dtype)
            for column, dtype in RADIO_SUMMARY_COLUMNS.items()
        }
    )


def validate_no_missing_cu(
    df,
    ap_name,
    start_time,
    end_time,
):
    missing_mask = df["wlan.qbss.cu"].isna()
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return

    total_count = len(df)
    missing_percent = (
        (missing_count / total_count) * 100 if total_count else 0
    )
    example_columns = [
        column
        for column in (
            "aruba_erm.time",
            "wlan.ta",
            "wlan.bssid",
            "wlan.ssid",
        )
        if column in df.columns
    ]
    examples = (
        df.loc[missing_mask, example_columns]
        .head(5)
        .to_dict("records")
    )
    raise ValueError(
        f"Grouped AP data for {ap_name} from {start_time} to "
        f"{end_time} contains {missing_count} of {total_count} "
        "deduplicated in-scope rows "
        f"({missing_percent:.4f}%) with missing wlan.qbss.cu. "
        "Current analysis does not define missing-CU semantics and "
        "would otherwise convert these values to measured zero. "
        f"Example source rows: {examples}"
    )


def analyze_ap_class_session(
    ap_name,
    start_time,
    end_time,
    location,
    course,
    enrolled,
    capacity,
    ap_count,
    ap_df,
    radio_summary_df,
    *,
    timezone,
    beacon_rate,
    plots_folder,
    plot_timezone,
    log_and_print,
    remove_duplicate_beacons,
    add_aruba_transmitter_integers,
    assign_aruba_radio_numbers,
    calculate_total_scount,
    add_to_all_class_beacons,
    analyze_radio_group,
    plot_channel_stations,
    validate_missing_cu=validate_no_missing_cu,
):
    log_and_print(
        "\n-----------------------------------------------------------------\n"
    )
    log_and_print(f"AP name: {ap_name}")

    filtered_df = ap_df.copy()
    filtered_df.reset_index(inplace=True)

    if filtered_df["aruba_erm.time"].dt.tz is None:
        filtered_df["aruba_erm.time"] = filtered_df[
            "aruba_erm.time"
        ].dt.tz_localize("UTC")

    filtered_df["aruba_erm.time"] = filtered_df[
        "aruba_erm.time"
    ].dt.tz_convert(timezone)
    filtered_df = filtered_df[
        (filtered_df["aruba_erm.time"] >= start_time)
        & (filtered_df["aruba_erm.time"] <= end_time)
    ]

    if filtered_df.empty:
        log_and_print(
            f"No data available for {ap_name} from {start_time} "
            f"to {end_time}."
        )
        return

    filtered_df = remove_duplicate_beacons(
        filtered_df,
        beacon_rate,
    )
    validate_missing_cu(
        filtered_df,
        ap_name,
        start_time,
        end_time,
    )
    filtered_df = filtered_df.sort_values(
        by="aruba_erm.time"
    ).reset_index(drop=True)

    filtered_df["wlan.qbss.scount"] = (
        filtered_df["wlan.qbss.scount"].fillna(0).astype(int)
    )
    filtered_df["wlan.qbss.cu"] = (
        filtered_df["wlan.qbss.cu"].fillna(0).astype(int)
    )
    filtered_df["wlan.qbss.adc"] = (
        filtered_df["wlan.qbss.adc"].fillna(0).astype(int)
    )

    filtered_df = add_aruba_transmitter_integers(filtered_df)

    if filtered_df["wlan.vs.aruba.ap_name"].nunique() != 1:
        raise ValueError(
            "Values in wlan.vs.aruba.ap_name are not all the same."
        )

    bssid_count = filtered_df["wlan.ta"].nunique()
    ssid_count = filtered_df["wlan.ssid"].nunique()

    filtered_df = assign_aruba_radio_numbers(filtered_df)
    filtered_df = calculate_total_scount(filtered_df)
    add_to_all_class_beacons(filtered_df)

    radio_numbers = sorted(
        filtered_df["radio_number"].dropna().unique()
    )
    radio_count = len(radio_numbers)
    log_and_print(f"Number of radios: {radio_count}")
    log_and_print(f"Number of BSSIDs: {bssid_count}")
    log_and_print(f"Number of SSIDs: {ssid_count}")

    for current_radio_number in radio_numbers:
        radio_number_value = int(current_radio_number)
        filtered_radio_df = filtered_df[
            filtered_df["radio_number"] == current_radio_number
        ]
        if filtered_radio_df.empty:
            continue

        (
            total_beacons,
            high_cu_beacons,
            percent_high_cu,
            highest_cu,
            median_cu,
            lowest_cu,
            high_scount,
            median_scount,
            low_scount,
            longest_duration_high_cu,
        ) = analyze_radio_group(
            filtered_radio_df,
            radio_number_value,
            beacon_rate,
            log_and_print,
        )

        eduroam_radio_df = None
        byu_wifi_radio_df = None
        plot_file = plot_channel_stations(
            filtered_radio_df,
            eduroam_radio_df,
            byu_wifi_radio_df,
            ap_name,
            start_time,
            end_time,
            plots_folder,
            plot_timezone,
        )

        radio_summary_df.loc[len(radio_summary_df)] = [
            course,
            location,
            ap_name,
            radio_number_value,
            start_time,
            end_time,
            capacity,
            enrolled,
            ap_count,
            radio_count,
            bssid_count,
            ssid_count,
            total_beacons,
            high_cu_beacons,
            percent_high_cu,
            highest_cu,
            median_cu,
            lowest_cu,
            high_scount,
            median_scount,
            low_scount,
            longest_duration_high_cu,
            plot_file,
        ]

    if radio_count == 1:
        log_and_print(f"{ap_name} has only one radio")


def process_class_samples(
    sample_aps_df,
    sample_classes_df,
    sample_classrooms_df,
    *,
    grouped_data_folder,
    log_and_print,
    graph,
    append_to_parquet,
    get_all_class_beacons,
    clear_all_class_beacons,
    validate_grouped_ap_schema,
):
    radio_summary_df = create_radio_summary_dataframe()
    base_dir = grouped_data_folder
    ap_df = pd.DataFrame()
    ap_name = ""

    for _index, class_row in tqdm(
        sample_classes_df.iterrows(),
        total=len(sample_classes_df),
    ):
        location = class_row["location"]
        start_time = class_row["start_time"]
        end_time = class_row["end_time"]
        course = class_row["course"]
        enrolled = class_row["enrolled"]
        capacity = class_row["capacity"]
        ap_count = sample_classrooms_df.loc[
            sample_classrooms_df["Location"] == location,
            "ap_count",
        ].iloc[0]

        log_and_print("\n-----------------------------------------------------------------")
        log_and_print(
            "-----------------------------------------------------------------\n"
        )
        log_and_print(f"Subject: {course}")
        log_and_print(f"Location: {location}")
        log_and_print(f"Start Time: {start_time}")
        log_and_print(f"End Time: {end_time}")
        log_and_print(f"Capacity: {capacity}")
        log_and_print(f"Enrolled: {enrolled}")
        log_and_print(f"Number of APs: {ap_count}")
        log_and_print("")

        for _ap_index, ap_row in sample_aps_df.iterrows():
            building_room = ap_row["Building-Room"]

            if building_room == location:
                if ap_name != ap_row["Hostname"]:
                    ap_name = ap_row["Hostname"]
                    file_name = ap_name + ".parquet"
                    ap_path = f"{base_dir}/{file_name}"
                    ap_df = pd.read_parquet(
                        ap_path,
                        engine="pyarrow",
                    )
                    validate_grouped_ap_schema(
                        ap_df,
                        ap_path,
                        ap_name,
                    )

                graph(
                    ap_name,
                    start_time,
                    end_time,
                    location,
                    course,
                    enrolled,
                    capacity,
                    ap_count,
                    ap_df,
                    radio_summary_df,
                )

            append_to_parquet(
                ap_name,
                get_all_class_beacons(),
            )
            clear_all_class_beacons()

    return radio_summary_df
