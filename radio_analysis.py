"""Per-radio dataframe calculations for beacon analysis."""

import pandas as pd


def calculate_total_scount(df):
    radio_transmitters_by_number = {
        radio_number: group["wlan.ta"].drop_duplicates().tolist()
        for radio_number, group in df.groupby(
            "radio_number",
            dropna=False,
            observed=True,
        )
    }

    if "total_scount" not in df.columns:
        df["total_scount"] = 0

    for i in range(len(df)):
        current_ta = df.at[i, "wlan.ta"]
        current_scount = df.at[i, "wlan.qbss.scount"]
        current_radio = df.at[i, "radio_number"]

        if pd.isna(current_radio):
            raise ValueError(
                "Missing radio_number while calculating total_scount."
            )

        radio_transmitters = radio_transmitters_by_number.get(
            current_radio
        )
        if radio_transmitters is None:
            raise ValueError(
                "No transmitter list found for "
                f"radio_number {current_radio}."
            )
        radio_transmitters = radio_transmitters.copy()

        if current_ta not in radio_transmitters:
            raise ValueError(
                f"Transmitter {current_ta} not found for "
                f"radio_number {current_radio}."
            )
        radio_transmitters.remove(current_ta)

        for transmitter in radio_transmitters:
            j = i + 1
            while j < len(df):
                next_ta = df.at[j, "wlan.ta"]
                if next_ta == transmitter:
                    next_scount = df.at[j, "wlan.qbss.scount"]
                    current_scount += next_scount
                    break
                j += 1

            if j >= len(df):
                k = i - 1
                while k > 0:
                    prev_ta = df.at[k, "wlan.ta"]
                    if prev_ta == transmitter:
                        prev_scount = df.at[k, "wlan.qbss.scount"]
                        current_scount += prev_scount
                        break
                    k -= 1

        df.at[i, "total_scount"] = current_scount

    df["total_scount"] = df["total_scount"].astype(int)
    return df


def analyze_radio_group(
    radio_df,
    radio_number,
    beacon_rate,
    log_and_print,
):
    radio_df = radio_df.sort_values(
        by="aruba_erm.time"
    ).reset_index(drop=True)

    total_data_points = len(radio_df)
    greatest_cutil = (radio_df["wlan.qbss.cu"].max() / 255) * 100
    least_cutil = (radio_df["wlan.qbss.cu"].min() / 255) * 100
    greater_than_191_count = (
        radio_df["wlan.qbss.cu"] > 191
    ).sum()
    percentage_greater_than_191 = (
        greater_than_191_count / total_data_points
    ) * 100
    median_cutil = (
        radio_df["wlan.qbss.cu"].median() / 255
    ) * 100

    greatest_scount = radio_df["total_scount"].max()
    median_scount = radio_df["total_scount"].median()
    least_scount = radio_df["total_scount"].min()

    log_and_print("")
    log_and_print(f"--- Radio Number {radio_number} ---")
    log_and_print(f"Total beacons: {total_data_points}")
    log_and_print(
        "Quantity with very high channel utilization(>75%): "
        f"{greater_than_191_count}"
    )
    log_and_print(
        "Percent with very high channel utilization: "
        f"{percentage_greater_than_191:.2f}%"
    )
    log_and_print(
        f"Highest channel utilization: {greatest_cutil:.2f}%"
    )
    log_and_print(
        f"Median channel utilization: {median_cutil:.2f}%"
    )
    log_and_print(
        f"Lowest channel utilization: {least_cutil:.2f}%"
    )
    log_and_print(f"Highest station count: {greatest_scount:.0f}")
    log_and_print(f"Median station count: {median_scount:.0f}")
    log_and_print(f"Lowest station count: {least_scount:.0f}")

    condition = radio_df["wlan.qbss.cu"] > 191
    radio_df["group"] = (
        (condition != condition.shift()).cumsum() * condition
    )
    valid_groups = radio_df[radio_df["group"] > 0]

    if valid_groups.empty:
        log_and_print(
            "No records found where wlan.qbss.cu > 191 for "
            f"radio_number {radio_number}."
        )
        longest_duration = 0
    else:
        group_details = valid_groups.groupby("group").agg(
            first_index=("aruba_erm.time", "idxmin"),
            last_index=("aruba_erm.time", "idxmax"),
        )
        group_details["first_in_sequence_time"] = (
            valid_groups.groupby("group")["aruba_erm.time"].min()
        )
        group_details["end_time"] = valid_groups.groupby(
            "group"
        )["aruba_erm.time"].max()
        group_details["duration"] = (
            group_details["end_time"]
            - group_details["first_in_sequence_time"]
        ).dt.total_seconds() + beacon_rate

        longest_group = group_details["duration"].idxmax()
        longest_row = group_details.loc[longest_group]
        longest_duration = longest_row["duration"]
        longest_start_time = longest_row[
            "first_in_sequence_time"
        ]
        longest_end_time = longest_row["end_time"]

        log_and_print(
            "Longest duration very high channel utilization: "
            f"{longest_duration:.2f} seconds"
        )
        log_and_print(
            f"First beacon of sequence: {longest_start_time}"
        )
        log_and_print(
            f"Last beacon of sequence: {longest_end_time}"
        )

    return (
        total_data_points,
        greater_than_191_count,
        percentage_greater_than_191,
        greatest_cutil,
        median_cutil,
        least_cutil,
        greatest_scount,
        median_scount,
        least_scount,
        longest_duration,
    )
