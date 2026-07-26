"""Current Aruba-specific transmitter-based radio inference."""


def _aruba_mac_to_int(mac):
    return int(mac.replace(":", ""), 16)


def add_aruba_transmitter_integers(filtered_df):
    filtered_df["wlan.ta_int"] = filtered_df["wlan.ta"].apply(
        _aruba_mac_to_int
    )
    return filtered_df


def assign_aruba_radio_numbers(filtered_df):
    filtered_df = filtered_df.sort_values(
        "wlan.ta_int"
    ).reset_index(drop=True)

    radio_number = 1
    radio_numbers = [radio_number]

    for i in range(1, len(filtered_df)):
        if abs(
            filtered_df.loc[i, "wlan.ta_int"]
            - filtered_df.loc[i - 1, "wlan.ta_int"]
        ) > 1:
            radio_number += 1
        radio_numbers.append(radio_number)

    filtered_df["radio_number"] = radio_numbers

    return filtered_df.sort_values(
        by="aruba_erm.time"
    ).reset_index(drop=True)
