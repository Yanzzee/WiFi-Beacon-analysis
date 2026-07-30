"""Per-radio class-session plotting."""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pytz


def plot_channel_stations(
    radio_df,
    eduroam_df,
    byu_wifi_df,
    AP_name,
    start_time,
    end_time,
    plots_folder,
    plot_timezone,
):
    plt.figure(figsize=(14, 7))

    plot_radio_number = radio_df.iloc[0]["radio_number"]

    plt.plot(
        radio_df["aruba_erm.time"],
        (radio_df["wlan.qbss.cu"] / 255) * 100,
        label="Channel Utilization (%)",
    )
    plt.fill_between(
        radio_df["aruba_erm.time"],
        (radio_df["wlan.qbss.cu"] / 255) * 100,
    )
    plt.plot(
        radio_df["aruba_erm.time"],
        radio_df["total_scount"],
        label="Total Station Count",
    )

    formatted_date = start_time.strftime("%Y-%m-%d")
    formatted_start = start_time.strftime("%H:%M")
    formatted_end = end_time.strftime("%H:%M")

    plt.xlabel(f"Time ({formatted_date})", fontsize=14)
    plt.ylabel("Values", fontsize=14)
    plt.title(
        f"{AP_name} Radio {plot_radio_number} "
        f"{formatted_start} to {formatted_end}",
        fontsize=16,
    )
    plt.ylim(0, 100)

    time_zone = pytz.timezone(plot_timezone)
    time_formatter = mdates.DateFormatter("%H:%M", tz=time_zone)
    plt.gca().xaxis.set_major_formatter(time_formatter)

    plt.legend()

    plot_file = (
        plots_folder
        + "/"
        + AP_name
        + "_"
        + start_time.strftime("%Y-%m-%d_%H-%M")
        + "_R"
        + str(plot_radio_number)
        + ".png"
    )
    plt.savefig(plot_file)
    plt.close()
    return plot_file
