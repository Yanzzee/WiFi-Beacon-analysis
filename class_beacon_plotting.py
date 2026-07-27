"""Plotting functions for all-class-beacons analysis."""

import matplotlib.pyplot as plt


def plot_station_count_histogram(
    total_scount,
    num_bins,
    all_class_beacons_folder,
):
    plt.figure(figsize=(10, 6))
    plt.hist(
        total_scount,
        bins=num_bins,
        edgecolor="black",
    )
    plt.xlabel("Total Station Count")
    plt.ylabel("Frequency", labelpad=15)
    plt.title(
        "Histogram of total station count values for all class "
        "beacons"
    )
    plt.tight_layout()
    plot_file = (
        f"{all_class_beacons_folder}/Scount_histogram.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close()


def plot_channel_utilization_pdf_cdf(
    bin_centers,
    pdf,
    cdf,
    all_class_beacons_folder,
):
    plt.figure(figsize=(10, 6))
    plt.plot(
        bin_centers,
        pdf,
        color="purple",
        label="PDF - wlan.qbss.cu (%)",
    )
    plt.xlabel("Channel Utilization (%)")
    plt.ylabel("Probability Density")
    plt.title("PDF of Channel Utilization")
    plt.legend()
    plt.grid()
    plot_file = f"{all_class_beacons_folder}/Channel_use_pdf.png"
    plt.savefig(plot_file)
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(
        bin_centers,
        cdf,
        color="orange",
        label="CDF - wlan.qbss.cu (%)",
    )
    plt.xlabel("Channel Utilization (%)")
    plt.ylabel("Cumulative Probability")
    plt.title("CDF of Channel Utilization")
    plt.legend()
    plt.grid()
    plot_file = f"{all_class_beacons_folder}/Channel_use_cdf.png"
    plt.savefig(plot_file)
    plt.show()


def plot_channel_utilization_histogram(
    cu_percent,
    bins,
    all_class_beacons_folder,
):
    plt.figure(figsize=(10, 6))
    plt.hist(
        cu_percent,
        bins=bins,
        edgecolor="black",
        align="mid",
    )
    plt.xlabel("Channel Utilization (%)")
    plt.ylabel("Frequency", labelpad=15)
    plt.title("Histogram of Channel Utilization Values")
    plt.tight_layout()
    plot_file = (
        f"{all_class_beacons_folder}/Channel_use_histogram.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close()
