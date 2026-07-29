"""Plotting functions for all-class-beacons analysis."""

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt


def plot_channel_utilization_admission_capacity_hexbin(
    channel_utilization,
    admission_capacity,
    all_class_beacons_folder,
):
    plt.figure()
    plt.hexbin(
        channel_utilization * (100 / 255),
        admission_capacity,
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm(),
    )
    plt.colorbar(label="Log Count")
    plt.xlabel("Channel Utilization (%)")
    plt.ylabel("Admission Capacity")
    plt.title(
        "Hexbin of Channel Utilization (%) vs. wlan.qbss.adc"
    )
    plot_file = (
        f"{all_class_beacons_folder}/adc_channel_use_hexbin.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close()


def plot_station_count_admission_capacity_hexbin(
    total_scount,
    admission_capacity,
    all_class_beacons_folder,
):
    plt.hexbin(
        total_scount,
        admission_capacity,
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm(),
    )
    plt.xlabel("Total Station Count")
    plt.ylabel("Admission Capacity")
    plt.title(
        "Hexbin Plot: Admission Capacity vs. Station Count"
    )
    plt.colorbar(label="Log Count")
    plot_file = (
        f"{all_class_beacons_folder}/Scount_adc_hexbin.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close()


def plot_station_count_channel_utilization_hexbin(
    total_scount,
    channel_utilization,
    all_class_beacons_folder,
):
    plt.hexbin(
        total_scount,
        channel_utilization * (100 / 255),
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm(),
    )
    plt.xlabel("Total Station Count")
    plt.ylabel("Channel Utilization (%)")
    plt.title(
        "Hexbin Plot: Channel Utilization (%) vs. Station Count"
    )
    plt.colorbar(label="Log Count")
    plot_file = (
        f"{all_class_beacons_folder}/"
        "Scount_channel_use_hexbin.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close()


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
