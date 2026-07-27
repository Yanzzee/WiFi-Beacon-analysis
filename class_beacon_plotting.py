"""Plotting functions for all-class-beacons analysis."""

import matplotlib.pyplot as plt


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
