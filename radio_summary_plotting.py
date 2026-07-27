"""Plotting functions for radio-summary analysis."""

import matplotlib.pyplot as plt


def plot_beacon_rate_channel_utilization_scatter(
    filtered_radio_summary_df,
    output_folder,
):
    plt.figure(figsize=(10, 6))
    plt.scatter(
        filtered_radio_summary_df["highest_cu"],
        (
            filtered_radio_summary_df["beacons_per_second"]
            / filtered_radio_summary_df["ssid_count"]
        ),
        alpha=0.5,
        edgecolors="k",
    )
    plt.xlabel("Highest Channel Utilization")
    plt.ylabel("Beacons per Second of Class")
    plt.title("Comparison of CU to Beacons per Second")
    plt.grid(True)
    plot_file = (
        f"{output_folder}/radio_summary/"
        "Beacons_per_second_class_cu.png"
    )
    plt.savefig(plot_file)
    plt.show()
