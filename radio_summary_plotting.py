"""Plotting functions for radio-summary analysis."""

import matplotlib.pyplot as plt


def plot_beacon_rate_pdf_cdf(
    bin_centers1,
    pdf1,
    cdf1,
    bin_centers2,
    pdf2,
    cdf2,
    bin_centers_comb,
    pdf_comb,
    cdf_comb,
    radio_summary_folder,
):
    plt.figure(figsize=(10, 6))
    plt.plot(
        bin_centers1,
        pdf1,
        label="PDF - Radio Count 1",
        color="blue",
    )
    plt.plot(
        bin_centers2,
        pdf2,
        label="PDF - Radio Count 2",
        color="red",
    )
    plt.plot(
        bin_centers_comb,
        pdf_comb,
        label="PDF - Combined",
        color="green",
    )
    plt.xlabel("Beacon Rate (per second)")
    plt.ylabel("Probability Density")
    plt.title("Probability Distribution Function (PDF)")
    plt.legend()
    plt.grid()
    plot_file = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_PDF.png"
    )
    plt.savefig(plot_file)
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(
        bin_centers1,
        cdf1,
        label="CDF - Radio Count 1",
        color="blue",
    )
    plt.plot(
        bin_centers2,
        cdf2,
        label="CDF - Radio Count 2",
        color="red",
    )
    plt.plot(
        bin_centers_comb,
        cdf_comb,
        label="CDF - Combined",
        color="green",
    )
    plt.xlabel("Beacon Rate (per second)")
    plt.ylabel("Cumulative Probability")
    plt.title("Cumulative Distribution Function (CDF)")
    plt.legend()
    plt.grid()
    plot_file = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_CDF.png"
    )
    plt.savefig(plot_file)
    plt.show()


def plot_stacked_beacon_rate_histogram(
    df_radio_1,
    df_radio_2,
    radio_summary_folder,
):
    plt.figure(figsize=(10, 6))
    plt.hist(
        [df_radio_1, df_radio_2],
        bins=50,
        edgecolor="black",
        alpha=0.7,
        stacked=True,
        color=["blue", "red"],
        label=["Radio Count 1", "Radio Count 2"],
    )
    plt.xlabel("Beacon Rate (per second)")
    plt.ylabel("Frequency")
    plt.title(
        "Stacked Histogram of Beacons per Second of Class by "
        "Radio Count"
    )
    plt.legend()
    plt.grid(axis="y", alpha=0.75)
    plot_file = (
        f"{radio_summary_folder}/Beacons_per_second_stacked.png"
    )
    plt.savefig(plot_file)
    plt.show()


def plot_beacon_rate_histogram(
    filtered_radio_summary_df,
    radio_summary_folder,
):
    plt.figure(figsize=(10, 6))
    plt.hist(
        (
            filtered_radio_summary_df["beacons_per_second"]
            / filtered_radio_summary_df["ssid_count"]
        ),
        bins=50,
        edgecolor="black",
        alpha=0.7,
    )
    plt.xlabel("Beacon Rate (per second)")
    plt.ylabel("Frequency")
    plt.title("Histogram of Beacons per Second of Class")
    plt.grid(axis="y", alpha=0.75)
    plot_file = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_histogram.png"
    )
    plt.savefig(plot_file)
    plt.show()


def plot_radio_count_split_scatter(
    filtered_radio_summary_df,
    df_radio_1,
    df_radio_2,
    radio_summary_folder,
):
    fig, axes = plt.subplots(
        1,
        3,
        figsize=(18, 6),
        sharey=True,
    )

    axes[0].scatter(
        filtered_radio_summary_df["highest_cu"],
        (
            filtered_radio_summary_df["beacons_per_second"]
            / filtered_radio_summary_df["ssid_count"]
        ),
        alpha=0.5,
        edgecolors="k",
        c=filtered_radio_summary_df["radio_count"].map(
            {1: "blue", 2: "red"}
        ),
    )
    axes[0].set_title("All Data (Both Radio Counts)")
    axes[0].set_xlabel("Highest Channel Utilization")
    axes[0].set_ylabel("Beacons per Second of Class")
    axes[0].set_xlim(0, 100)
    axes[0].grid(True)

    axes[1].scatter(
        df_radio_1["highest_cu"],
        (
            df_radio_1["beacons_per_second"]
            / df_radio_1["ssid_count"]
        ),
        alpha=0.5,
        edgecolors="k",
        c="blue",
    )
    axes[1].set_title("AP Radio Count 1 (Blue)")
    axes[1].set_xlabel("Highest Channel Utilization")
    axes[1].set_xlim(0, 100)
    axes[1].grid(True)

    axes[2].scatter(
        df_radio_2["highest_cu"],
        (
            df_radio_2["beacons_per_second"]
            / df_radio_2["ssid_count"]
        ),
        alpha=0.5,
        edgecolors="k",
        c="red",
    )
    axes[2].set_title("AP Radio Count 2 (Red)")
    axes[2].set_xlabel("Highest Channel Utilization")
    axes[2].set_xlim(0, 100)
    axes[2].grid(True)

    plt.tight_layout()
    plot_file = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_split.png"
    )
    plt.savefig(plot_file)
    plt.show()


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
