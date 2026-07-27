"""Plotting functions for class-session analysis."""

import matplotlib.pyplot as plt


def plot_stations_per_enrolled_histogram(
    class_sessions_df,
    class_sessions_folder,
):
    class_sessions_df["stations_enrolled"].hist(bins=60)
    plt.title("Distribution of Stations per Enrolled")
    plt.xlabel("Stations per Enrolled")
    plt.ylabel("Number of Class Sessions")
    plot_file = (
        f"{class_sessions_folder}/stations_enrolled_histogram.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")


def plot_high_cu_streak_histograms(
    class_sessions_df,
    class_sessions_folder,
):
    max_val = int(
        max(
            class_sessions_df["max_streak_50"].max(),
            class_sessions_df["max_streak_75"].max(),
        )
    )
    bins = range(0, max_val + 2)

    plt.figure(figsize=(10, 5))
    plt.hist(
        class_sessions_df["max_streak_50"],
        bins=bins,
        alpha=0.6,
        label="CU > 50%",
    )
    plt.hist(
        class_sessions_df["max_streak_75"],
        bins=bins,
        alpha=0.6,
        label="CU > 75%",
    )
    plt.xlabel("Max Continuous High CU Duration (seconds)")
    plt.ylabel("Number of Class Sessions")
    plt.title("Max High CU Streaks (showing outliers)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_file = (
        f"{class_sessions_folder}/High_CU_streaks.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")

    plt.figure(figsize=(10, 5))
    plt.hist(
        class_sessions_df["max_streak_50"],
        bins=bins,
        alpha=0.6,
        label="CU > 50%",
    )
    plt.hist(
        class_sessions_df["max_streak_75"],
        bins=bins,
        alpha=0.6,
        label="CU > 75%",
    )
    plt.xlim(0, 60)
    plt.xlabel("Max Continuous High CU Duration (seconds)")
    plt.ylabel("Number of Class Sessions")
    plt.title("Max High CU Streaks (zoomed in to 60 seconds)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_file = (
        f"{class_sessions_folder}/High_CU_streaks_zoomed.png"
    )
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")
