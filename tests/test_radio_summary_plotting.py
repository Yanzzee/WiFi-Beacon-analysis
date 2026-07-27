from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from radio_summary_plotting import (
    plot_beacon_rate_channel_utilization_scatter,
)


def test_beacon_rate_channel_utilization_scatter_current_calls_and_file(
    tmp_path,
):
    filtered_radio_summary_df = pd.DataFrame(
        {
            "highest_cu": [10.0, 20.0, 30.0],
            "beacons_per_second": [4.0, 9.0, 16.0],
            "ssid_count": [2.0, 3.0, 4.0],
        }
    )
    output_folder = tmp_path / "Output"
    (output_folder / "radio_summary").mkdir(parents=True)
    plot_filename = (
        f"{output_folder}/radio_summary/"
        "Beacons_per_second_class_cu.png"
    )
    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "scatter", wraps=plt.scatter) as scatter,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "grid", wraps=plt.grid) as grid,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        plot_beacon_rate_channel_utilization_scatter(
            filtered_radio_summary_df,
            str(output_folder),
        )

    assert Path(plot_filename).is_file()
    figure.assert_called_once_with(figsize=(10, 6))
    scatter.assert_called_once()
    pd.testing.assert_series_equal(
        scatter.call_args.args[0],
        filtered_radio_summary_df["highest_cu"],
    )
    pd.testing.assert_series_equal(
        scatter.call_args.args[1],
        (
            filtered_radio_summary_df["beacons_per_second"]
            / filtered_radio_summary_df["ssid_count"]
        ),
    )
    assert scatter.call_args.kwargs == {
        "alpha": 0.5,
        "edgecolors": "k",
    }
    xlabel.assert_called_once_with("Highest Channel Utilization")
    ylabel.assert_called_once_with("Beacons per Second of Class")
    title.assert_called_once_with(
        "Comparison of CU to Beacons per Second"
    )
    grid.assert_called_once_with(True)
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_not_called()

    plt.close("all")
