from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from class_beacon_plotting import (
    plot_channel_utilization_histogram,
)
from script_sections import script_cell_source


def test_channel_utilization_histogram_current_calls_and_file(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {"wlan.qbss.cu": [0.0, 127.5, 255.0]}
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    plot_filename = (
        f"{all_class_beacons_folder}/Channel_use_histogram.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        5,
    )
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "plot_channel_utilization_histogram": (
            plot_channel_utilization_histogram
        ),
        "np": np,
        "plt": plt,
    }

    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hist", wraps=plt.hist) as hist,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(
            plt,
            "tight_layout",
            wraps=plt.tight_layout,
        ) as tight_layout,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        exec(
            compile(
                cell_source,
                f"{script_path}::cell_5",
                "exec",
            ),
            namespace,
        )

    expected_cu_percent = (
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255)
    )
    expected_bins = np.linspace(0, 100, 256)
    pd.testing.assert_series_equal(
        namespace["cu_percent"],
        expected_cu_percent,
    )
    np.testing.assert_allclose(namespace["bins"], expected_bins)
    assert Path(plot_filename).is_file()
    figure.assert_called_once_with(figsize=(10, 6))
    hist.assert_called_once()
    pd.testing.assert_series_equal(
        hist.call_args.args[0],
        expected_cu_percent,
    )
    np.testing.assert_allclose(
        hist.call_args.kwargs["bins"],
        expected_bins,
    )
    assert {
        key: value
        for key, value in hist.call_args.kwargs.items()
        if key != "bins"
    } == {
        "edgecolor": "black",
        "align": "mid",
    }
    xlabel.assert_called_once_with("Channel Utilization (%)")
    ylabel.assert_called_once_with("Frequency", labelpad=15)
    title.assert_called_once_with(
        "Histogram of Channel Utilization Values"
    )
    tight_layout.assert_called_once_with()
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with()
