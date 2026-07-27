from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from radio_session_plotting import plot_channel_stations


def test_plot_channel_stations_current_calls_and_file(tmp_path):
    start = pd.Timestamp(
        "2025-01-01 10:00:00",
        tz="America/Denver",
    )
    end = start + pd.Timedelta(minutes=5)
    radio_df = pd.DataFrame(
        {
            "aruba_erm.time": [start, end],
            "wlan.qbss.cu": [0, 255],
            "total_scount": [10, 20],
            "radio_number": [2, 2],
        }
    )
    expected_filename = (
        f"{tmp_path}/AP-TEST_2025-01-01_10-00_R2.png"
    )
    expected_path = Path(expected_filename)

    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "plot", wraps=plt.plot) as plot,
        patch.object(
            plt,
            "fill_between",
            wraps=plt.fill_between,
        ) as fill_between,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "ylim", wraps=plt.ylim) as ylim,
        patch.object(plt, "legend", wraps=plt.legend) as legend,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
        patch.object(
            mdates,
            "DateFormatter",
            wraps=mdates.DateFormatter,
        ) as date_formatter,
    ):
        result = plot_channel_stations(
            radio_df,
            None,
            None,
            "AP-TEST",
            start,
            end,
            str(tmp_path),
            "America/Denver",
        )

    assert result == expected_filename
    assert expected_path.is_file()
    figure.assert_called_once_with(figsize=(14, 7))
    assert plot.call_count == 2
    first_plot = plot.call_args_list[0]
    pd.testing.assert_series_equal(
        first_plot.args[0],
        radio_df["aruba_erm.time"],
    )
    np.testing.assert_allclose(first_plot.args[1], [0, 100])
    assert first_plot.kwargs == {
        "label": "Channel Utilization (%)"
    }
    second_plot = plot.call_args_list[1]
    pd.testing.assert_series_equal(
        second_plot.args[0],
        radio_df["aruba_erm.time"],
    )
    pd.testing.assert_series_equal(
        second_plot.args[1],
        radio_df["total_scount"],
    )
    assert second_plot.kwargs == {"label": "Total Station Count"}
    fill_between.assert_called_once()
    np.testing.assert_allclose(
        fill_between.call_args.args[1],
        [0, 100],
    )
    xlabel.assert_called_once_with(
        "Time (2025-01-01)",
        fontsize=14,
    )
    ylabel.assert_called_once_with("Values", fontsize=14)
    title.assert_called_once_with(
        "AP-TEST Radio 2 10:00 to 10:05",
        fontsize=16,
    )
    ylim.assert_called_once_with(0, 100)
    legend.assert_called_once_with()
    assert date_formatter.call_args.args == ("%H:%M",)
    assert (
        date_formatter.call_args.kwargs["tz"].zone
        == "America/Denver"
    )
    savefig.assert_called_once_with(expected_filename)
    show.assert_not_called()
    close.assert_called_once_with()
