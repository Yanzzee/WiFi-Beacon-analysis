from __future__ import annotations

from pathlib import Path
from unittest.mock import call, patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

from class_session_plotting import (
    plot_high_cu_streak_histograms,
    plot_stations_per_enrolled_histogram,
)


def test_high_cu_streak_histograms_current_calls_and_files(
    tmp_path,
):
    class_sessions_df = pd.DataFrame(
        {
            "max_streak_50": [0.0, 2.0, 4.0],
            "max_streak_75": [1.0, 3.0, 5.0],
        }
    )
    regular_filename = f"{tmp_path}/High_CU_streaks.png"
    zoomed_filename = (
        f"{tmp_path}/High_CU_streaks_zoomed.png"
    )

    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hist", wraps=plt.hist) as hist,
        patch.object(plt, "xlim", wraps=plt.xlim) as xlim,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "legend", wraps=plt.legend) as legend,
        patch.object(plt, "grid", wraps=plt.grid) as grid,
        patch.object(
            plt,
            "tight_layout",
            wraps=plt.tight_layout,
        ) as tight_layout,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        plot_high_cu_streak_histograms(
            class_sessions_df,
            str(tmp_path),
        )

    assert Path(regular_filename).is_file()
    assert Path(zoomed_filename).is_file()
    assert figure.call_args_list == [
        call(figsize=(10, 5)),
        call(figsize=(10, 5)),
    ]
    assert hist.call_count == 4
    for index, expected_column in enumerate(
        [
            "max_streak_50",
            "max_streak_75",
            "max_streak_50",
            "max_streak_75",
        ]
    ):
        pd.testing.assert_series_equal(
            hist.call_args_list[index].args[0],
            class_sessions_df[expected_column],
        )
        assert hist.call_args_list[index].kwargs["bins"] == range(
            0,
            7,
        )
        assert hist.call_args_list[index].kwargs["alpha"] == 0.6
    assert [
        hist_call.kwargs["label"]
        for hist_call in hist.call_args_list
    ] == [
        "CU > 50%",
        "CU > 75%",
        "CU > 50%",
        "CU > 75%",
    ]
    xlim.assert_called_once_with(0, 60)
    assert xlabel.call_args_list == [
        call("Max Continuous High CU Duration (seconds)"),
        call("Max Continuous High CU Duration (seconds)"),
    ]
    assert ylabel.call_args_list == [
        call("Number of Class Sessions"),
        call("Number of Class Sessions"),
    ]
    assert title.call_args_list == [
        call("Max High CU Streaks (showing outliers)"),
        call("Max High CU Streaks (zoomed in to 60 seconds)"),
    ]
    assert legend.call_args_list == [call(), call()]
    assert grid.call_args_list == [call(True), call(True)]
    assert tight_layout.call_args_list == [call(), call()]
    assert savefig.call_args_list == [
        call(regular_filename),
        call(zoomed_filename),
    ]
    assert show.call_args_list == [call(), call()]
    assert close.call_args_list == [
        call("all"),
        call("all"),
    ]


def test_stations_per_enrolled_histogram_current_calls_and_file(
    tmp_path,
):
    class_sessions_df = pd.DataFrame(
        {"stations_enrolled": [0.5, 1.0, 1.5]}
    )
    plot_filename = (
        f"{tmp_path}/stations_enrolled_histogram.png"
    )
    original_hist = pd.Series.hist

    with (
        patch.object(
            pd.Series,
            "hist",
            autospec=True,
            side_effect=original_hist,
        ) as hist,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        plot_stations_per_enrolled_histogram(
            class_sessions_df,
            str(tmp_path),
        )

    assert Path(plot_filename).is_file()
    hist.assert_called_once()
    pd.testing.assert_series_equal(
        hist.call_args.args[0],
        class_sessions_df["stations_enrolled"],
    )
    assert hist.call_args.kwargs == {"bins": 60}
    title.assert_called_once_with(
        "Distribution of Stations per Enrolled"
    )
    xlabel.assert_called_once_with("Stations per Enrolled")
    ylabel.assert_called_once_with("Number of Class Sessions")
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with("all")
