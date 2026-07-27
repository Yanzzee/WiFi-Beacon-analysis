from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
import numpy as np
import pandas as pd

from radio_summary_plotting import (
    plot_beacon_rate_histogram,
    plot_beacon_rate_channel_utilization_scatter,
    plot_radio_count_split_scatter,
    plot_stacked_beacon_rate_histogram,
)
from script_sections import script_cell_source


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


def test_radio_count_split_scatter_current_calls_and_file(
    tmp_path,
):
    filtered_radio_summary_df = pd.DataFrame(
        {
            "highest_cu": [10.0, 20.0, 30.0, 40.0],
            "beacons_per_second": [4.0, 9.0, 16.0, 25.0],
            "ssid_count": [2.0, 3.0, 4.0, 5.0],
            "radio_count": [1, 2, 1, 2],
        }
    )
    df_radio_1 = filtered_radio_summary_df[
        filtered_radio_summary_df["radio_count"] == 1
    ]
    df_radio_2 = filtered_radio_summary_df[
        filtered_radio_summary_df["radio_count"] == 2
    ]
    radio_summary_folder = tmp_path / "radio_summary"
    radio_summary_folder.mkdir()
    plot_filename = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_split.png"
    )
    with (
        patch.object(plt, "subplots", wraps=plt.subplots) as subplots,
        patch.object(
            plt,
            "tight_layout",
            wraps=plt.tight_layout,
        ) as tight_layout,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        plot_radio_count_split_scatter(
            filtered_radio_summary_df,
            df_radio_1,
            df_radio_2,
            str(radio_summary_folder),
        )

    axes = plt.gcf().axes
    assert Path(plot_filename).is_file()
    subplots.assert_called_once_with(
        1,
        3,
        figsize=(18, 6),
        sharey=True,
    )
    expected_frames = [
        filtered_radio_summary_df,
        df_radio_1,
        df_radio_2,
    ]
    for axis, expected_frame in zip(
        axes,
        expected_frames,
        strict=True,
    ):
        assert len(axis.collections) == 1
        collection = axis.collections[0]
        np.testing.assert_allclose(
            collection.get_offsets(),
            np.column_stack(
                [
                    expected_frame["highest_cu"],
                    (
                        expected_frame["beacons_per_second"]
                        / expected_frame["ssid_count"]
                    ),
                ]
            ),
        )
        assert collection.get_alpha() == 0.5
        np.testing.assert_allclose(
            collection.get_edgecolors(),
            [to_rgba("k", alpha=0.5)],
        )
        assert axis.get_xlabel() == "Highest Channel Utilization"
        assert axis.get_xlim() == (0.0, 100.0)
        assert any(
            line.get_visible()
            for line in (
                *axis.get_xgridlines(),
                *axis.get_ygridlines(),
            )
        )
    np.testing.assert_allclose(
        axes[0].collections[0].get_facecolors(),
        [
            to_rgba("blue", alpha=0.5),
            to_rgba("red", alpha=0.5),
        ]
        * 2,
    )
    np.testing.assert_allclose(
        axes[1].collections[0].get_facecolors(),
        [to_rgba("blue", alpha=0.5)],
    )
    np.testing.assert_allclose(
        axes[2].collections[0].get_facecolors(),
        [to_rgba("red", alpha=0.5)],
    )
    assert [axis.get_title() for axis in axes] == [
        "All Data (Both Radio Counts)",
        "AP Radio Count 1 (Blue)",
        "AP Radio Count 2 (Red)",
    ]
    assert axes[0].get_ylabel() == "Beacons per Second of Class"
    tight_layout.assert_called_once_with()
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_not_called()

    plt.close("all")


def test_beacon_rate_histogram_current_calls_and_file(
    tmp_path,
):
    filtered_radio_summary_df = pd.DataFrame(
        {
            "beacons_per_second": [4.0, 9.0, 16.0],
            "ssid_count": [2.0, 3.0, 4.0],
        }
    )
    radio_summary_folder = tmp_path / "radio_summary"
    radio_summary_folder.mkdir()
    plot_filename = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_class_histogram.png"
    )
    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hist", wraps=plt.hist) as hist,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "grid", wraps=plt.grid) as grid,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        plot_beacon_rate_histogram(
            filtered_radio_summary_df,
            str(radio_summary_folder),
        )

    assert Path(plot_filename).is_file()
    figure.assert_called_once_with(figsize=(10, 6))
    hist.assert_called_once()
    pd.testing.assert_series_equal(
        hist.call_args.args[0],
        (
            filtered_radio_summary_df["beacons_per_second"]
            / filtered_radio_summary_df["ssid_count"]
        ),
    )
    assert hist.call_args.kwargs == {
        "bins": 50,
        "edgecolor": "black",
        "alpha": 0.7,
    }
    xlabel.assert_called_once_with("Beacon Rate (per second)")
    ylabel.assert_called_once_with("Frequency")
    title.assert_called_once_with(
        "Histogram of Beacons per Second of Class"
    )
    grid.assert_called_once_with(axis="y", alpha=0.75)
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_not_called()

    plt.close("all")


def test_stacked_beacon_rate_histogram_current_calls_and_file(
    tmp_path,
):
    df_radio_1 = pd.DataFrame(
        {
            "beacons_per_second": [4.0, None, 12.0],
            "ssid_count": [2.0, 2.0, 3.0],
        },
        index=[10, 11, 12],
    )
    df_radio_2 = pd.DataFrame(
        {
            "beacons_per_second": [9.0, 20.0],
            "ssid_count": [3.0, 4.0],
        },
        index=[20, 21],
    )
    combined_df = pd.concat([df_radio_1, df_radio_2])
    expected_radio_1 = pd.Series(
        [2.0, 4.0],
        index=[10, 12],
        name=None,
    )
    expected_radio_2 = pd.Series(
        [3.0, 5.0],
        index=[20, 21],
        name=None,
    )
    expected_combined = pd.Series(
        [2.0, 4.0, 3.0, 5.0],
        index=[10, 12, 20, 21],
        name=None,
    )
    radio_summary_folder = tmp_path / "radio_summary"
    radio_summary_folder.mkdir()
    plot_filename = (
        f"{radio_summary_folder}/"
        "Beacons_per_second_stacked.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_radio_summary.py",
        6,
    )
    namespace = {
        "df_radio_1": df_radio_1,
        "df_radio_2": df_radio_2,
        "combined_df": combined_df,
        "plot_stacked_beacon_rate_histogram": (
            plot_stacked_beacon_rate_histogram
        ),
        "RADIO_SUMMARY_FOLDER": str(radio_summary_folder),
        "plt": plt,
    }

    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hist", wraps=plt.hist) as hist,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "legend", wraps=plt.legend) as legend,
        patch.object(plt, "grid", wraps=plt.grid) as grid,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        exec(
            compile(
                cell_source,
                f"{script_path}::cell_6",
                "exec",
            ),
            namespace,
        )

    pd.testing.assert_series_equal(
        namespace["df_radio_1"],
        expected_radio_1,
    )
    pd.testing.assert_series_equal(
        namespace["df_radio_2"],
        expected_radio_2,
    )
    pd.testing.assert_series_equal(
        namespace["data_combined"],
        expected_combined,
    )
    assert Path(plot_filename).is_file()
    figure.assert_called_once_with(figsize=(10, 6))
    hist.assert_called_once()
    assert len(hist.call_args.args[0]) == 2
    pd.testing.assert_series_equal(
        hist.call_args.args[0][0],
        expected_radio_1,
    )
    pd.testing.assert_series_equal(
        hist.call_args.args[0][1],
        expected_radio_2,
    )
    assert hist.call_args.kwargs == {
        "bins": 50,
        "edgecolor": "black",
        "alpha": 0.7,
        "stacked": True,
        "color": ["blue", "red"],
        "label": ["Radio Count 1", "Radio Count 2"],
    }
    xlabel.assert_called_once_with("Beacon Rate (per second)")
    ylabel.assert_called_once_with("Frequency")
    title.assert_called_once_with(
        "Stacked Histogram of Beacons per Second of Class by "
        "Radio Count"
    )
    legend.assert_called_once_with()
    grid.assert_called_once_with(axis="y", alpha=0.75)
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_not_called()

    plt.close("all")
