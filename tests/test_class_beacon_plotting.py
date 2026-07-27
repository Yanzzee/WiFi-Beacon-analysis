from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, call, patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis_statistics import compute_pdf_cdf
from class_beacon_plotting import (
    plot_channel_utilization_histogram,
    plot_channel_utilization_pdf_cdf,
    plot_station_count_histogram,
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


def test_channel_utilization_pdf_cdf_current_calls_and_files(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {
            "wlan.qbss.cu": [
                0.0,
                127.5,
                191.25,
                255.0,
                None,
            ]
        }
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    pdf_filename = (
        f"{all_class_beacons_folder}/Channel_use_pdf.png"
    )
    cdf_filename = (
        f"{all_class_beacons_folder}/Channel_use_cdf.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        6,
    )
    compute_pdf_cdf_mock = Mock(wraps=compute_pdf_cdf)
    logged_messages: list[str] = []
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "compute_pdf_cdf": compute_pdf_cdf_mock,
        "log_and_print": logged_messages.append,
        "plot_channel_utilization_pdf_cdf": (
            plot_channel_utilization_pdf_cdf
        ),
        "plt": plt,
    }

    with (
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "plot", wraps=plt.plot) as plot,
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

    expected_cu_data = pd.Series(
        [0.0, 50.0, 75.0, 100.0],
        index=[0, 1, 2, 3],
        name="wlan.qbss.cu",
    )
    pd.testing.assert_series_equal(
        namespace["cu_data"],
        expected_cu_data,
    )
    compute_pdf_cdf_mock.assert_called_once()
    pd.testing.assert_series_equal(
        compute_pdf_cdf_mock.call_args.args[0],
        expected_cu_data,
    )
    assert compute_pdf_cdf_mock.call_args.kwargs == {"bins": 256}
    assert namespace["cdf_at_50"] == 0.5
    assert namespace["cdf_at_75"] == 0.75
    assert logged_messages == [
        "CDF at wlan.qbss.cu = 50%: 0.5000",
        "50.00 % of beacons during class show channel utilization "
        "under 50%",
        "CDF at wlan.qbss.cu = 75%: 0.7500",
        "75.00 % of beacons during class show channel utilization "
        "under 75%",
    ]
    assert Path(pdf_filename).is_file()
    assert Path(cdf_filename).is_file()
    assert figure.call_args_list == [
        call(figsize=(10, 6)),
        call(figsize=(10, 6)),
    ]
    assert plot.call_count == 2
    np.testing.assert_allclose(
        plot.call_args_list[0].args[0],
        namespace["bin_centers"],
    )
    np.testing.assert_allclose(
        plot.call_args_list[0].args[1],
        namespace["pdf"],
    )
    np.testing.assert_allclose(
        plot.call_args_list[1].args[0],
        namespace["bin_centers"],
    )
    np.testing.assert_allclose(
        plot.call_args_list[1].args[1],
        namespace["cdf"],
    )
    assert [item.kwargs for item in plot.call_args_list] == [
        {
            "color": "purple",
            "label": "PDF - wlan.qbss.cu (%)",
        },
        {
            "color": "orange",
            "label": "CDF - wlan.qbss.cu (%)",
        },
    ]
    assert xlabel.call_args_list == [
        call("Channel Utilization (%)"),
        call("Channel Utilization (%)"),
    ]
    assert ylabel.call_args_list == [
        call("Probability Density"),
        call("Cumulative Probability"),
    ]
    assert title.call_args_list == [
        call("PDF of Channel Utilization"),
        call("CDF of Channel Utilization"),
    ]
    assert legend.call_args_list == [call(), call()]
    assert grid.call_args_list == [call(), call()]
    assert savefig.call_args_list == [
        call(pdf_filename),
        call(cdf_filename),
    ]
    assert show.call_args_list == [call(), call()]
    close.assert_not_called()

    plt.close("all")


def test_station_count_histogram_current_calls_and_file(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {"total_scount": [0, 2, 5, 5]}
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    plot_filename = (
        f"{all_class_beacons_folder}/Scount_histogram.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        7,
    )
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "plot_station_count_histogram": (
            plot_station_count_histogram
        ),
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
                f"{script_path}::cell_7",
                "exec",
            ),
            namespace,
        )

    assert namespace["num_bins"] == 6
    assert Path(plot_filename).is_file()
    figure.assert_called_once_with(figsize=(10, 6))
    hist.assert_called_once()
    pd.testing.assert_series_equal(
        hist.call_args.args[0],
        all_class_beacons_df["total_scount"],
    )
    assert hist.call_args.kwargs == {
        "bins": 6,
        "edgecolor": "black",
    }
    xlabel.assert_called_once_with("Total Station Count")
    ylabel.assert_called_once_with("Frequency", labelpad=15)
    title.assert_called_once_with(
        "Histogram of total station count values for all class "
        "beacons"
    )
    tight_layout.assert_called_once_with()
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with()
