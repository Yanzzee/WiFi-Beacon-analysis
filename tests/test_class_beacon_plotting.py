from __future__ import annotations

from pathlib import Path
from unittest.mock import call, patch

import matplotlib

matplotlib.use("Agg")

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from class_beacon_analysis import (
    calculate_station_count_bins,
    prepare_channel_utilization_distribution,
    prepare_channel_utilization_histogram,
    report_channel_utilization_cdf,
)
from class_beacon_plotting import (
    plot_channel_utilization_admission_capacity_hexbin,
    plot_channel_utilization_histogram,
    plot_channel_utilization_pdf_cdf,
    plot_station_count_admission_capacity_hexbin,
    plot_station_count_channel_utilization_hexbin,
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
    cu_percent, bins = prepare_channel_utilization_histogram(
        all_class_beacons_df,
    )

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
        plot_channel_utilization_histogram(
            cu_percent,
            bins,
            str(all_class_beacons_folder),
        )

    expected_cu_percent = (
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255)
    )
    expected_bins = np.linspace(0, 100, 256)
    pd.testing.assert_series_equal(
        cu_percent,
        expected_cu_percent,
    )
    np.testing.assert_allclose(bins, expected_bins)
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
    logged_messages: list[str] = []
    cu_data, bin_centers, pdf, cdf = (
        prepare_channel_utilization_distribution(
            all_class_beacons_df,
        )
    )

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
        plot_channel_utilization_pdf_cdf(
            bin_centers,
            pdf,
            cdf,
            str(all_class_beacons_folder),
        )
        cdf_at_50, cdf_at_75 = report_channel_utilization_cdf(
            cu_data,
            logged_messages.append,
        )

    expected_cu_data = pd.Series(
        [0.0, 50.0, 75.0, 100.0],
        index=[0, 1, 2, 3],
        name="wlan.qbss.cu",
    )
    pd.testing.assert_series_equal(
        cu_data,
        expected_cu_data,
    )
    assert cdf_at_50 == 0.5
    assert cdf_at_75 == 0.75
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
        bin_centers,
    )
    np.testing.assert_allclose(
        plot.call_args_list[0].args[1],
        pdf,
    )
    np.testing.assert_allclose(
        plot.call_args_list[1].args[0],
        bin_centers,
    )
    np.testing.assert_allclose(
        plot.call_args_list[1].args[1],
        cdf,
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
    num_bins = calculate_station_count_bins(all_class_beacons_df)

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
        plot_station_count_histogram(
            all_class_beacons_df["total_scount"],
            num_bins,
            str(all_class_beacons_folder),
        )

    assert num_bins == 6
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


def test_station_count_channel_utilization_hexbin_current_calls_and_file(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {
            "total_scount": [1, 2, 3],
            "wlan.qbss.cu": [0.0, 127.5, 255.0],
        }
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    plot_filename = (
        f"{all_class_beacons_folder}/"
        "Scount_channel_use_hexbin.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        9,
    )
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "mcolors": mcolors,
        "plot_station_count_channel_utilization_hexbin": (
            plot_station_count_channel_utilization_hexbin
        ),
        "plt": plt,
    }

    with (
        patch.object(
            mcolors,
            "LogNorm",
            wraps=mcolors.LogNorm,
        ) as log_norm,
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hexbin", wraps=plt.hexbin) as hexbin,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "colorbar", wraps=plt.colorbar) as colorbar,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        exec(
            compile(
                cell_source,
                f"{script_path}::cell_9",
                "exec",
            ),
            namespace,
        )

    assert Path(plot_filename).is_file()
    figure.assert_called_once_with()
    hexbin.assert_called_once()
    pd.testing.assert_series_equal(
        hexbin.call_args.args[0],
        all_class_beacons_df["total_scount"],
    )
    pd.testing.assert_series_equal(
        hexbin.call_args.args[1],
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255),
    )
    assert hexbin.call_args.kwargs["gridsize"] == 50
    assert hexbin.call_args.kwargs["cmap"] == "Blues"
    norm = hexbin.call_args.kwargs["norm"]
    assert isinstance(norm, mcolors.LogNorm)
    log_norm.assert_called_once_with()
    assert norm.clip is False
    xlabel.assert_called_once_with("Total Station Count")
    ylabel.assert_called_once_with("Channel Utilization (%)")
    title.assert_called_once_with(
        "Hexbin Plot: Channel Utilization (%) vs. Station Count"
    )
    colorbar.assert_called_once_with(label="Log Count")
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with()


def test_station_count_admission_capacity_hexbin_current_calls_and_file(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {
            "total_scount": [1, 2, 3],
            "wlan.qbss.adc": [10.0, 20.0, 30.0],
        }
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    plot_filename = (
        f"{all_class_beacons_folder}/Scount_adc_hexbin.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        10,
    )
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "mcolors": mcolors,
        "plot_station_count_admission_capacity_hexbin": (
            plot_station_count_admission_capacity_hexbin
        ),
        "plt": plt,
    }

    with (
        patch.object(
            mcolors,
            "LogNorm",
            wraps=mcolors.LogNorm,
        ) as log_norm,
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hexbin", wraps=plt.hexbin) as hexbin,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "colorbar", wraps=plt.colorbar) as colorbar,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        exec(
            compile(
                cell_source,
                f"{script_path}::cell_10",
                "exec",
            ),
            namespace,
        )

    assert Path(plot_filename).is_file()
    figure.assert_called_once_with()
    hexbin.assert_called_once()
    pd.testing.assert_series_equal(
        hexbin.call_args.args[0],
        all_class_beacons_df["total_scount"],
    )
    pd.testing.assert_series_equal(
        hexbin.call_args.args[1],
        all_class_beacons_df["wlan.qbss.adc"],
    )
    assert hexbin.call_args.kwargs["gridsize"] == 50
    assert hexbin.call_args.kwargs["cmap"] == "Blues"
    norm = hexbin.call_args.kwargs["norm"]
    assert isinstance(norm, mcolors.LogNorm)
    log_norm.assert_called_once_with()
    assert norm.clip is False
    xlabel.assert_called_once_with("Total Station Count")
    ylabel.assert_called_once_with("Admission Capacity")
    title.assert_called_once_with(
        "Hexbin Plot: Admission Capacity vs. Station Count"
    )
    colorbar.assert_called_once_with(label="Log Count")
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with()


def test_channel_utilization_admission_capacity_hexbin_current_calls_and_file(
    tmp_path,
):
    all_class_beacons_df = pd.DataFrame(
        {
            "wlan.qbss.cu": [0.0, 127.5, 255.0],
            "wlan.qbss.adc": [10.0, 20.0, 30.0],
        }
    )
    all_class_beacons_folder = tmp_path / "all_class_beacons"
    all_class_beacons_folder.mkdir()
    plot_filename = (
        f"{all_class_beacons_folder}/adc_channel_use_hexbin.png"
    )
    cell_source, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        12,
    )
    namespace = {
        "all_class_beacons_df": all_class_beacons_df,
        "ALL_CLASS_BEACONS_FOLDER": str(all_class_beacons_folder),
        "mcolors": mcolors,
        "plot_channel_utilization_admission_capacity_hexbin": (
            plot_channel_utilization_admission_capacity_hexbin
        ),
        "plt": plt,
    }

    with (
        patch.object(
            mcolors,
            "LogNorm",
            wraps=mcolors.LogNorm,
        ) as log_norm,
        patch.object(plt, "figure", wraps=plt.figure) as figure,
        patch.object(plt, "hexbin", wraps=plt.hexbin) as hexbin,
        patch.object(plt, "xlabel", wraps=plt.xlabel) as xlabel,
        patch.object(plt, "ylabel", wraps=plt.ylabel) as ylabel,
        patch.object(plt, "title", wraps=plt.title) as title,
        patch.object(plt, "colorbar", wraps=plt.colorbar) as colorbar,
        patch.object(plt, "savefig", wraps=plt.savefig) as savefig,
        patch.object(plt, "show", wraps=plt.show) as show,
        patch.object(plt, "close", wraps=plt.close) as close,
    ):
        exec(
            compile(
                cell_source,
                f"{script_path}::cell_12",
                "exec",
            ),
            namespace,
        )

    assert Path(plot_filename).is_file()
    figure.assert_called_once_with()
    hexbin.assert_called_once()
    pd.testing.assert_series_equal(
        hexbin.call_args.args[0],
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255),
    )
    pd.testing.assert_series_equal(
        hexbin.call_args.args[1],
        all_class_beacons_df["wlan.qbss.adc"],
    )
    assert hexbin.call_args.kwargs["gridsize"] == 50
    assert hexbin.call_args.kwargs["cmap"] == "Blues"
    norm = hexbin.call_args.kwargs["norm"]
    assert isinstance(norm, mcolors.LogNorm)
    log_norm.assert_called_once_with()
    assert norm.clip is False
    colorbar.assert_called_once_with(label="Log Count")
    xlabel.assert_called_once_with("Channel Utilization (%)")
    ylabel.assert_called_once_with("Admission Capacity")
    title.assert_called_once_with(
        "Hexbin of Channel Utilization (%) vs. wlan.qbss.adc"
    )
    savefig.assert_called_once_with(plot_filename)
    show.assert_called_once_with()
    close.assert_called_once_with()
