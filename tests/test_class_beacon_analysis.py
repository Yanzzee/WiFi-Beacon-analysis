from __future__ import annotations

from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

from analysis_statistics import compute_pdf_cdf
from class_beacon_analysis import (
    calculate_station_count_bins,
    filter_configured_ssids,
    prepare_channel_utilization_distribution,
    prepare_channel_utilization_histogram,
    report_channel_utilization_cdf,
    report_class_beacon_aggregates,
    report_class_beacon_correlations,
    report_class_beacon_linear_regression,
)


def test_filter_configured_ssids_preserves_rows_indices_and_output(capsys):
    source_df = pd.DataFrame(
        {
            "wlan.ssid": [
                "CampusSecure",
                "OtherNetwork",
                "CampusGuest",
                "CampusSecure",
            ],
            "row_id": [1, 2, 3, 4],
        },
        index=[10, 20, 30, 40],
    )

    filtered_df = filter_configured_ssids(
        source_df,
        ["CampusSecure", "CampusGuest"],
    )

    pd.testing.assert_frame_equal(
        filtered_df,
        source_df.loc[[10, 30, 40]],
    )
    assert capsys.readouterr().out == (
        "['CampusSecure' 'OtherNetwork' 'CampusGuest']\n"
        "['CampusSecure' 'CampusGuest']\n"
    )


def test_report_class_beacon_aggregates_preserves_messages():
    all_class_beacons_df = pd.DataFrame(
        {
            "wlan.qbss.cu": [100.0, 128.0, 192.0, None],
            "total_scount": [2.0, 4.0, 6.0, 8.0],
        }
    )
    messages: list[str] = []

    report_class_beacon_aggregates(
        all_class_beacons_df,
        messages.append,
    )

    assert messages == [
        "percent of beacons with very high utilization (> 75% cu): "
        "25.0000 %",
        "percent of beacons with high utilization (> 50% cu): "
        "50.0000 %",
        "median number of associated devices per radio: 5.0",
        "average number of associated devices per radio: 5.00",
        "median channel utilization qbss: 128.0",
        "average channel utilization qbss: 140.00",
    ]


def test_channel_utilization_preparation_and_cdf_reporting(monkeypatch):
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
    expected_cu_percent = (
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255)
    )
    expected_cu_data = expected_cu_percent.dropna()
    compute_mock = Mock(wraps=compute_pdf_cdf)
    monkeypatch.setattr(
        "class_beacon_analysis.compute_pdf_cdf",
        compute_mock,
    )

    cu_percent, bins = prepare_channel_utilization_histogram(
        all_class_beacons_df,
    )
    cu_data, bin_centers, pdf, cdf = (
        prepare_channel_utilization_distribution(
            all_class_beacons_df,
        )
    )
    messages: list[str] = []
    cdf_at_50, cdf_at_75 = report_channel_utilization_cdf(
        cu_data,
        messages.append,
    )

    pd.testing.assert_series_equal(cu_percent, expected_cu_percent)
    np.testing.assert_allclose(bins, np.linspace(0, 100, 256))
    pd.testing.assert_series_equal(cu_data, expected_cu_data)
    compute_mock.assert_called_once()
    pd.testing.assert_series_equal(
        compute_mock.call_args.args[0],
        expected_cu_data,
    )
    assert compute_mock.call_args.kwargs == {"bins": 256}
    expected_distribution = compute_pdf_cdf(
        expected_cu_data,
        bins=256,
    )
    for actual, expected in zip(
        (bin_centers, pdf, cdf),
        expected_distribution,
        strict=True,
    ):
        np.testing.assert_allclose(actual, expected)
    assert cdf_at_50 == 0.5
    assert cdf_at_75 == 0.75
    assert messages == [
        "CDF at wlan.qbss.cu = 50%: 0.5000",
        "50.00 % of beacons during class show channel utilization "
        "under 50%",
        "CDF at wlan.qbss.cu = 75%: 0.7500",
        "75.00 % of beacons during class show channel utilization "
        "under 75%",
    ]


def test_station_bins_correlations_and_regression_preserve_results():
    all_class_beacons_df = pd.DataFrame(
        {
            "total_scount": [1.0, 2.0, 3.0, 4.0],
            "wlan.qbss.cu": [10.0, 20.0, 30.0, 40.0],
            "wlan.qbss.adc": [40.0, 30.0, 20.0, 10.0],
        }
    )
    messages: list[str] = []

    num_bins = calculate_station_count_bins(all_class_beacons_df)
    correlations = report_class_beacon_correlations(
        all_class_beacons_df,
        messages.append,
    )
    regression = report_class_beacon_linear_regression(
        all_class_beacons_df,
        messages.append,
    )

    assert num_bins == 5.0
    assert correlations == pytest.approx((1.0, -1.0, -1.0))
    assert regression[0] == pytest.approx(10.0)
    assert regression[1] == pytest.approx(0.0)
    assert regression[2] == pytest.approx(1.0)
    assert regression[3] == pytest.approx(1e-20)
    assert regression[4] == pytest.approx(0.0)
    assert messages == [
        "The correlation between total_scount and wlan.qbss.cu is: "
        "1.0000",
        "The correlation between wlan.qbss.adc and wlan.qbss.cu is: "
        "-1.0000",
        "The correlation between total_scount and wlan.qbss.adc is: "
        "-1.0000",
        "Linear regression values of total_scount to wlan.qbss.cu:",
        "Slope: 10.000",
        "Intercept: 0.000",
        "R-squared: 1.000",
        "P-value: 1e-20",
    ]
