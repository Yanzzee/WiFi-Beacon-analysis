from __future__ import annotations

import pandas as pd
import pytest

from radio_analysis import analyze_radio_group, calculate_total_scount


def test_calculate_total_scount_current_lookup_order():
    frame = pd.DataFrame(
        {
            "wlan.ta": ["a", "b", "a", "b", "c", "d"],
            "wlan.qbss.scount": [10, 20, 11, 21, 5, 7],
            "radio_number": [1, 1, 1, 1, 2, 2],
        }
    )

    result = calculate_total_scount(frame)

    assert result is frame
    assert result["wlan.ta"].tolist() == ["a", "b", "a", "b", "c", "d"]
    assert result["total_scount"].tolist() == [30, 31, 32, 32, 12, 12]
    assert result["total_scount"].dtype == "int64"


def test_analyze_radio_group_current_metrics_tuple_and_logs():
    messages = []
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    frame = pd.DataFrame(
        {
            "aruba_erm.time": [
                start + pd.Timedelta(seconds=4),
                start,
                start + pd.Timedelta(seconds=2),
                start + pd.Timedelta(seconds=1),
                start + pd.Timedelta(seconds=3),
            ],
            "wlan.qbss.cu": [255, 191, 200, 192, 100],
            "total_scount": [50, 10, 30, 20, 40],
        }
    )

    result = analyze_radio_group(
        frame,
        7,
        beacon_rate=2.0,
        log_and_print=messages.append,
    )

    assert result == pytest.approx(
        (
            5,
            3,
            60,
            100,
            (192 / 255) * 100,
            (100 / 255) * 100,
            50,
            30,
            10,
            3,
        )
    )
    assert messages == [
        "",
        "--- Radio Number 7 ---",
        "Total beacons: 5",
        "Quantity with very high channel utilization(>75%): 3",
        "Percent with very high channel utilization: 60.00%",
        "Highest channel utilization: 100.00%",
        "Median channel utilization: 75.29%",
        "Lowest channel utilization: 39.22%",
        "Highest station count: 50",
        "Median station count: 30",
        "Lowest station count: 10",
        "Longest duration very high channel utilization: 3.00 seconds",
        f"First beacon of sequence: {start + pd.Timedelta(seconds=1)}",
        f"Last beacon of sequence: {start + pd.Timedelta(seconds=2)}",
    ]


def test_analyze_radio_group_current_no_high_cu_log():
    messages = []
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    frame = pd.DataFrame(
        {
            "aruba_erm.time": [
                start + pd.Timedelta(seconds=1),
                start,
            ],
            "wlan.qbss.cu": [100, 191],
            "total_scount": [20, 10],
        }
    )

    result = analyze_radio_group(
        frame,
        4,
        beacon_rate=2.0,
        log_and_print=messages.append,
    )

    assert result[1] == 0
    assert result[-1] == 0
    assert messages[-1] == (
        "No records found where wlan.qbss.cu > 191 for radio_number 4."
    )
