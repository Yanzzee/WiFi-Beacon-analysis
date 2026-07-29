from __future__ import annotations

import pandas as pd

from beacon_filtering import filter_by_time, remove_duplicate_beacons


def _filter_frame(rows):
    frame = pd.DataFrame(
        rows,
        columns=[
            "aruba_erm.time",
            "wlan.ta",
            "wlan.vs.aruba.ap_name",
            "wlan.ssid",
            "source_row",
        ],
    )
    return frame.astype(
        {
            "wlan.ta": "string",
            "wlan.vs.aruba.ap_name": "string",
            "wlan.ssid": "string",
        }
    )


def test_filter_by_time_exact_and_just_under_boundary():
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    frame = _filter_frame(
        [
            (start + pd.Timedelta(milliseconds=1900), "radio-a", "AP-1", "S", "exact-2"),
            (start + pd.Timedelta(milliseconds=949), "radio-a", "AP-1", "S", "under-1"),
            (start, "radio-a", "AP-1", "S", "first"),
            (start + pd.Timedelta(milliseconds=1899), "radio-a", "AP-1", "S", "under-2"),
            (start + pd.Timedelta(milliseconds=950), "radio-a", "AP-1", "S", "exact-1"),
        ]
    )
    frame.index = [50, 30, 10, 40, 20]

    result = filter_by_time(frame, beacon_rate=1.0)

    assert result["source_row"].tolist() == [
        "first",
        "exact-1",
        "exact-2",
    ]
    assert result.index.tolist() == [10, 20, 50]
    assert result["aruba_erm.time"].is_monotonic_increasing


def test_remove_duplicate_beacons_groups_each_transmitter():
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    frame = _filter_frame(
        [
            (start + pd.Timedelta(milliseconds=1150), "radio-b", "AP-1", "S", "b-exact"),
            (start + pd.Timedelta(milliseconds=950), "radio-a", "AP-1", "S", "a-exact"),
            (start + pd.Timedelta(milliseconds=200), "radio-b", "AP-1", "S", "b-first"),
            (start, "radio-a", "AP-1", "S", "a-first"),
            (start + pd.Timedelta(milliseconds=949), "radio-a", "AP-1", "S", "a-under"),
        ]
    )

    result = remove_duplicate_beacons(frame, beacon_rate=1.0)

    assert result["source_row"].tolist() == [
        "a-first",
        "a-exact",
        "b-first",
        "b-exact",
    ]
    assert result["index"].tolist() == [3, 1, 2, 0]
    assert result.index.equals(pd.RangeIndex(4))
    for column in [
        "wlan.ta",
        "wlan.vs.aruba.ap_name",
        "wlan.ssid",
    ]:
        assert str(result[column].dtype) == "string"


def test_remove_duplicate_beacons_single_row():
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    frame = _filter_frame(
        [(start, "only-radio", "AP-1", "S", "only-row")]
    )

    result = remove_duplicate_beacons(frame, beacon_rate=1.0)

    assert result["source_row"].tolist() == ["only-row"]
    assert result["index"].tolist() == [0]
    assert result.index.equals(pd.RangeIndex(1))
