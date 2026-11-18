from __future__ import annotations

from collections import Counter
from pathlib import Path
import warnings

import pandas as pd
import pyarrow.parquet as pq
import pytest

from corrected_station_reports import (
    AGGREGATE_REPORT_FILENAME,
    BAND_HISTOGRAM_FILENAME,
    CLASS_REPORT_FILENAME,
    CLASSROOM_HISTOGRAM_FILENAME,
    ENROLLMENT_WEIGHTED_METHOD,
    METHODOLOGY_FILENAME,
    REPORT_DIRECTORY,
    ROLLUP_FILENAME,
    STATION_AGGREGATE_REPORT_COLUMNS,
    STATION_CLASS_REPORT_COLUMNS,
    STATION_CLASS_REPORT_DTYPES,
    STATION_METRIC_ID,
    build_station_aggregate_report,
    build_station_bin_rollups,
    build_station_class_report,
    build_station_band_histogram,
    write_station_report_package,
)


def test_band_station_histograms_preserve_counts_and_share_vertical_scale():
    counts = {"band:5": Counter({10: 50}), "band:6": Counter({20: 3})}
    figure = build_station_band_histogram(counts, {"band:6": Counter({20: 1})})
    left, right = figure.axes
    assert left.get_shared_y_axes().joined(left, right)
    assert left.get_ylim() == right.get_ylim()
    assert left.get_ylim()[1] >= 50
    assert max(patch.get_height() for patch in left.patches if hasattr(patch, "get_height")) == 50
    assert max(patch.get_height() for patch in right.patches if hasattr(patch, "get_height")) == 3


def _bins(count: int) -> pd.DataFrame:
    start = pd.Timestamp("2026-10-03T15:00:00Z")
    starts = [start + pd.Timedelta(seconds=index) for index in range(count)]
    return pd.DataFrame(
        {
            "class_session_id": ["C1"] * count,
            "classroom": ["ROOM1"] * count,
            "bin_number": list(range(count)),
            "bin_start": starts,
            "bin_end": [value + pd.Timedelta(seconds=1) for value in starts],
            "bin_duration_us": [1_000_000] * count,
        }
    )


def _selected(rows: list[tuple[str, int, int | None]]) -> pd.DataFrame:
    records = []
    for bssid, bin_number, station_count in rows:
        records.append(
            {
                "class_session_id": "C1",
                "classroom": "ROOM1",
                "bin_number": bin_number,
                "bssid": bssid,
                "ssid": "eduroam",
                "radio_group_id": "AP-01/radio/5",
                "band": "5",
                "qbss_station_count": station_count,
            }
        )
    return pd.DataFrame(
        records,
        columns=[
            "class_session_id",
            "classroom",
            "bin_number",
            "bssid",
            "ssid",
            "radio_group_id",
            "band",
            "qbss_station_count",
        ],
    )


def _entity(
    rollups: pd.DataFrame, rollup_type: str, rollup_id: str
) -> pd.DataFrame:
    return rollups.loc[
        rollups["rollup_type"].astype(str).eq(rollup_type)
        & rollups["rollup_id"].astype(str).eq(rollup_id)
    ].sort_values("bin_number")


def test_forward_extension_is_inclusive_and_incomplete_totals_are_null():
    rollups = build_station_bin_rollups(
        _bins(32),
        _selected(
            [
                ("aa:00:00:00:00:01", 0, 10),
                ("aa:00:00:00:00:02", 0, 5),
                ("aa:00:00:00:00:02", 31, 7),
            ]
        ),
        run_id="run-1",
    )

    radio = _entity(rollups, "radio", "AP-01/radio/5")
    assert int(radio.loc[radio["bin_number"].eq(30), "station_count"].iloc[0]) == 15
    assert bool(radio.loc[radio["bin_number"].eq(30), "stale_assisted"].iloc[0])
    final = radio.loc[radio["bin_number"].eq(31)].iloc[0]
    assert not bool(final["available"])
    assert pd.isna(final["station_count"])
    assert not bool(final["stale_assisted"])

    classroom = _entity(rollups, "classroom", "ROOM1")
    assert pd.isna(classroom.loc[classroom["bin_number"].eq(31), "station_count"]).all()
    assert set(rollups["rollup_type"].astype(str)) == {
        "radio",
        "ssid",
        "band",
        "classroom",
    }


def test_leading_backward_extension_is_inclusive_but_not_unlimited():
    rollups = build_station_bin_rollups(
        _bins(32),
        _selected([("aa:00:00:00:00:01", 31, 9)]),
        run_id="run-1",
    )

    classroom = _entity(rollups, "classroom", "ROOM1")
    bin_zero = classroom.loc[classroom["bin_number"].eq(0)].iloc[0]
    bin_one = classroom.loc[classroom["bin_number"].eq(1)].iloc[0]
    assert not bool(bin_zero["available"])
    assert pd.isna(bin_zero["station_count"])
    assert int(bin_one["station_count"]) == 9
    assert bool(bin_one["stale_assisted"])


def test_future_value_does_not_fill_an_interior_gap():
    rollups = build_station_bin_rollups(
        _bins(41),
        _selected(
            [
                ("aa:00:00:00:00:01", 0, 5),
                ("aa:00:00:00:00:01", 40, 10),
            ]
        ),
        run_id="run-1",
    )

    classroom = _entity(rollups, "classroom", "ROOM1")
    assert int(classroom.loc[classroom["bin_number"].eq(30), "station_count"].iloc[0]) == 5
    assert pd.isna(
        classroom.loc[classroom["bin_number"].eq(39), "station_count"].iloc[0]
    )
    assert int(classroom.loc[classroom["bin_number"].eq(40), "station_count"].iloc[0]) == 10


def test_measured_zero_is_valid_and_malformed_values_are_rejected():
    rollups = build_station_bin_rollups(
        _bins(2),
        _selected([("aa:00:00:00:00:01", 0, 0)]),
        run_id="run-1",
    )
    classroom = _entity(rollups, "classroom", "ROOM1")
    assert classroom["station_count"].astype("UInt64").tolist() == [0, 0]
    assert classroom["available"].astype(bool).all()
    assert not bool(classroom.iloc[0]["stale_assisted"])
    assert bool(classroom.iloc[1]["stale_assisted"])

    for invalid in (-1, 65_536, 1.5):
        with pytest.raises(ValueError, match="qbss_station_count"):
            build_station_bin_rollups(
                _bins(1),
                _selected([("aa:00:00:00:00:01", 0, invalid)]),
                run_id="run-1",
            )


def test_rollups_sum_base_bssids_once_and_unavailability_is_contained():
    selected = _selected(
        [
            ("aa:00:00:00:00:01", 0, 4),
            ("aa:00:00:00:00:02", 0, 5),
        ]
    )
    complete = build_station_bin_rollups(_bins(1), selected, run_id="run-1")
    assert set(complete["station_count"].astype("UInt64")) == {9}

    separated = _selected(
        [
            ("aa:00:00:00:00:01", 0, 4),
            ("aa:00:00:00:00:02", 31, 5),
        ]
    )
    second = separated["bssid"].eq("aa:00:00:00:00:02")
    separated.loc[second, "radio_group_id"] = "AP-02/radio/6"
    separated.loc[second, "ssid"] = "guest"
    separated.loc[second, "band"] = "6"
    rollups = build_station_bin_rollups(_bins(32), separated, run_id="run-1")

    final = rollups.loc[rollups["bin_number"].eq(31)]
    available_entities = set(
        final.loc[final["available"].astype(bool), ["rollup_type", "rollup_id"]]
        .itertuples(index=False, name=None)
    )
    assert available_entities == {
        ("radio", "AP-02/radio/6"),
        ("ssid", "guest"),
        ("band", "6"),
    }
    unavailable_entities = set(
        final.loc[~final["available"].astype(bool), ["rollup_type", "rollup_id"]]
        .itertuples(index=False, name=None)
    )
    assert unavailable_entities == {
        ("radio", "AP-01/radio/5"),
        ("ssid", "eduroam"),
        ("band", "5"),
        ("classroom", "ROOM1"),
    }
    assert final.loc[~final["available"].astype(bool), "station_count"].isna().all()


def test_absent_session_bssid_is_not_expected_and_empty_session_is_unknown():
    rollups = build_station_bin_rollups(
        _bins(2),
        _selected([("aa:00:00:00:00:01", 0, 4)]),
        run_id="run-1",
    )
    classroom = _entity(rollups, "classroom", "ROOM1")
    assert classroom["station_count"].astype("UInt64").tolist() == [4, 4]

    empty = build_station_bin_rollups(
        _bins(2),
        _selected([]),
        run_id="run-1",
    )
    assert empty["rollup_type"].astype(str).tolist() == ["classroom", "classroom"]
    assert not empty["available"].astype(bool).any()
    assert empty["station_count"].isna().all()


def test_class_report_uses_peak_combined_enrollment_and_prefers_fresh_tie():
    bins = _bins(3)
    rollups = build_station_bin_rollups(
        bins,
        _selected([("aa:00:00:00:00:01", 0, 10)]),
        run_id="run-1",
    )
    report = build_station_class_report(
        rollups,
        {
            "class_session_id": "C1",
            "classroom": "ROOM1",
            "class_start": bins.loc[0, "bin_start"],
            "class_end": bins.loc[2, "bin_end"],
            "enrolled": 5,
        },
    )
    classroom = report.loc[report["rollup_type"].eq("classroom")].iloc[0]
    assert classroom["peak_client_count"] == 10
    assert classroom["peak_bin_number"] == 0
    assert not bool(classroom["peak_stale_assisted"])
    assert classroom["combined_enrollment"] == 5
    assert classroom["peak_client_count_per_enrolled"] == 2.0
    assert report.loc[report["rollup_type"].eq("band"), "combined_enrollment"].isna().all()

    no_enrollment = build_station_class_report(
        rollups,
        {
            "class_session_id": "C1",
            "classroom": "ROOM1",
            "class_start": bins.loc[0, "bin_start"],
            "class_end": bins.loc[2, "bin_end"],
            "enrolled": 0,
        },
    )
    assert no_enrollment["peak_client_count_per_enrolled"].isna().all()


def test_stale_assisted_aggregate_can_supply_classroom_peak():
    selected = _selected(
        [
            ("aa:00:00:00:00:01", 0, 10),
            ("aa:00:00:00:00:01", 2, 0),
            ("aa:00:00:00:00:02", 0, 0),
            ("aa:00:00:00:00:02", 1, 10),
        ]
    )
    bins = _bins(3)
    rollups = build_station_bin_rollups(bins, selected, run_id="run-1")
    report = build_station_class_report(
        rollups,
        {
            "class_session_id": "C1",
            "classroom": "ROOM1",
            "class_start": bins.loc[0, "bin_start"],
            "class_end": bins.loc[2, "bin_end"],
            "enrolled": 10,
        },
    )

    classroom = report.loc[report["rollup_type"].eq("classroom")].iloc[0]
    assert classroom["peak_client_count"] == 20
    assert classroom["peak_bin_number"] == 1
    assert bool(classroom["peak_stale_assisted"])
    assert classroom["peak_client_count_per_enrolled"] == 2.0


@pytest.mark.parametrize("reverse", [False, True])
def test_class_report_concat_preserves_nullable_schema_and_unknowns(reverse):
    parts = []
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        for sid, enrollment, observed in (("VALID", 5, True), ("ZERO", 0, True),
                                          ("MISSING", None, True), ("UNKNOWN", 5, False)):
            bins = _bins(2).assign(class_session_id=sid)
            selected = _selected([("aa:00:00:00:00:01", 0, 10)] if observed else []).assign(class_session_id=sid)
            rollups = build_station_bin_rollups(bins, selected, run_id="run-1")
            session = {"class_session_id": sid, "classroom": "ROOM1", "class_start": bins.bin_start.iloc[0],
                       "class_end": bins.bin_end.iloc[-1], "enrolled": enrollment}
            part = build_station_class_report(rollups, session)
            for column, dtype in STATION_CLASS_REPORT_DTYPES.items():
                assert str(part[column].dtype) == dtype
            parts.append(part)
        empty = build_station_class_report(rollups.iloc[:0], session)
        assert empty.empty
        assert tuple(empty.columns) == STATION_CLASS_REPORT_COLUMNS
        combined = pd.concat([empty, *(parts[::-1] if reverse else parts), empty], ignore_index=True)
        aggregate = build_station_aggregate_report(combined)
    for column, dtype in STATION_CLASS_REPORT_DTYPES.items():
        assert str(empty[column].dtype) == str(combined[column].dtype) == dtype
    classroom = combined.loc[combined.rollup_type.eq("classroom")].set_index("class_session_id")
    assert classroom.loc["VALID", "peak_client_count_per_enrolled"] == 2.0
    assert classroom.loc[["ZERO", "MISSING", "UNKNOWN"], "peak_client_count_per_enrolled"].isna().all()
    assert classroom.loc[["VALID", "ZERO", "MISSING"], "peak_client_count"].eq(10).all()
    assert pd.isna(classroom.loc["MISSING", "combined_enrollment"])
    assert pd.isna(classroom.loc["UNKNOWN", "peak_client_count"])
    assert pd.isna(classroom.loc["UNKNOWN", "peak_bin_number"])
    assert pd.isna(classroom.loc["UNKNOWN", "peak_stale_assisted"])
    assert combined.loc[~combined.rollup_type.eq("classroom"), "combined_enrollment"].isna().all()
    overall = aggregate.loc[aggregate.population_type.eq("overall")].iloc[0]
    assert overall.eligible_session_count == 1
    assert overall.average_peak_client_count_per_enrolled == 2.0


def test_class_report_typed_counts_preserve_large_integer_and_missing_values():
    bins = _bins(1)
    rollups = build_station_bin_rollups(bins, _selected([("aa:00:00:00:00:01", 0, 10)]), run_id="run-1")
    # Mixed valid/unknown entity rows must not round large UInt64 values through
    # an inferred float64 column before the explicit nullable dtype is applied.
    rollups["station_count"] = pd.array([2**53 + 1, *([None] * (len(rollups) - 1))], dtype="UInt64")
    rollups["available"] = [True, *([False] * (len(rollups) - 1))]
    report = build_station_class_report(rollups, {"class_session_id": "C1", "classroom": "ROOM1",
        "class_start": bins.bin_start.iloc[0], "class_end": bins.bin_end.iloc[-1], "enrolled": 5})
    assert report.peak_client_count.dropna().tolist() == [2**53 + 1]
    assert report.peak_client_count.isna().sum() == len(rollups) - 1


def test_aggregate_report_uses_enrollment_weighted_ratio_of_sums():
    def row(
        session: str,
        rollup_type: str,
        rollup_id: str,
        peak: int,
        *,
        enrollment: int | None = None,
        stale: bool = False,
    ) -> dict[str, object]:
        values = {column: None for column in STATION_CLASS_REPORT_COLUMNS}
        values.update(
            {
                "run_id": "run-1",
                "schema_id": "beacon.schema/analysis-results/v1",
                "metric_id": STATION_METRIC_ID,
                "class_session_id": session,
                "classroom": f"ROOM-{session}",
                "rollup_type": rollup_type,
                "rollup_id": rollup_id,
                "peak_client_count": peak,
                "peak_stale_assisted": stale,
                "combined_enrollment": enrollment,
            }
        )
        return values

    report = pd.DataFrame(
        [
            row("C1", "classroom", "ROOM-C1", 20, enrollment=10),
            row("C1", "ssid", "eduroam", 20),
            row("C1", "ssid", "guest", 5),
            row("C1", "band", "5", 20),
            row("C2", "classroom", "ROOM-C2", 100, enrollment=100, stale=True),
            row("C2", "ssid", "eduroam", 100, stale=True),
            row("C2", "band", "5", 100, stale=True),
            row("C3", "classroom", "ROOM-C3", 50, enrollment=0),
            row("C3", "ssid", "eduroam", 50),
            row("C3", "band", "6", 50),
        ],
        columns=STATION_CLASS_REPORT_COLUMNS,
    )

    aggregate = build_station_aggregate_report(report)
    assert tuple(aggregate.columns) == STATION_AGGREGATE_REPORT_COLUMNS
    aggregate = aggregate.set_index(["population_type", "population_id"])

    overall = aggregate.loc[("overall", "all")]
    assert overall["aggregation_method"] == ENROLLMENT_WEIGHTED_METHOD
    assert overall["eligible_session_count"] == 2
    assert overall["peak_client_count_sum"] == 120
    assert overall["combined_enrollment_sum"] == 110
    assert overall["stale_assisted_peak_session_count"] == 1
    assert overall["average_peak_client_count_per_enrolled"] == pytest.approx(
        120 / 110
    )
    assert overall["average_peak_client_count_per_enrolled"] != pytest.approx(
        (2.0 + 1.0) / 2
    )

    assert aggregate.loc[
        ("ssid", "guest"), "average_peak_client_count_per_enrolled"
    ] == pytest.approx(0.5)
    excluded = aggregate.loc[("band", "6")]
    assert excluded["eligible_session_count"] == 0
    assert pd.isna(excluded["average_peak_client_count_per_enrolled"])


def test_station_package_streams_parquet_and_writes_methodology(tmp_path: Path):
    bins = _bins(2)
    rollups = build_station_bin_rollups(
        bins,
        _selected([("aa:00:00:00:00:01", 0, 10)]),
        run_id="run-1",
    )
    report = build_station_class_report(
        rollups,
        {
            "class_session_id": "C1",
            "classroom": "ROOM1",
            "class_start": bins.loc[0, "bin_start"],
            "class_end": bins.loc[1, "bin_end"],
            "enrolled": 5,
        },
    )

    written = write_station_report_package(
        tmp_path,
        rollup_parts=[rollups],
        class_report=report,
    )

    root = tmp_path / REPORT_DIRECTORY
    assert written.rollup_rows == len(rollups)
    assert written.class_report_rows == len(report)
    assert written.aggregate_report_rows == 3
    assert pq.read_table(root / ROLLUP_FILENAME).num_rows == len(rollups)
    assert pd.read_csv(root / CLASS_REPORT_FILENAME)["metric_id"].eq(
        STATION_METRIC_ID
    ).all()
    aggregate = pd.read_csv(root / AGGREGATE_REPORT_FILENAME)
    assert set(aggregate["population_type"]) == {"overall", "ssid", "band"}
    assert aggregate["aggregation_method"].eq(ENROLLMENT_WEIGHTED_METHOD).all()
    assert (root / CLASSROOM_HISTOGRAM_FILENAME).is_file()
    assert (root / BAND_HISTOGRAM_FILENAME).is_file()
    methodology = (root / METHODOLOGY_FILENAME).read_text(encoding="utf-8")
    assert "30 seconds" in methodology
    assert "peak client count per enrolled" in methodology
    assert "all-band/all-SSID" in methodology
