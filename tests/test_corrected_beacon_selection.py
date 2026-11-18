from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from corrected_beacon_selection import (
    build_class_bins,
    preferred_observation_times,
    select_bssid_session_observations,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "corrected_semantics"
INPUT_ROOT = FIXTURE_ROOT / "input"
EXPECTED_ROOT = FIXTURE_ROOT / "expected"
GOLDEN_PATH = REPO_ROOT / "tests" / "golden" / "corrected_semantics_v1.json"


def _load_inputs(fixture_id: str) -> tuple[pd.DataFrame, pd.Series]:
    observations = pd.read_csv(
        INPUT_ROOT / "observations.csv",
        dtype="string",
        keep_default_na=False,
    )
    observations = observations.loc[
        observations["fixture_id"] == fixture_id
    ].copy()
    observations = observations.drop(
        columns=["expected_observation_time", "expected_observation_source"]
    )
    identity = pd.read_csv(
        INPUT_ROOT / "identity_evidence.csv",
        dtype="string",
        keep_default_na=False,
    )
    identity = identity.loc[identity["fixture_id"] == fixture_id]
    if not identity.empty:
        observations = observations.merge(
            identity[["bssid", "band", "channel"]],
            on="bssid",
            how="left",
            validate="many_to_one",
        )

    classes = pd.read_csv(INPUT_ROOT / "classes.csv", dtype="string")
    class_rows = classes.loc[classes["fixture_id"] == fixture_id]
    if class_rows.empty:
        raise AssertionError(f"No class fixture for {fixture_id}")
    return observations, class_rows.iloc[0]


def _select(
    fixture_id: str,
    *,
    class_session_id: str | None = None,
    observations: pd.DataFrame | None = None,
):
    loaded, class_row = _load_inputs(fixture_id)
    if observations is None:
        observations = loaded
    if class_session_id is not None:
        classes = pd.read_csv(INPUT_ROOT / "classes.csv", dtype="string")
        class_row = classes.loc[
            (classes["fixture_id"] == fixture_id)
            & (classes["class_session_id"] == class_session_id)
        ].iloc[0]

    return select_bssid_session_observations(
        observations,
        class_session_id=class_row["class_session_id"],
        class_start=class_row["class_start"],
        class_end=class_row["class_end"],
        bin_width_us=int(class_row["bin_width_us"]),
    )


def test_corrected_golden_references_existing_expected_artifacts():
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

    assert golden["schema_id"] == "beacon.schema/analysis-results/v1"
    assert golden["metric_id"] == "beacon.metric/corrected-cu/v1"
    assert golden["comparison"] == {
        "mode": "logical_content",
        "compare_parquet_file_bytes": False,
        "compare_plot_bytes": False,
        "float_policy": "isclose",
        "relative_tolerance": 1e-12,
        "absolute_tolerance": 1e-12,
    }

    for artifact, relative_path in golden["integrated_fixture"][
        "expected_files"
    ].items():
        path = REPO_ROOT / relative_path
        assert path.exists(), f"Missing {artifact}: {path}"

    for artifact, expected_count in golden["integrated_fixture"][
        "expected_row_counts"
    ].items():
        path = REPO_ROOT / Path(
            golden["integrated_fixture"]["expected_files"][artifact]
        )
        assert len(pd.read_csv(path)) == expected_count


def test_corrected_summary_oracles_and_supplements_are_internally_consistent():
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    radio = pd.read_csv(EXPECTED_ROOT / "radio_class_summary.csv")
    classroom = pd.read_csv(EXPECTED_ROOT / "classroom_class_summary.csv")
    manifest = json.loads(
        (EXPECTED_ROOT / "manifest.json").read_text(encoding="utf-8")
    )

    assert (
        radio["valid_cu_duration_us"]
        + radio["missing_cu_duration_us"]
        + radio["no_beacon_duration_us"]
    ).equals(radio["class_duration_us"])
    r5 = radio.set_index("radio_group_id").loc["R5"]
    cf07 = golden["focused_cases"]["CF07"]
    for field, expected in cf07.items():
        if field != "radio_group_id":
            assert r5[field] == pytest.approx(expected)

    threshold_rows, _class_row = _load_inputs("CF08")
    threshold_expected = golden["focused_cases"]["CF08"]["threshold_edges"]
    for row in threshold_rows.itertuples():
        raw = int(row.qbss_cu_raw)
        assert [raw >= 128, raw >= 192] == threshold_expected[str(raw)]

    identity = pd.read_csv(INPUT_ROOT / "identity_evidence.csv", dtype="string")
    same_channel = identity.loc[identity["fixture_id"] == "CF09"]
    cf09 = golden["focused_cases"]["CF09"][
        "same_channel_distinct_radio_case"
    ]
    assert same_channel["radio_group_id"].nunique() == cf09["radio_count_5_ghz"]
    assert same_channel["channel"].nunique() == 1

    selected = pd.read_csv(
        EXPECTED_ROOT / "selected_observations.csv",
        dtype={"bssid": "string", "ssid": "string", "band": "string"},
    )
    bin_zero = selected.loc[selected["bin_number"].eq(0)]
    cf06_station = golden["focused_cases"]["CF06"][
        "station_count_rollup"
    ]
    r5 = bin_zero.loc[bin_zero["radio_group_id"].eq("R5")]
    assert dict(zip(r5["bssid"], r5["qbss_station_count"])) == (
        cf06_station["member_bssid_values"]
    )
    assert int(r5["qbss_station_count"].sum()) == cf06_station["radio_total"]
    assert (
        bin_zero.groupby("ssid")["qbss_station_count"].sum().astype(int).to_dict()
        == cf06_station["classroom_ssid_totals"]
    )
    assert (
        bin_zero.groupby("band")["qbss_station_count"].sum().astype(int).to_dict()
        == cf06_station["classroom_band_totals"]
    )
    assert not cf06_station["cu_preference_filters_station_count"]

    classroom_row = classroom.iloc[0]
    assert classroom_row["radio_count_total"] == (
        classroom_row["radio_count_2_4_ghz"]
        + classroom_row["radio_count_5_ghz"]
        + classroom_row["radio_count_6_ghz"]
        + classroom_row["radio_count_unknown"]
    )
    assert len(manifest["stages"]) == 3
    assert all(stage["status"] == "completed" for stage in manifest["stages"])
    assert manifest["detailed_log"] is None


def test_cf01_observation_time_precedence_and_exclusion():
    result = _select("CF01")

    observed = result.observations.set_index("row_id")
    assert observed.loc["T1", "observation_time"] == pd.Timestamp(
        "2026-01-15T10:00:00.180000000Z"
    )
    assert observed.loc["T1", "observation_time_source"] == "aruba_erm"
    assert observed.loc["T2", "observation_time_source"] == "frame_time_epoch"
    assert observed.loc["T3", "observation_time_source"] == "frame_time_epoch"
    assert observed.loc["T3", "observation_time_diagnostic"] == (
        "aruba_erm_time_malformed"
    )
    assert observed.loc["T5", "observation_time_source"] == "aruba_erm"

    excluded = result.excluded.set_index("row_id")
    assert excluded.loc["T4", "exclusion_reason"] == (
        "source_timestamp_unavailable"
    )


def test_cf02_native_tsf_is_diagnostic_and_observation_time_controls_bins():
    observations, _ = _load_inputs("CF02")
    observations["capture_profile"] = "native_80211"
    observations["frame_time_epoch"] = observations["aruba_erm_time"]
    observations["aruba_erm_time"] = ""
    result = _select("CF02", observations=observations)
    timing = result.timing.set_index("bssid")

    assert timing.loc["A", "tsf_status"] == "usable_advancing"
    assert pd.isna(timing.loc["A", "tsf_origin"])
    assert timing.loc["B", "tsf_status"] == "insufficient_progression"
    assert timing.loc["C", "tsf_status"] == "frozen"
    assert timing.loc["D", "tsf_status"] == "reversing"
    assert timing.loc["E", "tsf_status"] == "discontinuous"
    assert timing.loc["G", "tsf_status"] == "usable_advancing"

    a_rows = result.observations.loc[
        result.observations["bssid"] == "A"
    ].sort_values("sequence_order")
    assert a_rows["row_id"].tolist() == ["D2", "D5", "D4"]
    assert a_rows["bin_number"].tolist() == [1, 1, 1]
    assert a_rows["tsf_derived_time"].isna().all()
    assert a_rows.set_index("row_id").loc[
        "D4", "skipped_intervals_before"
    ] == 1
    assert a_rows.set_index("row_id").loc[
        "D2", "duplicate_views_collapsed"
    ] == 1

    assert result.selected.loc[
        result.selected["bssid"] == "A", "row_id"
    ].tolist() == ["D2"]

    excluded = result.excluded.set_index("row_id")
    assert excluded.loc["D1", "exclusion_reason"] == "duplicate_view"
    assert excluded.loc["D12", "exclusion_reason"] == (
        "outside_class_observation_time"
    )
    assert "D13" in set(result.observations.row_id)

    for bssid in ["B", "C", "D", "E"]:
        rows = result.observations.loc[result.observations["bssid"] == bssid]
        assert rows["timing_method"].eq("observation_time").all()
    assert result.observations.loc[
        result.observations["bssid"] == "C", "row_id"
    ].tolist() == ["D6", "D7"]


def test_cf03_half_open_edges_partial_bin_and_short_class():
    observations, _class_row = _load_inputs("CF03")
    edge = _select("CF03", class_session_id="C-edge", observations=observations)

    assert edge.selected["row_id"].tolist() == ["E2", "E4", "E5"]
    assert edge.selected["bin_number"].tolist() == [0, 1, 2]
    excluded = edge.excluded.set_index("row_id")
    assert excluded.loc["E1", "exclusion_reason"] == (
        "outside_class_observation_time"
    )
    assert excluded.loc["E7", "exclusion_reason"] == (
        "outside_class_observation_time"
    )

    next_class = _select(
        "CF03",
        class_session_id="C-edge-next",
        observations=observations.loc[observations["row_id"] == "E7"],
    )
    assert next_class.selected[["row_id", "bin_number"]].values.tolist() == [
        ["E7", 0]
    ]

    edge_bins = build_class_bins(
        "2026-01-15T10:00:00Z",
        "2026-01-15T10:00:02.560Z",
        1_024_000,
    )
    assert edge_bins["bin_duration_us"].tolist() == [1_024_000, 1_024_000, 512_000]
    assert edge_bins["width_status"].tolist() == [
        "complete",
        "complete",
        "partial",
    ]

    short_bins = build_class_bins(
        "2026-01-15T10:00:00Z",
        "2026-01-15T10:00:00.400Z",
        1_024_000,
    )
    assert short_bins[["bin_duration_us", "width_status"]].values.tolist() == [
        [400_000, "partial"]
    ]


def test_cf04_selects_first_observation_independently_in_each_fixed_bin():
    result = _select("CF04")

    assert result.selected["row_id"].tolist() == ["X1", "X3", "Y1"]
    assert result.selected[["bssid", "bin_number"]].values.tolist() == [
        ["X", 0],
        ["X", 1],
        ["Y", 0],
    ]
    selection = result.observations.set_index("row_id")["is_selected"]
    assert selection.to_dict() == {
        "X1": True,
        "Y1": True,
        "X2": False,
        "Y2": False,
        "X3": True,
        "X4": False,
    }


def test_integrated_observation_time_measurements_preserve_historical_oracle_values():
    result = _select("CF05_10")
    expected = pd.read_csv(
        EXPECTED_ROOT / "selected_observations.csv",
        dtype={
            "source_frame": "string",
            "band": "string",
            "channel": "string",
        },
    )

    columns = [
        "class_session_id",
        "classroom",
        "bin_number",
        "bin_start",
        "bin_end",
        "bin_duration_us",
        "width_status",
        "bssid",
        "ssid",
        "radio_group_id",
        "ap_id",
        "band",
        "channel",
        "observation_time",
        "observation_time_source",
        "tsf_us",
        "tsf_origin",
        "tsf_derived_time",
        "timing_method",
        "tsf_status",
        "unusable_tsf_reason",
        "source_capture",
        "source_frame",
        "duplicate_views_collapsed",
        "qbss_cu_raw",
        "qbss_station_count",
        "qbss_admission_capacity",
    ]

    actual = result.selected.copy()
    actual["classroom"] = "ROOM1"
    actual = actual[columns].reset_index(drop=True)
    expected = expected[columns].reset_index(drop=True)

    timestamp_columns = [
        "bin_start",
        "bin_end",
        "observation_time",
        "tsf_origin",
        "tsf_derived_time",
    ]
    for column in timestamp_columns:
        actual[column] = pd.to_datetime(actual[column], utc=True)
        expected[column] = pd.to_datetime(expected[column], utc=True)

    for column in [
        "bin_number",
        "bin_duration_us",
        "tsf_us",
        "duplicate_views_collapsed",
        "qbss_cu_raw",
        "qbss_station_count",
        "qbss_admission_capacity",
    ]:
        actual[column] = pd.to_numeric(actual[column]).astype("Int64")
        expected[column] = pd.to_numeric(expected[column]).astype("Int64")

    for column in set(columns) - set(timestamp_columns):
        if column not in {
            "bin_number",
            "bin_duration_us",
            "tsf_us",
            "duplicate_views_collapsed",
            "qbss_cu_raw",
            "qbss_station_count",
            "qbss_admission_capacity",
        }:
            actual[column] = actual[column].fillna("").astype("string")
            expected[column] = expected[column].fillna("").astype("string")

    # Historical v1 mapped-clock metadata is retained as an old-run reference.
    # This fixture's selected measurements are unchanged under observation time.
    historical_timing = {"tsf_origin", "tsf_derived_time", "timing_method", "tsf_status",
                         "unusable_tsf_reason", "duplicate_views_collapsed"}
    compare = [column for column in columns if column not in historical_timing]
    pd.testing.assert_frame_equal(actual[compare], expected[compare], check_like=False)
    assert actual.timing_method.eq("observation_time").all()
    assert actual.tsf_origin.isna().all() and actual.tsf_derived_time.isna().all()


def test_delivery_reordering_keeps_observation_order_without_mutating_input():
    observations = pd.DataFrame(
        {
            "row_id": ["earlier-tbtt-delayed", "later-tbtt-arrived-first"],
            "input_order": [1, 2],
            "bssid": ["A", "A"],
            "aruba_erm_time": [
                "2026-01-15T10:00:00.200000000Z",
                "2026-01-15T10:00:00.150000000Z",
            ],
            "frame_time_epoch": ["", ""],
            "tsf_us": [1_000_000, 1_102_400],
            "beacon_interval_tu": [100, 100],
            "source_rank": [1, 1],
            "source_capture": ["capture.pcapng", "capture.pcapng"],
            "source_frame": [1, 2],
        }
    )
    original = observations.copy(deep=True)

    result = select_bssid_session_observations(
        observations,
        class_session_id="reordered",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01.024Z",
        bin_width_us=1_024_000,
    )

    pd.testing.assert_frame_equal(observations, original)
    assert result.timing.loc[0, "tsf_status"] == "unknown_capture_profile"
    ordered = result.observations.sort_values("sequence_order")
    assert ordered["row_id"].tolist() == [
        "later-tbtt-arrived-first",
        "earlier-tbtt-delayed",
    ]
    assert result.selected["row_id"].tolist() == [
        "later-tbtt-arrived-first"
    ]


def test_mixed_unverified_tsf_does_not_override_the_earliest_observation():
    observations = pd.DataFrame(
        {
            "row_id": ["frame-view", "aruba-view", "next-beacon"],
            "input_order": [1, 2, 3],
            "bssid": ["A", "A", "A"],
            "aruba_erm_time": [
                "",
                "2026-01-15T10:00:00.200000000Z",
                "2026-01-15T10:00:00.300000000Z",
            ],
            "frame_time_epoch": [
                "2026-01-15T10:00:00.100000000Z",
                "2026-01-15T10:00:00.205000000Z",
                "2026-01-15T10:00:00.305000000Z",
            ],
            "tsf_us": [1_000_000, 1_000_000, 1_102_400],
            "beacon_interval_tu": [100, 100, 100],
            "source_rank": [1, 2, 2],
            "source_capture": ["frame.pcapng", "aruba.pcapng", "aruba.pcapng"],
            "source_frame": [1, 1, 2],
        }
    )

    result = select_bssid_session_observations(
        observations,
        class_session_id="source-precedence",
        class_start="2026-01-15T10:00:00Z",
        class_end="2026-01-15T10:00:01.024Z",
        bin_width_us=1_024_000,
    )

    assert result.selected["row_id"].tolist() == ["frame-view"]
    assert result.excluded.empty


def test_rejects_naive_class_boundaries_and_invalid_width():
    observations, class_row = _load_inputs("CF04")

    with pytest.raises(ValueError, match="timezone-aware"):
        select_bssid_session_observations(
            observations,
            class_session_id=class_row["class_session_id"],
            class_start="2026-01-15 10:00:00",
            class_end=class_row["class_end"],
            bin_width_us=1_024_000,
        )

    with pytest.raises(ValueError, match="positive"):
        build_class_bins(
            class_row["class_start"],
            class_row["class_end"],
            0,
        )


def test_forward_tsf_jump_never_moves_or_excludes_observed_measurements():
    start = pd.Timestamp("2026-11-09T18:00:00Z")
    times = [start + pd.Timedelta(seconds=.01 + i * 1.024) for i in range(4)]
    native = pd.DataFrame({"bssid": ["A"] * 4, "ssid": ["eduroam"] * 4,
        "capture_profile": ["native_80211"] * 4, "frame_time_epoch": times,
        "tsf_us": [1000000, 2024000, 1664074390, 1665098390],
        "source_capture": ["native.pcapng"] * 4, "source_frame": [1, 2, 3, 4],
        "qbss_cu_raw": [44, 19, 60, 10], "qbss_station_count": [17, 24, 30, 20]})
    arguments = dict(class_session_id="HBLL", class_start=start,
                     class_end=start + pd.Timedelta(minutes=75), bin_width_us=1024000)
    result = select_bssid_session_observations(native, **arguments)
    assert result.excluded.empty
    assert result.selected.bin_number.tolist() == [0, 1, 2, 3]
    assert result.selected.qbss_cu_raw.tolist() == [44, 19, 60, 10]
    assert result.selected.placement_time.equals(result.selected.observation_time)
    assert result.selected.tsf_derived_time.isna().all()
    assert result.timing.tsf_status.tolist() == ["discontinuous"]
    assert result.timing.tsf_clock_jump_count.tolist() == [1]
    assert result.timing.tsf_anomaly_source_frame.tolist() == ["3"]
    erm = native.copy()
    erm["capture_profile"], erm["tsf_us"], erm["source_capture"] = "aruba_erm_type0", 56, "erm.pcapng"
    erm["aruba_erm_time"] = pd.to_datetime(erm.frame_time_epoch) + pd.Timedelta(milliseconds=10)
    mixed = select_bssid_session_observations(pd.concat([native, erm], ignore_index=True), **arguments)
    pd.testing.assert_frame_equal(result.selected[["bin_number", "qbss_cu_raw", "qbss_station_count"]],
                                  mixed.selected[["bin_number", "qbss_cu_raw", "qbss_station_count"]])
    assert mixed.timing.set_index("capture_family").loc["native", "tsf_status"] == "discontinuous"
    assert mixed.timing.set_index("capture_family").loc["erm", "tsf_status"] == "not_used_for_erm"


def test_large_uint64_tsf_is_retained_without_epoch_conversion_or_precision_loss():
    start = pd.Timestamp("2026-11-09T18:00Z")
    frame = pd.DataFrame({"bssid": ["A"] * 3, "capture_profile": ["native_80211"] * 3,
        "frame_time_epoch": [start + pd.Timedelta(seconds=value) for value in (.01, .11, .21)],
        "tsf_us": pd.array([2**64 - 1048576, 2**64 - 948576, 2**64 - 1], dtype="UInt64"),
        "source_capture": ["native.pcapng"] * 3, "source_frame": [1, 2, 3]})
    result = select_bssid_session_observations(frame, class_session_id="large-tsf",
        class_start=start, class_end=start + pd.Timedelta(seconds=1), bin_width_us=102400)
    assert result.excluded.empty
    assert result.observations.tsf_us.tolist() == frame.tsf_us.tolist()
    assert result.timing.tsf_status.tolist() == ["discontinuous"]
    assert result.timing.tsf_anomaly_value_us.tolist() == [str(2**64 - 1)]


def test_native_profile_uses_capture_time_even_if_an_aruba_column_is_present():
    frame = pd.DataFrame({"bssid": ["A", "B"], "capture_profile": ["native_80211", "aruba_erm_type0"],
        "frame_time_epoch": ["2026-11-09T18:00:00.1Z"] * 2,
        "aruba_erm_time": ["2026-11-09T18:00:01.1Z"] * 2})
    result = select_bssid_session_observations(frame, class_session_id="profiles",
        class_start="2026-11-09T18:00Z", class_end="2026-11-09T18:00:02Z", bin_width_us=1024000)
    assert result.selected.bin_number.tolist() == [0, 1]
    assert result.selected.observation_time_source.tolist() == ["frame_time_epoch", "aruba_erm"]
    assert preferred_observation_times(frame).tolist() == result.selected.observation_time.tolist()
