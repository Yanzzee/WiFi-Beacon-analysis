from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from corrected_beacon_selection import (
    build_class_bins,
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


def test_cf02_tsf_anchor_order_duplicates_gaps_and_fallback():
    result = _select("CF02")
    timing = result.timing.set_index("bssid")

    assert timing.loc["A", "tsf_status"] == "usable_advancing"
    assert timing.loc["A", "tsf_origin"] == pd.Timestamp(
        "2026-01-15T09:59:59.100000000Z"
    )
    assert timing.loc["B", "tsf_status"] == "insufficient_progression"
    assert timing.loc["C", "tsf_status"] == "frozen"
    assert timing.loc["D", "tsf_status"] == "reversing"
    assert timing.loc["E", "tsf_status"] == "time_mismatch"
    assert timing.loc["G", "tsf_status"] == "usable_advancing"

    a_rows = result.observations.loc[
        result.observations["bssid"] == "A"
    ].sort_values("sequence_order")
    assert a_rows["row_id"].tolist() == ["D2", "D5", "D4"]
    assert a_rows["bin_number"].tolist() == [0, 1, 1]
    assert a_rows["tsf_derived_time"].tolist() == [
        pd.Timestamp("2026-01-15T10:00:00.990000000Z"),
        pd.Timestamp("2026-01-15T10:00:01.100000000Z"),
        pd.Timestamp("2026-01-15T10:00:01.304800000Z"),
    ]
    assert a_rows.set_index("row_id").loc[
        "D4", "skipped_intervals_before"
    ] == 1
    assert a_rows.set_index("row_id").loc[
        "D2", "duplicate_views_collapsed"
    ] == 1

    assert result.selected.loc[
        result.selected["bssid"] == "A", "row_id"
    ].tolist() == ["D2", "D5"]

    excluded = result.excluded.set_index("row_id")
    assert excluded.loc["D1", "exclusion_reason"] == "duplicate_view"
    assert excluded.loc["D12", "exclusion_reason"] == (
        "outside_class_observation_time"
    )
    assert excluded.loc["D13", "exclusion_reason"] == (
        "tsf_time_outside_class"
    )

    for bssid in ["B", "C", "D", "E"]:
        rows = result.observations.loc[result.observations["bssid"] == bssid]
        assert rows["timing_method"].eq("observation_time_fallback").all()
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


def test_integrated_selected_observations_match_corrected_oracle():
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

    pd.testing.assert_frame_equal(actual, expected, check_like=False)


def test_one_interval_delivery_reordering_uses_tsf_without_mutating_input():
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
    assert result.timing.loc[0, "tsf_status"] == "usable_advancing"
    ordered = result.observations.sort_values("sequence_order")
    assert ordered["row_id"].tolist() == [
        "earlier-tbtt-delayed",
        "later-tbtt-arrived-first",
    ]
    assert result.selected["row_id"].tolist() == [
        "later-tbtt-arrived-first"
    ]


def test_aruba_timestamp_source_precedes_capture_rank_for_duplicate_views():
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

    assert result.selected["row_id"].tolist() == ["aruba-view"]
    assert result.excluded.set_index("row_id").loc[
        "frame-view", "exclusion_reason"
    ] == "duplicate_view"


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
