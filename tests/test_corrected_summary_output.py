from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from corrected_beacon_selection import build_class_bins
from corrected_cu_reports import (
    _average_cu_edges_percent,
    _cdf_below_raw_threshold,
    _time_histogram_edges,
    build_corrected_cu_report_tables,
)
from corrected_summary_output import (
    CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
    RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
    build_corrected_summaries,
    write_corrected_summaries,
)


FIXTURE_ROOT = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "corrected_semantics"
)
EXPECTED_ROOT = FIXTURE_ROOT / "expected"
INPUT_ROOT = FIXTURE_ROOT / "input"
RUN_ID = "oracle-corrected-v1"
COMPARISON_GOLDEN = (
    Path(__file__).resolve().parent
    / "golden"
    / "legacy_corrected_comparison_v1.json"
)


def _fixture_inputs():
    selected = pd.read_csv(EXPECTED_ROOT / "selected_observations.csv")
    identity = pd.read_csv(EXPECTED_ROOT / "project_identity_map.csv")
    classes = pd.read_csv(INPUT_ROOT / "classes.csv")
    class_row = classes.loc[classes["fixture_id"].eq("CF05_10")].iloc[0]
    bins = build_class_bins(
        class_row["class_start"],
        class_row["class_end"],
        int(class_row["bin_width_us"]),
    )
    bins["class_session_id"] = class_row["class_session_id"]
    bins["classroom"] = class_row["classroom"]
    return bins, identity, selected


def _list_value(value):
    if pd.isna(value):
        return None
    return str(value).split(";")


def test_corrected_summaries_match_the_accepted_logical_oracles():
    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(
        bins,
        identity,
        selected,
        run_id=RUN_ID,
    )
    actual_radio = summaries.radio_class.set_index("radio_group_id")
    expected_radio = pd.read_csv(
        EXPECTED_ROOT / "radio_class_summary.csv"
    ).set_index("radio_group_id")

    assert set(actual_radio.index) == set(expected_radio.index)
    assert actual_radio.loc["R5", "member_bssids"] == ["A5", "B5"]
    assert actual_radio.loc["R5", "preferred_bssid"] == "A5"
    assert actual_radio.loc["R5", "class_start"] == pd.Timestamp(
        "2026-01-15T10:00:00Z"
    )
    assert actual_radio.loc["R5", "class_end"] == pd.Timestamp(
        "2026-01-15T10:00:08.500Z"
    )
    assert sum(actual_radio.loc["R5", "valid_cu_raw_counts"]) == 7
    assert actual_radio.loc["R5", "valid_cu_raw_counts"][0] == 1
    assert actual_radio.loc["R5", "valid_cu_raw_counts"][153] == 2
    assert actual_radio.loc["R5", "valid_cu_raw_counts"][204] == 4
    for radio_id in expected_radio.index:
        actual = actual_radio.loc[radio_id]
        expected = expected_radio.loc[radio_id]
        assert actual["band"] == str(expected["band"]).removesuffix(".0")
        assert actual["channel"] == expected["channel"]
        assert actual["preferred_bssid"] == expected["preferred_bssid"]
        assert actual["member_bssids"] == str(expected["member_bssids"]).split(";")
        for column in [
            "class_duration_us",
            "valid_cu_duration_us",
            "missing_cu_duration_us",
            "no_beacon_duration_us",
            "cu_coverage",
            "average_cu_raw",
            "average_cu_ratio",
            "high_duration_us",
            "high_possible_connected_streak_us",
            "high_actual_observed_streak_us",
            "very_high_duration_us",
            "very_high_possible_connected_streak_us",
            "very_high_actual_observed_streak_us",
        ]:
            assert actual[column] == pytest.approx(expected[column])

    actual_classroom = summaries.classroom_class.iloc[0]
    assert actual_classroom["class_start"] == pd.Timestamp(
        "2026-01-15T10:00:00Z"
    )
    assert actual_classroom["class_end"] == pd.Timestamp(
        "2026-01-15T10:00:08.500Z"
    )
    expected_classroom = pd.read_csv(
        EXPECTED_ROOT / "classroom_class_summary.csv"
    ).iloc[0]
    for column in [
        "class_duration_us",
        "radio_count_2_4_ghz",
        "radio_count_5_ghz",
        "radio_count_6_ghz",
        "radio_count_unknown",
        "radio_count_total",
        "high_possible_connected_max_us",
        "high_actual_observed_max_us",
        "very_high_possible_connected_max_us",
        "very_high_actual_observed_max_us",
    ]:
        assert actual_classroom[column] == expected_classroom[column]
    for column in [
        "high_possible_connected_responsible_radio_ids",
        "high_actual_observed_responsible_radio_ids",
        "very_high_possible_connected_responsible_radio_ids",
        "very_high_actual_observed_responsible_radio_ids",
    ]:
        assert actual_classroom[column] == _list_value(expected_classroom[column])

    reports = build_corrected_cu_report_tables(
        summaries.radio_class,
        summaries.classroom_class,
        identity,
    )
    assert reports.valid_cu_raw_counts.sum() == 11
    assert reports.valid_cu_raw_counts[0] == 3
    assert reports.valid_cu_raw_counts[153] == 2
    assert reports.valid_cu_raw_counts[204] == 6
    assert {
        band: int(counts.sum())
        for band, counts in reports.band_valid_cu_raw_counts.items()
    } == {"2.4": 1, "5": 7, "6": 3, "unknown": 0}
    population = reports.band_population.set_index("population_id")
    assert population.loc["all_5_ghz", "radio_count"] == 1
    assert population.loc["5_ghz_on_6_ghz_ap", "radio_count"] == 1
    assert population.loc["6_ghz_on_6_ghz_ap", "radio_count"] == 1
    assert population.loc[
        "all_5_ghz", "valid_cu_radio_time_coverage_percent"
    ] == pytest.approx(75.9058823529)
    assert population.loc["6_ghz_on_6_ghz_ap", "average_cu_percent"] == (
        pytest.approx(53.3333333333)
    )
    report_by_radio = reports.radio_class.set_index("radio_group_id")
    assert report_by_radio.loc["R5", "ap_model"] == "TEST-MODEL"
    assert report_by_radio.loc["R5", "ap_band_radio_count"] == 1
    assert report_by_radio.loc["R5", "capture_cohort"] == (
        "TEST-MODEL / 1x 5 GHz radio"
    )


def test_frozen_legacy_comparison_uses_the_corrected_fixture_and_current_result():
    comparison = json.loads(COMPARISON_GOLDEN.read_text(encoding="utf-8"))
    selected_path = EXPECTED_ROOT / "selected_observations.csv"
    assert hashlib.sha256(selected_path.read_bytes()).hexdigest() == (
        comparison["shared_input"]["sha256"]
    )

    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(
        bins,
        identity,
        selected,
        run_id=RUN_ID,
    )
    classroom = summaries.classroom_class.iloc[0]
    corrected = comparison["corrected_snapshot"]
    assert classroom["radio_count_total"] == corrected["radio_count_total"]
    for output_column, snapshot_name in (
        (
            "high_possible_connected_max_us",
            "high_possible_connected_max_seconds",
        ),
        ("high_actual_observed_max_us", "high_actual_observed_max_seconds"),
        (
            "very_high_possible_connected_max_us",
            "very_high_possible_connected_max_seconds",
        ),
        (
            "very_high_actual_observed_max_us",
            "very_high_actual_observed_max_seconds",
        ),
    ):
        assert classroom[output_column] / 1_000_000 == pytest.approx(
            corrected[snapshot_name]
        )
    assert corrected["terminal_partial_interval_included"] is True
    assert corrected["classroom_average_cu"] is None
    assert comparison["legacy_snapshot"]["status"] == (
        "frozen_reference_not_executed"
    )


def test_no_observations_are_no_beacon_time_and_null_cu_results():
    bins, identity, selected = _fixture_inputs()
    one_radio = identity.loc[identity["radio_group_id"].eq("R5")].copy()
    summaries = build_corrected_summaries(
        bins,
        one_radio,
        selected.iloc[0:0],
        run_id="empty-class",
    )

    radio = summaries.radio_class.iloc[0]
    assert radio["valid_cu_duration_us"] == 0
    assert radio["missing_cu_duration_us"] == 0
    assert radio["no_beacon_duration_us"] == radio["class_duration_us"]
    assert radio["cu_coverage"] == 0.0
    assert pd.isna(radio["average_cu_raw"])
    assert pd.isna(radio["high_duration_us"])
    classroom = summaries.classroom_class.iloc[0]
    assert pd.isna(classroom["high_actual_observed_max_us"])
    assert classroom["high_actual_observed_responsible_radio_ids"] is None

    reports = build_corrected_cu_report_tables(
        summaries.radio_class,
        summaries.classroom_class,
        one_radio,
        display_timezone="America/Denver",
    )
    report_radio = reports.radio_class.iloc[0]
    assert report_radio["class_start_utc"] == "2026-01-15T10:00:00Z"
    assert report_radio["class_start_local"].endswith("-07:00")
    assert report_radio["cu_coverage_percent"] == 0.0
    assert pd.isna(report_radio["average_cu_percent"])
    report_classroom = reports.classroom_class.iloc[0]
    assert report_classroom["radios_with_valid_cu_count"] == 0
    assert report_classroom["radios_without_valid_cu_count"] == 1
    assert report_classroom["radio_cu_coverage_median_percent"] == 0.0
    assert reports.valid_cu_raw_counts.sum() == 0


def test_report_derives_same_band_radio_count_from_identity_not_model():
    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(
        bins,
        identity,
        selected,
        run_id=RUN_ID,
    )
    second_radio = summaries.radio_class.loc[
        summaries.radio_class["radio_group_id"].eq("R5")
    ].copy()
    second_radio.loc[:, "radio_group_id"] = "R5B"
    radio_summary = pd.concat(
        [summaries.radio_class, second_radio], ignore_index=True
    )

    second_identity = identity.loc[identity["bssid"].eq("A5")].copy()
    second_identity.loc[:, "bssid"] = "C5"
    second_identity.loc[:, "radio_group_id"] = "R5B"
    second_identity.loc[:, "observed_channels"] = 149
    identity_map = pd.concat([identity, second_identity], ignore_index=True)

    classroom_summary = summaries.classroom_class.copy()
    classroom_summary.loc[:, "radio_count_5_ghz"] += 1
    classroom_summary.loc[:, "radio_count_total"] += 1
    reports = build_corrected_cu_report_tables(
        radio_summary,
        classroom_summary,
        identity_map,
    )

    five_ghz = reports.radio_class.loc[reports.radio_class["band"].eq("5")]
    assert set(five_ghz["radio_group_id"]) == {"R5", "R5B"}
    assert five_ghz["ap_band_radio_count"].eq(2).all()
    assert five_ghz["capture_cohort"].eq(
        "TEST-MODEL / 2x 5 GHz radios"
    ).all()


def test_report_histogram_edges_follow_accepted_units():
    average_edges = _average_cu_edges_percent() * (255.0 / 100.0)
    assert len(average_edges) == 65
    assert average_edges[0] == pytest.approx(0.0)
    assert average_edges[-1] == pytest.approx(256.0)
    np.testing.assert_allclose(np.diff(average_edges), 4.0)

    time_edges = _time_histogram_edges(41.984, interval_us=1_024_000)
    assert time_edges[0] == pytest.approx(0.0)
    assert time_edges[-1] == pytest.approx(43.008)
    np.testing.assert_allclose(np.diff(time_edges), 1.024)


def test_cdf_threshold_values_use_values_strictly_below_each_boundary():
    counts = np.zeros(256, dtype=np.int64)
    counts[[0, 127, 128, 191, 192, 255]] = 1

    assert _cdf_below_raw_threshold(counts, 128) == pytest.approx(2 / 6)
    assert _cdf_below_raw_threshold(counts, 192) == pytest.approx(4 / 6)
    assert _cdf_below_raw_threshold(np.zeros(256, dtype=np.int64), 128) is None


def test_threshold_edges_are_evaluated_independently():
    bins, identity, selected = _fixture_inputs()
    bins = bins.iloc[:4].copy()
    one_bssid = identity.loc[identity["bssid"].eq("A5")].copy()
    observations = selected.loc[selected["bssid"].eq("A5")].iloc[:4].copy()
    observations["bin_number"] = [0, 1, 2, 3]
    observations["qbss_cu_raw"] = [127, 128, 191, 192]
    summaries = build_corrected_summaries(
        bins,
        one_bssid,
        observations,
        run_id="threshold-edges",
    )

    radio = summaries.radio_class.iloc[0]
    assert radio["high_duration_us"] == 3 * 1_024_000
    assert radio["high_actual_observed_streak_us"] == 3 * 1_024_000
    assert radio["very_high_duration_us"] == 1_024_000
    assert radio["very_high_actual_observed_streak_us"] == 1_024_000


def test_preferred_bssid_uses_declared_capture_source_rank():
    bins, identity, selected = _fixture_inputs()
    one_bin = bins.iloc[[0]].copy()
    one_radio = identity.loc[identity["radio_group_id"].eq("R5")].copy()
    observations = selected.loc[
        selected["bssid"].isin(["A5", "B5"])
        & selected["bin_number"].eq(0)
    ].copy()
    observations["source_rank"] = observations["bssid"].map({"A5": 2, "B5": 1})
    summaries = build_corrected_summaries(
        one_bin,
        one_radio,
        observations,
        run_id="source-rank",
    )

    assert summaries.radio_class.iloc[0]["preferred_bssid"] == "B5"


def test_single_ordering_restores_precedence_for_shuffled_nontied_rows():
    bins, identity, selected = _fixture_inputs()
    expected = build_corrected_summaries(
        bins,
        identity,
        selected,
        run_id=RUN_ID,
    )
    shuffled = selected.sample(frac=1, random_state=42).reset_index(drop=True)

    actual = build_corrected_summaries(
        bins,
        identity,
        shuffled,
        run_id=RUN_ID,
    )

    pd.testing.assert_frame_equal(actual.radio_class, expected.radio_class)
    pd.testing.assert_frame_equal(actual.classroom_class, expected.classroom_class)


def test_summary_writer_uses_exact_schemas_and_never_overwrites(tmp_path: Path):
    bins, identity, selected = _fixture_inputs()
    written = write_corrected_summaries(
        bins,
        identity,
        selected,
        output_root=tmp_path,
        run_id=RUN_ID,
    )

    assert pq.read_schema(written.radio_class_path).equals(
        RADIO_CLASS_SUMMARY_ARROW_SCHEMA
    )
    assert pq.read_schema(written.classroom_class_path).equals(
        CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA
    )
    assert written.radio_class_rows == 3
    assert written.classroom_class_rows == 1
    with pytest.raises(FileExistsError, match="already exists"):
        write_corrected_summaries(
            bins,
            identity,
            selected,
            output_root=tmp_path,
            run_id=RUN_ID,
        )


def test_selected_identity_or_bin_conflicts_are_rejected():
    bins, identity, selected = _fixture_inputs()
    conflict = selected.iloc[[0]].copy()
    conflict.loc[:, "radio_group_id"] = "wrong-radio"
    with pytest.raises(ValueError, match="conflicting radio identity"):
        build_corrected_summaries(
            bins,
            identity,
            conflict,
            run_id=RUN_ID,
        )

    unknown_bin = selected.iloc[[0]].copy()
    unknown_bin.loc[:, "bin_number"] = 999
    with pytest.raises(ValueError, match="unknown class bin"):
        build_corrected_summaries(
            bins,
            identity,
            unknown_bin,
            run_id=RUN_ID,
        )
