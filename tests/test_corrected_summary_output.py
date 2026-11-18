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
    _new_figure,
    _time_histogram_edges,
    build_corrected_cu_report_tables,
    build_radio_class_mean_cu_histogram,
    build_radio_bin_cu_by_band,
    build_radio_coverage_histograms,
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


def test_radio_bin_rows_prefer_configured_ssid_and_keep_explicit_unknowns():
    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(
        bins,
        identity,
        selected,
        run_id=RUN_ID,
        primary_ssid="eduroam",
    )

    assert summaries.radio_bin_cu is not None
    detail = summaries.radio_bin_cu.loc[
        summaries.radio_bin_cu["radio_group_id"].eq("R5")
    ].set_index("bin_number")
    assert len(detail) == len(bins)
    assert detail.loc[0, "source_ssid"] == "eduroam"
    assert detail.loc[0, "source_bssid"] == "A5"
    assert detail.loc[0, "source_selection_method"] == "primary_ssid"
    assert detail.loc[4, "source_ssid"] == "guest"
    assert detail.loc[4, "source_selection_method"] == "fallback_member_bssid"
    assert detail.loc[2, "cu_state"] == "missing_cu"
    assert detail.loc[2, "observed_member_bssid_count"] == 2
    assert detail.loc[3, "cu_state"] == "no_beacon"
    assert pd.isna(detail.loc[3, "qbss_cu_raw"])


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
    assert report_radio["average_cu_percent_lower_bound"] == 0
    assert report_radio["average_cu_percent_upper_bound"] == 100
    population = reports.band_population
    present = population.class_radio_hours.gt(0)
    assert population.loc[present, "average_cu_percent_lower_bound"].eq(0).all()
    assert population.loc[present, "average_cu_percent_upper_bound"].eq(100).all()
    assert population.loc[~present, ["average_cu_percent_lower_bound", "average_cu_percent_upper_bound"]].isna().all().all()


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


@pytest.mark.parametrize("all_rooms_unobserved", [False, True])
def test_report_keeps_zero_radio_classrooms_unavailable(all_rooms_unobserved):
    bins, identity, selected = _fixture_inputs()
    empty_bins = bins.copy()
    empty_bins["class_session_id"], empty_bins["classroom"] = "C0", "NO-RADIOS"
    if all_rooms_unobserved:
        bins, identity, selected = empty_bins, identity.iloc[:0], selected.iloc[:0]
    else:
        baseline = build_corrected_summaries(bins, identity, selected, run_id=RUN_ID)
        expected = build_corrected_cu_report_tables(baseline.radio_class, baseline.classroom_class, identity)
        bins = pd.concat([empty_bins, bins], ignore_index=True)
    summaries = build_corrected_summaries(bins, identity, selected, run_id=RUN_ID)
    reports = build_corrected_cu_report_tables(summaries.radio_class, summaries.classroom_class, identity)
    room = reports.classroom_class.set_index("classroom").loc["NO-RADIOS"]
    assert room.radio_count_total == 0
    assert room.radios_with_valid_cu_count == room.radios_without_valid_cu_count == 0
    for field in ("radio_cu_coverage_min_percent", "radio_cu_coverage_median_percent",
                  "radio_cu_coverage_max_percent", "high_possible_connected_max_seconds",
                  "high_actual_observed_max_seconds", "very_high_possible_connected_max_seconds",
                  "very_high_actual_observed_max_seconds"):
        assert pd.isna(room[field]), field
    if not all_rooms_unobserved:
        pd.testing.assert_frame_equal(
            reports.classroom_class.loc[reports.classroom_class.classroom.ne("NO-RADIOS")].reset_index(drop=True),
            expected.classroom_class)
        pd.testing.assert_frame_equal(reports.radio_class, expected.radio_class)
        pd.testing.assert_frame_equal(reports.band_population, expected.band_population)


@pytest.mark.parametrize("damage", ["missing_rows", "false_zero", "wrong_count"])
def test_zero_radio_support_does_not_hide_genuine_summary_mismatches(damage):
    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(bins, identity, selected, run_id=RUN_ID)
    radio, classroom = summaries.radio_class.copy(), summaries.classroom_class.copy()
    if damage in {"missing_rows", "false_zero"}:
        radio = radio.iloc[:0]
        if damage == "false_zero":
            classroom["radio_count_total"] = 0
        message = "classroom summaries have no matching radio summaries"
    else:
        classroom["radio_count_total"] += 1
        message = "classroom radio counts disagree with radio summaries"
    with pytest.raises(ValueError, match=message):
        build_corrected_cu_report_tables(radio, classroom, identity)


def test_report_histogram_edges_follow_accepted_units():
    average_edges = _average_cu_edges_percent() * (255.0 / 100.0)
    assert len(average_edges) == 129
    assert average_edges[0] == pytest.approx(0.0)
    assert average_edges[-1] == pytest.approx(256.0)
    np.testing.assert_allclose(np.diff(average_edges), 2.0)

    time_edges = _time_histogram_edges(41.984, interval_us=1_024_000)
    assert time_edges[0] == pytest.approx(0.0)
    assert time_edges[-1] == pytest.approx(43.008)
    np.testing.assert_allclose(np.diff(time_edges), 1.024)


def test_report_figures_use_double_width_and_height():
    np.testing.assert_allclose(_new_figure().get_size_inches(), [18.0, 10.0])


def test_radio_class_mean_histogram_keeps_counts_bins_and_clear_units(monkeypatch):
    from matplotlib.axes import Axes

    saved = Axes.hist
    results = []

    def record_histogram(self, *args, **kwargs):
        result = saved(self, *args, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(Axes, "hist", record_histogram)
    figure = build_radio_class_mean_cu_histogram(pd.DataFrame({
        "band": ["5", "5", "6", "6"], "average_cu_percent": [10, 20, 5, np.nan]}))
    assert len(results) == 2
    assert [int(result[0].sum()) for result in results] == [2, 1]
    for _, bins, _ in results:
        np.testing.assert_allclose(bins, _average_cu_edges_percent())
    axis = figure.axes[0]
    assert "Radio/class-session" in axis.get_title() and "histogram" in axis.get_title()
    assert "0.78 percentage points" in axis.get_xlabel()
    assert axis.get_ylabel() == "Radio/class-sessions"


@pytest.mark.parametrize("valid_bands", [[], ["6"], ["5", "6"], ["2.4", "5", "6"]])
@pytest.mark.parametrize("kind", ["histogram", "cdf"])
def test_cu_band_figures_omit_empty_bands_share_scale_and_keep_frequencies(valid_bands, kind):
    counts = {band: np.zeros(256, dtype=np.int64) for band in ("2.4", "5", "6")}
    for index, band in enumerate(valid_bands):
        counts[band][[0, 127, 255]] = [1, 5 + index, 2]
    figure = build_radio_bin_cu_by_band(counts, kind=kind)
    assert len(figure.axes) == max(1, len(valid_bands))
    for axis, band in zip(figure.axes, valid_bands):
        assert axis.get_title().startswith(band + " GHz")
        if kind == "histogram":
            assert sum(patch.get_height() for patch in axis.patches) == int(counts[band].sum())
        else:
            np.testing.assert_allclose(axis.lines[0].get_ydata(), np.cumsum(counts[band]) / counts[band].sum())
    if len(figure.axes) > 1:
        for axis in figure.axes[1:]:
            assert figure.axes[0].get_shared_y_axes().joined(figure.axes[0], axis)
            assert figure.axes[0].get_ylim() == axis.get_ylim()


def test_cu_availability_histograms_have_one_radio_session_count_scale():
    figure = build_radio_coverage_histograms(pd.DataFrame({
        "cu_coverage_percent": [100, 50, 100], "class_duration_seconds": [10, 10, 10],
        "missing_cu_seconds": [0, 0, 0], "no_beacon_seconds": [0, 5, 0]}))
    for axis in figure.axes:
        assert figure.axes[0].get_shared_y_axes().joined(figure.axes[0], axis)
        assert figure.axes[0].get_ylim() == axis.get_ylim()
        assert sum(patch.get_height() for patch in axis.patches) == 3


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
