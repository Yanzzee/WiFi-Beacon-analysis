"""Logical and display checks for CU ranges; no rendered-byte comparisons."""
import numpy as np
import pandas as pd
import pytest
import json
from pathlib import Path
from matplotlib.collections import LineCollection, PathCollection

from corrected_cu_bounds import cu_cdf_bounds, cu_cdf_summary_rows, interval_histogram_bounds
from corrected_cu_reports import (
    build_corrected_cu_report_tables, build_radio_bin_cu_by_band,
    build_radio_bin_cu_cdf, build_radio_class_mean_cu_histogram,
    build_radio_mean_cu_vs_coverage, cu_cdf_report_table,
)
from corrected_summary_output import build_corrected_summaries
from test_corrected_summary_output import _fixture_inputs
from test_corrected_band_comparison_reports import _accumulator, _fingerprints
import corrected_band_comparison_reports as bands
import corrected_cu_reports as cu_reports


def _counts():
    counts = np.zeros(256, dtype=np.int64)
    counts[[0, 127, 255]] = [1, 1, 2]
    return counts


def test_cdf_partial_full_and_all_missing_keep_support_endpoints():
    counts = _counts()
    lower, upper = cu_cdf_bounds(counts, 5)
    assert lower[0] == .2 and upper[0] == .4
    assert lower[127] == .4 and upper[127] == .6
    assert lower[-1] == upper[-1] == 1
    observed = counts.cumsum() / counts.sum()
    lo, hi = cu_cdf_bounds(counts, 4)
    np.testing.assert_array_equal(lo, observed)
    np.testing.assert_array_equal(hi, observed)
    lo, hi = cu_cdf_bounds(np.zeros(256, dtype=np.int64), 5)
    np.testing.assert_array_equal(lo[:-1], 0)
    np.testing.assert_array_equal(hi, 1)
    assert lo[-1] == 1
    assert cu_cdf_bounds(counts, None) == (None, None)
    assert cu_cdf_bounds(np.zeros(256, dtype=np.int64), 0) == (None, None)


@pytest.mark.parametrize("counts,total", [([0] * 255, 1), ([.5] * 256, 256),
    ([True] * 256, 256), ([-1] + [0] * 255, 1), ([1] * 256, 255),
    ([0] * 256, -1), ([0] * 256, True), ([0] * 256, 1.5)])
def test_cdf_invalid_weights_are_rejected(counts, total):
    with pytest.raises(ValueError):
        cu_cdf_bounds(counts, total)


def test_cdf_preserves_large_integer_weights_and_threshold_convention():
    huge = 2 ** 62
    lo, hi = cu_cdf_bounds([huge] + [0] * 254 + [huge], 4 * huge)
    assert lo[0] == .25 and hi[0] == .75
    assert lo[-1] == hi[-1] == 1
    counts = np.zeros(256, dtype=np.int64)
    counts[[0, 127, 128, 191, 192, 255]] = 1
    first, second = cu_cdf_summary_rows(counts, 8, scope="overall", population="all", weighting="radio_bins")
    assert first["cu_raw_exclusive_threshold"] == 128
    assert first["cdf_observed_percent"] == pytest.approx(100 / 3)
    assert first["cdf_lower_bound_percent"] == 25
    assert first["cdf_upper_bound_percent"] == 50
    assert second["cu_raw_exclusive_threshold"] == 192
    assert second["cdf_observed_percent"] == pytest.approx(200 / 3)
    assert second["cdf_lower_bound_percent"] == 50
    assert second["cdf_upper_bound_percent"] == 75


def test_count_and_duration_cdfs_use_their_distinct_expected_weights():
    counts, durations = np.zeros((2, 256), dtype=np.int64)
    counts[[0, 255]] = [1, 1]
    durations[[0, 255]] = [1_000_000, 500_000]
    kwargs = dict(scope="overall", population="all")
    bins = cu_cdf_summary_rows(counts, 3, weighting="radio_bins", **kwargs)[0]
    time = cu_cdf_summary_rows(durations, 2_500_000, weighting="radio_duration_us", **kwargs)[0]
    assert bins["cdf_observed_percent"] == 50
    assert time["cdf_observed_percent"] == pytest.approx(200 / 3)
    assert bins["cdf_lower_bound_percent"] == pytest.approx(100 / 3)
    assert time["cdf_lower_bound_percent"] == 40
    assert bins["cdf_upper_bound_percent"] == pytest.approx(200 / 3)
    assert time["cdf_upper_bound_percent"] == 80


def test_interval_histogram_counts_whole_intervals_and_intersections():
    lo, hi = interval_histogram_bounds([0, 50, 25, 100, np.nan], [0, 50, 75, 100, np.nan], [0, 50, 100])
    np.testing.assert_array_equal(lo, [1, 2])
    np.testing.assert_array_equal(hi, [2, 3])
    with pytest.raises(ValueError):
        interval_histogram_bounds([2], [1], [0, 100])
    with pytest.raises(ValueError):
        interval_histogram_bounds([1], [1], [0, np.inf])


def test_observed_cdf_line_is_unchanged_and_all_missing_has_no_observed_curve():
    figure = build_radio_bin_cu_cdf(_counts(), 5)
    axis = figure.axes[0]
    assert axis.lines[0].get_label() == "Observed"
    np.testing.assert_array_equal(axis.lines[0].get_ydata(), _counts().cumsum() / 4)
    assert len(axis.collections) == 3  # range plus two observed threshold markers
    assert any("Min 40.0% / max 60.0%" in text.get_text() for text in axis.texts)
    assert axis.lines[1].get_ydata()[-1] == axis.lines[2].get_ydata()[-1] == 1
    missing = build_radio_bin_cu_cdf(np.zeros(256, dtype=np.int64), 3).axes[0]
    assert not any(line.get_label() == "Observed" for line in missing.lines)
    assert len(missing.collections) == 1
    assert any("Observed unavailable" in text.get_text() for text in missing.texts)


def test_band_cdf_distinguishes_absent_from_present_all_missing():
    figure = build_radio_bin_cu_by_band({"5": _counts()}, kind="cdf", expected_counts={"2.4": 0, "5": 5, "6": 3})
    assert len(figure.axes) == 2
    assert [axis.get_title().split()[0] for axis in figure.axes] == ["5", "6"]
    assert figure.axes[0].lines[0].get_label() == "Observed"
    assert not any(line.get_label() == "Observed" for line in figure.axes[1].lines)
    assert len(figure.axes[1].collections) == 1


def _reports(expected=None):
    bins, identity, selected = _fixture_inputs()
    summaries = build_corrected_summaries(bins, identity, selected, run_id="cdf-test")
    return build_corrected_cu_report_tables(summaries.radio_class, summaries.classroom_class,
                                           identity, radio_bin_counts=expected)


def test_exact_expected_counts_control_cdf_bounds_without_default_width_inference():
    # Different per-radio counts represent mixed analysis widths. The 8.5 s
    # class duration alone cannot determine them; terminal partial bins count.
    base = _reports()
    counts = base.radio_class[["class_session_id", "radio_group_id"]].copy()
    counts["radio_bin_rows"] = [9, 17, 9]
    tables = _reports(counts)
    assert tables.radio_class.expected_radio_bin_count.tolist() == [9, 17, 9]
    summary = cu_cdf_report_table(tables)
    overall = summary.query("population_scope == 'overall'")
    assert overall.expected_weight.eq(35).all()
    assert overall.weighting.eq("radio_bins").all()
    assert overall.valid_weight.eq(int(tables.valid_cu_raw_counts.sum())).all()
    assert base.radio_class.expected_radio_bin_count.isna().all()
    assert cu_cdf_report_table(base).cdf_lower_bound_percent.isna().all()


@pytest.mark.parametrize("damage", ["duplicate", "missing", "small", "fractional", "bool", "infinite"])
def test_bad_expected_counts_do_not_produce_bounds(damage):
    frame = _reports().radio_class[["class_session_id", "radio_group_id"]].copy()
    frame["radio_bin_rows"] = 9
    if damage == "duplicate":
        frame = pd.concat([frame, frame.iloc[[0]]])
    elif damage == "missing":
        frame = frame.iloc[1:]
    else:
        frame["radio_bin_rows"] = frame.radio_bin_rows.astype(object)
        frame.loc[0, "radio_bin_rows"] = {"small": 0, "fractional": 9.5, "bool": True, "infinite": np.inf}[damage]
    with pytest.raises(ValueError):
        _reports(frame)


def test_mean_histogram_and_coverage_scatter_preserve_observed_data(monkeypatch):
    from matplotlib.axes import Axes
    source = pd.DataFrame({"band": ["5", "5", "6"], "capture_cohort": ["AP"] * 3,
        "average_cu_percent": [20., 50., np.nan], "cu_coverage_percent": [50., 100., 0.],
        "average_cu_percent_lower_bound": [10., 50., 0.], "average_cu_percent_upper_bound": [60., 50., 100.]})
    recorded = []
    original = Axes.hist
    def hist(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        recorded.append(result[0])
        return result
    monkeypatch.setattr(Axes, "hist", hist)
    figure = build_radio_class_mean_cu_histogram(source)
    assert sum(values.sum() for values in recorded) == 2
    assert len(figure.axes[0].collections) == 2
    figure = build_radio_mean_cu_vs_coverage(source)
    points = [collection.get_offsets().tolist() for axis in figure.axes for collection in axis.collections
              if isinstance(collection, PathCollection)]
    assert points == [[[50., 20.], [100., 50.]]]
    assert len(figure.axes[1].collections) == 0  # no observed CU, and no intervals
    assert not any(isinstance(collection, LineCollection) for axis in figure.axes for collection in axis.collections)
    assert not any("range" in text.get_text().lower() for axis in figure.axes for text in axis.texts)


def test_cohort_cdfs_use_exact_duration_totals_and_keep_observed_curves():
    accumulator = _accumulator()
    tables = accumulator.tables()
    figure = bands.build_cohort_cu_cdf(accumulator.histograms, tables)
    summary = tables["population_summary.csv"].set_index(["population_scope", "population_id"])
    labels, _, _ = bands._cohort_plot_context(tables)
    assert len(figure.axes) == 6
    for scope_index, scope in enumerate(("ap_cohort", "classroom_cohort")):
        for population_index, population in enumerate(labels):
            axis = figure.axes[scope_index * 3 + population_index]
            histogram = accumulator.histograms[scope, population][1]
            lines = [line for line in axis.lines if line.get_label() == "Observed"]
            assert len(lines) == int(bool(histogram.sum()))
            if lines:
                np.testing.assert_array_equal(lines[0].get_ydata(), histogram.cumsum() / histogram.sum())
            assert len(axis.collections) == int(summary.loc[(scope, population), "radio_duration_us"] > 0)
            assert axis.get_xlim() == figure.axes[0].get_xlim()
            assert axis.get_ylim() == figure.axes[0].get_ylim()
    cdf = tables["cu_cdf_bounds.csv"]
    assert cdf.weighting.eq("radio_duration_us").all()
    for row in cdf.itertuples(index=False):
        assert row.expected_weight == summary.loc[(row.population_scope, row.population_id), "radio_duration_us"]
    broken = {**tables, "population_summary.csv": tables["population_summary.csv"].copy()}
    broken["population_summary.csv"].loc[broken["population_summary.csv"].population_scope.eq("ap_cohort"), "valid_cu_duration_us"] += 1
    with pytest.raises(ValueError, match="does not match"):
        bands.build_cohort_cu_cdf(accumulator.histograms, broken)


def test_cohort_mean_bars_and_matched_points_remain_observed():
    accumulator = _accumulator()
    tables = accumulator.tables()
    before = bands.build_cohort_metrics({**tables, "population_summary.csv": tables["population_summary.csv"].drop(
        columns=["mean_cu_percent_lower_bound", "mean_cu_percent_upper_bound"])})
    after = bands.build_cohort_metrics(tables)
    for first, second in zip(before.axes, after.axes):
        np.testing.assert_array_equal([p.get_height() for p in first.patches], [p.get_height() for p in second.patches])
    assert len(after.axes[0].collections) == 6  # interval + two caps, two cohorts
    pairs = tables["matched_ap_radio_sessions.csv"]
    figure = bands.build_matched_ap_cu(pairs)
    valid = pairs.loc[pairs.common_valid_cu_duration_us.gt(0)]
    np.testing.assert_array_equal(figure.axes[0].collections[0].get_offsets(),
        valid[["matched_mean_5_cu_percent", "matched_mean_6_cu_percent"]].to_numpy())
    assert len(figure.axes[0].collections) == 1  # observed points only
    assert len(figure.axes[1].collections) > 0  # histogram bounds retained
    assert not any("range" in text.get_text().lower() for text in figure.axes[0].texts)
    assert sum(p.get_height() for p in figure.axes[1].patches) == len(valid)


@pytest.mark.parametrize("widths,use_counts,expected_total", [
    ((1_024_000,), False, 27), ((1_024_000, 512_000), False, None),
    ((1_024_000, 512_000), True, 35)])
def test_writer_uses_exact_counts_or_single_explicit_width_including_partial_bins(
        tmp_path, monkeypatch, widths, use_counts, expected_total):
    from corrected_summary_output import write_corrected_summaries
    bins, identity, selected = _fixture_inputs()
    write_corrected_summaries(bins, identity, selected, run_directory=tmp_path, run_id="cdf-writer")
    identity.to_parquet(tmp_path / "project_identity_map.parquet")
    if use_counts:
        path = tmp_path / "reports/band_comparison"
        path.mkdir(parents=True)
        frame = _reports().radio_class[["class_session_id", "radio_group_id"]].copy()
        frame["radio_bin_rows"] = [9, 17, 9]
        frame.to_csv(path / "radio_class_comparison.csv", index=False)
    monkeypatch.setattr(cu_reports, "_save_plots", lambda *args, **kwargs: None)
    result = cu_reports.write_corrected_cu_report_package(tmp_path, bin_widths_us=widths)
    cdf = pd.read_csv(result.directory / "cu_cdf_bounds.csv")
    row = cdf.query("population_scope == 'overall'").iloc[0]
    assert len(cdf) == result.cdf_bounds_rows
    if expected_total is None:
        assert pd.isna(row.expected_weight) and pd.isna(row.cdf_lower_bound_percent)
        assert pd.notna(row.cdf_observed_percent)
    else:
        assert row.expected_weight == expected_total
        assert pd.notna(row.cdf_lower_bound_percent)


def test_cohort_cdf_all_missing_is_a_range_and_absent_group_has_no_curve():
    accumulator = _accumulator()
    tables = accumulator.tables()
    summary = tables["population_summary.csv"]
    scope, population = "ap_cohort", "5_on_with_6"
    mask = summary.population_scope.eq(scope) & summary.population_id.eq(population)
    summary.loc[mask, "valid_cu_duration_us"] = 0
    accumulator.histograms[scope, population][:] = 0
    figure = bands.build_cohort_cu_cdf(accumulator.histograms, tables)
    assert any(text.get_text() == "No observed CU" for text in figure.axes[1].texts)
    assert not any(line.get_label() == "Observed" for line in figure.axes[1].lines)
    assert len(figure.axes[1].collections) == 1
    assert not any(line.get_label() == "Observed" for line in figure.axes[3].lines)
    assert len(figure.axes[3].collections) == 0


def test_cohort_all_missing_mean_has_ranges_without_a_zero_bar():
    tables = _accumulator().tables()
    summary = tables["population_summary.csv"]
    summary["mean_cu_percent"] = np.nan
    present = summary.radio_duration_us.gt(0)
    summary.loc[present, "mean_cu_percent_lower_bound"] = 0.
    summary.loc[present, "mean_cu_percent_upper_bound"] = 100.
    axis = bands.build_cohort_metrics(tables).axes[0]
    assert all(np.isnan(patch.get_height()) for patch in axis.patches)
    assert len(axis.collections) > 0
    assert any(text.get_text() == "No observed CU; ranges only" for text in axis.texts)


def test_cdf_logical_golden_preserves_separate_weighting_and_missing_populations():
    counts = _reports().radio_class[["class_session_id", "radio_group_id"]].copy()
    counts["radio_bin_rows"] = [9, 17, 9]
    tables = {"cu_coverage_streak/cu_cdf_bounds.csv": cu_cdf_report_table(_reports(counts)),
              "band_comparison/cu_cdf_bounds.csv": _accumulator().tables()["cu_cdf_bounds.csv"]}
    golden = json.loads((Path(__file__).parent / "golden/cu_cdf_bounds_v1.json").read_text())
    assert _fingerprints(tables) == golden["logical_tables"]
