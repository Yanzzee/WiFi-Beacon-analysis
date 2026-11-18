"""Historical backfill must add bounds without rewriting observations."""
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from corrected_cu_bounds_refresh import backfill_cu_bounds
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID
from test_corrected_band_comparison_reports import _accumulator, _inputs, _stage1_columns


def _write(root, relative, frame):
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False, lineterminator="\n")


def _old_band_run(root, *, detail=True):
    tables = _accumulator().tables()
    legacy = {name: frame.drop(columns=["mean_cu_percent_lower_bound", "mean_cu_percent_upper_bound", "cu_bounds_metric_id"], errors="ignore")
              for name, frame in _stage1_columns(tables).items()}
    for name, frame in legacy.items():
        _write(root, "reports/band_comparison/" + name, frame)
    if detail:
        path = root / "reports/radio_class_detail/radio_bin_detail.parquet"
        path.parent.mkdir(parents=True)
        _inputs()[3].to_parquet(path, index=False)
    return tables


def _text_tables(root):
    return {path.relative_to(root).as_posix(): pd.read_csv(path, dtype=str, keep_default_na=False)
            for path in root.rglob("*.csv")}


def _assert_observations_unchanged(before, root):
    for name, frame in before.items():
        after = pd.read_csv(root / name, dtype=str, keep_default_na=False)
        pd.testing.assert_frame_equal(after[list(frame.columns)], frame)


def test_legacy_band_backfill_matches_fresh_bounds_for_all_grains(tmp_path):
    source, target = tmp_path / "source", tmp_path / "copy"
    expected = _old_band_run(source)
    before = _text_tables(source)
    shutil.copytree(source, target)
    records = backfill_cu_bounds(target, source_root=source, configuration={})
    _assert_observations_unchanged(before, target)
    _assert_observations_unchanged(before, source)
    for filename, fresh in expected.items():
        if filename == "cu_cdf_bounds.csv":
            continue
        saved = pd.read_csv(target / "reports/band_comparison" / filename, float_precision="round_trip")
        additions = [column for column in fresh if column not in before["reports/band_comparison/" + filename]]
        for column in additions:
            if column == "cu_bounds_metric_id":
                assert saved[column].eq(CU_BOUNDS_METRIC_ID).all()
            else:
                np.testing.assert_allclose(pd.to_numeric(saved[column]), pd.to_numeric(fresh[column]), rtol=1e-14, equal_nan=True)
    cdf = pd.read_csv(target / "reports/band_comparison/cu_cdf_bounds.csv", dtype={"population_id": "string"})
    pd.testing.assert_frame_equal(cdf, expected["cu_cdf_bounds.csv"], check_dtype=False, check_exact=False, atol=1e-12)
    assert any(record["path"].endswith("matched_ap_radio_sessions.csv") for record in records)
    assert all(len(item["sha256"]) == 64 for record in records for item in record["sources"])
    assert backfill_cu_bounds(target, source_root=source, configuration={}) == []


def test_missing_detail_preserves_conditional_unknowns_and_uses_persisted_matched_means(tmp_path):
    _old_band_run(tmp_path, detail=False)
    backfill_cu_bounds(tmp_path, source_root=tmp_path, configuration={})
    radio = pd.read_csv(tmp_path / "reports/band_comparison/radio_class_comparison.csv")
    assert radio.zero_station_available_duration_us.isna().all()
    assert radio.mean_cu_with_zero_stations_percent_lower_bound.isna().all()
    assert radio.mean_cu_percent_lower_bound.notna().all()
    pairs = pd.read_csv(tmp_path / "reports/band_comparison/matched_ap_radio_sessions.csv")
    assert pairs.common_5_cu_raw_duration_sum.isna().all()  # no fabricated integer sums
    assert pairs.paired_window_mean_5_cu_percent_lower_bound.notna().all()
    partial = pairs.loc[pairs.common_valid_cu_duration_us.lt(pairs.possible_duration_us)].iloc[0]
    coverage = partial.common_valid_cu_duration_us / partial.possible_duration_us
    assert partial.paired_window_mean_5_cu_percent_lower_bound == pytest.approx(partial.matched_mean_5_cu_percent * coverage)


def test_inconsistent_saved_pair_geometry_fails_instead_of_fabricating_bounds(tmp_path):
    _old_band_run(tmp_path)
    path = tmp_path / "reports/band_comparison/matched_ap_radio_sessions.csv"
    pairs = pd.read_csv(path)
    pairs.loc[0, "common_valid_cu_duration_us"] += 1
    pairs.to_csv(path, index=False)
    with pytest.raises(ValueError, match="disagrees"):
        backfill_cu_bounds(tmp_path, source_root=tmp_path, configuration={})


def test_station_exposure_backfill_is_identical_to_fresh_and_keeps_peaks(tmp_path):
    from test_corrected_affected_station_reports import _accumulator as exposure_accumulator, BOUNDS_COLUMNS
    expected = exposure_accumulator().tables()
    for name, frame in expected.items():
        _write(tmp_path, "reports/affected_stations/" + name, frame.drop(columns=BOUNDS_COLUMNS, errors="ignore"))
    before = _text_tables(tmp_path)
    backfill_cu_bounds(tmp_path, source_root=tmp_path, configuration={})
    _assert_observations_unchanged(before, tmp_path)
    for filename, frame in expected.items():
        actual = pd.read_csv(tmp_path / "reports/affected_stations" / filename, float_precision="round_trip")
        for field in set(BOUNDS_COLUMNS) & set(frame):
            if field == "cu_bounds_metric_id":
                assert actual[field].eq(CU_BOUNDS_METRIC_ID).all()
            else:
                np.testing.assert_allclose(actual[field], frame[field], equal_nan=True, rtol=1e-14)


@pytest.mark.parametrize("widths,expected", [([1_024_000], 27), ([1_024_000, 512_000], None)])
def test_core_and_cdf_backfill_uses_explicit_counts_without_guessing_mixed_widths(tmp_path, monkeypatch, widths, expected):
    from test_corrected_summary_output import _fixture_inputs
    from corrected_summary_output import write_corrected_summaries
    from corrected_csv_exports import write_compact_csv_exports
    import corrected_cu_reports as reports
    bins, identity, selected = _fixture_inputs()
    write_corrected_summaries(bins, identity, selected, run_directory=tmp_path, run_id="core-bounds")
    identity.to_parquet(tmp_path / "project_identity_map.parquet")
    write_compact_csv_exports(tmp_path)
    monkeypatch.setattr(reports, "_save_plots", lambda *args, **kwargs: None)
    reports.write_corrected_cu_report_package(tmp_path, bin_widths_us=tuple(widths))
    expected_review = pd.read_csv(tmp_path / "exports/csv/radio_class_summary.csv", float_precision="round_trip")
    for relative in ("exports/csv/radio_class_summary.csv", "reports/cu_coverage_streak/radio_class_report.csv",
                     "reports/cu_coverage_streak/band_population_report.csv"):
        frame = pd.read_csv(tmp_path / relative, dtype=str, keep_default_na=False)
        columns = [name for name in frame if name.endswith(("_lower_bound", "_upper_bound")) or name in
                   {"cu_bounds_metric_id", "expected_radio_bin_count", "expected_radio_bin_values"}]
        frame.drop(columns=columns).to_csv(tmp_path / relative, index=False)
    (tmp_path / "reports/cu_coverage_streak/cu_cdf_bounds.csv").unlink()
    before = _text_tables(tmp_path)
    backfill_cu_bounds(tmp_path, source_root=tmp_path, configuration={"bin_widths_us": widths})
    _assert_observations_unchanged(before, tmp_path)
    review = pd.read_csv(tmp_path / "exports/csv/radio_class_summary.csv", float_precision="round_trip")
    for column in ("average_cu_raw_lower_bound", "average_cu_ratio_lower_bound", "average_cu_raw_upper_bound", "average_cu_ratio_upper_bound"):
        np.testing.assert_allclose(review[column], expected_review[column], equal_nan=True, rtol=1e-14)
    cdf = pd.read_csv(tmp_path / "reports/cu_coverage_streak/cu_cdf_bounds.csv")
    overall = cdf.loc[cdf.population_scope.eq("overall")].iloc[0]
    assert overall.valid_weight == 11
    if expected is None:
        assert pd.isna(overall.expected_weight) and pd.isna(overall.cdf_lower_bound_percent)
    else:
        assert overall.expected_weight == expected
        assert overall.cdf_lower_bound_percent == pytest.approx(3 / expected * 100)


def test_saved_window_backfill_preserves_observations_and_previous_partial_duration(tmp_path):
    from test_corrected_beacon_quality_reports import _context, _rows, _identity, _session, START
    from corrected_beacon_quality_reports import build_beacon_quality_part
    first, gap, final = _context(5.12), _context(10.24), _context(12)
    first["qbss_cu_raw"] = 102
    gap["bin_start"], gap["cu_state"], gap["qbss_cu_raw"] = START + pd.Timedelta(seconds=5.12), "no_beacon", np.nan
    final["bin_start"] = START + pd.Timedelta(seconds=10.24)
    expected = build_beacon_quality_part(_rows([.01, 10.3]), _identity(), pd.concat([first, gap, final]), _session(12), run_id="bounds").windows
    additions = [name for name in expected if name.endswith(("_lower_bound", "_upper_bound")) or name in
                 {"cu_bounds_metric_id", "cu_raw_duration_sum", "previous_window_cu_raw_duration_sum", "previous_window_duration_us"}]
    old = expected.drop(columns=additions)
    path = tmp_path / "reports/beacon_quality/beacon_quality_windows.parquet"
    path.parent.mkdir(parents=True)
    old.to_parquet(path, index=False)
    records = backfill_cu_bounds(tmp_path, source_root=tmp_path, configuration={})
    actual = pd.read_parquet(path)
    pd.testing.assert_frame_equal(actual[old.columns], old)
    assert actual.cu_mean_percent_lower_bound.tolist() == [20, 0]
    assert actual.cu_mean_percent_upper_bound.tolist() == [70, 0]
    assert pd.isna(actual.previous_window_cu_mean_percent_lower_bound.iloc[0])
    assert actual.previous_window_duration_us.iloc[1] == 10_240_000
    assert actual.previous_window_cu_mean_percent_upper_bound.iloc[1] == 70
    assert actual.cu_raw_duration_sum.isna().all()
    assert "persisted" in records[0]["mean_source"]


def test_refresh_updates_text_metrics_and_records_provenance_without_analysis(tmp_path):
    from test_corrected_report_refresh import _source_run, _fingerprints
    from test_corrected_affected_station_reports import _accumulator as exposure_accumulator, BOUNDS_COLUMNS
    from corrected_report_refresh import refresh_run_report
    source = _source_run(tmp_path)
    _old_band_run(source)
    for name, frame in exposure_accumulator().tables().items():
        _write(source, "reports/affected_stations/" + name, frame.drop(columns=BOUNDS_COLUMNS, errors="ignore"))
    original = ("Processing evidence\nRuntime: 123 seconds\n\nBand comparison and longitudinal metrics\n"
                "old mean line\n\nPotentially affected station associations (not proven impairment or unique clients)\n"
                "old exposure line\n\nSummary outputs\nRows: 123\n")
    (source / "reports/run_summary.txt").write_text(original, encoding="utf-8")
    before = _fingerprints(source)
    bundle = refresh_run_report(source, output_root=tmp_path / "bundles")
    text = (bundle / "reports/run_summary.txt").read_text(encoding="utf-8")
    assert text.startswith("Processing evidence\nRuntime: 123 seconds\n\n")
    assert text.endswith("Summary outputs\nRows: 123\n")
    assert "mean CU observed" in text and "min " in text and "max " in text
    assert "Station-time with high CU: observed" in text
    assert "Unknown station counts are outside these bounds" in text
    metadata = json.loads((bundle / "report_refresh.json").read_text())
    assert metadata["cu_bounds"]["metric_id"] == CU_BOUNDS_METRIC_ID
    assert len(metadata["cu_bounds"]["loader_sha256"]) == 64
    assert len(metadata["cu_bounds"]["updated_tables"]) > 0
    assert _fingerprints(source) == before
