from pathlib import Path
from collections import Counter

from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from corrected_admission_reports import (
    _HexDensity, _draw, AdmissionAccumulator, CORRELATION_FILENAME, METHODOLOGY_FILENAME,
    PLOT_FILENAMES, REPORT_DIRECTORY, SUMMARY_FILENAME,
    build_admission_band_histogram_from_detail,
)
from corrected_admission_selection import ADMISSION_METRIC_ID, RADIO_DETAIL_SCHEMA_ID
from corrected_beacon_selection import build_class_bins
from corrected_radio_bin_reports import (
    RADIO_BIN_FILENAME, REPORT_DIRECTORY as RADIO_DIRECTORY,
    build_radio_bin_detail, write_radio_bin_report_package,
)
from corrected_summary_output import build_corrected_summaries


def _inputs():
    start = pd.Timestamp("2026-10-08T15:00:00Z")
    bins = build_class_bins(start, start + pd.Timedelta(seconds=9), 1_000_000)
    bins["class_session_id"], bins["classroom"] = "C1", "ROOM"
    identity = pd.DataFrame([
        {"bssid": bssid, "radio_group_id": "R1", "classroom": "ROOM", "band": "5", "ap_id": "AP1"}
        for bssid in ("A", "B", "C")
    ])
    records = [
        (0, "A", 100, 10000), (0, "B", 40, 20000),
        (1, "A", 110, 65535), (1, "B", 50, 0),
        (2, "A", None, 15625), (2, "B", None, 31250),
        (3, "A", 120, None), (3, "B", None, None),
        (4, "A", 125, 65535), (4, "B", None, 40000),
        (6, "B", 50, 25000),
        (7, "C", None, 9000), (7, "B", None, 18000),
        (8, "A", None, 0),
    ]
    selected = pd.DataFrame([
        {
            "class_session_id": "C1", "classroom": "ROOM", "bin_number": bin_number,
            "bssid": bssid, "radio_group_id": "R1",
            "ssid": "eduroam" if bssid in {"A", "C"} else "guest",
            "observation_time": start + pd.Timedelta(seconds=bin_number, milliseconds=100 if bssid == "B" else 200),
            "observation_time_source": "frame_time", "source_capture": "capture",
            "source_frame": index, "qbss_cu_raw": cu, "qbss_admission_capacity": capacity,
        }
        for index, (bin_number, bssid, cu, capacity) in enumerate(records, 1)
    ])
    return bins, identity, selected


def _detail():
    bins, identity, selected = _inputs()
    summaries = build_corrected_summaries(bins, identity, selected, run_id="run", primary_ssid="eduroam")
    stations = pd.DataFrame({
        "class_session_id": ["C1"] * 9, "classroom": ["ROOM"] * 9,
        "bin_number": range(9), "rollup_type": ["radio"] * 9,
        "rollup_id": ["R1"] * 9, "station_count": np.arange(1, 10) * 10,
        "available": [True] * 9,
        "stale_assisted": [False, True, True, False, False, True, False, False, False],
    })
    return build_radio_bin_detail(summaries.radio_bin_cu, stations)


def test_capacity_selection_independent_missingness_conflicts_and_lineage():
    bins, identity, selected = _inputs()
    before = build_corrected_summaries(bins, identity, selected.drop(columns="qbss_admission_capacity"), run_id="run", primary_ssid="eduroam")
    after = build_corrected_summaries(bins, identity, selected.sample(frac=1, random_state=42), run_id="run", primary_ssid="eduroam")
    pd.testing.assert_frame_equal(before.radio_class, after.radio_class)
    pd.testing.assert_frame_equal(before.classroom_class, after.classroom_class)
    detail = after.radio_bin_cu.set_index("bin_number")
    assert detail["admission_state"].tolist() == [
        "valid", "valid", "valid", "missing_capacity", "invalid_capacity",
        "no_beacon", "valid", "valid", "valid",
    ]
    assert detail.loc[[0, 1, 2, 6, 7, 8], "qbss_admission_capacity"].tolist() == [10000, 0, 15625, 25000, 18000, 0]
    assert detail.loc[[3, 4, 5], "qbss_admission_capacity"].isna().all()
    assert detail.loc[0, "admission_source_selection_method"] == "cu_source"
    assert detail.loc[1, "source_bssid"] == "A"
    assert detail.loc[1, "admission_source_bssid"] == "B"
    assert not detail.loc[1, "admission_source_matches_cu"]
    assert detail.loc[1, "invalid_admission_candidate_count"] == 1
    assert detail.loc[4, "invalid_admission_candidate_count"] == 2
    assert detail.loc[2, "cu_state"] == "missing_cu"
    assert detail.loc[2, "admission_source_selection_method"] == "primary_ssid"
    assert detail.loc[7, "admission_source_selection_method"] == "fallback_member_bssid"
    assert detail["admission_disagreement"].sum() == 3
    assert detail["admission_metric_id"].eq(ADMISSION_METRIC_ID).all()
    for bin_number, row in detail.loc[detail["admission_state"].eq("valid")].iterrows():
        source = selected.loc[selected["bin_number"].eq(bin_number) & selected["bssid"].eq(row["admission_source_bssid"])].iloc[0]
        assert row["qbss_admission_capacity"] == source["qbss_admission_capacity"]
        assert row["admission_source_observation_time"] == source["observation_time"]


def test_no_cu_anywhere_still_prefers_primary_capacity_and_fallback():
    bins, identity, selected = _inputs()
    selected["qbss_cu_raw"] = None
    detail = build_corrected_summaries(bins, identity, selected, run_id="run", primary_ssid="eduroam").radio_bin_cu
    assert detail.loc[0, "cu_state"] == "missing_cu"
    assert detail.loc[0, "admission_source_bssid"] == "A"
    assert detail.loc[0, "admission_source_selection_method"] == "primary_ssid"
    assert detail.loc[1, "admission_source_bssid"] == "B"
    assert detail.loc[1, "qbss_admission_capacity"] == 0


def test_fixed_hex_grid_matches_matplotlib_and_retains_all_pairs():
    random = np.random.default_rng(12)
    x = np.r_[random.integers(0, 31251, 2000), 0, 31250, 0, 31250]
    y = np.r_[random.integers(0, 101, 2000), 0, 0, 100, 100]
    density = _HexDensity(100)
    density.add(x[:1000], y[:1000])
    density.add(x[1000:], y[1000:])
    artist = Figure().subplots().hexbin(x, y, gridsize=(40, 30), extent=(0, 31250, 0, 100), mincnt=1)
    px, py, weights = density.points()
    np.testing.assert_allclose(np.c_[px, py], artist.get_offsets(), atol=1e-10)
    np.testing.assert_array_equal(weights, artist.get_array())
    assert sum(weights) == len(x)


def test_histogram_centers_avoid_false_gaps_without_rounding_raw_values():
    accumulator = AdmissionAccumulator()
    counts = Counter({30000: 1, 30312: 1, 30625: 1, 30937: 1, 31250: 1})
    accumulator.counts["all"] = counts.copy()
    axis = Figure().subplots()
    _draw(axis, accumulator, "all", "histogram")
    heights = [patch.get_height() for patch in axis.patches]
    assert len(heights) == 101
    assert heights[96:101] == [20] * 5
    assert sum(heights) == 100
    assert accumulator.counts["all"] == counts


def test_cached_capacity_histograms_use_one_scale_and_preserve_valid_zero(tmp_path):
    path = tmp_path / "detail.parquet"
    pd.DataFrame({"band": ["5", "5", "5", "6", "6", "6"],
                  "admission_state": ["valid", "valid", "valid", "valid", "no_beacon", "valid"],
                  "qbss_admission_capacity": [0, 31250, 65535, 15625, None, 15625]}).to_parquet(path)
    figure = build_admission_band_histogram_from_detail(path)
    left, right = figure.axes
    assert left.get_shared_y_axes().joined(left, right)
    assert left.get_ylim() == right.get_ylim()
    left_heights = [patch.get_height() for patch in left.patches]
    right_heights = [patch.get_height() for patch in right.patches]
    assert left_heights[0] == left_heights[100] == 50
    assert right_heights[50] == 100
    assert sum(left_heights) == sum(right_heights) == 100


def test_capacity_package_exact_correlations_coverage_and_bounded_plots(tmp_path: Path):
    detail = _detail()
    # Split the session to exercise accumulation and the narrow second pass.
    result = write_radio_bin_report_package(tmp_path, detail_parts=[detail.iloc[:4], detail.iloc[4:]])
    root = tmp_path / REPORT_DIRECTORY
    summary = pd.read_csv(root / SUMMARY_FILENAME)
    overall = summary.loc[summary["population_type"].eq("overall")].iloc[0]
    assert overall["valid_rows"] == 6
    assert overall["zero_rows"] == 2
    assert overall["valid_coverage"] == pytest.approx(6 / 9)
    assert overall["invalid_capacity_rows"] == overall["missing_capacity_rows"] == overall["no_beacon_rows"] == 1
    assert overall["invalid_candidate_count"] == 3
    assert overall["disagreement_rows"] == 3
    assert overall["different_cu_source_rows"] == 1
    correlations = pd.read_csv(root / CORRELATION_FILENAME)
    for kind, expected_count, stale_count in (("cu", 3, 0), ("station", 6, 2)):
        row = correlations.loc[correlations["population_type"].eq("overall") & correlations["relationship"].eq(f"capacity_vs_{kind}")].iloc[0]
        assert row["valid_pair_rows"] == expected_count
        assert row["stale_assisted_pair_rows"] == stale_count
        mask = detail["admission_state"].eq("valid") & (detail["cu_state"].eq("valid") if kind == "cu" else detail["station_available"])
        x = detail.loc[mask, "qbss_admission_capacity"].astype(float)
        y = detail.loc[mask, "qbss_cu_raw" if kind == "cu" else "station_count"].astype(float)
        assert row["pearson_correlation"] == pytest.approx(x.corr(y))
        assert row["spearman_correlation"] == pytest.approx(x.rank().corr(y.rank()))
    assert correlations.loc[correlations["relationship"].eq("capacity_vs_cu"), "different_beacon_pair_rows"].eq(1).all()
    assert result.admission_summary_rows == 2
    assert result.admission_correlation_rows == 4
    assert {path.name for path in root.iterdir()} == {SUMMARY_FILENAME, CORRELATION_FILENAME, METHODOLOGY_FILENAME, *PLOT_FILENAMES}
    assert set(result.admission_files).issubset(result.files)
    schema = pq.ParquetFile(tmp_path / RADIO_DIRECTORY / RADIO_BIN_FILENAME).schema_arrow
    assert schema.metadata[b"report_schema_id"].decode() == RADIO_DETAIL_SCHEMA_ID
    assert "No admission capacities are summed" in (root / METHODOLOGY_FILENAME).read_text()


def test_constant_zero_warns_without_reclassifying_or_zero_correlation(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(Figure, "savefig", lambda *args, **kwargs: None)
    detail = _detail().iloc[:3].copy()
    detail["qbss_admission_capacity"] = 0
    write_radio_bin_report_package(tmp_path, detail_parts=[detail])
    root = tmp_path / REPORT_DIRECTORY
    summary = pd.read_csv(root / SUMMARY_FILENAME)
    assert summary["warning"].eq("constant_zero_advertisement").all()
    assert summary["zero_rows"].eq(3).all()
    correlations = pd.read_csv(root / CORRELATION_FILENAME)
    assert correlations[["pearson_correlation", "spearman_correlation"]].isna().all().all()


def test_empty_package_and_failure_cleanup(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(Figure, "savefig", lambda *args, **kwargs: None)
    write_radio_bin_report_package(tmp_path / "empty", detail_parts=[])
    summary = pd.read_csv(tmp_path / "empty" / REPORT_DIRECTORY / SUMMARY_FILENAME)
    assert summary.iloc[0]["warning"] == "no_valid_capacity"
    assert summary.iloc[0]["total_radio_bin_rows"] == 0
    def fail(*args, **kwargs):
        raise RuntimeError("plot failed")
    monkeypatch.setattr("corrected_admission_reports._draw", fail)
    with pytest.raises(RuntimeError, match="plot failed"):
        write_radio_bin_report_package(tmp_path / "failure", detail_parts=[_detail()])
    assert not (tmp_path / "failure" / REPORT_DIRECTORY).exists()
    assert not (tmp_path / "failure" / RADIO_DIRECTORY).exists()


def test_existing_capacity_report_is_not_overwritten(tmp_path: Path):
    destination = tmp_path / REPORT_DIRECTORY
    destination.mkdir(parents=True)
    marker = destination / "existing.txt"
    marker.write_text("preserve")
    with pytest.raises(FileExistsError):
        write_radio_bin_report_package(tmp_path, detail_parts=[_detail()])
    assert marker.read_text() == "preserve"
    assert not (tmp_path / RADIO_DIRECTORY).exists()
