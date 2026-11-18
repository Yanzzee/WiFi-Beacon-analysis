"""Refresh comparison figures from saved compact tables and narrow rollups."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from band_comparison_inputs import optional_enabled
from corrected_admission_reports import build_admission_band_histogram_from_detail
from corrected_affected_station_reports import build_station_time_exposure
from corrected_band_comparison_reports import (
    build_cohort_cu_cdf, build_cohort_cu_station_density, build_cohort_metrics, build_matched_ap_cu, build_station_adoption,
)
from corrected_beacon_quality_reports import build_beacon_slot_gap_cdf, build_capture_slot_availability
from corrected_cu_reports import (
    build_radio_bin_cu_by_band, build_radio_bin_cu_cdf,
    build_radio_coverage_histograms, build_radio_mean_cu_vs_coverage,
)
from run_lifecycle import sha256_file


def _saved_cohort_station_grids(detail_path, ap_infrastructure, classroom_infrastructure):
    """Rebuild the existing display buckets from bounded, narrow saved batches."""
    grids = defaultdict(lambda: np.zeros((40, 50), dtype=np.int64))
    ap_enabled = ap_infrastructure.set_index("ap_id").six_ghz_enabled.map(optional_enabled)
    room_enabled = classroom_infrastructure.set_index("classroom").six_ghz_enabled.map(optional_enabled)
    columns = ["ap_id", "classroom", "band", "cu_state", "qbss_cu_raw",
               "station_available", "station_count", "bin_duration_us"]
    for batch in pq.ParquetFile(detail_path).iter_batches(batch_size=65_536, columns=columns):
        frame = batch.to_pandas()
        cu = pd.to_numeric(frame.qbss_cu_raw, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        station = pd.to_numeric(frame.station_count, errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        paired = frame.cu_state.eq("valid").to_numpy() & np.isfinite(cu) & frame.station_available.to_numpy(dtype=bool) & np.isfinite(station)
        duration = frame.bin_duration_us.to_numpy(dtype=np.int64)
        for scope, enabled in (("ap_cohort", frame.ap_id.map(ap_enabled)),
                               ("classroom_cohort", frame.classroom.map(room_enabled))):
            if enabled.isna().any():
                raise ValueError("saved radio detail lacks explicit cohort metadata")
            for band in ("2.4", "5", "6"):
                for flag, suffix in ((False, "without_6"), (True, "with_6")):
                    selected = paired & frame.band.eq(band).to_numpy() & enabled.eq(flag).to_numpy()
                    if selected.any():
                        x = np.minimum((cu[selected] / 256 * 40).astype(int), 39)
                        y = np.minimum((station[selected] / 5).astype(int), 49)
                        np.add.at(grids[scope, f"{band}_on_{suffix}"], (x, y), duration[selected])
    return grids


def refresh_saved_comparison_plots(root: Path) -> list[dict]:
    records = []

    def exists(relative):
        return (root / relative).is_file()

    def read(relative):
        return pd.read_csv(root / relative, float_precision="round_trip", dtype={"population_id": "string", "band": "string"})

    def save(figure, relative, sources, module):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(path, dpi=140)
        records.append({"path": relative, "sources": sources,
                        "renderer_sha256": sha256_file(Path(__file__).with_name(module)),
                        "loader_sha256": sha256_file(Path(__file__))})
        if module in {"corrected_cu_reports.py", "corrected_band_comparison_reports.py", "corrected_affected_station_reports.py"}:
            records[-1]["cu_bounds_dependencies"] = {
                name: sha256_file(Path(__file__).with_name(name))
                for name in ("corrected_cu_bounds.py", "corrected_cu_plot_bounds.py")}

    population_path = "reports/band_comparison/population_summary.csv"
    population = read(population_path) if exists(population_path) else pd.DataFrame()
    expected = {(row.population_scope, str(row.population_id)): int(row.radio_bin_rows)
                for row in population.itertuples(index=False)
                if hasattr(row, "radio_bin_rows") and pd.notna(row.radio_bin_rows)}
    distribution_path = "reports/band_comparison/cu_distribution.csv"
    histograms = None
    if exists(distribution_path):
        distribution = read(distribution_path)
        histograms = defaultdict(lambda: np.zeros((2, 256), dtype=np.int64))
        band_counts = {}
        for row in distribution.itertuples(index=False):
            raw, count, duration = int(row.cu_raw), int(row.valid_radio_bin_count), int(row.valid_radio_duration_us)
            if not 0 <= raw <= 255 or count < 0 or duration < 0:
                raise ValueError("saved CU marginal contains invalid values")
            histograms[row.population_scope, str(row.population_id)][:, raw] += (count, duration)
            if row.population_scope == "band":
                band_counts.setdefault(str(row.population_id), np.zeros(256, dtype=np.int64))[raw] += count
        for kind in ("histogram", "cdf"):
            save(build_radio_bin_cu_by_band(band_counts, kind=kind,
                 expected_counts={band: expected.get(("band", band)) for band in ("2.4", "5", "6")}),
                 f"reports/cu_coverage_streak/plots/radio_bin_cu_{kind}_by_band.png",
                 [distribution_path, *([population_path] if len(population) else [])], "corrected_cu_reports.py")
        overall = ("overall", "all")
        all_missing = (len(population) and "valid_cu_duration_us" in population and
                       ((population.population_scope.eq("overall")) & population.valid_cu_duration_us.eq(0)).any())
        if overall in histograms or (overall in expected and all_missing):
            counts = histograms[overall][0]
            save(build_radio_bin_cu_cdf(counts, expected.get(overall)), "reports/cu_coverage_streak/plots/radio_bin_cu_cdf.png",
                 [distribution_path, *([population_path] if len(population) else [])], "corrected_cu_reports.py")

    radio_path = "reports/cu_coverage_streak/radio_class_report.csv"
    if exists(radio_path):
        radio = read(radio_path)
        if {"cu_coverage_percent", "class_duration_seconds", "missing_cu_seconds", "no_beacon_seconds"}.issubset(radio.columns):
            save(build_radio_coverage_histograms(radio), "reports/cu_coverage_streak/plots/radio_coverage_and_missingness.png",
                 [radio_path], "corrected_cu_reports.py")
        if {"band", "capture_cohort", "cu_coverage_percent", "average_cu_percent"}.issubset(radio.columns):
            save(build_radio_mean_cu_vs_coverage(radio), "reports/cu_coverage_streak/plots/radio_cu_vs_coverage.png",
                 [radio_path], "corrected_cu_reports.py")

    room_path = "reports/band_comparison/classroom_infrastructure.csv"
    ap_path = "reports/band_comparison/ap_infrastructure.csv"
    detail_path = "reports/radio_class_detail/radio_bin_detail.parquet"
    if exists(population_path) and exists(room_path):
        tables = {"population_summary.csv": population, "classroom_infrastructure.csv": read(room_path)}
        save(build_cohort_metrics(tables), "reports/band_comparison/cohort_metrics.png",
             [population_path, room_path], "corrected_band_comparison_reports.py")
        if histograms is not None:
            save(build_cohort_cu_cdf(histograms, tables), "reports/band_comparison/cu_cohort_cdf.png",
                 [distribution_path, room_path, population_path], "corrected_band_comparison_reports.py")
        if exists(ap_path) and exists(detail_path):
            grids = _saved_cohort_station_grids(root / detail_path, read(ap_path), tables["classroom_infrastructure.csv"])
            save(build_cohort_cu_station_density(grids, tables), "reports/band_comparison/cohort_cu_station_density.png",
                 [detail_path, ap_path, room_path, population_path], "corrected_band_comparison_reports.py")
    for filename, builder, output in (("matched_ap_radio_sessions.csv", build_matched_ap_cu, "matched_ap_cu.png"),
                                      ("station_adoption_sessions.csv", build_station_adoption, "station_adoption.png")):
        source = "reports/band_comparison/" + filename
        if exists(source):
            save(builder(read(source)), "reports/band_comparison/" + output, [source], "corrected_band_comparison_reports.py")

    if exists(detail_path) and exists("reports/admission_capacity/admission_capacity_histogram_by_band.png"):
        save(build_admission_band_histogram_from_detail(root / detail_path),
             "reports/admission_capacity/admission_capacity_histogram_by_band.png", [detail_path], "corrected_admission_reports.py")
    exposure_path = "reports/affected_stations/population_exposure.csv"
    if exists(exposure_path):
        exposure = read(exposure_path)
        if {"population_scope", "population_id", "radio_duration_us", "high_exposure_percent", "very_high_exposure_percent"}.issubset(exposure.columns):
            save(build_station_time_exposure(exposure), "reports/affected_stations/station_time_exposure.png",
                 [exposure_path], "corrected_affected_station_reports.py")
    gaps_path = "reports/beacon_quality/beacon_slot_gap_counts.csv"
    if exists(gaps_path):
        save(build_beacon_slot_gap_cdf(read(gaps_path)), "reports/beacon_quality/beacon_slot_gap_cdf.png",
             [gaps_path], "corrected_beacon_quality_reports.py")
    strata_path = "reports/beacon_quality/beacon_quality_covariate_bands.csv"
    if exists(strata_path):
        strata = read(strata_path)
        for predictor, filename in (("previous_window_cu_mean_percent", "cu_capture_slot_availability.png"),
                                    ("previous_window_station_mean", "station_capture_slot_availability.png")):
            save(build_capture_slot_availability(strata, predictor), "reports/beacon_quality/" + filename,
                 [strata_path], "corrected_beacon_quality_reports.py")
    return records
