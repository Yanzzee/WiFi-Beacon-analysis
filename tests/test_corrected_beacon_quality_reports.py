from __future__ import annotations

from types import SimpleNamespace
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import corrected_beacon_quality_reports as quality
from corrected_beacon_selection import select_bssid_session_observations
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID


START = pd.Timestamp("2026-10-12T10:00:00Z")


def _session(seconds=30):
    return SimpleNamespace(class_start=START, class_end=START + pd.Timedelta(seconds=seconds),
                           class_session_id="C1", classroom="ROOM1", bin_width_us=1_024_000)


def _identity():
    return pd.DataFrame([
        {"bssid": "aa:bb:cc:dd:ee:01", "radio_group_id": "R5", "ap_id": "AP1", "ap_model": "AP-555", "band": "5"},
        {"bssid": "aa:bb:cc:dd:ee:02", "radio_group_id": "R5", "ap_id": "AP1", "ap_model": "AP-555", "band": "5"},
        {"bssid": "aa:bb:cc:dd:ee:03", "radio_group_id": "R5b", "ap_id": "AP1", "ap_model": "AP-555", "band": "5"},
        {"bssid": "aa:bb:cc:dd:ee:04", "radio_group_id": "R6", "ap_id": "AP1", "ap_model": "AP-555", "band": "6"},
    ])


def _rows(offsets, *, family="erm", bssid="aa:bb:cc:dd:ee:01", tsf=None):
    n = len(offsets)
    times = [START + pd.Timedelta(seconds=offset) for offset in offsets]
    return pd.DataFrame({
        "bssid": [bssid] * n, "radio_group_id": ["R5"] * n, "ssid": ["eduroam"] * n,
        "capture_profile": ["aruba_erm_type0" if family == "erm" else "native_80211"] * n,
        "aruba_erm_time": times if family == "erm" else [pd.NaT] * n,
        "frame_time_epoch": times, "tsf_us": tsf if tsf is not None else [56] * n,
        "beacon_interval_tu": [100] * n, "source_capture": ["captures/0000-test.pcapng"] * n,
        "source_rank": [0] * n, "source_frame": list(range(1, n + 1)), "qbss_cu_raw": [100] * n,
    })


def _context(seconds=30):
    return pd.DataFrame({
        "radio_group_id": ["R5"], "bin_start": [START],
        "bin_end": [START + pd.Timedelta(seconds=seconds)], "cu_state": ["valid"],
        "qbss_cu_raw": [0], "station_available": [True], "station_count": [0],
        "station_stale_assisted": [False],
    })


def test_erm_cap_ignores_tsf_and_never_removes_canonical_rows():
    rows = _rows([0.01, 0.11, 0.21, 0.31, 0.41, 1.1])
    original = rows.copy(deep=True)
    part = quality.build_beacon_quality_part(rows, _identity(), _context(3.072), _session(3.072), run_id="test")

    pd.testing.assert_frame_equal(rows, original)
    record = part.streams.iloc[0]
    assert record.raw_views == 6
    assert record.occupied_slots == 2
    assert record.expected_slots == 3
    assert record.effective_slot_extra_views == 4
    assert record.empty_slot_fraction == pytest.approx(1 / 3)
    assert record.capture_slot_width_us == 1_024_000
    assert record.tsf_status == "not_used_for_erm"
    assert record.duplicate_identification_status == "effective_slot_cap_not_physical_deduplication"
    assert record.observation_role == "unknown"
    assert not record.listening_exposure_known
    assert part.windows.cu_mean_percent.eq(0).all()
    assert part.windows.station_mean.eq(0).all()
    assert part.windows.observed_ap_5ghz_radio_groups.eq(2).all()
    assert part.windows.observed_ap_6ghz_radio_groups.eq(1).all()


def test_native_usable_tsf_collapses_views_not_different_beacons():
    rows = _rows([0.001, 0.1034, 0.1044, 0.2058], family="native", tsf=[0, 102400, 102400, 204800])
    rows.loc[2, "source_capture"] = "captures/0001-other.pcapng"
    session = _session(0.3072)
    selection = select_bssid_session_observations(rows, class_session_id="C1", class_start=session.class_start,
                                                class_end=session.class_end, bin_width_us=1_024_000)
    selected_before = selection.selected.copy(deep=True)
    part = quality.build_beacon_quality_part(rows, _identity(), _context(0.3072), session,
                                            run_id="test", selection=selection)

    pd.testing.assert_frame_equal(selection.selected, selected_before)
    record = part.streams.iloc[0]
    assert record.raw_views == 4
    assert record.known_tsf_duplicate_views == 1
    assert record.occupied_slots == record.expected_slots == 3
    assert record.empty_slot_fraction == 0
    assert record.capture_slot_width_us == 102400
    assert record.duplicate_identification_status == "validated_native_tsf_and_matching_fields"
    assert part.gaps.occupied_slot_gap.tolist() == [1]
    assert part.gaps["count"].tolist() == [2]


def test_mixed_family_native_timing_is_assessed_independently():
    native = _rows([0.001, 0.1034, 0.2058], family="native", tsf=[0, 102400, 204800])
    erm = _rows([0.0015, 0.104], tsf=[56, 56])
    rows = pd.concat([native, erm], ignore_index=True)
    part = quality.build_beacon_quality_part(rows, _identity(), _context(0.3072), _session(0.3072), run_id="test")

    streams = part.streams.set_index("capture_family")
    assert streams.loc["native", "tsf_status"] == "usable_advancing"
    assert streams.loc["native", "occupied_slots"] == 3
    assert streams.loc["erm", "occupied_slots"] == 1
    assert set(part.windows.capture_family) == {"erm", "native"}


def test_frozen_native_tsf_is_uncertain_and_equal_cu_does_not_deduplicate():
    rows = _rows([0.001, 0.1034, 0.2058], family="native", tsf=[56, 56, 56])
    part = quality.build_beacon_quality_part(rows, _identity(), _context(0.3072), _session(0.3072), run_id="test")

    record = part.streams.iloc[0]
    assert record.tsf_status == "frozen"
    assert record.duplicate_identification_status == "uncertain_frozen"
    assert record.known_tsf_duplicate_views == 0
    assert record.occupied_slots == 3


@pytest.mark.parametrize("mixed", [False, True])
def test_native_forward_jump_keeps_capture_slots_and_saved_anomaly_lineage(mixed):
    offsets = .001 + np.arange(100) * .1024
    tsf = np.arange(100) * 102400 + np.where(np.arange(100) >= 50, 3000000, 0)
    native = _rows(offsets, family="native", tsf=tsf)
    rows = pd.concat([native, _rows([.01, 1.1])], ignore_index=True) if mixed else native
    session = _session(10.24)
    selection = select_bssid_session_observations(rows, class_session_id="C1", class_start=START,
        class_end=session.class_end, bin_width_us=1024000)
    assert selection.excluded.empty
    part = quality.build_beacon_quality_part(rows, _identity(), _context(10.24), session,
                                             run_id="jump", selection=selection)
    stream = part.streams.set_index("capture_family").loc["native"]
    assert stream.occupied_slots == stream.expected_slots == 100
    assert stream.native_timing_excluded_rows == 0
    assert stream.empty_slot_fraction == 0
    assert stream.tsf_status == "discontinuous"
    assert stream.tsf_clock_jump_count == 1
    assert stream.tsf_max_step_error_us == 3000000
    assert stream.tsf_anomaly_source_frame == "51"


def test_same_tsf_with_conflicting_beacon_fields_is_not_collapsed():
    rows = _rows([.001, .1034, .1044, .2058], family="native", tsf=[0, 102400, 102400, 204800])
    rows.loc[2, "source_capture"] = "other.pcapng"
    rows.loc[2, "qbss_cu_raw"] = 101
    result = select_bssid_session_observations(rows, class_session_id="C1", class_start=START,
        class_end=START + pd.Timedelta(seconds=.3072), bin_width_us=1024000)
    assert result.timing.tsf_status.tolist() == ["usable_advancing"]
    assert result.excluded.empty
    assert len(result.observations) == 4


def test_any_beacon_union_does_not_sum_ssids_and_interior_gaps_are_separate():
    rows = pd.concat([_rows([0.01, 2.06]), _rows([1.03], bssid="aa:bb:cc:dd:ee:02")], ignore_index=True)
    part = quality.build_beacon_quality_part(rows, _identity(), _context(3.072), _session(3.072), run_id="test")

    window = part.windows.iloc[0]
    assert window.member_bssid_count == 2
    assert window.possible_member_slots == 6
    assert window.occupied_member_slots == 3
    assert window.any_beacon_occupied_slots == 3
    assert window.any_beacon_empty_slot_fraction == 0
    assert window.member_empty_slot_fraction == 0.5
    assert part.streams.interior_empty_slots.tolist() == [1, 0]


def test_default_expectation_does_not_follow_advertised_mismatch():
    rows = _rows([0.01, 1.03])
    rows["beacon_interval_tu"] = [300, None]
    part = quality.build_beacon_quality_part(rows, _identity(), _context(3.072), _session(3.072), run_id="test")

    record = part.streams.iloc[0]
    assert record.expected_beacon_interval_tu == 100
    assert record.capture_slot_width_us == 1_024_000
    assert record.interval_mismatch_rows == record.interval_missing_rows == 1
    override = quality.build_beacon_quality_part(rows, _identity(), _context(3.072), _session(3.072),
                                                run_id="test", expected_interval_tu=300)
    assert override.streams.capture_slot_width_us.iloc[0] == 3_072_000
    assert override.streams.interval_mismatch_rows.iloc[0] == 0


def test_window_context_splits_bins_exactly_and_lag_does_not_fill_missing_cu():
    context = pd.concat([_context(12), _context(22)], ignore_index=True)
    context.loc[1, "bin_start"] = START + pd.Timedelta(seconds=12)
    context.loc[1, "qbss_cu_raw"] = 255
    context.loc[1, "station_count"] = 20
    context.loc[1, "station_stale_assisted"] = True
    rows = _rows([0.01, 10.35, 20.7])
    part = quality.build_beacon_quality_part(rows, _identity(), context, _session(22),
                                            run_id="test", expected_interval_tu=101)

    windows = part.windows
    assert windows.window_duration_us.tolist() == [10_342_400, 10_342_400, 1_315_200]
    assert windows.cu_valid_duration_us.sum() == 22_000_000
    assert windows.station_stale_duration_us.sum() == 10_000_000
    assert windows.previous_window_cu_mean_percent.iloc[1] == 0
    assert windows.previous_window_cu_mean_percent.iloc[2] == pytest.approx(8_684_800 / 10_342_400 * 100)
    context.loc[0, "cu_state"] = "missing_cu"
    missing = quality.build_beacon_quality_part(rows, _identity(), context, _session(22),
                                               run_id="test", expected_interval_tu=101)
    assert pd.isna(missing.windows.previous_window_cu_mean_percent.iloc[1])
    assert missing.windows.cu_valid_duration_us.iloc[0] == 0


def test_unknown_profile_is_not_assigned_an_expected_rate():
    rows = _rows([0.01]).drop(columns="capture_profile")
    part = quality.build_beacon_quality_part(rows, _identity(), _context(), _session(), run_id="test")

    assert part.streams.capture_family.iloc[0] == "unknown"
    assert part.streams.capture_slot_width_us.isna().all()
    assert part.windows.empty


@pytest.mark.parametrize("mixed", [False, True])
def test_window_bounds_use_current_duration_and_lag_the_whole_previous_interval(mixed):
    first, gap, final = _context(5.12), _context(10.24), _context(12)
    first["qbss_cu_raw"] = 102  # 40% CU in half the first window.
    gap["bin_start"], gap["cu_state"], gap["qbss_cu_raw"] = START + pd.Timedelta(seconds=5.12), "no_beacon", np.nan
    final["bin_start"] = START + pd.Timedelta(seconds=10.24)
    rows = _rows([.01, 10.3])
    if mixed:
        rows = pd.concat([rows, _rows([.01, 10.3], family="native")], ignore_index=True)
    part = quality.build_beacon_quality_part(rows, _identity(), pd.concat([first, gap, final], ignore_index=True),
        _session(12), run_id="bounds")
    for _, windows in part.windows.groupby("capture_family", sort=False):
        windows = windows.sort_values("window_number")
        assert windows.cu_mean_percent.tolist() == [40., 0.]
        assert windows.cu_mean_percent_lower_bound.tolist() == [20., 0.]
        assert windows.cu_mean_percent_upper_bound.tolist() == [70., 0.]
        assert windows.cu_raw_duration_sum.tolist() == [102 * 5120000, 0]
        assert pd.isna(windows.previous_window_cu_mean_percent_lower_bound.iloc[0])
        assert pd.isna(windows.previous_window_cu_mean_percent_upper_bound.iloc[0])
        assert windows.previous_window_cu_mean_percent_lower_bound.iloc[1] == 20
        assert windows.previous_window_cu_mean_percent_upper_bound.iloc[1] == 70
        assert windows.previous_window_duration_us.iloc[1] == 10240000
        assert windows.window_duration_us.iloc[1] == 1760000
        assert windows.cu_bounds_metric_id.eq(CU_BOUNDS_METRIC_ID).all()


def test_missing_window_cu_is_bounded_but_absent_context_is_not_fabricated():
    context = _context()
    context["cu_state"], context["qbss_cu_raw"] = "missing_cu", np.nan
    missing = quality.build_beacon_quality_part(_rows([.01]), _identity(), context, _session(), run_id="missing").windows
    assert missing.cu_mean_percent.isna().all()
    assert missing.cu_mean_percent_lower_bound.eq(0).all()
    assert missing.cu_mean_percent_upper_bound.eq(100).all()
    assert missing.previous_window_cu_mean_percent_upper_bound.iloc[1:].eq(100).all()
    absent = quality.build_beacon_quality_part(_rows([.01]), _identity(), pd.DataFrame(), _session(), run_id="absent").windows
    assert absent.cu_mean_percent.isna().all()
    assert absent.cu_mean_percent_lower_bound.isna().all()
    assert absent.cu_mean_percent_upper_bound.isna().all()


def test_full_window_bounds_preserve_observed_floating_point_means_exactly():
    context = _context()
    context["qbss_cu_raw"] = 127
    windows = quality.build_beacon_quality_part(_rows([.01]), _identity(), context, _session(), run_id="full").windows
    for bound in ("lower", "upper"):
        np.testing.assert_array_equal(windows[f"cu_mean_percent_{bound}_bound"], windows.cu_mean_percent)


def test_package_schema_exports_correlations_and_create_new_cleanup(tmp_path):
    part = quality.build_beacon_quality_part(_rows([0.01, 10.3, 20.5]), _identity(), _context(), _session(), run_id="test")
    windows = part.windows.copy()
    windows["previous_window_cu_mean_percent"] = [10.0, 20.0, 30.0]
    windows["previous_window_station_mean"] = [0.0, 0.0, 0.0]
    windows["member_empty_slot_fraction"] = [0.1, 0.2, 0.3]
    windows["occupied_member_slots"] = windows.possible_member_slots * (1 - windows.member_empty_slot_fraction)
    windows["occupied_member_slots"] = windows.occupied_member_slots.round().astype(int)
    synthetic = quality.BeaconQualityPart(part.streams, windows, part.gaps)
    files = quality.write_beacon_quality_package(tmp_path, [synthetic])
    root = tmp_path / quality.REPORT_DIRECTORY

    assert len(files) == 9
    table = pq.read_table(root / quality.WINDOW_FILENAME)
    assert table.schema.metadata[b"report_schema_id"].decode() == quality.REPORT_SCHEMA_ID
    assert table.num_rows == 3
    correlations = pd.read_csv(root / quality.ASSOCIATION_FILENAME).set_index("predictor")
    assert correlations.loc["previous_window_cu_mean_percent", "pearson_correlation"] == pytest.approx(1)
    assert pd.isna(correlations.loc["previous_window_station_mean", "pearson_correlation"])
    assert len(list(root.glob("*.png"))) == 3
    with pytest.raises(FileExistsError):
        quality.write_beacon_quality_package(tmp_path, [synthetic])
    assert pq.read_table(root / quality.WINDOW_FILENAME).num_rows == 3

    def fail():
        yield synthetic
        raise RuntimeError("failure during streaming")

    with pytest.raises(RuntimeError, match="during streaming"):
        quality.write_beacon_quality_package(tmp_path / "failed", fail())
    assert not (tmp_path / "failed" / quality.REPORT_DIRECTORY).exists()


def test_empty_package_and_bad_expectations(tmp_path):
    empty = quality.build_beacon_quality_part(pd.DataFrame(), _identity(), pd.DataFrame(), _session(), run_id="test")
    assert empty.streams.empty and empty.windows.empty
    quality.write_beacon_quality_package(tmp_path, [empty])
    assert pq.read_table(tmp_path / quality.REPORT_DIRECTORY / quality.WINDOW_FILENAME).num_rows == 0
    for invalid in (0, -1, True, 65536, 100.5):
        with pytest.raises(ValueError, match="expected_interval_tu"):
            quality.build_beacon_quality_part(pd.DataFrame(), _identity(), pd.DataFrame(), _session(),
                                              run_id="test", expected_interval_tu=invalid)


def test_centered_correlation_is_invariant_to_part_boundaries():
    x = np.array([1e8 + 1, 1e8 + 2, 1e8 + 3, 1e8 + 4])
    y = np.array([0.1, 0.2, 0.1, 0.4])
    whole, split = quality._Moments(), quality._Moments()
    whole.add(x, y)
    split.add(x[:2], y[:2])
    split.add(x[2:], y[2:])
    assert whole.correlation() == pytest.approx(np.corrcoef(x, y)[0, 1])
    assert split.correlation() == pytest.approx(whole.correlation())


def _fingerprint_tables(root, *, observed_only=False):
    """Freeze logical CSV/Parquet content, not container or PNG bytes."""
    names = (quality.STREAM_FILENAME, quality.WINDOW_FILENAME, quality.ASSOCIATION_FILENAME,
             quality.STRATA_FILENAME, quality.GAP_FILENAME)
    fingerprints = {}
    for name in names:
        frame = pd.read_parquet(root / name) if name.endswith(".parquet") else pd.read_csv(root / name, keep_default_na=False)
        if observed_only and name == quality.WINDOW_FILENAME:
            additions = ["cu_bounds_metric_id", "cu_raw_duration_sum", "previous_window_cu_raw_duration_sum", "previous_window_duration_us",
                         *[f"{prefix}cu_mean_percent_{bound}_bound" for prefix in ("", "previous_window_") for bound in ("lower", "upper")]]
            frame = frame.drop(columns=additions)
        canonical = frame.to_json(orient="split", index=False, date_format="iso", date_unit="ns", double_precision=12)
        fingerprints[name] = {"rows": len(frame), "sha256": hashlib.sha256(canonical.encode()).hexdigest()}
    return fingerprints


def _golden_quality_part():
    rows = pd.concat([_rows([0.01, 0.11, 1.1, 10.3, 20.5]),
                      _rows([0.001, 0.1034, 0.1044, 0.2058], family="native", tsf=[0, 102400, 102400, 204800])],
                     ignore_index=True)
    context = _context()
    context["station_stale_assisted"] = True
    return quality.build_beacon_quality_part(rows, _identity(), context, _session(), run_id="golden-quality-v1")


def test_exploratory_package_matches_logical_golden(tmp_path):
    quality.write_beacon_quality_package(tmp_path, [_golden_quality_part()])
    golden = json.loads((Path(__file__).parent / "golden" / "beacon_quality_v2.json").read_text(encoding="utf-8"))
    assert _fingerprint_tables(tmp_path / quality.REPORT_DIRECTORY, observed_only=True) == golden["logical_tables"]
    associations = pd.read_csv(tmp_path / quality.REPORT_DIRECTORY / quality.ASSOCIATION_FILENAME)
    station = associations.loc[associations.predictor.eq("previous_window_station_mean")]
    assert station.stale_assisted_station_pair_windows.eq(2).all()


def test_window_mean_bounds_match_their_logical_golden(tmp_path):
    quality.write_beacon_quality_package(tmp_path, [_golden_quality_part()])
    golden = json.loads((Path(__file__).parent / "golden" / "beacon_quality_mean_bounds_v1.json").read_text())
    assert golden["bounds_metric_id"] == CU_BOUNDS_METRIC_ID
    assert _fingerprint_tables(tmp_path / quality.REPORT_DIRECTORY) == golden["logical_tables"]


def test_plots_keep_shared_categories_when_only_one_band_has_pairs(tmp_path, monkeypatch):
    figures = []
    monkeypatch.setattr(quality.Figure, "savefig", lambda figure, *_args, **_kwargs: figures.append(figure))
    strata = pd.DataFrame([{"capture_family": "native", "predictor": "previous_window_cu_mean_percent",
                            "covariate_band": "0-25", "paired_windows": 3,
                            "empty_member_slots": 2, "possible_member_slots": 10}])
    quality._write_plots(tmp_path, strata, pd.DataFrame(columns=quality.GAP_COLUMNS))
    for axis in figures[1].axes:
        assert axis.get_xlim() == pytest.approx((-0.6, 3.6))
        assert [tick.get_text() for tick in axis.get_xticklabels()] == ["0-25", "25-50", "50-75", "75-100"]
    assert "No pairs" in [text.get_text() for text in figures[1].axes[1].texts]


@pytest.mark.parametrize("predictor,title", [
    ("previous_window_cu_mean_percent", "CU capture slot availability"),
    ("previous_window_station_mean", "Station capture slot availability"),
])
def test_slot_availability_pools_slot_denominators_and_counts_windows(predictor, title):
    strata = pd.DataFrame([
        dict(capture_family="erm", predictor=predictor, covariate_band="0-25",
             paired_windows=2, empty_member_slots=1, possible_member_slots=10),
        dict(capture_family="erm", predictor=predictor, covariate_band="0-25",
             paired_windows=3, empty_member_slots=19, possible_member_slots=90),
    ])
    original = strata.copy(deep=True)
    figure = quality.build_capture_slot_availability(strata, predictor)
    assert figure._suptitle.get_text() == title
    assert figure.axes[0].patches[0].get_height() == 20
    assert "n=5" in [text.get_text() for text in figure.axes[0].texts]
    assert not figure.axes[1].patches
    pd.testing.assert_frame_equal(strata, original)
