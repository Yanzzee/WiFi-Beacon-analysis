from pathlib import Path
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

import corrected_band_comparison_reports as bands
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID
from corrected_input_adapter import adapt_ap_mapping

START = pd.Timestamp("2026-10-12T15:00:00Z")


def _inputs():
    configured = pd.DataFrame({"Hostname": ["AP1", "AP2", "AP3", "AP4"],
        "Building-Room": ["ROOM", "ROOM", "OTHER", "EMPTY"], "Model": ["555", "515", "635", "635"],
        "6GHz_Enabled": [None, None, True, True]})
    identity = pd.DataFrame([
        {"bssid": "A", "radio_group_id": "R5", "ap_id": "AP1", "ap_model": "555", "classroom": "ROOM", "band": "5"},
        {"bssid": "B", "radio_group_id": "R5b", "ap_id": "AP1", "ap_model": "555", "classroom": "ROOM", "band": "5"},
        {"bssid": "C", "radio_group_id": "R6", "ap_id": "AP1", "ap_model": "555", "classroom": "ROOM", "band": "6"},
        {"bssid": "D", "radio_group_id": "Rold", "ap_id": "AP2", "ap_model": "515", "classroom": "ROOM", "band": "5"},
        {"bssid": "E", "radio_group_id": "R24", "ap_id": "AP2", "ap_model": "515", "classroom": "ROOM", "band": "2.4"},
        {"bssid": "F", "radio_group_id": "Runseen", "ap_id": "AP3", "ap_model": "635", "classroom": "OTHER", "band": "5"},
    ])
    sessions = pd.DataFrame({"class_session_id": ["C1", "C2", "C3"], "classroom": ["ROOM", "OTHER", "EMPTY"],
        "class_start": [START] * 3, "class_end": [START + pd.Timedelta(seconds=2.5)] * 3,
        "bin_width_us": [1_000_000] * 3, "course_labels": ["COURSE"] * 3, "enrolled": [20, 10, 0]})
    rows = []
    values = {"R5": ([255, 0, 128], [10, 0, 20]), "R5b": ([128, None, 128], [10, 10, None]),
              "R6": ([128, 64, 0], [10, 30, 20]), "Rold": ([64, 64, 64], [5, 5, 5]),
              "R24": ([0, 0, 0], [10, 10, 10]), "Runseen": ([64, 64, 64], [10, 10, 10])}
    for meta in identity.to_dict("records"):
        for number, (cu, stations) in enumerate(zip(*values[meta["radio_group_id"]])):
            duration = 1_000_000 if number < 2 else 500_000
            rows.append({**meta, "class_session_id": "C2" if meta["classroom"] == "OTHER" else "C1",
                "bin_number": number, "bin_start": START + pd.Timedelta(seconds=number),
                "bin_end": START + pd.Timedelta(seconds=number, microseconds=duration), "bin_duration_us": duration,
                "cu_state": "valid" if cu is not None else "missing_cu", "qbss_cu_raw": cu,
                "station_count": stations, "station_available": stations is not None,
                "station_stale_assisted": number == 1, "admission_state": "valid",
                "qbss_admission_capacity": 15625, "channel": 36 if meta["band"] == "5" else 1})
    detail = pd.DataFrame(rows)
    rollups = []
    for row in detail.to_dict("records"):
        rollups.append({"class_session_id": row["class_session_id"], "classroom": row["classroom"],
            "bin_number": row["bin_number"], "bin_duration_us": row["bin_duration_us"],
            "rollup_type": "radio", "rollup_id": row["radio_group_id"], "station_count": row["station_count"],
            "available": row["station_available"], "stale_assisted": row["station_stale_assisted"]})
    for (sid, room, band, number), group in detail.groupby(["class_session_id", "classroom", "band", "bin_number"]):
        available = bool(group.station_available.all())
        rollups.append({"class_session_id": sid, "classroom": room, "bin_number": number,
            "bin_duration_us": int(group.bin_duration_us.iloc[0]), "rollup_type": "band", "rollup_id": band,
            "station_count": group.station_count.sum() if available else None, "available": available,
            "stale_assisted": bool(group.station_stale_assisted.any())})
    return configured, identity, sessions, detail, pd.DataFrame(rollups)


def _accumulator(*, reverse=False):
    configured, identity, sessions, detail, stations = _inputs()
    original = detail.copy(deep=True)
    accumulator = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity, class_sessions=sessions,
        run_id="band-golden-v1", project_name="2026-test", display_timezone="America/Denver")
    session_ids = ["C2", "C1"] if reverse else ["C1", "C2"]
    for sid in session_ids:
        accumulator.add_detail(detail.loc[detail.class_session_id.eq(sid)].copy())
        accumulator.add_station(stations.loc[stations.class_session_id.eq(sid)].copy())
    pd.testing.assert_frame_equal(detail, original)
    return accumulator


def test_enablement_observation_default_explicit_override_and_mixed_rooms():
    accumulator = _accumulator()
    ap = accumulator.ap.set_index("ap_id")
    assert ap.loc["AP1", "six_ghz_enabled"]
    assert not ap.loc["AP2", "six_ghz_enabled"]
    assert ap.loc["AP3", "enabled_6_ghz_not_observed"]
    assert ap.loc["AP4", "enabled_6_ghz_not_observed"]
    room = accumulator.rooms.set_index("classroom").loc["ROOM"]
    assert room.mixed_ap_enablement and room.six_ghz_enabled
    configured, identity, *_ = _inputs()
    configured.loc[0, "6GHz_Enabled"] = False
    with pytest.raises(ValueError, match="contradicts.*AP AP1"):
        bands.build_infrastructure(configured, identity)


@pytest.mark.parametrize("value,expected", [(None, None), ("auto", None), ("YES", True), (0, False), (1.0, True), ("off", False)])
def test_input_enablement_column_is_optional_and_validated(value, expected):
    frame, _ = adapt_ap_mapping(pd.DataFrame({"Hostname": ["AP"], "Building-Room": ["ROOM"], "6GHz_Enabled": [value]}))
    result = frame["6GHz_Enabled"].iloc[0]
    assert pd.isna(result) if expected is None else bool(result) == expected


def test_bad_enablement_is_rejected_not_truthiness():
    with pytest.raises(ValueError, match="6GHz_Enabled"):
        adapt_ap_mapping(pd.DataFrame({"Hostname": ["AP"], "Building-Room": ["ROOM"], "6GHz_Enabled": ["maybe"]}))


def test_time_weighting_terminal_zero_pairs_stale_and_no_cu_sums():
    tables = _accumulator().tables()
    radio = tables["radio_class_comparison.csv"].set_index("radio_group_id")
    r5 = radio.loc["R5"]
    assert r5.valid_cu_duration_us == 2_500_000
    assert r5.mean_cu_percent == pytest.approx((255 + 128 * .5) / 2.5 * 100 / 255)
    assert r5.cu_relative_to_associated_station_count == pytest.approx((255 + 128 * .5) / (10 + 20 * .5) * 100 / 255)
    assert r5.zero_station_valid_cu_duration_us == 1_000_000
    assert r5.mean_cu_with_zero_stations_percent == 0
    assert r5.station_stale_duration_us == 1_000_000
    assert r5.mean_admission_capacity_percent == 50
    assert r5.class_start_local == "2026-10-12T09:00:00-06:00"
    summary = tables["population_summary.csv"].set_index(["population_scope", "population_id"])
    assert summary.loc[("ap_cohort", "5_on_without_6"), "radio_count"] == 1
    assert summary.loc[("classroom_cohort", "5_on_with_6"), "radio_count"] == 4
    assert summary.loc[("band", "2.4"), "mean_cu_percent"] == 0
    overall = summary.loc[("overall", "all")]
    assert overall.mean_cu_percent == pytest.approx(radio.cu_raw_duration_sum.sum() / radio.valid_cu_duration_us.sum() * 100 / 255)
    assert overall.equal_ap_session_mean_cu_percent != pytest.approx(overall.mean_cu_percent)


def test_matched_comparisons_use_only_common_valid_bins_and_each_radio():
    pairs = _accumulator().tables()["matched_ap_radio_sessions.csv"].set_index("radio_5_id")
    assert set(pairs.index) == {"R5", "R5b"}
    assert pairs.loc["R5b", "common_valid_cu_duration_us"] == 1_500_000
    assert pairs.loc["R5b", "matched_mean_6_cu_percent"] == pytest.approx(128 / 1.5 * 100 / 255)
    assert pairs.loc["R5b", "matched_6_minus_5_cu_percentage_points"] < 0


def test_simultaneous_adoption_complete_totals_not_peak_ratios_or_double_counting():
    tables = _accumulator().tables()
    adoption = tables["station_adoption_sessions.csv"].set_index(["scope", "entity_id"])
    ap = adoption.loc[("ap", "AP1")]
    assert ap["5_6_eligible_duration_us"] == 2_000_000  # one missing member in the terminal bin
    assert ap["5_6_six_station_share_percent"] == pytest.approx(40 / 70 * 100)
    assert ap["5_6_stale_assisted_duration_us"] == 1_000_000
    room = adoption.loc[("classroom", "ROOM")]
    assert room["5_6_six_station_share_percent"] == 50
    assert room["all_bands_six_station_share_percent"] == 40
    assert pd.isna(adoption.loc[("ap", "AP3"), "5_6_six_station_share_percent"])
    assert pd.isna(adoption.loc[("ap", "AP4"), "5_6_six_station_share_percent"])
    assert not adoption.loc[("ap", "AP2"), "band_6_expected_in_sample"]
    assert pd.isna(adoption.loc[("ap", "AP2"), "band_6_mean_station_count"])
    assert adoption.loc[("ap", "AP2"), "5_6_six_station_share_percent"] == 0
    project = tables["station_adoption_summary.csv"].set_index(["population_scope", "population_id"]).loc[("overall", "all")]
    assert project["5_6_six_station_duration_sum"] == 40_000_000
    assert project["5_6_total_station_duration_sum"] == 80_000_000


def test_create_only_cleanup_and_empty_package(tmp_path, monkeypatch):
    accumulator = _accumulator()
    files = accumulator.write(tmp_path)
    assert len(files) == 19
    assert len([path for path in files if path.suffix == ".png"]) == 5
    context = json.loads((tmp_path / bands.REPORT_DIRECTORY / "comparison_context.json").read_text())
    assert context["cu_relative_station_units"].startswith("CU_percentage_points")
    with pytest.raises(FileExistsError):
        accumulator.write(tmp_path)
    configured, identity, sessions, *_ = _inputs()
    empty = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity, class_sessions=sessions,
        run_id="empty", project_name="empty")
    empty.write(tmp_path / "empty")
    assert pd.read_csv(tmp_path / "empty" / bands.REPORT_DIRECTORY / "population_summary.csv").mean_cu_percent.isna().all()
    monkeypatch.setattr(bands, "_plots", lambda *_args: (_ for _ in ()).throw(RuntimeError("plot failure")))
    with pytest.raises(RuntimeError, match="plot failure"):
        _accumulator().write(tmp_path / "failed")
    assert not (tmp_path / "failed" / bands.REPORT_DIRECTORY).exists()


def _fingerprints(tables):
    result = {}
    for name, frame in tables.items():
        # Columns and logical values, independent of CSV line endings/PNG bytes.
        text = frame.to_json(orient="split", index=False, date_format="iso", date_unit="ns", double_precision=12)
        result[name] = {"rows": len(frame), "sha256": hashlib.sha256(text.encode()).hexdigest()}
    return result


def _stage1_columns(tables):
    additions = ["zero_station_available_duration_us",
        *[f"mean_cu_with_zero_stations_percent_{bound}_bound" for bound in ("lower", "upper")],
        *[f"equal_{entity}_session_mean_cu_percent_{bound}_bound" for entity in ("ap", "classroom") for bound in ("lower", "upper")]]
    pair_additions = ["common_5_cu_raw_duration_sum", "common_6_cu_raw_duration_sum", "cu_bounds_metric_id",
        *[f"paired_window_mean_{band}_cu_percent_{bound}_bound" for band in (5, 6) for bound in ("lower", "upper")],
        *[f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound" for bound in ("lower", "upper")]]
    return {name: frame.drop(columns=additions + (pair_additions if name == "matched_ap_radio_sessions.csv" else []), errors="ignore")
            for name, frame in tables.items() if name != "cu_cdf_bounds.csv"}


def test_logical_golden(tmp_path):
    # Compatible companion fields must not change any observed v1 logical data.
    additions = ["mean_cu_percent_lower_bound", "mean_cu_percent_upper_bound", "cu_bounds_metric_id"]
    original_columns = {name: frame.drop(columns=additions, errors="ignore")
                        for name, frame in _stage1_columns(_accumulator().tables()).items()}
    actual = _fingerprints(original_columns)
    golden = json.loads((Path(__file__).parent / "golden" / "band_comparison_v1.json").read_text())
    assert actual == golden["logical_tables"]


def test_ordinary_mean_bounds_cover_missing_duration_for_every_summary_population():
    tables = _accumulator().tables()
    radio = tables["radio_class_comparison.csv"].set_index("radio_group_id")
    assert radio.loc["R5b", "mean_cu_percent_lower_bound"] == pytest.approx(128 / 255 * 60)
    assert radio.loc["R5b", "mean_cu_percent_upper_bound"] == pytest.approx(128 / 255 * 60 + 40)
    for name in ("radio_class_comparison.csv", "population_summary.csv", "model_population_summary.csv", "classroom_band_summary.csv"):
        frame = tables[name]
        assert frame.cu_bounds_metric_id.eq(CU_BOUNDS_METRIC_ID).all()
        present = frame.radio_duration_us.gt(0)
        expected_lower = frame.loc[present, "cu_raw_duration_sum"] / frame.loc[present, "radio_duration_us"] * 100 / 255
        expected_upper = expected_lower + (1 - frame.loc[present, "valid_cu_duration_us"] / frame.loc[present, "radio_duration_us"]) * 100
        np.testing.assert_allclose(frame.loc[present, "mean_cu_percent_lower_bound"], expected_lower)
        np.testing.assert_allclose(frame.loc[present, "mean_cu_percent_upper_bound"], expected_upper)
        complete = present & frame.valid_cu_duration_us.eq(frame.radio_duration_us)
        for bound in ("lower", "upper"):
            np.testing.assert_array_equal(frame.loc[complete, f"mean_cu_percent_{bound}_bound"],
                                          frame.loc[complete, "mean_cu_percent"])
        assert frame.loc[~present, ["mean_cu_percent_lower_bound", "mean_cu_percent_upper_bound"]].isna().all().all()


def test_active_all_missing_radios_have_bounds_without_an_observed_mean():
    configured, identity, sessions, detail, _ = _inputs()
    detail = detail.loc[detail.class_session_id.eq("C1")].copy()
    detail["qbss_cu_raw"], detail["cu_state"] = np.nan, "no_beacon"
    accumulator = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity,
        class_sessions=sessions, run_id="missing", project_name="missing")
    accumulator.add_detail(detail)
    tables = accumulator.tables()
    for name in ("radio_class_comparison.csv", "population_summary.csv", "model_population_summary.csv", "classroom_band_summary.csv"):
        frame = tables[name]
        present = frame.radio_duration_us.gt(0)
        assert frame.loc[present, "mean_cu_percent"].isna().all()
        assert frame.loc[present, "mean_cu_percent_lower_bound"].eq(0).all()
        assert frame.loc[present, "mean_cu_percent_upper_bound"].eq(100).all()


def test_session_part_order_does_not_change_logical_outputs():
    assert _fingerprints(_accumulator().tables()) == _fingerprints(_accumulator(reverse=True).tables())


def test_ordinary_mean_bounds_match_their_logical_golden():
    golden = json.loads((Path(__file__).parent / "golden" / "band_comparison_mean_bounds_v1.json").read_text())
    assert golden["bounds_metric_id"] == CU_BOUNDS_METRIC_ID
    assert _fingerprints(_stage1_columns(_accumulator().tables())) == golden["logical_tables"]


def test_conditional_mean_bounds_match_their_logical_golden():
    golden = json.loads((Path(__file__).parent / "golden" / "band_comparison_conditional_mean_bounds_v1.json").read_text())
    assert golden["bounds_metric_id"] == CU_BOUNDS_METRIC_ID
    assert _fingerprints({name: frame for name, frame in _accumulator().tables().items() if name != "cu_cdf_bounds.csv"}) == golden["logical_tables"]


def test_equal_session_bounds_keep_equal_weights_and_exclude_all_missing_entities():
    configured, identity, sessions, detail, _ = _inputs()
    detail = detail.loc[detail.radio_group_id.isin(["R5", "R5b", "Rold", "Runseen"])].copy()
    detail["cu_state"], detail["qbss_cu_raw"] = "no_beacon", np.nan
    sampled = detail.radio_group_id.isin(["R5", "R5b"]) & detail.bin_number.eq(0)
    detail.loc[sampled, ["cu_state", "qbss_cu_raw"]] = ["valid", 255]
    detail.loc[detail.radio_group_id.eq("Rold"), ["cu_state", "qbss_cu_raw"]] = ["valid", 0]
    accumulator = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity,
        class_sessions=sessions, run_id="equal", project_name="equal")
    for _, part in detail.groupby("class_session_id"):
        accumulator.add_detail(part)
    populations = accumulator.tables()["population_summary.csv"]
    overall = populations.loc[populations.population_scope.eq("overall")].iloc[0]
    assert overall.eligible_ap_session_count == 2
    assert overall.eligible_classroom_session_count == 1
    assert overall.equal_ap_session_mean_cu_percent == 50
    assert overall.equal_ap_session_mean_cu_percent_lower_bound == 20
    assert overall.equal_ap_session_mean_cu_percent_upper_bound == 50
    assert overall.equal_classroom_session_mean_cu_percent_lower_bound == pytest.approx(100 * 2 / 7.5)
    assert overall.equal_classroom_session_mean_cu_percent_upper_bound == pytest.approx(100 * 5 / 7.5)


@pytest.mark.parametrize("state,station_available,observed,expected", [
    ("missing_cu", True, None, (0, 100)),
    ("valid", True, 0, (0, 0)),
    ("missing_cu", False, None, (None, None)),
])
def test_zero_station_bounds_require_available_zero_station_time(state, station_available, observed, expected):
    _, _, _, detail, _ = _inputs()
    frame = detail.loc[detail.radio_group_id.eq("R5")].copy()
    frame["cu_state"], frame["qbss_cu_raw"] = state, 0 if state == "valid" else np.nan
    frame["station_count"], frame["station_available"] = 0, station_available
    record = bands._metrics(frame)
    assert record["mean_cu_with_zero_stations_percent"] == observed
    assert (record["mean_cu_with_zero_stations_percent_lower_bound"], record["mean_cu_with_zero_stations_percent_upper_bound"]) == expected
    assert record["zero_station_available_duration_us"] == (2500000 if station_available else 0)


def test_zero_station_bounds_include_missing_cu_and_stale_zero_but_exclude_unknown_stations():
    configured, identity, sessions, detail, _ = _inputs()
    detail = detail.loc[detail.class_session_id.eq("C1")].copy()
    detail["station_count"], detail["station_available"] = 10, True
    radio = detail.radio_group_id.eq("R5")
    detail.loc[radio, "station_count"] = 0
    detail.loc[radio & detail.bin_number.eq(0), "qbss_cu_raw"] = 0
    detail.loc[radio & detail.bin_number.eq(1), ["cu_state", "qbss_cu_raw"]] = ["missing_cu", np.nan]
    detail.loc[radio & detail.bin_number.eq(2), "station_available"] = False
    accumulator = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity,
        class_sessions=sessions, run_id="zero", project_name="zero")
    accumulator.add_detail(detail)
    for name in ("radio_class_comparison.csv", "population_summary.csv", "model_population_summary.csv", "classroom_band_summary.csv"):
        frame = accumulator.tables()[name]
        included = frame.zero_station_available_duration_us.gt(0)
        assert frame.loc[included, "zero_station_available_duration_us"].eq(2000000).all()
        assert frame.loc[included, "mean_cu_with_zero_stations_percent"].eq(0).all()
        assert frame.loc[included, "mean_cu_with_zero_stations_percent_lower_bound"].eq(0).all()
        assert frame.loc[included, "mean_cu_with_zero_stations_percent_upper_bound"].eq(50).all()


def test_paired_window_bounds_keep_common_observations_and_same_window_difference():
    pairs = _accumulator().tables()["matched_ap_radio_sessions.csv"].set_index("radio_5_id")
    partial = pairs.loc["R5b"]
    assert partial.common_5_cu_raw_duration_sum == 128 * 1500000
    assert partial.common_6_cu_raw_duration_sum == 128 * 1000000
    assert partial.paired_window_mean_5_cu_percent_lower_bound == pytest.approx(128 / 255 * 60)
    assert partial.paired_window_mean_5_cu_percent_upper_bound == pytest.approx(128 / 255 * 60 + 40)
    assert partial.paired_window_mean_6_cu_percent_lower_bound == pytest.approx(128 / 255 * 40)
    assert partial.paired_window_6_minus_5_cu_percentage_points_lower_bound == pytest.approx(-128 / 255 * 20 - 40)
    assert partial.paired_window_6_minus_5_cu_percentage_points_upper_bound == pytest.approx(-128 / 255 * 20 + 40)
    complete = pairs.loc["R5"]
    for bound in ("lower", "upper"):
        for band in (5, 6):
            assert complete[f"paired_window_mean_{band}_cu_percent_{bound}_bound"] == complete[f"matched_mean_{band}_cu_percent"]
        assert complete[f"paired_window_6_minus_5_cu_percentage_points_{bound}_bound"] == complete.matched_6_minus_5_cu_percentage_points


def test_pair_with_no_common_cu_retains_unknown_range_without_a_matched_mean():
    configured, identity, sessions, detail, _ = _inputs()
    detail = detail.loc[detail.class_session_id.eq("C1")].copy()
    detail.loc[detail.band.eq("5"), ["cu_state", "qbss_cu_raw"]] = ["no_beacon", np.nan]
    accumulator = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity,
        class_sessions=sessions, run_id="no-pairs", project_name="no-pairs")
    accumulator.add_detail(detail)
    pairs = accumulator.tables()["matched_ap_radio_sessions.csv"]
    assert pairs.matched_mean_5_cu_percent.isna().all() and pairs.matched_mean_6_cu_percent.isna().all()
    for band in (5, 6):
        assert pairs[f"paired_window_mean_{band}_cu_percent_lower_bound"].eq(0).all()
        assert pairs[f"paired_window_mean_{band}_cu_percent_upper_bound"].eq(100).all()
    assert pairs.paired_window_6_minus_5_cu_percentage_points_lower_bound.eq(-100).all()
    assert pairs.paired_window_6_minus_5_cu_percentage_points_upper_bound.eq(100).all()


def test_density_preserves_time_and_overflow_does_not_clip_metrics():
    configured, identity, sessions, detail, _ = _inputs()
    detail.loc[detail.radio_group_id.eq("Rold"), "station_count"] = 500
    acc = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity, class_sessions=sessions,
        run_id="overflow", project_name="overflow")
    acc.add_detail(detail.loc[detail.class_session_id.eq("C1")])
    assert acc.cu_station_grids["ap_cohort", "5_on_without_6"][:, -1].sum() == 2_500_000
    table = acc.tables()["population_summary.csv"].set_index(["population_scope", "population_id"])
    assert table.loc[("ap_cohort", "5_on_without_6"), "mean_radio_station_count"] == 500
    radio = acc.tables()["radio_class_comparison.csv"]
    assert acc.cu_station_grids["overall", "all"].sum() == radio.positive_station_pair_duration_us.sum() + radio.zero_station_valid_cu_duration_us.sum()


@pytest.mark.parametrize("stations,limit", [(93, 100), (100, 150), (123, 150), (500, 500)])
def test_density_display_range_keeps_all_occupied_buckets_and_shared_normalization(stations, limit):
    acc = _accumulator()
    tables = acc.tables()
    for grid in acc.cu_station_grids.values():
        grid[:] = 0
    grid = acc.cu_station_grids["ap_cohort", "5_on_without_6"]
    grid[0, 0], grid[39, min(stations // 5, 49)] = 100, 300
    summary = tables["population_summary.csv"]
    summary.loc[summary.population_id.eq("5_on_without_6"), "maximum_radio_station_count"] = stations
    before = _fingerprints(tables)
    figure = bands.build_cohort_cu_station_density(acc.cu_station_grids, tables)
    axes = figure.axes[:-1]  # last axis is the shared color scale
    assert all(axis.get_ylim() == (0, limit) for axis in axes)
    mesh = axes[0].collections[0]
    assert sorted(mesh.get_array().compressed()) == [25, 75]
    assert mesh.get_coordinates()[-1, 0, 1] >= limit
    assert ("last bin ≥245" in axes[0].get_ylabel()) == (stations >= 245)
    assert _fingerprints(tables) == before


def test_saved_density_buckets_match_full_analysis_with_missing_stale_zero_and_mixed_cohorts(tmp_path):
    from corrected_cached_plots import _saved_cohort_station_grids

    acc = _accumulator()
    _, _, _, detail, _ = _inputs()
    path = tmp_path / "detail.parquet"
    detail.to_parquet(path, index=False, row_group_size=3)
    # CSV boolean strings must not be treated as Python truthy strings.
    ap, rooms = acc.ap.copy(), acc.rooms.copy()
    ap["six_ghz_enabled"] = ap.six_ghz_enabled.astype(str)
    rooms["six_ghz_enabled"] = rooms.six_ghz_enabled.astype(str)
    actual = _saved_cohort_station_grids(path, ap, rooms)
    for scope in ("ap_cohort", "classroom_cohort"):
        for band in bands.BANDS:
            for suffix in ("with_6", "without_6"):
                key = scope, f"{band}_on_{suffix}"
                np.testing.assert_array_equal(actual[key], acc.cu_station_grids[key])


def test_no_valid_cu_retains_unknown_high_durations():
    *_, detail, _ = _inputs()
    detail["cu_state"], detail["qbss_cu_raw"] = "no_beacon", np.nan
    metrics = bands._metrics(detail)
    assert metrics["mean_cu_percent"] is None
    assert metrics["high_cu_duration_us"] is None
    assert metrics["very_high_cu_duration_us"] is None
    assert metrics["high_cu_percent_of_valid_time"] is None


@pytest.mark.parametrize("mixed", [False, True])
def test_plot_scopes_collapse_only_uniform_classrooms_without_changing_tables(tmp_path, monkeypatch, mixed):
    configured, identity, sessions, detail, stations = _inputs()
    if not mixed:
        configured.loc[configured.Hostname.eq("AP2"), "6GHz_Enabled"] = True
    acc = bands.BandComparisonAccumulator(configured_aps=configured, identity_map=identity, class_sessions=sessions,
        run_id="layout", project_name="layout")
    for sid in ("C1", "C2"):
        acc.add_detail(detail.loc[detail.class_session_id.eq(sid)])
        acc.add_station(stations.loc[stations.class_session_id.eq(sid)])
    tables = acc.tables()
    expected = _fingerprints(tables)
    figures = {}
    monkeypatch.setattr(bands.Figure, "savefig", lambda figure, path, **kwargs: figures.setdefault(Path(path).name, figure))
    bands._plots(tmp_path, acc, tables)
    assert len(figures["cu_cohort_cdf.png"].axes) == (6 if mixed else 3)
    for axis in figures["cohort_metrics.png"].axes:
        assert len(axis.patches) == (6 if mixed else 3)
    assert len(figures["cohort_cu_station_density.png"].axes) == (7 if mixed else 4)
    assert len(figures["station_adoption.png"].axes) == 2  # different counting units, not duplicate cohorts
    adoption_axes = figures["station_adoption.png"].axes
    assert adoption_axes[0].get_shared_y_axes().joined(*adoption_axes)
    assert adoption_axes[0].get_ylim() == adoption_axes[1].get_ylim()
    assert "not inferred performance effects" not in figures["cu_cohort_cdf.png"]._suptitle.get_text()
    assert "available time" not in figures["cohort_metrics.png"].axes[2].get_title()
    assert "percentage points per associated station" in figures["cohort_metrics.png"].axes[3].get_title()
    assert "common valid bins" not in figures["matched_ap_cu.png"]._suptitle.get_text()
    assert "stale assistance" not in figures["station_adoption.png"]._suptitle.get_text()
    assert _fingerprints(tables) == expected
    assert {"ap_cohort", "classroom_cohort"}.issubset(set(tables["population_summary.csv"].population_scope))
    if not mixed:
        assert "no mixed classrooms" in figures["cu_cohort_cdf.png"]._suptitle.get_text()


def test_cohort_scope_detector_accepts_csv_boolean_flags():
    assert bands._cohort_plot_scopes(pd.DataFrame({"mixed_ap_enablement": ["False", "False"]})) == ("ap_cohort",)
    assert bands._cohort_plot_scopes(pd.DataFrame({"mixed_ap_enablement": ["False", "True"]})) == ("ap_cohort", "classroom_cohort")


def test_adoption_annotations_distinguish_zero_near_zero_and_missing():
    frame = pd.DataFrame({"six_ghz_enabled": [False, True, True, True, True, False],
                          "5_6_six_station_share_percent": [0, 0, .5, 2.5, np.nan, 100]})
    assert bands._adoption_plot_counts(frame) == {"eligible": 5, "unknown": 1, "zero": 2, "first_bin": 3}


@pytest.mark.parametrize("enabled_values", [[False, True, True, True, True], ["False", "True", "True", "True", "True"]])
def test_adoption_histograms_only_enabled_keep_true_zero_and_exclude_unknown(tmp_path, monkeypatch, enabled_values):
    acc = _accumulator()
    tables = acc.tables()
    tables["station_adoption_sessions.csv"] = pd.DataFrame([
        {"scope": scope, "six_ghz_enabled": enabled, "5_6_six_station_share_percent": share}
        for scope in ("ap", "classroom")
        for enabled, share in zip(enabled_values, [0, 0, .5, 2.5, np.nan])])
    expected = _fingerprints(tables)
    figures = {}
    monkeypatch.setattr(bands.Figure, "savefig", lambda figure, path, **kwargs: figures.setdefault(Path(path).name, figure))
    bands._plots(tmp_path, acc, tables)
    for axis in figures["station_adoption.png"].axes:
        counts = [patch.get_height() for patch in axis.patches]
        assert sum(counts) == 3  # enabled 0, .5 and 2.5; disabled 0 and unknown excluded
        assert counts[:2] == [2, 1]
        text = "\n".join(item.get_text() for item in axis.texts)
        assert "unavailable: 1" in text
        assert "Exact 0% use where enabled: 1" in text
        assert "excluded non-enabled sessions: 1" in text
        assert "6 GHz-enabled" in axis.get_title()
    assert _fingerprints(tables) == expected  # includes unchanged overall and cohort summaries


def test_adoption_histograms_no_enabled_entities(tmp_path, monkeypatch):
    acc = _accumulator()
    tables = acc.tables()
    tables["station_adoption_sessions.csv"]["six_ghz_enabled"] = False
    figures = {}
    monkeypatch.setattr(bands.Figure, "savefig", lambda figure, path, **kwargs: figures.setdefault(Path(path).name, figure))
    bands._plots(tmp_path, acc, tables)
    for axis in figures["station_adoption.png"].axes:
        assert sum(patch.get_height() for patch in axis.patches) == 0
        assert any("No eligible station totals where 6 GHz is enabled" in text.get_text() for text in axis.texts)
