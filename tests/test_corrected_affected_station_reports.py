from pathlib import Path
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

import corrected_affected_station_reports as affected
from corrected_band_comparison_reports import build_infrastructure
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID, station_time_cu_bounds_fields

START = pd.Timestamp("2026-10-12T15:00:00Z")
BOUNDS_COLUMNS = ["missing_cu_station_time_us", "cu_bounds_metric_id",
    *[f"{name}_{metric}_{bound}_bound" for name in ("high", "very_high")
      for metric in ("station_time_us", "exposure_percent") for bound in ("lower", "upper")]]


def _inputs():
    aps = pd.DataFrame({"Hostname": ["AP1", "AP2"], "Building-Room": ["ROOM", "ROOM"],
                        "Model": ["635", "515"]})
    identity = pd.DataFrame([
        dict(bssid="A", radio_group_id="R1", ap_id="AP1", ap_model="635", classroom="ROOM", band="5"),
        dict(bssid="B", radio_group_id="R2", ap_id="AP1", ap_model="635", classroom="ROOM", band="6"),
        dict(bssid="C", radio_group_id="R3", ap_id="AP2", ap_model="515", classroom="ROOM", band="5")])
    sessions = pd.DataFrame([dict(class_session_id=sid, classroom="ROOM", class_start=START,
                                  class_end=START + pd.Timedelta(seconds=2.5), bin_width_us=1_000_000,
                                  course_labels="COURSE", enrolled=20) for sid in ("C1", "C2")])
    rows = []
    for meta, cus, stations in zip(identity.to_dict("records"), ([128, 0, 192], [0, 192, None], [255, 64, 192]),
                                   ([10, 20, 30], [40, 5, 50], [3, 7, 11])):
        for number, (cu, count) in enumerate(zip(cus, stations)):
            rows.append({**meta, "class_session_id": "C1", "bin_number": number,
                "bin_duration_us": 1_000_000 if number < 2 else 500_000,
                "cu_state": "valid" if cu is not None else "missing_cu", "qbss_cu_raw": cu,
                "station_count": count, "station_available": True,
                "station_stale_assisted": number == 1 and meta["ap_id"] == "AP1"})
    return aps, identity, sessions, pd.DataFrame(rows)


def _accumulator(detail=True):
    aps, identity, sessions, frame = _inputs()
    ap, rooms, _ = build_infrastructure(aps, identity)
    accumulator = affected.AffectedStationAccumulator(class_sessions=sessions, ap_infrastructure=ap,
        classroom_infrastructure=rooms, run_id="affected-golden-v1", project_name="fixture",
        display_timezone="America/Denver")
    if detail:
        accumulator.add_detail(frame)
    return accumulator


def test_exact_station_time_thresholds_terminal_bin_stale_and_no_double_counting():
    tables = _accumulator().tables()
    overall = tables["population_exposure.csv"].iloc[0]
    assert overall.radio_duration_us == 7_500_000
    assert overall.pair_duration_us == 7_000_000
    assert overall.paired_station_time_us == 105_500_000
    assert overall.available_station_time_us == 130_500_000
    assert overall.high_station_time_us == 38_500_000
    assert overall.very_high_station_time_us == 28_500_000
    assert overall.high_only_station_time_us == 10_000_000
    assert overall.below_high_station_time_us == 67_000_000
    assert overall.high_exposure_percent == pytest.approx(38.5 / 105.5 * 100)
    assert overall.stale_paired_station_time_us == 25_000_000
    assert overall.stale_very_high_station_time_us == 5_000_000
    assert overall.pair_coverage_percent == pytest.approx(7 / 7.5 * 100)
    assert overall.known_station_time_cu_coverage_percent == pytest.approx(105.5 / 130.5 * 100)
    # Overall uses radio rows exactly once, not their overlapping AP/classroom views.
    assert overall.radio_session_count == 3
    assert overall.high_station_time_us + overall.below_high_station_time_us == overall.paired_station_time_us
    population = tables["population_exposure.csv"].set_index(["population_scope", "population_id"])
    assert population.loc[("ap_cohort", "5_on_without_6"), "paired_station_time_us"] == 15_500_000
    assert population.loc[("classroom_cohort", "5_on_without_6"), "paired_station_time_us"] == 0
    assert population.loc[("classroom_cohort", "5_on_with_6"), "paired_station_time_us"] == 60_500_000


def test_peaks_are_concurrent_and_complete_not_sums_of_independent_peaks():
    rows = _accumulator().tables()["entity_session_exposure.csv"]
    rows = rows.loc[rows.class_session_id.eq("C1")].set_index(["scope", "entity_id", "band"])
    assert rows.loc[("radio", "R1", "5"), "peak_high_associations"] == 30
    ap = rows.loc[("ap", "AP1", "all")]
    assert ap.peak_high_associations == 10  # not 30+5, and not incomplete terminal-bin 30
    assert ap.peak_very_high_associations == 5
    assert ap.complete_bin_duration_us == 2_000_000
    assert ap.complete_bin_coverage_percent == 80
    assert ap.peak_high_bin_number == 0
    assert not ap.peak_high_stale_assisted
    assert ap.peak_very_high_stale_assisted
    room = rows.loc[("classroom", "ROOM", "all")]
    assert room.peak_high_associations == 13
    assert room.peak_very_high_associations == 5
    assert rows.loc[("classroom", "ROOM", "5"), "peak_high_associations"] == 41
    assert room.peak_high_bin_start_utc == START.isoformat()
    assert room.class_start_local == "2026-10-12T09:00:00-06:00"


def test_empty_session_and_zero_station_denominator_are_not_zero_exposure():
    acc = _accumulator(False)
    frame = _inputs()[-1]
    frame["station_count"] = 0
    acc.add_detail(frame)
    tables = acc.tables()
    assert pd.isna(tables["population_exposure.csv"].iloc[0].high_exposure_percent)
    rows = tables["entity_session_exposure.csv"]
    assert rows.loc[rows.class_session_id.eq("C1"), "peak_high_associations"].eq(0).all()
    empty = rows.loc[rows.class_session_id.eq("C2")].iloc[0]
    assert pd.isna(empty.peak_high_associations)
    assert empty.unavailable_bin_duration_us == 2_500_000
    assert empty.complete_bin_coverage_percent == 0
    assert pd.isna(empty.pair_coverage_percent)
    distribution = tables["classroom_peak_distribution.csv"].query("band == 'all' and threshold == 'high'").iloc[0]
    assert distribution.available_peak_count == 1
    assert distribution.unavailable_peak_count == 1
    assert distribution.maximum == 0


@pytest.mark.parametrize("kind", ["no_beacon", "missing_cu", "station"])
def test_unknowns_never_become_below_threshold_or_partial_peak(kind):
    acc = _accumulator(False)
    frame = _inputs()[-1]
    if kind == "station":
        frame["station_count"], frame["station_available"] = np.nan, False
    else:
        frame["cu_state"], frame["qbss_cu_raw"] = kind, np.nan
    acc.add_detail(frame)
    tables = acc.tables()
    overall = tables["population_exposure.csv"].iloc[0]
    assert pd.isna(overall.high_exposure_percent)
    assert overall.pair_duration_us == 0
    assert overall.pair_coverage_percent == 0
    assert tables["entity_session_exposure.csv"].peak_high_associations.isna().all()


def test_geometry_duplicate_parts_and_duplicate_bins_fail():
    frame = _inputs()[-1]
    acc = _accumulator()
    with pytest.raises(ValueError, match="duplicate affected"):
        acc.add_detail(frame)
    for broken in (pd.concat([frame, frame.iloc[:1]]), frame.iloc[:-1], frame.assign(bin_duration_us=999)):
        with pytest.raises(ValueError, match="geometry"):
            _accumulator(False).add_detail(broken)


def test_project_is_ratio_of_sums_not_mean_ratios_and_peak_quantiles_do_not_sum():
    acc = _accumulator()
    second = _inputs()[-1].assign(class_session_id="C2", cu_state="valid", qbss_cu_raw=0)
    second["station_count"] *= 100
    acc.add_detail(second)
    tables = acc.tables()
    overall = tables["population_exposure.csv"].iloc[0]
    assert overall.high_exposure_percent == pytest.approx(38.5 / (105.5 + 13050) * 100)
    distribution = tables["classroom_peak_distribution.csv"].query("band == 'all' and threshold == 'high'").iloc[0]
    assert distribution.available_peak_count == 2
    assert distribution.maximum == 13
    assert distribution["median"] == 6.5
    assert distribution.p90 == pytest.approx(11.7)


def test_sampled_24ghz_participates_and_input_is_not_mutated():
    acc = _accumulator(False)
    frame = _inputs()[-1]
    frame.loc[frame.radio_group_id.eq("R3"), "band"] = "2.4"
    original = frame.copy(deep=True)
    acc.add_detail(frame)
    population = acc.tables()["population_exposure.csv"].set_index(["population_scope", "population_id"])
    assert population.loc[("band", "2.4"), "paired_station_time_us"] == 15_500_000
    pd.testing.assert_frame_equal(frame, original)


@pytest.mark.parametrize("include_24", [False, True])
def test_exposure_figure_omits_unsampled_bands_without_changing_values(include_24):
    acc = _accumulator(False)
    frame = _inputs()[-1]
    if include_24:
        frame.loc[frame.radio_group_id.eq("R3"), "band"] = "2.4"
    acc.add_detail(frame)
    populations = acc.tables()["population_exposure.csv"]
    before = populations.copy(deep=True)
    figure = affected.build_station_time_exposure(populations)
    labels = ["Overall", *(["2.4 GHz"] if include_24 else []), "5 GHz", "6 GHz"]
    for axis, name in zip(figure.axes, ("high", "very_high")):
        assert [tick.get_text() for tick in axis.get_xticklabels()] == labels
        source = populations.loc[populations.population_scope.eq("overall") | (
            populations.population_scope.eq("band") & populations.radio_duration_us.gt(0)), f"{name}_exposure_percent"]
        np.testing.assert_allclose([patch.get_height() for patch in axis.patches], source)
    assert figure.axes[0].get_ylim() == figure.axes[1].get_ylim()
    assert figure._suptitle.get_text() == "Station-time with high and very high CU"
    pd.testing.assert_frame_equal(populations, before)


def _fingerprints(tables):
    return {name: {"rows": len(frame), "sha256": hashlib.sha256(frame.to_json(
        orient="split", index=False, date_format="iso", double_precision=12).encode()).hexdigest()}
        for name, frame in tables.items()}


def test_order_determinism_and_overflow_safety():
    frame = _inputs()[-1]
    acc = _accumulator(False)
    acc.add_detail(frame.sample(frac=1, random_state=4))
    assert _fingerprints(acc.tables()) == _fingerprints(_accumulator().tables())
    values = np.array([65535, 65535], dtype=np.int64)
    durations = np.array([10**14, 10**14], dtype=np.int64)
    assert affected._weighted(values, durations) == 13_107_000_000_000_000_000


def test_package_create_only_cleanup_and_logical_golden(tmp_path, monkeypatch):
    acc = _accumulator()
    expected = json.loads((Path(__file__).parent / "golden/affected_stations_v1.json").read_text())
    original = {name: frame.drop(columns=BOUNDS_COLUMNS, errors="ignore") for name, frame in acc.tables().items()}
    assert _fingerprints(original) == expected["logical_tables"]
    files = acc.write(tmp_path)
    assert len(files) == 7
    assert len([path for path in files if path.suffix == ".png"]) == 2
    context = json.loads((tmp_path / affected.REPORT_DIRECTORY / "context.json").read_text())
    assert context["station_time_cu_bounds"]["metric_id"] == CU_BOUNDS_METRIC_ID
    assert context["station_time_cu_bounds"]["unknown_station_counts_bounded"] is False
    with pytest.raises(FileExistsError):
        acc.write(tmp_path)
    monkeypatch.setattr(affected, "_plots", lambda *_: (_ for _ in ()).throw(RuntimeError("plot failure")))
    with pytest.raises(RuntimeError, match="plot failure"):
        _accumulator().write(tmp_path / "failed")
    assert not (tmp_path / "failed" / affected.REPORT_DIRECTORY).exists()


@pytest.mark.parametrize("available,paired,high,very,expected", [
    (100, 80, 30, 10, (30, 50, 10, 30)),
    (100, 100, 30, 10, (30, 30, 10, 10)),
    (100, 0, 0, 0, (0, 100, 0, 100)),
    (100, 80, 0, 0, (0, 20, 0, 20)),
    (0, 0, 0, 0, (None, None, None, None)),
])
def test_station_time_bounds_use_available_denominator_and_keep_unknown_exposure(
        available, paired, high, very, expected):
    result = station_time_cu_bounds_fields(available, paired, high, very)
    assert tuple(result[f"{name}_exposure_percent_{bound}_bound"]
                 for name in ("high", "very_high") for bound in ("lower", "upper")) == expected
    assert result["missing_cu_station_time_us"] == available - paired
    assert result["high_station_time_us_lower_bound"] == high
    assert result["high_station_time_us_upper_bound"] == high + available - paired
    assert result["very_high_station_time_us_lower_bound"] == very
    assert result["very_high_station_time_us_upper_bound"] == very + available - paired
    assert result["cu_bounds_metric_id"] == CU_BOUNDS_METRIC_ID


@pytest.mark.parametrize("components", [(1, 2, 1, 0), (2, 1, 2, 0), (2, 2, 1, 2),
    (-1, 0, 0, 0), (2, -1, 0, 0), (2, 1, -.5, 0), (2, 1, 0, -1),
    (True, 1, 0, 0), (2., 1, 0, 0), (2, 1, 0, None)])
def test_invalid_station_time_components_cannot_produce_plausible_ranges(components):
    with pytest.raises(ValueError):
        station_time_cu_bounds_fields(*components)


def test_station_time_bound_amounts_remain_exact_above_int64():
    huge = 2 ** 62
    result = station_time_cu_bounds_fields(4 * huge, np.int64(huge), np.int64(huge), 0)
    assert result["high_station_time_us_upper_bound"] == 4 * huge
    assert isinstance(result["high_station_time_us_upper_bound"], int)
    assert result["very_high_station_time_us_upper_bound"] == 3 * huge
    assert result["high_exposure_percent_lower_bound"] == 25
    assert result["high_exposure_percent_upper_bound"] == 100


def test_bounds_follow_exact_available_station_time_for_every_entity_and_population():
    tables = _accumulator().tables()
    for filename in ("entity_session_exposure.csv", "population_exposure.csv"):
        for row in tables[filename].itertuples(index=False):
            assert row.cu_bounds_metric_id == CU_BOUNDS_METRIC_ID
            assert row.missing_cu_station_time_us == row.available_station_time_us - row.paired_station_time_us
            for name in ("high", "very_high"):
                high = getattr(row, f"{name}_station_time_us")
                assert getattr(row, f"{name}_station_time_us_lower_bound") == high
                assert getattr(row, f"{name}_station_time_us_upper_bound") == high + row.missing_cu_station_time_us
                if row.available_station_time_us:
                    assert getattr(row, f"{name}_exposure_percent_lower_bound") == pytest.approx(high / row.available_station_time_us * 100)
                    assert getattr(row, f"{name}_exposure_percent_upper_bound") == pytest.approx((high + row.missing_cu_station_time_us) / row.available_station_time_us * 100)
                else:
                    assert pd.isna(getattr(row, f"{name}_exposure_percent_lower_bound"))
                    assert pd.isna(getattr(row, f"{name}_exposure_percent_upper_bound"))
    overall = tables["population_exposure.csv"].iloc[0]
    assert overall.high_exposure_percent == pytest.approx(38.5 / 105.5 * 100)
    assert overall.high_exposure_percent_lower_bound == pytest.approx(38.5 / 130.5 * 100)
    assert overall.high_exposure_percent_upper_bound == pytest.approx(63.5 / 130.5 * 100)
    assert overall.very_high_station_time_us_upper_bound == 53_500_000
    assert overall.high_station_time_us_upper_bound == 63_500_000


@pytest.mark.parametrize("kind", ["missing_cu", "no_beacon", "unknown_stations", "zero_stations"])
def test_all_missing_cu_or_unknown_counts_remain_distinct(kind):
    acc = _accumulator(False)
    frame = _inputs()[-1]
    frame["cu_state"], frame["qbss_cu_raw"] = "missing_cu", np.nan
    if kind == "no_beacon":
        frame["cu_state"] = "no_beacon"
    elif kind == "unknown_stations":
        frame["station_available"], frame["station_count"] = False, np.nan
    elif kind == "zero_stations":
        frame["station_count"] = 0
    acc.add_detail(frame)
    row = acc.tables()["population_exposure.csv"].iloc[0]
    assert pd.isna(row.high_exposure_percent)
    if kind in {"missing_cu", "no_beacon"}:
        assert row.high_exposure_percent_lower_bound == row.very_high_exposure_percent_lower_bound == 0
        assert row.high_exposure_percent_upper_bound == row.very_high_exposure_percent_upper_bound == 100
        assert row.high_station_time_us_upper_bound == row.available_station_time_us == 130_500_000
    else:
        assert pd.isna(row.high_exposure_percent_lower_bound)
        assert pd.isna(row.high_exposure_percent_upper_bound)
        assert row.high_station_time_us_upper_bound == 0
        assert row.station_coverage_percent == (0 if kind == "unknown_stations" else 100)


def test_missing_beacons_and_missing_fields_include_stale_counts_but_not_unknown_stations():
    frame = _inputs()[-1]
    first = frame.radio_group_id.eq("R1") & frame.bin_number.eq(0)
    last = frame.radio_group_id.eq("R2") & frame.bin_number.eq(2)
    frame.loc[first, ["cu_state", "qbss_cu_raw", "station_stale_assisted"]] = ["no_beacon", np.nan, True]
    frame.loc[last, "station_stale_assisted"] = True
    acc = _accumulator(False)
    acc.add_detail(frame)
    overall = acc.tables()["population_exposure.csv"].iloc[0]
    assert overall.missing_cu_station_time_us == 35_000_000
    assert overall.high_station_time_us_lower_bound == 28_500_000
    assert overall.high_station_time_us_upper_bound == 63_500_000
    frame.loc[first, ["station_available", "station_count"]] = [False, np.nan]
    acc = _accumulator(False)
    acc.add_detail(frame)
    overall = acc.tables()["population_exposure.csv"].iloc[0]
    assert overall.missing_cu_station_time_us == 25_000_000
    assert overall.high_station_time_us_upper_bound == 53_500_000
    assert overall.station_coverage_percent == pytest.approx(6.5 / 7.5 * 100)


def test_zero_station_missing_cu_does_not_widen_station_time_bounds():
    frame = _inputs()[-1]
    frame.loc[frame.radio_group_id.eq("R2") & frame.bin_number.eq(2), "station_count"] = 0
    acc = _accumulator(False)
    acc.add_detail(frame)
    row = acc.tables()["population_exposure.csv"].iloc[0]
    assert row.cu_coverage_percent < 100
    assert row.known_station_time_cu_coverage_percent == 100
    assert row.missing_cu_station_time_us == 0
    assert row.high_exposure_percent_lower_bound == row.high_exposure_percent_upper_bound == row.high_exposure_percent


def test_plot_bounds_do_not_change_observed_bars_and_do_not_clip_ranges():
    source = _accumulator().tables()["population_exposure.csv"]
    bounded = affected.build_station_time_exposure(source)
    unbounded = affected.build_station_time_exposure(source.drop(columns=BOUNDS_COLUMNS))
    for axis, original in zip(bounded.axes, unbounded.axes):
        np.testing.assert_array_equal([p.get_height() for p in axis.patches], [p.get_height() for p in original.patches])
        assert len(axis.collections) == 3
    assert any("available station-time" in text.get_text() for text in bounded.legends[0].get_texts())
    # An entirely missing observed population still needs a visible 0-100 range.
    acc = _accumulator(False)
    frame = _inputs()[-1].assign(cu_state="missing_cu", qbss_cu_raw=np.nan)
    acc.add_detail(frame)
    figure = affected.build_station_time_exposure(acc.tables()["population_exposure.csv"])
    for axis in figure.axes:
        assert axis.get_ylim()[1] >= 100
        assert all(np.isnan(p.get_height()) for p in axis.patches)
        np.testing.assert_array_equal(axis.collections[0].get_segments()[0], [[0, 0], [0, 100]])


def test_station_time_bounds_logical_golden():
    tables = _accumulator().tables()
    expected = json.loads((Path(__file__).parent / "golden/affected_stations_cu_bounds_v1.json").read_text())
    assert _fingerprints(tables) == expected["logical_tables"]
