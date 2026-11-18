import csv
import json
from pathlib import Path
import re

import pytest
from PIL import Image

import corrected_report_overview as overview


def _csv(root, relative, values):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=values[0])
        writer.writeheader()
        writer.writerows(values)


def _fixture(root):
    (root / "manifest.json").write_text("{}")
    _csv(root, "reports/band_comparison/population_summary.csv", [
        dict(population_scope="overall", population_id="all", mean_cu_percent=0,
             mean_radio_station_count="", cu_coverage_percent=42.125, station_coverage_percent=55),
        dict(population_scope="ap_cohort", population_id="5_on_without_6", mean_cu_percent=12.34,
             mean_radio_station_count=25, cu_coverage_percent=80, station_coverage_percent=90)])
    _csv(root, "reports/band_comparison/station_adoption_summary.csv", [
        {"population_scope": "overall", "population_id": "all", "5_6_six_station_share_percent": 0}])
    _csv(root, "reports/band_comparison/deployment_summary.csv", [
        dict(configured_ap_count=4, six_ghz_enabled_ap_count=1, six_ghz_enabled_ap_percent=25)])
    _csv(root, "reports/class_sessions/class_session_report.csv", [
        dict(class_session_id="C1", classroom="Room <A>", class_start_utc="2026-11-01T15:00:00Z",
             local_date="2026-11-01", local_start="08:00", local_end="09:00", display_timezone="America/Denver",
             course_labels="Event <script>alert(1)</script>", combined_enrollment=20, peak_client_count=40,
             radio_cu_coverage_min=.512)])


@pytest.mark.parametrize("all_cu_missing", [False, True])
def test_station_exposure_subsection_displays_available_time_bounds_and_denominators(tmp_path, all_cu_missing):
    from corrected_cu_bounds import station_time_cu_bounds_fields
    (tmp_path / "manifest.json").write_text("{}")
    hour = 3_600_000_000
    paired, high, very = (0, 0, 0) if all_cu_missing else (80, 30, 10)
    row = dict(population_scope="overall", population_id="all",
        high_exposure_percent=high/paired*100 if paired else None,
        very_high_exposure_percent=very/paired*100 if paired else None,
        available_station_time_us=100*hour, paired_station_time_us=paired*hour,
        high_station_time_us=high*hour, very_high_station_time_us=very*hour,
        pair_coverage_percent=paired, station_coverage_percent=100, stale_pair_percent=0,
        known_station_time_cu_coverage_percent=paired,
        **station_time_cu_bounds_fields(100*hour, paired*hour, high*hour, very*hour))
    _csv(tmp_path, "reports/affected_stations/population_exposure.csv", [row])
    path = overview.write_run_overview(tmp_path, project_name="Bounds", run_id="fixture")
    text = path.read_text(encoding="utf-8")
    content = text.split('<div id="exposure" class="report-subsection">', 1)[1].split('</section>', 1)[0]
    intro = text.split('<section id="overview">', 1)[1].split('</section>', 1)[0]
    cards = dict(re.findall(r'<article class="card"><h4>(.*?)</h4>(.*?)</article>', intro))
    assert "Station-time with high CU" in cards and "Station-time with very high CU" in cards
    expected_range = "Between 0.00% and 100.00% possible" if all_cu_missing else "Between 30.00% and 50.00% possible"
    assert expected_range in cards["Station-time with high CU"]
    if all_cu_missing:
        assert 'Station-time with high CU observed</dt><dd>Unavailable</dd>' in content
        assert 'Station-time with high CU possible</dt><dd>0.00%–100.00%</dd>' in content
        assert 'Association-hours with high CU observed</dt><dd>0.00</dd>' in content
        assert 'Association-hours with high CU possible</dt><dd>0.00–100.00</dd>' in content
    else:
        assert 'Station-time with high CU observed</dt><dd>37.50%</dd>' in content
        assert 'Station-time with high CU possible</dt><dd>30.00%–50.00%</dd>' in content
        assert 'Station-time with very high CU observed</dt><dd>12.50%</dd>' in content
        assert 'Station-time with very high CU possible</dt><dd>10.00%–30.00%</dd>' in content
        assert 'Association-hours with high CU observed</dt><dd>30.00</dd>' in content
        assert 'Association-hours with high CU possible</dt><dd>30.00–50.00</dd>' in content
    assert 'Association-hours with available station counts</dt><dd>100.00' in content
    assert 'Station-count coverage</dt><dd>100.00%' in content
    assert 'Periods with unknown station counts are outside these bounds' in content
    assert 'not confidence intervals' in content
    assert '37.5% observed exposure' in content
    assert '<section id="exposure">' not in text
    overview.validate_overview_links(text, tmp_path)


def test_minimal_offline_overview_handles_missing_families_and_zero(tmp_path):
    (tmp_path / "manifest.json").write_text("{}")
    path = overview.write_run_overview(tmp_path, project_name="Empty", run_id="run")
    text = path.read_text(encoding="utf-8")
    overview.validate_overview_links(text, tmp_path)
    assert "Unavailable" in text
    assert "This report family was not generated" in text
    assert "id=\"sessions\"" in text
    assert text.count('<script>') == 1 and f'<script>{overview.NAVIGATION_SCRIPT}</script>' in text
    assert "https://" not in text
    with pytest.raises(FileExistsError):
        overview.write_run_overview(tmp_path, project_name="Again", run_id="again")


def test_existing_values_units_coverage_and_html_escaping(tmp_path):
    _fixture(tmp_path)
    path = overview.write_run_overview(tmp_path, project_name='Project <img onerror="oops">', run_id="run&1")
    text = path.read_text(encoding="utf-8")
    assert '<p class="value">0.00%</p>' in text  # genuine zero is retained
    assert '<p class="value">Unavailable</p>' in text
    assert '<h4>CU coverage</h4><p class="value">42.12%</p>' in text
    assert "12.34%" in text
    assert "80.00%" in text
    assert "25.00" in text
    assert "Of 5 and 6 GHz associated station-time on all APs" in text
    assert "Room &lt;A&gt;" in text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in text
    assert text.count('<script>') == 1 and f'<script>{overview.NAVIGATION_SCRIPT}</script>' in text
    assert '<script>alert(1)</script>' not in text
    assert "run&amp;1" in text
    assert "America/Denver" in text
    assert "08:00–09:00" in text
    overview.validate_overview_links(text, tmp_path)


def test_compact_navigation_keeps_section_titles_and_removes_requested_quality_guide(tmp_path):
    _fixture(tmp_path)
    text = overview.write_run_overview(tmp_path, project_name="Navigation", run_id="run").read_text(encoding="utf-8")
    nav = text.split('<nav aria-label="Report sections">', 1)[1].split('</nav>', 1)[0]
    assert '<a href="#radio">CU/station</a>' in nav
    assert '<a href="#sessions">Class sessions</a>' in nav
    assert '<h2>CU and station-count relationship</h2>' in text
    assert '<h2>Classrooms and class sessions</h2>' in text
    assert "The summary quantifies CU and station-count availability." not in text
    assert text.count('<script>') == 1
    assert 'src="http' not in text


def test_optional_radio_links_are_grouped_under_sessions_and_url_encoded(tmp_path):
    _fixture(tmp_path)
    plot = tmp_path / "reports/radio_class_detail/plots/AP #1.png"
    plot.parent.mkdir(parents=True)
    plot.write_bytes(b"PNG")
    (plot.parent / "AP #2.png").write_bytes(b"PNG")
    _csv(tmp_path, "reports/radio_class_detail/radio_class_plot_index.csv", [
        dict(class_session_id="C1", radio_group_id="AP1/radio/1", path="plots/AP #1.png"),
        dict(class_session_id="C1", radio_group_id="AP1/radio/2", path="plots/AP #2.png")])
    path = overview.write_run_overview(tmp_path, project_name="Optional plots", run_id="run")
    text = path.read_text(encoding="utf-8")
    assert "AP%20%231.png" in text
    assert "<summary>Radio plots</summary>" not in text
    assert '<ul class="radio-plot-links"><li><a href="radio_class_detail/plots/AP%20%231.png">AP1/radio/1</a></li><li><a href="radio_class_detail/plots/AP%20%232.png">AP1/radio/2</a></li></ul>' in text
    assert '<img src="radio_class_detail/plots/' not in text
    overview.validate_overview_links(text, tmp_path)


def test_curated_images_reserve_layout_space_before_lazy_loading(tmp_path):
    _fixture(tmp_path)
    image = tmp_path / "reports/band_comparison/cohort_metrics.png"
    Image.new("RGB", (120, 60), "white").save(image)
    path = overview.write_run_overview(tmp_path, project_name="Image layout", run_id="run")
    assert 'width="120" height="60" loading="lazy"' in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("target", ["../../outside.png", "https://example.test/x", "#unknown", "missing.csv"])
def test_link_validation_rejects_escapes_remote_missing_and_bad_anchors(tmp_path, target):
    with pytest.raises(ValueError):
        overview.validate_overview_links(f'<a href="{target}">Bad</a>', tmp_path)


def test_only_manifest_may_be_pending_and_bad_index_does_not_leave_html(tmp_path):
    overview.write_run_overview(tmp_path, project_name="Staging", run_id="run", manifest_pending=True)
    other = tmp_path / "bad"
    other.mkdir()
    _fixture(other)
    _csv(other, "reports/radio_class_detail/radio_class_plot_index.csv", [
        dict(class_session_id="C1", radio_group_id="R1", path="../../../../escape.png")])
    with pytest.raises(ValueError, match="escapes"):
        overview.write_run_overview(other, project_name="Bad index", run_id="run")
    assert not (other / overview.OVERVIEW_PATH).exists()


def test_nonfinite_numbers_are_unavailable():
    for value in (None, "", "nan", "inf", "-inf"):
        assert overview._number(value, "%") == "Unavailable"
    assert overview._number(0, "%") == "0.00%"


def test_evidence_counts_use_manifest_records_without_original_input_files(tmp_path):
    context = {"input_set": {"files": [
        {"path": "E:/captures/not-present.pcapng"},
        {"path": "C:/archived-samples/not-present.csv"},
    ]}, "stages": [{"name": "extract"}, {"name": "summarize"}, {"name": "report"}]}
    (tmp_path / "manifest.json").write_text(json.dumps(context), encoding="utf-8")
    path = overview.write_run_overview(tmp_path, project_name="Archived inputs", run_id="run")
    evidence = path.read_text(encoding="utf-8").split('<section id="evidence">', 1)[1]
    assert 'Recorded input files</dt><dd>2</dd>' in evidence
    assert 'Recorded processing stages</dt><dd>3</dd>' in evidence


@pytest.mark.parametrize("interval,multiple,widths,expected", [
    (100, 10, [1024000], ["100 TU (0.1024 seconds)", "9.77 expected beacon opportunities", "10 beacon intervals (1.024 seconds)", "0.977 per second", "1.024-second bins"]),
    (200, 5, [512000, 1024000], ["200 TU (0.2048 seconds)", "4.88 expected beacon opportunities", "5 beacon intervals (1.024 seconds)", "0.512 seconds, 1.024 seconds"]),
])
def test_cadence_description_uses_recorded_interval_and_analysis_widths(interval, multiple, widths, expected):
    text = overview._cadence_description(dict(beacon_quality=dict(
        expected_beacon_interval_tu=interval, erm_intervals_per_capture_slot=multiple), bin_widths_us=widths))
    assert all(value in text for value in expected)
    assert 'at most one selected CU measurement per radio per bin' in text
    assert "not the AP's transmitted beacon rate" in text
    assert 'cadence availability describes how many expected per-BSSID capture opportunities were occupied' in text


def test_cadence_description_does_not_guess_missing_settings():
    text = overview._cadence_description({})
    assert 'diagnostic beacon interval was not recorded' in text
    assert 'CU analysis interval was not recorded' in text
    assert '9.77' not in text and '1.024' not in text
    text = overview._cadence_description(dict(beacon_quality=dict(expected_beacon_interval_tu=100), bin_width_us=2000000))
    assert 'ERM capture-slot expectation was not recorded' in text
    assert 'CU analysis uses 2-second bins' in text


def test_cdf_values_preserve_raw_boundaries_and_distinguish_reading_time_weights():
    rows = [dict(population_scope="overall", population_id="all", cu_raw=raw,
                 valid_radio_bin_count=count, valid_radio_duration_us=duration)
            for raw, count, duration in [(127, 1, 10), (128, 2, 1), (191, 3, 20), (192, 4, 2)]]
    assert overview._cu_cdf_values(rows, "overall", "all") == (10, 60, 10)
    by_time = overview._cu_cdf_values(rows, "overall", "all", weight="valid_radio_duration_us")
    assert by_time == pytest.approx((1000 / 33, 3100 / 33, 33))
    assert overview._cu_cdf_values(rows, "band", "5") == (None, None, None)


@pytest.mark.parametrize("raw,count,expected", [(255, 1, (0, 0, 1)), (0, 0, (None, None, 0)),
                                               (256, 1, (None, None, None)), (0, -1, (None, None, None)),
                                               (0, "", (None, None, None))])
def test_cdf_genuine_zero_unknown_and_invalid_marginals(raw, count, expected):
    rows = [dict(population_scope="overall", population_id="all", cu_raw=raw, valid_radio_bin_count=count)]
    assert overview._cu_cdf_values(rows, "overall", "all") == expected


def test_cdf_tables_separate_observed_and_possible_values_with_their_own_weights(tmp_path):
    import numpy as np
    from corrected_cu_bounds import cu_cdf_summary_rows
    _fixture(tmp_path)
    _csv(tmp_path, "reports/band_comparison/cu_distribution.csv", [
        dict(population_scope=scope, population_id=population, cu_raw=raw,
             valid_radio_bin_count=1, valid_radio_duration_us=duration)
        for scope, population in (("overall", "all"), ("band", "5"), ("ap_cohort", "5_on_without_6"))
        for raw, duration in ((0, 10), (255, 90))])
    histogram = np.zeros(256, dtype="int64")
    histogram[[0, 255]] = 1
    count_rows = [row for scope, population in (("overall", "all"), ("band", "5"))
                  for row in cu_cdf_summary_rows(histogram, 4, scope=scope, population=population, weighting="radio_bins")]
    count_rows += cu_cdf_summary_rows(histogram * 0, 3, scope="band", population="6", weighting="radio_bins")
    count_rows += cu_cdf_summary_rows(histogram * 0, 0, scope="band", population="2.4", weighting="radio_bins")
    _csv(tmp_path, "reports/cu_coverage_streak/cu_cdf_bounds.csv", count_rows)
    histogram[[0, 255]] = [10, 90]
    _csv(tmp_path, "reports/band_comparison/cu_cdf_bounds.csv", cu_cdf_summary_rows(
        histogram, 200, scope="ap_cohort", population="5_on_without_6", weighting="radio_duration_us"))
    for relative in ("cu_coverage_streak/plots/radio_bin_cu_cdf.png",
                     "cu_coverage_streak/plots/radio_bin_cu_cdf_by_band.png", "band_comparison/cu_cohort_cdf.png"):
        path = tmp_path / "reports" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (12, 6), "white").save(path)
    text = overview.write_run_overview(tmp_path, project_name="CDF", run_id="fixture").read_text(encoding="utf-8")
    assert 'Readings at or below 50% CU</dt><dd>50.00%</dd>' in text
    assert 'Possible readings at or below 50% CU</dt><dd>25.00%–75.00%</dd>' in text
    assert '5 GHz</td><td>50.00%</td><td>25.00%–75.00%</td><td>50.00%</td><td>25.00%–75.00%</td><td>2</td>' in text
    assert '6 GHz</td><td>Unavailable</td><td>0.00%–100.00%</td><td>Unavailable</td><td>0.00%–100.00%</td><td>0</td>' in text
    assert '5 GHz without 6 GHz</td><td>10.00%</td><td>5.00%–55.00%</td><td>10.00%</td><td>5.00%–55.00%</td>' in text
    band_cdf = text.split('<h4>CU cumulative distributions by band</h4>', 1)[1].split('</figure>', 1)[0]
    assert '<td>2.4 GHz</td>' not in band_cdf
    overview.validate_overview_links(text, tmp_path)


def test_section_figures_metrics_and_cdf_denominators(tmp_path):
    _fixture(tmp_path)
    # Tiny valid assets verify placement and links without comparing rendered PNG bytes.
    for _, _, directory, _, plots in overview.FAMILIES:
        for filename in plots:
            path = tmp_path / "reports" / directory / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (12, 6), "white").save(path)
    _csv(tmp_path, "reports/band_comparison/population_summary.csv", [dict(
        population_scope="overall", population_id="all", mean_cu_percent=10, cu_coverage_percent=50,
        valid_cu_duration_us=3_600_000_000, missing_cu_duration_us=0, no_beacon_duration_us=3_600_000_000,
        high_cu_percent_of_valid_time=25, very_high_cu_percent_of_valid_time=5,
        maximum_high_actual_observed_streak_us=2_500_000, maximum_high_possible_connected_streak_us=4_000_000,
        maximum_very_high_actual_observed_streak_us=1_000_000, maximum_very_high_possible_connected_streak_us=2_000_000,
        station_coverage_percent=99, mean_radio_station_count=20, maximum_radio_station_count=40)])
    _csv(tmp_path, "reports/band_comparison/cu_distribution.csv", [
        dict(population_scope=scope, population_id=population, cu_raw=raw,
             valid_radio_bin_count=count, valid_radio_duration_us=duration)
        for scope, population, raw, count, duration in [
            ("overall", "all", 127, 1, 10), ("overall", "all", 128, 2, 1),
            ("overall", "all", 191, 3, 20), ("overall", "all", 192, 4, 2),
            ("band", "5", 0, 1, 10), ("band", "5", 255, 1, 90),
            ("ap_cohort", "5_on_without_6", 0, 1, 10), ("ap_cohort", "5_on_without_6", 255, 1, 90)]])
    _csv(tmp_path, "reports/admission_capacity/admission_capacity_summary.csv", [dict(
        population_type="overall", population_id="all", mean_percent=70, valid_rows=10,
        valid_coverage=.5, zero_rows=2, minimum_percent=0, maximum_percent=100)])
    _csv(tmp_path, "reports/admission_capacity/admission_capacity_correlation.csv", [dict(
        population_type="overall", population_id="all", relationship="capacity_vs_cu",
        pearson_correlation=.25, spearman_correlation=.4, valid_pair_rows=8, valid_pair_coverage=.4)])
    _csv(tmp_path, "reports/affected_stations/population_exposure.csv", [dict(
        population_scope="overall", population_id="all", high_exposure_percent=50, very_high_exposure_percent=10,
        pair_coverage_percent=50, stale_pair_percent=5, paired_station_time_us=3_600_000_000,
        high_station_time_us=1_800_000_000, very_high_station_time_us=360_000_000)])
    _csv(tmp_path, "reports/affected_stations/classroom_peak_distribution.csv", [
        dict(band=band, threshold="high", available_peak_count=5, unavailable_peak_count=2,
             median=median, p90=15, maximum=20) for band, median in [("all", 10), ("5", 999)]])
    for name in ("station_time_exposure.png", "classroom_peak_cdf.png"):
        Image.new("RGB", (12, 6), "white").save(tmp_path / "reports/affected_stations" / name)
    _csv(tmp_path, "reports/beacon_quality/beacon_quality_associations.csv", [dict(
        capture_family="erm", ap_model="555", band="5", observed_ap_5ghz_radio_groups=2,
        observed_ap_6ghz_radio_groups=0, predictor="previous_window_cu_mean_percent",
        pearson_correlation=-.5, paired_windows=10, paired_window_fraction=.5)])
    path = overview.write_run_overview(tmp_path, project_name="Section review", run_id="run")
    text = path.read_text(encoding="utf-8")
    sections = {section: text.split(f'<section id="{section}">', 1)[1].split('</section>', 1)[0]
                for section in ("cu", "capacity", "stations", "bands", "radio", "quality", "sessions", "evidence")}
    cu = sections["cu"]
    assert re.findall(r'<figure><h3>(.*?)</h3>', cu) == [
        "Overall CU histogram", "Overall CU cumulative distribution", "Classroom maximum CU streaks"]
    assert "2.50 s" in cu and "25.00%" in cu and "50.00%" in cu
    assert "Readings at or below 50% CU</dt><dd>10.00%</dd>" in cu
    assert "Readings at or below 75% CU</dt><dd>60.00%</dd>" in cu
    assert "Possible readings at or below 50% CU</dt><dd>Unavailable</dd>" in cu
    assert "CU coverage determines" in cu
    assert "radios do not need to be above the threshold simultaneously" in cu
    assert "high periods from different radios are not joined" in cu
    streak_at = cu.index('<figure><h3>Classroom maximum CU streaks</h3>')
    assert "Longest observed high-CU streak" not in cu[:streak_at]
    assert "Longest observed high-CU streak" in cu[streak_at:]
    band_headings = re.findall(r'<figure><h4>(.*?)</h4>', sections["bands"])
    assert re.findall(r'<h3 class="figure-group-title">(.*?)</h3>', sections["bands"]) == [
        "Channel utilization", "Admission capacity", "Station counts and 6 GHz use"]
    assert band_headings == [overview._label(filename) for _, names in overview.BAND_FIGURE_GROUPS for filename in names]
    at = band_headings.index("CU cumulative distributions by band")
    assert band_headings[at + 1] == "CU distributions by infrastructure cohort"
    assert "Radio/class-session mean CU histogram" in band_headings
    assert "5 GHz</td><td>50.00%</td><td>Unavailable</td><td>50.00%</td><td>Unavailable</td><td>2" in sections["bands"]
    assert "5 GHz without 6 GHz</td><td>10.00%</td><td>Unavailable</td><td>10.00%</td><td>Unavailable" in sections["bands"]
    assert "not AP model" in sections["bands"]
    assert "The steps preserve" not in text
    assert re.findall(r'<figure><h3>(.*?)</h3>', sections["quality"])[:2] == ["CU capture slot availability", "Station capture slot availability"]
    assert "Previous-window mean CU" in sections["quality"] and "-0.500" in sections["quality"]
    assert "70.00%" in sections["capacity"] and "0.250" in sections["capacity"]
    assert "Readings advertising zero capacity" not in sections["capacity"]
    assert "Available radio time using assisted counts" not in sections["stations"]
    assert "Admission capacity versus associated station count" in sections["capacity"]
    for name in ("Admission-capacity histograms by band", "Admission-capacity cumulative distributions by band",
                 "Admission capacity versus CU by band", "Admission capacity versus station count by band",
                 "Classroom/bin station histograms by band"):
        assert name in band_headings
        assert f'<figure><h3>{name}</h3>' not in sections["capacity"]
    assert "Both range from −1 to +1" in sections["capacity"]
    assert "Spearman ρ (rho)" in sections["capacity"]
    assert "The summary reports mean CU, measurement coverage" not in cu
    assert "as well as the longest streak of high or very high channel utilization per radio in each class session." in cu
    exposure_at = cu.index('<div id="exposure" class="report-subsection">')
    assert exposure_at > streak_at
    assert "Association-hours with high CU observed</dt><dd>0.50</dd>" in cu[exposure_at:]
    assert "Station-time with high CU</dt>" not in cu[:exposure_at]
    assert '<figure><h4>Station-time exposure by band</h4>' in cu[exposure_at:]
    assert '<section id="exposure">' not in text
    assert '<a href="#exposure">Affected associations</a>' not in text.split('<nav', 1)[1].split('</nav>', 1)[0]
    assert 'href="#exposure"' in text.split('<section id="overview">', 1)[1].split('</section>', 1)[0]
    assert "Classroom/session peak associations with high CU" not in text
    assert "classroom_peak_cdf.png" not in text
    assert "classroom_peak_distribution.csv" not in text
    assert "Median peak (associations)" not in text
    assert "capture gaps a useful indicator" in sections["quality"]
    assert "before being used to estimate CU" in sections["quality"]
    assert all('class="facts section-metrics"' in section for section in sections.values())
    for section, guide in overview.SECTION_GUIDES.items():
        assert guide in sections[section]
    # Files follow their displayed figures without appearing in two file inventories.
    paths = overview._report_inventories(tmp_path)
    flattened = [path for members in paths.values() for path in members]
    assert len(flattened) == len(set(flattened))
    assert "reports/cu_coverage_streak/plots/radio_average_cu_distribution.png" in paths["bands"]
    assert "reports/cu_coverage_streak/plots/radio_cu_vs_coverage.png" in paths["quality"]
    assert "reports/class_sessions/enrollment_vs_peak_client_count.png" in paths["stations"]
    assert "reports/station_count/classroom_bin_station_histogram_by_band.png" in paths["bands"]
    assert "reports/admission_capacity/admission_capacity_station_hexbin_by_band.png" in paths["bands"]
    assert "reports/radio_class_detail/cu_station_hexbin_by_band.png" in paths["bands"]
    assert "reports/affected_stations/population_exposure.csv" in paths["cu"]
    assert "reports/affected_stations/station_time_exposure.png" in paths["cu"]
    assert "reports/affected_stations/classroom_peak_distribution.csv" not in flattened
    assert "reports/affected_stations/classroom_peak_cdf.png" not in flattened
    assert "CU versus station count by band" in band_headings
    assert "CU versus station count by band" not in sections["radio"]
    assert "percentage points per associated station" in sections["bands"]
    assert "different parts of a class session" in sections["bands"]
    overview.validate_overview_links(text, tmp_path)


def test_inventory_covers_shared_sources_moved_assets_and_session_plot_index(tmp_path):
    paths = ["reports/cu_coverage_streak/radio_class_report.csv", "reports/cu_coverage_streak/band_population_report.csv",
             "reports/cu_coverage_streak/plots/radio_bin_cu_histogram_by_band.png",
             "reports/radio_class_detail/radio_class_plot_index.csv", "reports/radio_class_detail/plots/R1.png",
             "reports/run_summary.txt", "reports/extra/new-output.csv", "reports/index.html"]
    for name in paths:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    inventory = overview._report_inventories(tmp_path)
    assert inventory["cu"] == [paths[0]]
    assert inventory["bands"] == sorted(paths[1:3])
    assert inventory["sessions"] == [paths[3]]
    assert inventory["evidence"] == sorted(paths[5:7])
    assert set(path for members in inventory.values() for path in members) == set(paths[:4] + paths[5:7])


def test_cohort_counts_include_classrooms_and_sampled_24ghz_without_adding_scopes(tmp_path):
    _fixture(tmp_path)
    _csv(tmp_path, "reports/band_comparison/population_summary.csv", [
        dict(population_scope=scope, population_id=population, status=status, ap_count=aps, radio_count=radios,
             classroom_count=rooms, class_session_count=sessions)
        for scope, population, status, aps, radios, rooms, sessions in [
            ("ap_cohort", "2.4_on_without_6", "observed", 2, 2, 1, 5),
            ("ap_cohort", "5_on_without_6", "observed", 3, 4, 2, 7),
            ("ap_cohort", "5_on_with_6", "observed", 1, 1, 1, 2),
            ("ap_cohort", "6_on_with_6", "observed", 1, 1, 1, 2),
            ("ap_cohort", "6_on_without_6", "no_matching_radios", 0, 0, 0, 0),
            ("classroom_cohort", "5_on_without_6", "observed", 9999, 9999, 9999, 9999)]])
    text = overview.write_run_overview(tmp_path, project_name="Counts", run_id="run").read_text(encoding="utf-8")
    counts = text.split('<h3>Comparison cohort sample sizes</h3>', 1)[1].split('</table>', 1)[0]
    assert '<th scope="col">Classrooms</th>' in counts
    assert '2.4 GHz · AP without 6 GHz</td><td>2</td><td>2</td><td>1</td><td>5</td>' in counts
    assert '5 GHz · AP without 6 GHz</td><td>3</td><td>4</td><td>2</td><td>7</td>' in counts
    assert counts.count('<td>1</td><td>1</td><td>1</td><td>2</td>') == 2
    assert '6 GHz · AP without 6 GHz' not in counts
    assert '9,999' not in counts


def test_model_and_classroom_band_tables_use_saved_band_rows_and_handle_missing_values(tmp_path):
    _fixture(tmp_path)
    base = dict(status="observed", ap_model="AP <635>", ap_observed_5_ghz_radio_count=1,
                ap_observed_6_ghz_radio_count=1, ap_count=1, radio_count=1, class_session_count=2,
                mean_cu_percent=12.5, cu_coverage_percent=80, mean_radio_station_count=0,
                station_coverage_percent=90, mean_admission_capacity_percent="")
    _csv(tmp_path, "reports/band_comparison/model_population_summary.csv", [
        dict(base, population_scope="band", population_id="5"),
        dict(base, population_scope="ap_cohort", population_id="5_on_with_6", mean_radio_station_count=9999),
        dict(base, population_scope="band", population_id="2.4", status="no_matching_radios")])
    _csv(tmp_path, "reports/band_comparison/classroom_band_summary.csv", [
        dict(base, classroom="Room <A>", band="5"),
        dict(base, classroom="Other room", band="5", mean_radio_station_count=9999)])
    text = overview.write_run_overview(tmp_path, project_name="Models", run_id="run").read_text(encoding="utf-8")
    models = text.split('<details class="model-summary">', 1)[1].split('</details>', 1)[0]
    assert '<summary>AP-model summary</summary>' in models
    assert models.count('AP &lt;635&gt;</td>') == 1
    assert '1 / 1</td><td>5 GHz</td>' in models
    assert '<th scope="col">Mean admission capacity</th>' in models
    assert 'time-weighted' not in models
    assert 'Mean CU and mean admission capacity both weight valid readings by duration' in models
    assert '12.50%</td><td>Unavailable</td><td>80.00%</td><td>0.00</td><td>Unavailable</td>' in models
    assert '9,999' not in models and '<td>2.4 GHz</td>' not in models
    room = text.split('<details class="room">', 1)[1].split('</details>', 1)[0]
    assert room.index('Band summary across class sessions') < room.index('Individual class sessions') < room.index('Local date')
    assert '<th scope="col">Mean admission capacity</th>' in room
    assert 'time-weighted' not in room
    assert '5 GHz</td><td>2</td><td>12.50%</td><td>Unavailable</td><td>80.00%</td><td>0.00</td><td>90.00%</td><td>Unavailable</td>' in room
    assert 'Other room' not in room and '9,999' not in room
    assert 'Full classroom-by-band metrics and coverage' in text
    overview.validate_overview_links(text, tmp_path)


@pytest.mark.parametrize("differences,expected,count", [
    ([(-10, 100), (2, 1000), (999, 0), (None, 100), ("inf", 100)], "-4.00", "2"),
    ([(None, 100), (1, 0)], "Unavailable", "0"),
])
def test_band_admission_means_and_equal_weighted_matched_difference(tmp_path, differences, expected, count):
    _fixture(tmp_path)
    _csv(tmp_path, "reports/band_comparison/population_summary.csv", [
        dict(population_scope=scope, population_id=population, mean_admission_capacity_percent=value)
        for scope, population, value in [("band", "5", 0), ("band", "6", 98.2),
                                         ("ap_cohort", "5_on_without_6", 89.04),
                                         ("ap_cohort", "5_on_with_6", 91.28), ("ap_cohort", "6_on_with_6", 98.2)]])
    _csv(tmp_path, "reports/band_comparison/matched_ap_radio_sessions.csv", [
        dict(matched_6_minus_5_cu_percentage_points=difference, common_valid_cu_duration_us=duration)
        for difference, duration in differences])
    path = tmp_path / "reports/band_comparison/matched_ap_cu.png"
    Image.new("RGB", (12, 6), "white").save(path)
    text = overview.write_run_overview(tmp_path, project_name="Matched", run_id="run").read_text(encoding="utf-8")
    bands = text.split('<section id="bands">', 1)[1].split('</section>', 1)[0]
    assert bands.count('<th scope="col">Mean admission capacity</th>') == 2
    assert '<td>0.00%</td>' in bands and '<td>98.20%</td>' in bands
    assert '<td>89.04%</td>' in bands and '<td>91.28%</td>' in bands
    assert f'Mean matched 6−5 GHz CU observed (percentage points)</dt><dd>{expected}</dd>' in bands
    assert f'Usable radio-pair/class-session comparisons</dt><dd>{count}</dd>' in bands
    assert "equal weight" in bands
    assert "Matched radio-pair/class-session CU values" in bands


def test_project_metadata_context_before_manifest_publication_and_dst(tmp_path):
    context = {"actual_selected_observation_range": {"start": "2026-11-01T07:30:00Z", "end": "2026-11-01T09:30:00Z"},
               "effective_configuration": {"input_adapter": {"schedule_timezone": "America/Denver"},
                                           "capture_count": 80, "bounded_working_set": {"selected_rows": 6511028}}}
    path = overview.write_run_overview(tmp_path, project_name="Context", run_id="run", manifest_pending=True, run_context=context)
    text = path.read_text(encoding="utf-8")
    assert "2026-11-01 01:30:00-06:00" in text
    assert "2026-11-01 02:30:00-07:00" in text
    assert "6,511,028" in text
    assert "First and last analyzed beacons" in text
    assert "not the full capture-file extent" not in text
    assert "Capture files" in text


def test_overview_band_means_enabled_ap_share_and_scope_results_are_distinct(tmp_path):
    _fixture(tmp_path)
    _csv(tmp_path, "reports/band_comparison/population_summary.csv", [
        dict(population_scope=scope, population_id=population, status=status,
             mean_cu_percent=cu, mean_radio_station_count=stations,
             cu_coverage_percent=40, station_coverage_percent=90)
        for scope, population, status, cu, stations in [
            ("overall", "all", "observed", 7, 14),
            ("band", "5", "observed", 11, 29),
            ("band", "6", "observed", 2, 8),
            ("band", "2.4", "no_matching_radios", "", ""),
            ("ap_cohort", "5_on_without_6", "observed", 88, 99),
        ]])
    _csv(tmp_path, "reports/band_comparison/station_adoption_summary.csv", [
        dict(population_scope=scope, population_id=population, **{"5_6_six_station_share_percent": share})
        for scope, population, share in [("overall", "all", 4.08), ("ap_cohort", "with_6", 23.88),
                                         ("classroom_cohort", "with_6", 44.44)]])
    path = overview.write_run_overview(tmp_path, project_name="Bands", run_id="run")
    text = path.read_text(encoding="utf-8")
    intro = text.split('<section id="overview">', 1)[1].split('</section>', 1)[0]
    cards = dict(re.findall(r'<article class="card"><h4>(.*?)</h4>(.*?)</article>', intro))
    assert len(cards) == 12
    assert '<p class="value">7.00%</p>' in cards["Mean CU overall"]
    assert '<p class="value">11.00%</p>' in cards["Mean CU 5 GHz"]
    assert '<p class="value">2.00%</p>' in cards["Mean CU 6 GHz"]
    assert "Mean CU 2.4 GHz" not in cards
    assert '<p class="value">14.00</p>' in cards["Mean stations per radio"]
    assert "5 GHz: 29.00" in cards["Mean stations per radio"]
    assert "6 GHz: 8.00" in cards["Mean stations per radio"]
    assert "2.4 GHz:" not in cards["Mean CU overall"]
    assert "88.00%" not in cards["Mean CU overall"]
    assert '<p class="value">4.08%</p>' in cards["6 GHz associated station-time"]
    assert '<p class="value">23.88%</p>' in cards["6 GHz associated station-time on 6 GHz APs"]
    assert '<p class="value">40.00%</p>' in cards["CU coverage"]
    assert '<p class="value">90.00%</p>' in cards["Station coverage"]
    assert intro.index('id="overview-scope"') < intro.index("Run ID:") < intro.index('id="overview-results"')
    titles = list(cards)
    index = titles.index("6 GHz-enabled APs")
    assert titles[index:index + 3] == ["6 GHz-enabled APs", "6 GHz associated station-time", "6 GHz associated station-time on 6 GHz APs"]
    assert "Eligible classroom/sessions" not in intro
    assert "100 station-minutes" in intro
    assert "200-student session has ten times" in intro
    assert "instructor" not in intro
    overview.validate_overview_links(text, tmp_path)


def test_schedule_hours_sum_sessions_not_wall_time_and_incomplete_stays_unknown():
    rows = [dict(class_session_id=sid, class_start_utc="2026-11-01T15:00:00Z", class_end_utc="2026-11-01T16:00:00Z")
            for sid in ("C1", "C2")]
    assert overview._scheduled_hours(rows) == 2
    assert overview._scheduled_hours([*rows, dict(class_session_id="C3")]) is None
    assert overview._scheduled_hours([]) is None
    with pytest.raises(ValueError, match="one row per class session"):
        overview._scheduled_hours([rows[0], rows[0]])


def test_session_mean_uses_exact_valid_radio_time_not_average_of_means():
    rows = [dict(class_session_id="C1", radio_group_id="R1", ap_id="AP1", cu_raw_duration_sum="255000000", valid_cu_duration_us="1000000"),
            dict(class_session_id="C1", radio_group_id="R2", ap_id="AP1", cu_raw_duration_sum="0", valid_cu_duration_us="9000000"),
            dict(class_session_id="C1", radio_group_id="R3", ap_id="AP2", cu_raw_duration_sum="0", valid_cu_duration_us="0")]
    stats = overview._session_radio_stats(rows)["C1"]
    assert stats == {"ap_count": 2, "radio_count": 3, "mean_radio_cu_percent": 10,
                     "mean_radio_cu_percent_lower_bound": None, "mean_radio_cu_percent_upper_bound": None}
    assert overview._session_radio_stats([rows[-1]])["C1"]["mean_radio_cu_percent"] is None
    rows[1]["cu_raw_duration_sum"] = ""
    assert overview._session_radio_stats(rows)["C1"]["mean_radio_cu_percent"] is None
    with pytest.raises(ValueError, match="unique radio/session"):
        overview._session_radio_stats([rows[0], rows[0]])


def test_session_bounds_pool_expected_time_including_entirely_missing_radios():
    rows = [dict(class_session_id="C1", radio_group_id=radio, ap_id="AP1", cu_raw_duration_sum=str(raw),
                 valid_cu_duration_us=str(valid), radio_duration_us=str(total))
            for radio, raw, valid, total in (("R1", 255000000, 1000000, 10000000),
                                           ("R2", 0, 9000000, 10000000), ("R3", 0, 0, 5000000))]
    row = overview._session_radio_stats(rows)["C1"]
    assert row["mean_radio_cu_percent"] == 10
    assert row["mean_radio_cu_percent_lower_bound"] == pytest.approx(4)
    assert row["mean_radio_cu_percent_upper_bound"] == pytest.approx(64)
    missing = overview._session_radio_stats([rows[2]])["C1"]
    assert missing["mean_radio_cu_percent"] is None
    assert missing["mean_radio_cu_percent_lower_bound"] == 0
    assert missing["mean_radio_cu_percent_upper_bound"] == 100
    rows[0]["radio_duration_us"] = "1"
    with pytest.raises(ValueError):
        overview._session_radio_stats([rows[0]])


def test_all_mean_displays_use_bounds_without_changing_observed_headlines(tmp_path):
    _fixture(tmp_path)
    (tmp_path / "reports/cu_coverage_streak").mkdir(parents=True, exist_ok=True)
    populations = [dict(population_scope=scope, population_id=population, status="observed",
        mean_cu_percent=mean, mean_cu_percent_lower_bound=lower, mean_cu_percent_upper_bound=upper,
        cu_coverage_percent=80, mean_radio_station_count=20, station_coverage_percent=90)
        for scope, population, mean, lower, upper in (("overall", "all", 10, 8, 28),
            ("band", "5", 12, 9, 34), ("ap_cohort", "5_on_without_6", 14, 10, 38))]
    _csv(tmp_path, "reports/band_comparison/population_summary.csv", populations)
    _csv(tmp_path, "reports/band_comparison/model_population_summary.csv", [{**populations[1], "mean_cu_percent": 15,
        "mean_cu_percent_lower_bound": 4, "mean_cu_percent_upper_bound": 57, "ap_model": "MODEL",
        "ap_observed_5_ghz_radio_count": 1, "ap_observed_6_ghz_radio_count": 0}])
    _csv(tmp_path, "reports/band_comparison/classroom_band_summary.csv", [{**populations[1], "classroom": "Room <A>",
        "band": "5", "mean_cu_percent": 13, "mean_cu_percent_lower_bound": 7, "mean_cu_percent_upper_bound": 44}])
    _csv(tmp_path, "reports/band_comparison/radio_class_comparison.csv", [dict(class_session_id="C1", radio_group_id="R1",
        ap_id="AP1", cu_raw_duration_sum=153000000, valid_cu_duration_us=1000000, radio_duration_us=2000000)])
    text = overview.write_run_overview(tmp_path, project_name="Bounds", run_id="fixture").read_text(encoding="utf-8")
    intro = text.split('<section id="overview">', 1)[1].split('</section>', 1)[0]
    assert '<p class="value">10.00%</p>' in intro
    assert 'Between 8.00% and 28.00% possible' in intro
    assert 'Between 9.00% and 34.00% possible' in intro
    cu = text.split('<section id="cu">', 1)[1].split('</section>', 1)[0]
    assert 'Mean CU observed</dt><dd>10.00%</dd>' in cu
    assert 'Mean CU possible</dt><dd>8.00%–28.00%</dd>' in cu
    bands = text.split('<section id="bands">', 1)[1].split('</section>', 1)[0]
    assert '12.00%</td><td>9.00%–34.00%</td>' in bands
    assert '14.00%</td><td>10.00%–38.00%</td>' in bands
    assert '15.00%</td><td>4.00%–57.00%</td>' in bands
    assert bands.count('<th scope="col">Mean CU observed</th>') == 3
    assert bands.count('<th scope="col">Mean CU possible</th>') == 3
    sessions = text.split('<section id="sessions">', 1)[1].split('</section>', 1)[0]
    assert '13.00%</td><td>7.00%–44.00%</td>' in sessions
    assert '60.00%</td><td>30.00%–80.00%</td>' in sessions
    assert '<th scope="col">Mean radio CU observed</th>' in sessions
    assert '<th scope="col">Mean radio CU possible</th>' in sessions
    assert overview._possible_range({"mean_lower_bound": 0, "mean_upper_bound": 0}, "mean", "%") == '0.00%–0.00%'
    assert overview._possible_range({"mean_lower_bound": 0}, "mean", "%") == 'Unavailable'
    assert overview._possible_description({"mean_lower_bound": 0, "mean_upper_bound": 100}, "mean") == 'Between 0.00% and 100.00% possible'
    assert not re.search(r'<(?:dd|td)>[^<]*(?:Observed |; min |; max )', text)
    overview.validate_overview_links(text, tmp_path)


def test_refined_hierarchy_enrollment_correlations_and_session_columns(tmp_path):
    _fixture(tmp_path)
    _csv(tmp_path, "reports/station_count/station_aggregate_report.csv", [dict(
        population_type="overall", population_id="all", average_peak_client_count_per_enrolled=1.49,
        eligible_session_count=369, stale_assisted_peak_session_count=29)])
    _csv(tmp_path, "reports/radio_class_detail/cu_station_correlation.csv", [dict(
        population_type="overall", population_id="all", pearson_correlation=.4315, spearman_correlation=.5942,
        valid_pair_rows=2198024, valid_pair_coverage=.7719, stale_assisted_pair_rows=102156)])
    _csv(tmp_path, "reports/band_comparison/radio_class_comparison.csv", [dict(
        class_session_id="C1", radio_group_id="R1", ap_id="AP1", cu_raw_duration_sum=128000000, valid_cu_duration_us=1000000)])
    path = overview.write_run_overview(tmp_path, project_name="Refined", run_id="run")
    text = path.read_text(encoding="utf-8")
    assert '<h2>Overview</h2>' in text
    assert "Start here" not in text
    assert "Open full-size figure" not in text
    positions = [text.index(f'<section id="{section}"') for section in ("overview", "cu", "stations", "radio", "capacity", "bands", "quality", "sessions", "evidence")]
    assert positions == sorted(positions)
    assert '<p class="value">1.49</p>' in text
    station_section = text.split('<section id="stations">', 1)[1].split('</section>', 1)[0]
    assert 'Mean peak stations per enrolled</dt><dd>1.49</dd>' in station_section
    assert station_section.index('Mean stations per radio</dt>') < station_section.index('Mean peak stations per enrolled</dt>')
    assert "0.431" in text or "0.432" in text
    assert "0.594" in text
    assert "2,198,024" in text
    assert "77.19%" in text
    assert "The density plots summarize observed pairs; no regression model is fitted." in text
    session_section = text.split('<section id="sessions">', 1)[1].split('</section>', 1)[0]
    assert "<th scope=\"col\">APs</th>" not in session_section
    assert "<th scope=\"col\">Radios</th>" not in session_section
    assert "<th scope=\"col\">Mean radio CU observed</th>" in text
    assert "50.20%" in text
    assert "physical sessions" not in text
    # Timezone belongs to the section/report, not every room disclosure.
    room_summary = text.split('<details class="room">')[1].split('</summary>')[0]
    assert "America/Denver" not in room_summary
    assert "1 AP · 1 radio" in room_summary
    overview.validate_overview_links(text, tmp_path)


@pytest.mark.parametrize("capacities,statuses,expected", [
    ([40, 40], ["available", "available"], "40 seats"),
    ([40, None], ["available", "missing"], "40 seats"),
    ([None, None], ["missing", "missing"], None),
    ([40, 50], ["available", "available"], "Seating capacity conflicting"),
    ([40, None], ["available", "conflict"], "Seating capacity conflicting"),
])
def test_room_heading_context_seating_and_stable_counts(capacities, statuses, expected):
    members = [dict(class_session_id=sid, room_capacity=capacity, capacity_status=status)
               for sid, capacity, status in zip(("C1", "C2"), capacities, statuses)]
    stats = {sid: dict(ap_count=2, radio_count=3) for sid in ("C1", "C2")}
    label = overview._room_heading_context(members, stats)
    assert label.startswith("2 APs · 3 radios")
    if expected is None:
        assert "seats" not in label and "Seating" not in label
    else:
        assert expected in label


def test_room_heading_does_not_hide_varying_or_missing_observed_counts():
    members = [dict(class_session_id=sid) for sid in ("C1", "C2", "C3")]
    label = overview._room_heading_context(members, {"C1": dict(ap_count=1, radio_count=2), "C2": dict(ap_count=2, radio_count=3)})
    assert "1–2 APs (varies by session)" in label
    assert "2–3 radios (varies by session)" in label
    assert "some unavailable" in label
