from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from PIL import Image
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import beacon_analysis as cli
import corrected_report_refresh as refresh
from corrected_cached_plots import refresh_saved_comparison_plots
from corrected_report_overview import validate_overview_links


def _source_run(tmp_path):
    source = tmp_path / "source-run"
    source.mkdir()
    oracle = Path(__file__).parent / "fixtures/corrected_semantics/expected/manifest.json"
    manifest = json.loads(oracle.read_text(encoding="utf-8"))
    manifest["project"] = {"name": "Saved Project", "slug": "saved-project"}
    (source / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    folder = source / "reports/band_comparison"
    folder.mkdir(parents=True)
    with (folder / "population_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["population_scope", "population_id", "mean_cu_percent"])
        writer.writeheader()
        writer.writerow(dict(population_scope="overall", population_id="all", mean_cu_percent=12.5))
    Image.new("RGB", (12, 8), "white").save(folder / "cohort_metrics.png")
    (source / "reports/index.html").write_text("old report", encoding="utf-8")
    exports = source / "exports/csv"
    exports.mkdir(parents=True)
    (exports / "radio_class_summary.csv").write_text("id,value\nradio,1\n", encoding="utf-8")
    (source / "selected_observations").mkdir()
    (source / "selected_observations/do-not-copy.parquet").write_bytes(b"unused by HTML refresh")
    return source


def _fingerprints(folder):
    return {path.relative_to(folder).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in folder.rglob("*") if path.is_file() and path.suffix != ".png"}


def test_report_cli_uses_saved_results_without_analysis_config_or_tshark(tmp_path, monkeypatch, capsys):
    source = _source_run(tmp_path)
    before = _fingerprints(source)
    root = tmp_path / "report-output"
    monkeypatch.setattr(cli, "DEFAULT_REPORT_ROOT", root)

    def forbidden(*args, **kwargs):
        raise AssertionError("report refresh must not start analysis, load config, resolve TShark or prompt")

    for name in ("_execute", "prepare_run", "_load_config", "_resolve_tshark"):
        monkeypatch.setattr(cli, name, forbidden)
    monkeypatch.setattr("builtins.input", forbidden)
    assert cli.main(["report", "--from-run", str(source)]) == 0
    paths = list(root.rglob("reports/index.html"))
    assert len(paths) == 1
    html = paths[0].read_text(encoding="utf-8")
    assert "Saved Project" in html and '<p class="value">12.50%</p>' in html
    bundle = paths[0].parent.parent
    validate_overview_links(html, bundle)
    assert _fingerprints(source) == before
    assert not (bundle / "selected_observations").exists()
    with Image.open(bundle / "reports/band_comparison/cohort_metrics.png") as image:
        assert image.size == (12, 8)
    assert (bundle / "manifest.json").read_bytes() == (source / "manifest.json").read_bytes()
    metadata = json.loads((bundle / "report_refresh.json").read_text(encoding="utf-8"))
    assert metadata["source_run_directory"] == str(source.resolve())
    assert metadata["source_manifest_sha256"] == before["manifest.json"]
    assert metadata["mode"] == "html_overview_from_saved_reports"
    assert len(metadata["renderer_sha256"]) == 64
    output = capsys.readouterr().out
    assert f"Open report: {paths[0]}" in output


def test_refresh_is_repeatable_and_copies_optional_radio_plots(tmp_path):
    source = _source_run(tmp_path)
    folder = source / "reports/radio_class_detail"
    (folder / "plots").mkdir(parents=True)
    Image.new("RGB", (16, 10), "white").save(folder / "plots/AP #1.png")
    (folder / "radio_class_plot_index.csv").write_text(
        "class_session_id,radio_group_id,path\nC1,R1,plots/AP #1.png\n", encoding="utf-8")
    before = _fingerprints(source)
    root = tmp_path / "reports"
    first = refresh.refresh_run_report(source, output_root=root)
    second = refresh.refresh_run_report(source, output_root=root)
    assert first != second and first.is_dir() and second.is_dir()
    for bundle in (first, second):
        assert (bundle / "reports/radio_class_detail/plots/AP #1.png").is_file()
        validate_overview_links((bundle / "reports/index.html").read_text(encoding="utf-8"), bundle)
    assert _fingerprints(source) == before
    # Copies are independent files; editing a refreshed table cannot change the source.
    (first / "exports/csv/radio_class_summary.csv").write_text("changed copy", encoding="utf-8")
    assert _fingerprints(source) == before
    assert not list(root.rglob(".report-*"))


def test_refresh_redraws_radio_class_mean_histogram_from_saved_csv(tmp_path):
    source = _source_run(tmp_path)
    folder = source / "reports/cu_coverage_streak"
    (folder / "plots").mkdir(parents=True)
    (folder / "radio_class_report.csv").write_text(
        "class_session_id,radio_group_id,ap_id,band,average_cu_percent\n"
        "C1,R1,AP1,5,10\nC1,R2,AP1,5,20\nC1,R3,AP2,6,5\nC1,R4,AP2,6,\n", encoding="utf-8")
    with (folder / "plots/radio_average_cu_distribution.png").open("wb") as stream:
        Image.new("RGB", (12, 8), "white").save(stream, format="PNG")
    before = _fingerprints(source)
    bundle = refresh.refresh_run_report(source, output_root=tmp_path / "reports")
    with Image.open(bundle / "reports/cu_coverage_streak/plots/radio_average_cu_distribution.png") as image:
        assert image.size == (2520, 1400)
    with Image.open(folder / "plots/radio_average_cu_distribution.png") as image:
        assert image.size == (12, 8)
    assert _fingerprints(source) == before
    record = json.loads((bundle / "report_refresh.json").read_text(encoding="utf-8"))
    assert record["regenerated_figures"][0]["source"] == "reports/cu_coverage_streak/radio_class_report.csv"
    text = (bundle / "reports/index.html").read_text(encoding="utf-8")
    assert "Radio/class-session mean CU histogram" in text
    validate_overview_links(text, bundle)


def test_refresh_redraws_station_band_histogram_from_saved_rollups(tmp_path):
    source = _source_run(tmp_path)
    folder = source / "reports/station_count"
    folder.mkdir()
    pq.write_table(pa.table({
        "rollup_type": ["band", "band", "band", "classroom", "ssid"],
        "rollup_id": ["5", "5", "6", "all", "eduroam"],
        "available": [True, False, True, True, True],
        "station_count": pa.array([10, None, 20, 30, 40], type=pa.uint64()),
        "stale_assisted": [False, False, True, False, False],
    }), folder / "station_bin_rollups.parquet")
    Image.new("RGB", (12, 8), "white").save(folder / "classroom_bin_station_histogram_by_band.png")
    before = _fingerprints(source)
    bundle = refresh.refresh_run_report(source, output_root=tmp_path / "reports")
    with Image.open(bundle / "reports/station_count/classroom_bin_station_histogram_by_band.png") as image:
        assert image.size == (3360, 1400)
    with Image.open(folder / "classroom_bin_station_histogram_by_band.png") as image:
        assert image.size == (12, 8)
    assert _fingerprints(source) == before
    record = json.loads((bundle / "report_refresh.json").read_text(encoding="utf-8"))
    assert record["regenerated_figures"][0]["source"] == "reports/station_count/station_bin_rollups.parquet"
    text = (bundle / "reports/index.html").read_text(encoding="utf-8")
    band_section = text.split('<section id="bands">', 1)[1].split('</section>', 1)[0]
    assert "Classroom/bin station histograms by band" in band_section
    validate_overview_links(text, bundle)


def test_saved_cu_distribution_regenerates_only_valid_band_panels(tmp_path, monkeypatch):
    from matplotlib.figure import Figure

    source = _source_run(tmp_path)
    path = source / "reports/band_comparison/cu_distribution.csv"
    path.write_text("population_scope,population_id,cu_raw,valid_radio_bin_count,valid_radio_duration_us\n"
                    "band,5,127,2,200\nband,5,255,1,100\nband,6,0,3,300\n", encoding="utf-8")
    figures = {}
    monkeypatch.setattr(Figure, "savefig", lambda figure, target, **kwargs: figures.setdefault(Path(target).name, figure))
    records = refresh_saved_comparison_plots(source)
    assert len(records) == 2
    assert len(figures["radio_bin_cu_histogram_by_band.png"].axes) == 2
    histogram = figures["radio_bin_cu_histogram_by_band.png"]
    assert [sum(patch.get_height() for patch in axis.patches) for axis in histogram.axes] == [3, 3]
    cdf = figures["radio_bin_cu_cdf_by_band.png"]
    assert len(cdf.axes) == 2
    assert [axis.lines[0].get_ydata()[-1] for axis in cdf.axes] == [1, 1]


def test_saved_cdf_refresh_uses_exact_counts_and_keeps_all_missing_band(tmp_path, monkeypatch):
    from matplotlib.figure import Figure
    source = _source_run(tmp_path)
    folder = source / "reports/band_comparison"
    (folder / "population_summary.csv").write_text(
        "population_scope,population_id,radio_bin_rows,radio_duration_us,valid_cu_duration_us\n"
        "overall,all,3,250,150\nband,5,2,150,150\nband,6,1,100,0\nband,2.4,0,0,0\n", encoding="utf-8")
    (folder / "cu_distribution.csv").write_text(
        "population_scope,population_id,cu_raw,valid_radio_bin_count,valid_radio_duration_us\n"
        "overall,all,0,1,100\noverall,all,255,1,50\n"
        "band,5,0,1,100\nband,5,255,1,50\n", encoding="utf-8")
    before = _fingerprints(source)
    figures = {}
    monkeypatch.setattr(Figure, "savefig", lambda figure, target, **kwargs: figures.setdefault(Path(target).name, figure))
    records = refresh_saved_comparison_plots(source)
    overall = figures["radio_bin_cu_cdf.png"].axes[0]
    assert overall.lines[0].get_ydata()[127] == .5
    assert overall.lines[1].get_ydata()[127] == pytest.approx(1 / 3)
    assert overall.lines[2].get_ydata()[127] == pytest.approx(2 / 3)
    cdf = figures["radio_bin_cu_cdf_by_band.png"]
    assert len(cdf.axes) == 2
    assert cdf.axes[1].get_title().startswith("6 GHz")
    assert not any(line.get_label() == "Observed" for line in cdf.axes[1].lines)
    assert len(figures["radio_bin_cu_histogram_by_band.png"].axes) == 1
    assert all("reports/band_comparison/population_summary.csv" in record["sources"] for record in records)
    assert all(set(record["cu_bounds_dependencies"]) == {"corrected_cu_bounds.py", "corrected_cu_plot_bounds.py"}
               for record in records)
    assert _fingerprints(source) == before


def test_refresh_passes_mean_bound_companions_to_histogram(tmp_path, monkeypatch):
    from matplotlib.figure import Figure
    source = _source_run(tmp_path)
    folder = source / "reports/cu_coverage_streak"
    folder.mkdir()
    (folder / "radio_class_report.csv").write_text(
        "class_session_id,radio_group_id,band,average_cu_percent,average_cu_percent_lower_bound,average_cu_percent_upper_bound\n"
        "C1,R5,5,50,25,75\nC1,R6,6,,0,100\n", encoding="utf-8")
    before = _fingerprints(source)
    figures = {}
    original = Figure.savefig
    def capture(figure, target, **kwargs):
        figures[Path(target).name] = figure
        return original(figure, target, **kwargs)
    monkeypatch.setattr(Figure, "savefig", capture)
    bundle = refresh.refresh_run_report(source, output_root=tmp_path / "reports")
    axis = figures["radio_average_cu_distribution.png"].axes[0]
    assert len(axis.collections) == 2
    metadata = json.loads((bundle / "report_refresh.json").read_text(encoding="utf-8"))
    record = metadata["regenerated_figures"][0]
    assert set(record["cu_bounds_dependencies"]) == {"corrected_cu_bounds.py", "corrected_cu_plot_bounds.py"}
    assert _fingerprints(source) == before


def test_saved_station_time_plot_uses_bound_companions_and_records_dependencies(tmp_path, monkeypatch):
    from matplotlib.figure import Figure
    from test_corrected_affected_station_reports import _accumulator
    source = _source_run(tmp_path)
    folder = source / "reports/affected_stations"
    folder.mkdir()
    _accumulator().tables()["population_exposure.csv"].to_csv(folder / "population_exposure.csv", index=False)
    before = _fingerprints(source)
    figures = {}
    monkeypatch.setattr(Figure, "savefig", lambda figure, target, **kwargs: figures.setdefault(Path(target).name, figure))
    records = refresh_saved_comparison_plots(source)
    figure = figures["station_time_exposure.png"]
    assert all(len(axis.collections) == 3 for axis in figure.axes)
    record = next(item for item in records if item["path"].endswith("station_time_exposure.png"))
    assert record["sources"] == ["reports/affected_stations/population_exposure.csv"]
    assert set(record["cu_bounds_dependencies"]) == {"corrected_cu_bounds.py", "corrected_cu_plot_bounds.py"}
    assert _fingerprints(source) == before


def test_saved_gap_cdf_redraw_preserves_slot_widths_and_count_weights(tmp_path, monkeypatch):
    from matplotlib.figure import Figure

    source = _source_run(tmp_path)
    path = source / "reports/beacon_quality/beacon_slot_gap_counts.csv"
    path.parent.mkdir(parents=True)
    path.write_text("capture_family,capture_slot_width_us,occupied_slot_gap,count\n"
                    "native,102400,1,3\nnative,102400,2,1\n", encoding="utf-8")
    figures = {}
    monkeypatch.setattr(Figure, "savefig", lambda figure, target, **kwargs: figures.setdefault(Path(target).name, figure))
    records = refresh_saved_comparison_plots(source)
    assert records[0]["sources"] == ["reports/beacon_quality/beacon_slot_gap_counts.csv"]
    axis = figures["beacon_slot_gap_cdf.png"].axes[0]
    assert axis.get_title() == "Beacon capture-slot gap CDF"
    assert axis.lines[0].get_xdata().tolist() == [.1024, .2048]
    assert axis.lines[0].get_ydata().tolist() == [.75, 1.0]


def test_saved_exposure_redraw_omits_empty_bands_and_preserves_percentages(tmp_path, monkeypatch):
    from matplotlib.figure import Figure

    source = _source_run(tmp_path)
    path = source / "reports/affected_stations/population_exposure.csv"
    path.parent.mkdir(parents=True)
    path.write_text("population_scope,population_id,radio_duration_us,high_exposure_percent,very_high_exposure_percent\n"
                    "overall,all,100,12,3\nband,2.4,0,,\nband,5,60,20,5\nband,6,40,0,0\n", encoding="utf-8")
    figures = {}
    monkeypatch.setattr(Figure, "savefig", lambda figure, target, **kwargs: figures.setdefault(Path(target).name, figure))
    records = refresh_saved_comparison_plots(source)
    assert len(records) == 1
    assert records[0]["sources"] == ["reports/affected_stations/population_exposure.csv"]
    figure = figures["station_time_exposure.png"]
    assert [tick.get_text() for tick in figure.axes[0].get_xticklabels()] == ["Overall", "5 GHz", "6 GHz"]
    assert [patch.get_height() for patch in figure.axes[0].patches] == [12, 20, 0]


@pytest.mark.parametrize("damage", ["missing_manifest", "invalid_manifest", "missing_reports", "incomplete_stage"])
def test_invalid_source_fails_cleanly_before_creating_report_output(tmp_path, capsys, damage):
    source = _source_run(tmp_path)
    manifest_path = source / "manifest.json"
    if damage == "missing_manifest":
        manifest_path.unlink()
    elif damage == "invalid_manifest":
        manifest_path.write_text("{}", encoding="utf-8")
    elif damage == "missing_reports":
        (source / "reports").rename(source / "old-reports")
    else:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["stages"][0]["status"] = "failed"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    root = tmp_path / "reports"
    assert cli.main(["report", "--from-run", str(source), "--output-root", str(root)]) == 1
    assert "Report refresh failed:" in capsys.readouterr().err
    assert not root.exists()


def test_refresh_refuses_output_inside_source_run(tmp_path):
    source = _source_run(tmp_path)
    before = _fingerprints(source)
    with pytest.raises(ValueError, match="outside the source"):
        refresh.refresh_run_report(source, output_root=source / "new-report")
    assert _fingerprints(source) == before


@pytest.mark.parametrize("failure", [ValueError("broken link"), KeyboardInterrupt()])
def test_generation_failure_or_interrupt_cleans_owned_staging_only(tmp_path, monkeypatch, failure):
    source = _source_run(tmp_path)
    before = _fingerprints(source)
    root = tmp_path / "reports"
    sentinel = root / "keep-me.txt"
    root.mkdir()
    sentinel.write_text("existing file", encoding="utf-8")

    def fail_generation(staging, **kwargs):
        (staging / "partial.html").write_text("partial", encoding="utf-8")
        raise failure

    monkeypatch.setattr(refresh, "write_run_overview", fail_generation)
    with pytest.raises(type(failure)):
        refresh.refresh_run_report(source, output_root=root)
    assert list(root.rglob("*.html")) == []
    assert list(root.rglob(".report-*")) == []
    assert sentinel.read_text(encoding="utf-8") == "existing file"
    assert _fingerprints(source) == before
