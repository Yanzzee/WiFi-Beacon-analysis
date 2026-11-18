from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import corrected_run as cr
import run_lifecycle as rl
import run_manifest as rm
from tshark_field_planner import TsharkExtractionPlan


def _identity() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bssid": ["aa:bb:cc:dd:ee:01"],
            "ssid": ["eduroam"],
            "radio_group_id": ["AP-01/radio/1"],
            "radio_number": [1],
            "ap_id": ["AP-01"],
            "ap_name": ["AP-01"],
            "ap_model": ["515"],
            "classroom": ["ROOM1"],
            "band": ["5"],
            "channels": [[36]],
            "frequencies_mhz": [[5180]],
            "ap_identity_method": ["vendor_ap_name"],
            "radio_grouping_method": ["band_channel_single_bssid"],
            "result_grain": ["ap_radio_group"],
            "mapping_status": ["resolved"],
            "mapping_diagnostics": [[]],
        }
    )


def _selected(*, bssid: str = "aa:bb:cc:dd:ee:01") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "class_session_id": ["C1"],
            "classroom": ["ROOM1"],
            "bin_number": [0],
            "bin_start": ["2026-09-23T15:00:00Z"],
            "bin_end": ["2026-09-23T15:00:01.024Z"],
            "bin_duration_us": [1_024_000],
            "width_status": ["complete"],
            "bssid": [bssid],
            "ssid": ["eduroam"],
            "radio_group_id": ["AP-01/radio/1"],
            "ap_id": ["AP-01"],
            "band": ["5"],
            "channel": [36],
            "observation_time": ["2026-09-23T15:00:00.100Z"],
            "observation_time_source": ["frame_time_epoch"],
            "tsf_us": [pd.NA],
            "tsf_origin": [pd.NaT],
            "tsf_derived_time": [pd.NaT],
            "timing_method": ["observation_time_fallback"],
            "tsf_status": ["insufficient_progression"],
            "unusable_tsf_reason": [
                "tsf_unusable_insufficient_progression"
            ],
            "source_capture": ["data/capture.pcapng"],
            "source_frame": ["1"],
            "duplicate_views_collapsed": [0],
            "skipped_intervals_before": [pd.NA],
            "qbss_cu_raw": [128],
            "qbss_station_count": [10],
            "qbss_admission_capacity": [1000],
            "is_selected": [True],
        }
    )


def _bins() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "class_session_id": ["C1"],
            "classroom": ["ROOM1"],
            "bin_number": [0],
            "bin_start": ["2026-09-23T15:00:00Z"],
            "bin_end": ["2026-09-23T15:00:01.024Z"],
            "bin_duration_us": [1_024_000],
        }
    )


def _inputs_and_plan(tmp_path: Path):
    capture = tmp_path / "capture.pcapng"
    schedule = tmp_path / "classes.csv"
    mapping = tmp_path / "mapping.csv"
    capture.write_bytes(b"capture")
    schedule.write_text("class,start,end\nC1,15:00,16:00\n", encoding="utf-8")
    mapping.write_text("Hostname,Building-Room\nAP-01,ROOM1\n", encoding="utf-8")
    inputs = [
        rl.AnalysisInput("capture", capture, "data/capture.pcapng"),
        rl.AnalysisInput("class_schedule", schedule, "data/classes.csv"),
        rl.AnalysisInput("ap_classroom_map", mapping, "data/mapping.csv"),
    ]
    capture_digest = rl.sha256_file(capture)
    plan = TsharkExtractionPlan(
        capture_profile="native_80211",
        fields=("frame.time_epoch", "wlan.bssid"),
        decode_args=(),
        display_filter="wlan.fc.type_subtype == 8",
        adapter_versions=(("beacon.adapter/extreme-native", 3),),
    )
    capture_plan = rm.build_capture_plan_record(
        source_path="data/capture.pcapng",
        source_bytes=capture.stat().st_size,
        source_sha256=capture_digest,
        capture_profile=plan.capture_profile,
        profile_detection="test",
        erm_type=None,
        tshark_version="4.6.6",
        plan=plan,
    )
    return inputs, capture_plan


def _clock():
    current = datetime(2026, 9, 23, 16, 0, tzinfo=timezone.utc)

    def tick() -> datetime:
        nonlocal current
        value = current
        current += timedelta(seconds=1)
        return value

    return tick


def _begin(tmp_path: Path) -> cr.CorrectedRun:
    inputs, capture_plan = _inputs_and_plan(tmp_path)
    return cr.begin_corrected_run(
        output_root=tmp_path / "output" / "runs",
        project_name="Fall 2026",
        analysis_inputs=inputs,
        requested_start="2026-09-23T15:00:00Z",
        requested_end="2026-09-23T16:00:00Z",
        effective_configuration={"bin_width_us": 1_024_000},
        source_precedence=["valid_frame_time_epoch"],
        storage={"timestamp": "timestamp_ns_utc"},
        capture_plans=[capture_plan],
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
        started_at=datetime(
            2026, 9, 23, 14, 30, 0, 123456, tzinfo=timezone.utc
        ),
        clock=_clock(),
    )


def test_runner_stages_core_artifacts_but_refuses_partial_publication(
    tmp_path: Path,
):
    run = _begin(tmp_path)
    cr.write_core_corrected_artifacts(
        run,
        identity_map=_identity(),
        selected_observations=_selected(),
    )

    assert (run.workspace.staging_directory / "project_identity_map.parquet").is_file()
    assert (run.workspace.staging_directory / "selected_observations").is_dir()
    assert not run.workspace.final_directory.exists()
    assert set(run.artifacts) == {
        cr.PROJECT_IDENTITY_ARTIFACT,
        cr.SELECTED_OBSERVATIONS_ARTIFACT,
    }
    with pytest.raises(cr.CorrectedRunError, match="incomplete"):
        cr.complete_corrected_run(run)
    assert run.workspace.staging_directory.is_dir()
    cr.abandon_corrected_run(run, reason="summary writers not invoked")


def test_runner_writes_summaries_then_publishes_all_four_tables(tmp_path: Path):
    run = _begin(tmp_path)
    cr.write_core_corrected_artifacts(
        run,
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    cr.write_corrected_summary_artifacts(
        run,
        class_bins=_bins(),
        identity_map=_identity(),
        selected_observations=_selected(),
    )

    final = cr.complete_corrected_run(run)

    assert final == run.workspace.final_directory
    assert final.is_dir()
    assert not run.workspace.staging_directory.exists()
    manifest = rm.load_run_manifest(final / "manifest.json")
    assert set(manifest["artifacts"]) == cr.REQUIRED_TABLE_ARTIFACTS
    overview = (final / "reports/index.html").read_text(encoding="utf-8")
    assert f'Recorded input files</dt><dd>{len(manifest["input_set"]["files"])}</dd>' in overview
    assert f'Recorded processing stages</dt><dd>{len(manifest["stages"])}</dd>' in overview
    assert [stage["name"] for stage in manifest["stages"]] == [
        "write_project_identity_map",
        "write_selected_observations",
        "write_corrected_summaries",
        "write_compact_csv_exports",
        "write_corrected_cu_report_package",
        "write_run_report_overview",
    ]
    assert manifest["actual_selected_observation_range"] == {
        "start": "2026-09-23T15:00:00.100000Z",
        "end": "2026-09-23T15:00:00.100000Z",
    }
    identity = pq.read_table(final / "project_identity_map.parquet")
    assert identity.column("run_id").to_pylist() == [run.workspace.run_id]
    radio = pq.read_table(final / "radio_class_summary.parquet")
    assert radio.column("valid_cu_duration_us").to_pylist() == [1_024_000]
    csv_root = final / "exports" / "csv"
    assert {path.name for path in csv_root.iterdir()} == {
        "project_identity_map.csv",
        "radio_class_summary.csv",
        "classroom_class_summary.csv",
    }
    report_root = final / "reports" / "cu_coverage_streak"
    assert {path.name for path in report_root.iterdir()} == {
        "radio_class_report.csv",
        "classroom_class_report.csv",
        "band_population_report.csv",
        "cu_cdf_bounds.csv",
        "methodology.txt",
        "plots",
    }
    assert {path.name for path in (final / "reports").iterdir()} == {
        "cu_coverage_streak",
        "run_summary.txt",
        "index.html",
    }
    assert {path.name for path in (report_root / "plots").iterdir()} == {
        "radio_average_cu_distribution.png",
        "radio_cu_vs_coverage.png",
        "radio_coverage_and_missingness.png",
        "radio_bin_cu_histogram.png",
        "radio_bin_cu_cdf.png",
        "classroom_streak_distributions.png",
        "radio_bin_cu_histogram_by_band.png",
        "radio_bin_cu_cdf_by_band.png",
    }
    radio_report = pd.read_csv(report_root / "radio_class_report.csv")
    assert radio_report.loc[0, "ap_id"] == "AP-01"
    assert str(radio_report.loc[0, "ap_model"]) == "515"
    assert radio_report.loc[0, "ap_band_radio_count"] == 1
    assert radio_report.loc[0, "capture_cohort"] == (
        "AP-515 / 1x 5 GHz radio"
    )
    assert not bool(radio_report.loc[0, "ap_has_6_ghz"])
    assert radio_report.loc[0, "class_start_utc"] == "2026-09-23T15:00:00Z"
    assert radio_report.loc[0, "cu_coverage_percent"] == 100.0
    band_population = pd.read_csv(
        report_root / "band_population_report.csv"
    ).set_index("population_id")
    assert band_population["run_id"].eq(run.workspace.run_id).all()
    assert band_population["schema_id"].eq(
        "beacon.schema/analysis-results/v1"
    ).all()
    assert band_population["metric_id"].eq(
        "beacon.metric/corrected-cu/v2"
    ).all()
    assert band_population.loc["all_5_ghz", "radio_count"] == 1
    assert band_population.loc[
        "all_5_ghz", "valid_cu_radio_time_coverage_percent"
    ] == 100.0
    assert band_population.loc[
        "5_ghz_on_6_ghz_ap", "status"
    ] == "no_matching_radios"
    assert pd.isna(
        band_population.loc[
            "5_ghz_on_6_ghz_ap",
            "valid_cu_radio_time_coverage_percent",
        ]
    )
    run_summary = (final / "reports" / "run_summary.txt").read_text(
        encoding="utf-8"
    )
    assert "Corrected beacon-analysis run summary" in run_summary
    assert "Radio/class rows: 1" in run_summary
    assert "Shared-room physical sessions merged: 0" in run_summary
    methodology = (report_root / "methodology.txt").read_text(encoding="utf-8")
    assert "../run_summary.txt" in methodology
    for filename in (
        "run_summary.txt",
        "radio_class_report.csv",
        "classroom_class_report.csv",
        "band_population_report.csv",
        "radio_bin_cu_cdf.png",
        "radio_bin_cu_cdf_by_band.png",
        "manifest.json",
    ):
        assert filename in methodology
    csv_identity = pd.read_csv(csv_root / "project_identity_map.csv")
    assert csv_identity["observed_channels"].tolist() == ["[36]"]
    assert csv_identity["mapping_diagnostics"].tolist() == ["[]"]
    review = pd.read_csv(csv_root / "radio_class_summary.csv").iloc[0]
    assert review.average_cu_raw == review.average_cu_raw_lower_bound == review.average_cu_raw_upper_bound == 128
    assert review.average_cu_ratio_lower_bound == pytest.approx(128 / 255)
    canonical_columns = pq.read_schema(final / "radio_class_summary.parquet").names
    assert "average_cu_raw_lower_bound" not in canonical_columns
    assert manifest["artifacts"]["project_identity_map"][
        "derived_exports"
    ] == [
        {
            "format": "csv",
            "path": (
                f"output/runs/{run.workspace.project_slug}/"
                f"{run.workspace.run_id}/exports/csv/project_identity_map.csv"
            ),
            "rows": 1,
        }
    ]
    radio_exports = manifest["artifacts"]["radio_class_summary"][
        "derived_exports"
    ]
    cdf_export = next(item for item in radio_exports if item.get("role") == "corrected_cu_cdf_bounds")
    cdf_values = pd.read_csv(final / "reports/cu_coverage_streak/cu_cdf_bounds.csv")
    assert cdf_export["rows"] == len(cdf_values)
    assert cdf_values.expected_weight.notna().all()
    assert cdf_values.cdf_observed_percent.equals(cdf_values.cdf_lower_bound_percent)
    assert cdf_values.cdf_observed_percent.equals(cdf_values.cdf_upper_bound_percent)
    overview = next(item for item in radio_exports if item.get("role") == "corrected_run_report_overview")
    assert overview["format"] == "html"
    assert overview["report_schema_id"] == "beacon.schema/run-report-overview/v1"
    assert (final / "reports/index.html").is_file()
    assert any(
        export.get("role") == "corrected_cu_band_population_report"
        and export["rows"] == 3
        for export in radio_exports
    )
    assert any(
        export.get("role") == "corrected_run_summary"
        and Path(export["path"]).name == "run_summary.txt"
        for export in radio_exports
    )
    assert {
        Path(export["path"]).name
        for export in radio_exports
        if export.get("format") == "png"
    }.issuperset(
        {
            "radio_bin_cu_histogram_by_band.png",
            "radio_bin_cu_cdf_by_band.png",
        }
    )


def test_runner_rejects_selected_bssid_missing_from_identity_and_cleans(
    tmp_path: Path,
):
    run = _begin(tmp_path)

    with pytest.raises(ValueError, match="absent from.*identity"):
        cr.write_core_corrected_artifacts(
            run,
            identity_map=_identity(),
            selected_observations=_selected(bssid="ff:ff:ff:ff:ff:ff"),
        )

    assert not run.workspace.staging_directory.exists()
    events = [
        json.loads(line)["event"]
        for line in run.workspace.log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["run_started", "run_failed"]


def test_runner_summary_validation_failure_is_logged_and_cleans(tmp_path: Path):
    run = _begin(tmp_path)
    cr.write_core_corrected_artifacts(
        run,
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    unknown_bin = _selected().copy()
    unknown_bin.loc[:, "bin_number"] = 99

    with pytest.raises(ValueError, match="unknown class bin"):
        cr.write_corrected_summary_artifacts(
            run,
            class_bins=_bins(),
            identity_map=_identity(),
            selected_observations=unknown_bin,
        )

    assert not run.workspace.staging_directory.exists()
    events = [
        json.loads(line)["event"]
        for line in run.workspace.log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["run_started", "run_failed"]


def test_csv_export_failure_prevents_publication_and_cleans(
    tmp_path: Path,
    monkeypatch,
):
    run = _begin(tmp_path)
    cr.write_core_corrected_artifacts(
        run,
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    cr.write_corrected_summary_artifacts(
        run,
        class_bins=_bins(),
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    monkeypatch.setattr(
        cr,
        "write_compact_csv_exports",
        lambda _directory: (_ for _ in ()).throw(
            OSError("synthetic CSV export failure")
        ),
    )

    with pytest.raises(OSError, match="synthetic CSV export failure"):
        cr.complete_corrected_run(run)

    assert not run.workspace.final_directory.exists()
    assert not run.workspace.staging_directory.exists()
    events = [
        json.loads(line)["event"]
        for line in run.workspace.log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["run_started", "run_failed"]


@pytest.mark.parametrize("writer", ["write_corrected_cu_report_package", "write_run_overview"])
def test_report_failure_prevents_publication_and_cleans(tmp_path: Path, monkeypatch, writer):
    run = _begin(tmp_path)
    cr.write_core_corrected_artifacts(
        run,
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    cr.write_corrected_summary_artifacts(
        run,
        class_bins=_bins(),
        identity_map=_identity(),
        selected_observations=_selected(),
    )
    monkeypatch.setattr(
        cr,
        writer,
        lambda _directory, **_kwargs: (_ for _ in ()).throw(
            OSError("synthetic report failure")
        ),
    )

    with pytest.raises(OSError, match="synthetic report failure"):
        cr.complete_corrected_run(run)

    assert not run.workspace.final_directory.exists()
    assert not run.workspace.staging_directory.exists()
    events = [
        json.loads(line)["event"]
        for line in run.workspace.log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert events == ["run_started", "run_failed"]
