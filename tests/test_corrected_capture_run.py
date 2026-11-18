from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import process_pcapng as pp
from corrected_capture_run import (
    _stage_class_windows,
    run_corrected_capture_project,
)
from process_pcapng import (
    CAPTURE_PROFILE_NATIVE,
    CaptureExtractionResult,
    CaptureProfile,
    build_tshark_extraction_plan,
)
from run_manifest import load_run_manifest


def _clock():
    current = datetime(2026, 9, 26, 16, 0, tzinfo=timezone.utc)

    def tick() -> datetime:
        nonlocal current
        value = current
        current += timedelta(milliseconds=1)
        return value

    return tick


def _beacons(
    *,
    ap_name_field: str,
    ap_name: str,
    bssid: str,
    classroom_start: str,
    source_frame_start: int,
) -> pd.DataFrame:
    start = pd.Timestamp(classroom_start)
    return pd.DataFrame(
        {
            "wlan.bssid": [bssid, bssid],
            "wlan.ta": [bssid, bssid],
            "wlan.ssid": ["eduroam", "eduroam"],
            ap_name_field: [ap_name, ap_name],
            "aruba_erm_time": (
                [start + pd.Timedelta(milliseconds=100), start + pd.Timedelta(milliseconds=1124)]
                if ap_name_field == "wlan.vs.aruba.ap_name"
                else [pd.NaT, pd.NaT]
            ),
            "frame_time_epoch": [
                start + pd.Timedelta(milliseconds=110),
                start + pd.Timedelta(milliseconds=1134),
            ],
            "tsf_us": [1_000_000, 2_024_000],
            "beacon_interval_tu": [100, 100],
            "source_frame": [source_frame_start, source_frame_start + 1],
            "frequency_mhz": [5180, 5180],
            "wlan.ds.current_channel": [36, 36],
            "wlan.qbss.cu": [128, 192],
            "wlan.qbss.scount": [10, 12],
            "wlan.qbss.adc": [1000, 900],
        }
    )


def _fixture_inputs(tmp_path: Path):
    capture_a = tmp_path / "a-aruba.pcapng"
    capture_b = tmp_path / "b-aruba-overlap.pcapng"
    capture_c = tmp_path / "c-extreme.pcapng"
    for position, capture in enumerate((capture_a, capture_b, capture_c)):
        capture.write_bytes(b"capture" + bytes([position]))

    configured = pd.DataFrame(
        {
            "Hostname": ["ARUBA-AP-01", "EXTREME-AP-01"],
            "Building-Room": ["ROOM1", "ROOM2"],
            "Model": ["515", "AP4000"],
        }
    )
    sessions = pd.DataFrame(
        {
            "class_session_id": ["C1", "C2"],
            "classroom": ["ROOM1", "ROOM2"],
            "class_start": [
                "2026-09-26T15:00:00Z",
                "2026-09-26T15:10:00Z",
            ],
            "class_end": [
                "2026-09-26T15:00:02.048Z",
                "2026-09-26T15:10:02.048Z",
            ],
            "bin_width_us": [1_024_000, 1_024_000],
        }
    )
    mapping_path = tmp_path / "Sample_APs.csv"
    schedule_path = tmp_path / "classes.csv"
    configured.to_csv(mapping_path, index=False)
    sessions.to_csv(schedule_path, index=False)

    aruba = _beacons(
        ap_name_field="wlan.vs.aruba.ap_name",
        ap_name="ARUBA-AP-01",
        bssid="aa:bb:cc:dd:ee:01",
        classroom_start="2026-09-26T15:00:00Z",
        source_frame_start=1,
    )
    extreme = _beacons(
        ap_name_field="wlan.vs.extreme.ap_name",
        ap_name="EXTREME-AP-01",
        bssid="aa:bb:cc:dd:ee:02",
        classroom_start="2026-09-26T15:10:00Z",
        source_frame_start=20,
    )
    frames = {
        capture_a.name: aruba,
        capture_b.name: aruba.copy(deep=True),
        capture_c.name: extreme,
    }
    return (
        [capture_c, capture_b, capture_a],
        configured,
        sessions,
        mapping_path,
        schedule_path,
        frames,
    )


def _extractor(frames: dict[str, pd.DataFrame]):
    def extract(path: Path, *, source_rank: int) -> CaptureExtractionResult:
        if "aruba" in path.name:
            profile = CaptureProfile(
                name="aruba_erm_type0",
                is_aruba_erm=True,
                erm_type="aruba_erm_type0",
                detection_method="test",
                plausible_beacon_count=2,
            )
        else:
            profile = CaptureProfile(
                name=CAPTURE_PROFILE_NATIVE,
                is_aruba_erm=False,
                erm_type=None,
                detection_method="test",
                plausible_beacon_count=2,
            )
        return CaptureExtractionResult(
            observations=frames[path.name].copy(deep=True),
            profile=profile,
            extraction_plan=build_tshark_extraction_plan(profile),
            tshark_version="4.6.6",
        )

    return extract


def test_class_window_staging_isolates_rooms_with_duplicate_source_indexes(
    tmp_path: Path,
):
    observations = pd.DataFrame(
        {
            "classroom": ["ROOM1", "ROOM2"],
            "aruba_erm_time": [
                "2026-09-26T15:00:00.100Z",
                "2026-09-26T15:10:00.100Z",
            ],
            "frame_time_epoch": [pd.NaT, pd.NaT],
            "bssid": ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"],
        },
        index=[0, 0],
    )
    sessions = {
        "ROOM1": pd.DataFrame(
            {
                "class_session_id": ["C1"],
                "class_start": [pd.Timestamp("2026-09-26T15:00:00Z")],
                "class_end": [pd.Timestamp("2026-09-26T15:00:01Z")],
            }
        ),
        "ROOM2": pd.DataFrame(
            {
                "class_session_id": ["C2"],
                "class_start": [pd.Timestamp("2026-09-26T15:10:00Z")],
                "class_end": [pd.Timestamp("2026-09-26T15:10:01Z")],
            }
        ),
    }
    directories = {
        "C1": tmp_path / "session-00000",
        "C2": tmp_path / "session-00001",
    }

    rows, parts = _stage_class_windows(
        observations,
        sessions_by_classroom=sessions,
        session_directories=directories,
        source_rank=0,
    )

    assert (rows, parts) == (2, 2)
    room1 = pd.read_parquet(directories["C1"] / "capture-00000.parquet")
    room2 = pd.read_parquet(directories["C2"] / "capture-00000.parquet")
    assert room1["classroom"].tolist() == ["ROOM1"]
    assert room2["classroom"].tolist() == ["ROOM2"]


def test_capture_project_publishes_five_artifacts_from_supported_adapters(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    (
        captures,
        configured,
        sessions,
        mapping_path,
        schedule_path,
        frames,
    ) = _fixture_inputs(tmp_path)

    final = run_corrected_capture_project(
        project_name="Fall 2026",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        class_schedule_path=schedule_path,
        output_root=tmp_path / "output" / "runs",
        capture_workers=4,
        extractor=_extractor(frames),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
        started_at=datetime(
            2026, 9, 26, 14, 30, 0, 123456, tzinfo=timezone.utc
        ),
        clock=_clock(),
    )

    progress = capsys.readouterr().out
    assert "[capture 1/3] a-aruba.pcapng: extracting" in progress
    assert "[capture 2/3] b-aruba-overlap.pcapng: extracting" in progress
    assert "[capture 3/3] c-extreme.pcapng: extracting" in progress
    assert "supported=2, retained=2, class_window=2" in progress
    assert "outside_project=0, missing_ap_name=0" in progress
    assert "[session 1/2] C1: staged=4, selected=2" in progress

    assert {path.name for path in final.iterdir()} == {
        "manifest.json",
        "project_identity_map.parquet",
        "selected_observations",
        "radio_class_summary.parquet",
            "classroom_class_summary.parquet",
            "exports",
            "reports",
        }
    selected = pd.read_parquet(final / "selected_observations")
    assert len(selected) == 4
    selected_files = list(
        (final / "selected_observations").rglob("*.parquet")
    )
    assert len(selected_files) == 1
    assert pq.ParquetFile(selected_files[0]).num_row_groups == 2
    assert set(selected["class_session_id"]) == {"C1", "C2"}
    assert set(selected["bssid"]) == {
        "aa:bb:cc:dd:ee:01",
        "aa:bb:cc:dd:ee:02",
    }
    aruba_selected = selected.loc[selected["class_session_id"].eq("C1")]
    assert aruba_selected["source_capture"].str.endswith(
        "a-aruba.pcapng"
    ).all()
    assert aruba_selected["duplicate_views_collapsed"].eq(0).all()

    identity = pq.read_table(final / "project_identity_map.parquet").to_pandas()
    assert len(identity) == 2
    assert identity["radio_group_id"].notna().all()
    assert set(identity["ap_model"]) == {"515", "AP4000"}
    report = pd.read_csv(
        final / "reports" / "cu_coverage_streak" / "radio_class_report.csv"
    )
    assert set(report["capture_cohort"]) == {
        "AP-515 / 1x 5 GHz radio",
        "AP4000 / 1x 5 GHz radio",
    }
    classroom = pq.read_table(
        final / "classroom_class_summary.parquet"
    ).to_pandas()
    assert set(classroom["class_session_id"]) == {"C1", "C2"}
    assert classroom["radio_count_total"].eq(1).all()
    radio_detail_root = final / "reports" / "radio_class_detail"
    assert {path.name for path in radio_detail_root.iterdir()} == {
        "radio_bin_detail.parquet",
        "cu_station_correlation.csv",
        "cu_station_hexbin.png",
        "cu_station_hexbin_by_band.png",
        "methodology.txt",
    }
    radio_detail = pq.read_table(
        radio_detail_root / "radio_bin_detail.parquet"
    ).to_pandas()
    assert len(radio_detail) == 4
    assert radio_detail["cu_state"].eq("valid").all()
    assert radio_detail["source_ssid"].eq("eduroam").all()
    assert radio_detail["source_selection_method"].eq("primary_ssid").all()
    assert radio_detail["station_available"].all()
    assert not radio_detail["station_stale_assisted"].any()
    assert set(radio_detail["station_count"].astype("UInt64")) == {10, 12}
    assert radio_detail["admission_state"].eq("valid").all()
    assert radio_detail["admission_source_matches_cu"].all()
    assert set(radio_detail["qbss_admission_capacity"]) == {900, 1000}
    admission_root = final / "reports" / "admission_capacity"
    assert len(list(admission_root.glob("*.png"))) == 8
    admission_summary = pd.read_csv(admission_root / "admission_capacity_summary.csv")
    assert admission_summary.loc[admission_summary["population_type"].eq("overall"), "valid_rows"].iloc[0] == 4
    station_root = final / "reports" / "station_count"
    assert {path.name for path in station_root.iterdir()} == {
        "station_bin_rollups.parquet",
        "station_class_report.csv",
        "station_aggregate_report.csv",
        "classroom_bin_station_histogram.png",
        "classroom_bin_station_histogram_by_band.png",
        "methodology.txt",
    }
    class_session_root = final / "reports" / "class_sessions"
    assert {path.name for path in class_session_root.iterdir()} == {
        "class_session_report.csv",
        "peak_client_count_per_enrolled_histogram.png",
        "enrollment_vs_peak_client_count.png",
        "methodology.txt",
    }
    assert len(pd.read_csv(class_session_root / "class_session_report.csv")) == 2
    station_rollups = pq.read_table(
        station_root / "station_bin_rollups.parquet"
    ).to_pandas()
    assert len(station_rollups) == 16
    assert set(station_rollups["rollup_type"]) == {
        "radio",
        "ssid",
        "band",
        "classroom",
    }
    assert station_rollups["available"].all()
    assert not station_rollups["stale_assisted"].any()
    station_class = pd.read_csv(station_root / "station_class_report.csv")
    assert len(station_class) == 8
    assert station_class["combined_enrollment"].isna().all()
    assert station_class["peak_client_count_per_enrolled"].isna().all()
    station_aggregate = pd.read_csv(
        station_root / "station_aggregate_report.csv"
    )
    assert len(station_aggregate) == 3
    assert station_aggregate["eligible_session_count"].eq(0).all()
    assert station_aggregate[
        "average_peak_client_count_per_enrolled"
    ].isna().all()
    cu_report_root = final / "reports" / "cu_coverage_streak"
    cu_methodology = (cu_report_root / "methodology.txt").read_text(
        encoding="utf-8"
    )
    assert "../station_count/station_bin_rollups.parquet" in cu_methodology
    assert "../radio_class_detail/radio_bin_detail.parquet" in cu_methodology
    run_summary = (final / "reports" / "run_summary.txt").read_text(
        encoding="utf-8"
    )
    assert "Station rollup rows: 16" in run_summary
    assert "Radio/bin detail rows: 4" in run_summary
    assert "Radio/class detail plots: disabled" in run_summary
    assert "Largest per-session station rollup part: 8" in run_summary
    assert "Enrollment-weighted peak client count per enrolled" in run_summary
    assert "- Overall: unavailable (eligible sessions 0" in run_summary
    assert "Radio/bin CU and station-count relationship" in run_summary
    assert "Band comparison and longitudinal metrics" in run_summary
    assert "Overall mean stations/radio, available time:" in run_summary
    assert "5 GHz on APs without 6 GHz: mean CU" in run_summary
    assert "reports/band_comparison/population_summary.csv" in run_summary
    assert "Potentially affected station associations" in run_summary
    assert "valid pairs 4/4 (100.0%)" in run_summary

    manifest = load_run_manifest(final / "manifest.json")
    quality_root = final / "reports" / "beacon_quality"
    assert len(list(quality_root.iterdir())) == 9
    quality_streams = pd.read_csv(quality_root / "beacon_cadence_sessions.csv")
    assert len(quality_streams) == 2
    assert set(quality_streams["capture_family"]) == {"erm", "native"}
    assert quality_streams["observation_role"].eq("unknown").all()
    assert quality_streams.loc[quality_streams["capture_family"].eq("erm"), "effective_slot_extra_views"].iloc[0] == 2
    assert manifest["effective_configuration"]["beacon_quality"]["expected_beacon_interval_tu"] == 100
    assert manifest["effective_configuration"]["beacon_quality"]["cu_imputation"] is False
    quality_exports = [item for item in manifest["artifacts"]["selected_observations"]["derived_exports"]
                       if item.get("role", "").startswith("exploratory_beacon_quality_")]
    assert len(quality_exports) == 9
    assert all(item["report_metric_id"] == "beacon.metric/beacon-capture-quality/v2" for item in quality_exports)
    band_root = final / "reports" / "band_comparison"
    assert len(list(band_root.iterdir())) == 19
    band_population = pd.read_csv(band_root / "population_summary.csv")
    overall = band_population.loc[band_population.population_scope.eq("overall")].iloc[0]
    assert overall.radio_count == 2
    assert overall.valid_cu_duration_us == 4_096_000
    assert overall.mean_cu_percent == pytest.approx(160 / 255 * 100)
    band_exports = [item for item in manifest["artifacts"]["radio_class_summary"]["derived_exports"]
                    if item.get("role", "").startswith("corrected_band_comparison_")]
    assert len(band_exports) == 19
    assert manifest["effective_configuration"]["band_comparison"]["cross_project_comparison"] == "manual"
    affected_root = final / "reports" / "affected_stations"
    assert len(list(affected_root.iterdir())) == 7
    affected_population = pd.read_csv(affected_root / "population_exposure.csv").iloc[0]
    assert affected_population.radio_session_count == 2
    assert affected_population.pair_duration_us == 4_096_000
    affected_exports = [item for item in manifest["artifacts"]["radio_class_summary"]["derived_exports"]
                        if item.get("role", "").startswith("corrected_affected_stations_")]
    assert len(affected_exports) == 7
    assert all(item["report_metric_id"] == "beacon.metric/potentially-affected-stations/v1" for item in affected_exports)
    assert manifest["effective_configuration"]["affected_stations"]["cu_filled"] is False
    assert len(manifest["capture_plans"]) == 3
    assert [stage["name"] for stage in manifest["stages"]] == [
        "extract_and_normalize_captures",
        "resolve_project_identity",
        "select_class_observations",
        "write_project_identity_map",
        "write_selected_observations",
        "write_corrected_summaries",
        "write_corrected_radio_bin_report_package",
        "write_corrected_station_report_package",
        "write_corrected_class_session_report_package",
        "write_corrected_beacon_quality_report_package",
        "write_corrected_band_comparison_package",
        "write_corrected_affected_station_package",
        "write_compact_csv_exports",
        "write_corrected_cu_report_package",
        "write_run_report_overview",
    ]
    assert manifest["effective_configuration"]["capture_workers"] == 4
    assert manifest["effective_configuration"]["cu_radio_bin"] == {
        "metric_id": "beacon.metric/corrected-radio-bin/v1",
        "grain": "class_session_radio_bin",
        "primary_ssid": "eduroam",
        "fallback": "earliest_valid_same_radio_member_bssid",
        "radio_class_plots": False,
        "display_timezone": "UTC",
        "cu_station_relationship": {
            "grain": "same_radio_same_bin_valid_pairs",
            "stale_assisted_station_values": True,
            "statistics": ["pearson", "spearman"],
            "inferential_statistics": False,
        },
    }
    assert manifest["effective_configuration"][
        "capture_source_scope_filter"
    ] == {
        "enabled": False,
        "basis": "configured_ap_hostname",
        "vendor_fields": [
            "wlan.vs.aruba.ap_name",
            "wlan.vs.extreme.ap_name",
        ],
        "discard_counts_available": True,
    }
    measurements = manifest["effective_configuration"][
        "capture_stage_measurements"
    ]
    assert [item["source_path"] for item in measurements] == [
        item["path"]
        for item in manifest["input_set"]["files"]
        if item["role"] == "capture"
    ]
    assert all(item["extraction_seconds"] >= 0 for item in measurements)
    assert manifest["effective_configuration"]["adapter_dispatch"][0][
        "source_path"
    ].endswith("a-aruba.pcapng")
    assert manifest["effective_configuration"]["project_scope_counts"] == {
        "input_rows": 6,
        "retained_rows": 6,
        "missing_ap_name_rows": 0,
        "outside_project_scope_rows": 0,
        "other_classroom_rows": 0,
    }
    assert manifest["effective_configuration"]["radio_resolution_counts"] == {
        "input_rows": 6,
        "input_bssids": 2,
        "resolved_rows": 6,
        "resolved_bssids": 2,
        "fallback_rows": 0,
        "fallback_bssids": 0,
        "conflict_bssids": 0,
    }
    assert manifest["effective_configuration"]["bounded_working_set"] == {
        "identity": "distinct_project_identity_evidence",
        "class_observations": "hidden_parquet_per_class_session",
        "selection": "one_complete_class_session_at_a_time",
        "temporary_files_published": False,
        "identity_evidence_rows": 2,
        "class_window_rows": 6,
        "class_window_part_files": 3,
        "maximum_session_input_rows": 4,
        "selected_rows": 4,
        "station_rollup_rows": 16,
        "radio_bin_detail_rows": 4,
        "maximum_session_radio_bin_detail_rows": 2,
        "maximum_session_station_rollup_rows": 8,
        "post_duplicate_handling_rows": 6,
        "tsf_duplicate_views_collapsed": 0,
    }
    assert manifest["actual_selected_observation_range"] == {
        "start": "2026-09-26T15:00:00.100000Z",
        "end": "2026-09-26T15:10:01.134000Z",
    }
    selected_exports = manifest["artifacts"]["selected_observations"][
        "derived_exports"
    ]
    assert any(
        export["role"] == "corrected_station_bin_rollups"
        and export["rows"] == 16
        for export in selected_exports
    )
    radio_exports = manifest["artifacts"]["radio_class_summary"][
        "derived_exports"
    ]
    assert any(
        export["role"] == "corrected_radio_bin_detail"
        and export["rows"] == 4
        for export in radio_exports
    )
    assert any(
        export["role"] == "corrected_cu_station_correlation"
        and export["rows"] == 2
        for export in radio_exports
    )
    admission_exports = [
        export for export in radio_exports
        if export.get("role", "").startswith("corrected_admission_capacity")
    ]
    assert len(admission_exports) == 11
    assert all((admission_root / Path(export["path"]).name).is_file() for export in admission_exports)
    assert all("/reports/admission_capacity/" in export["path"] for export in admission_exports)
    assert manifest["effective_configuration"]["admission_capacity"]["stale_extension"] is False
    classroom_exports = manifest["artifacts"][
        "classroom_class_summary"
    ]["derived_exports"]
    assert any(
        export["role"] == "corrected_station_class_report"
        and export["rows"] == 8
        for export in classroom_exports
    )
    assert any(
        export["role"] == "corrected_station_aggregate_report"
        and export["rows"] == 3
        for export in classroom_exports
    )
    assert any(
        export["role"] == "corrected_class_session_report"
        and export["rows"] == 2
        for export in classroom_exports
    )


def test_capture_project_counts_rows_filtered_per_capture_before_resolution(
    tmp_path: Path,
):
    (
        captures,
        _configured,
        _sessions,
        mapping_path,
        schedule_path,
        frames,
    ) = _fixture_inputs(tmp_path)
    capture_name = "a-aruba.pcapng"
    outside = frames[capture_name].iloc[[0]].copy(deep=True)
    outside["wlan.vs.aruba.ap_name"] = "OUTSIDE-AP"
    outside["wlan.bssid"] = "aa:bb:cc:dd:ee:99"
    outside["wlan.ta"] = "aa:bb:cc:dd:ee:99"
    outside["source_frame"] = 999
    frames[capture_name] = pd.concat(
        [frames[capture_name], outside],
        ignore_index=True,
    )

    final = run_corrected_capture_project(
        project_name="Early project scope",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        class_schedule_path=schedule_path,
        output_root=tmp_path / "output" / "runs",
        extractor=_extractor(frames),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
    )

    manifest = load_run_manifest(final / "manifest.json")
    assert manifest["effective_configuration"]["project_scope_counts"] == {
        "input_rows": 7,
        "retained_rows": 6,
        "missing_ap_name_rows": 0,
        "outside_project_scope_rows": 1,
        "other_classroom_rows": 0,
    }
    identity = pq.read_table(final / "project_identity_map.parquet").to_pandas()
    assert "aa:bb:cc:dd:ee:99" not in set(identity["bssid"])


def test_capture_project_logs_known_extraction_failure_without_publication(
    tmp_path: Path,
):
    (
        captures,
        configured,
        sessions,
        mapping_path,
        schedule_path,
        _frames,
    ) = _fixture_inputs(tmp_path)

    def fail_extraction(path: Path, *, source_rank: int):
        raise RuntimeError(f"synthetic extraction failure: {path.name}")

    with pytest.raises(RuntimeError, match="synthetic extraction failure"):
        run_corrected_capture_project(
            project_name="Fall 2026",
            capture_paths=captures,
            ap_mapping_path=mapping_path,
            class_schedule_path=schedule_path,
            output_root=tmp_path / "output" / "runs",
            extractor=fail_extraction,
            code_provenance={
                "source": "git",
                "revision": "a" * 40,
                "dirty": False,
            },
            started_at=datetime(
                2026, 9, 26, 14, 30, 0, 123456, tzinfo=timezone.utc
            ),
            clock=_clock(),
        )

    project = tmp_path / "output" / "runs" / "fall-2026"
    assert not list(project.glob(".*.staging"))
    assert not [path for path in project.iterdir() if path.is_dir() and path.name != "_logs"]
    log_path = next((project / "_logs").glob("*.jsonl"))
    events = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["event"] for event in events] == ["run_started", "run_failed"]
    assert events[-1]["understood"] is True


def test_capture_project_logs_interrupt_and_removes_owned_staging(
    tmp_path: Path,
):
    (
        captures,
        _configured,
        _sessions,
        mapping_path,
        schedule_path,
        _frames,
    ) = _fixture_inputs(tmp_path)

    def interrupt_extraction(path: Path, *, source_rank: int):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_corrected_capture_project(
            project_name="Interrupted run",
            capture_paths=captures,
            ap_mapping_path=mapping_path,
            class_schedule_path=schedule_path,
            output_root=tmp_path / "output" / "runs",
            extractor=interrupt_extraction,
            code_provenance={
                "source": "git",
                "revision": "a" * 40,
                "dirty": False,
            },
        )

    project = tmp_path / "output" / "runs" / "interrupted-run"
    assert not list(project.glob(".*.staging"))
    log_path = next((project / "_logs").glob("*.jsonl"))
    events = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["event"] for event in events] == [
        "run_started",
        "run_failed",
    ]
    assert events[-1]["reason"] == "KeyboardInterrupt: run interrupted"
    assert events[-1]["understood"] is True


def test_capture_project_publishes_no_beacon_summaries_for_empty_classes(
    tmp_path: Path,
):
    (
        captures,
        _configured,
        _sessions,
        mapping_path,
        schedule_path,
        frames,
    ) = _fixture_inputs(tmp_path)
    empty_sessions = pd.DataFrame(
        {
            "class_session_id": ["EMPTY-C1"],
            "classroom": ["ROOM1"],
            "class_start": ["2026-09-27T15:00:00Z"],
            "class_end": ["2026-09-27T15:00:02.048Z"],
            "bin_width_us": [1_024_000],
        }
    )
    empty_sessions.to_csv(schedule_path, index=False)

    final = run_corrected_capture_project(
        project_name="Empty class",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        class_schedule_path=schedule_path,
        output_root=tmp_path / "output" / "runs",
        extractor=_extractor(frames),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
    )

    selected = pd.read_parquet(final / "selected_observations")
    assert selected.empty
    radio = pq.read_table(final / "radio_class_summary.parquet").to_pandas()
    room1_radio = radio.loc[radio["classroom"].eq("ROOM1")]
    assert room1_radio["no_beacon_duration_us"].eq(2_048_000).all()
    manifest = load_run_manifest(final / "manifest.json")
    assert manifest["actual_selected_observation_range"] == {
        "start": None,
        "end": None,
    }
    assert manifest["effective_configuration"]["bounded_working_set"][
        "class_window_rows"
    ] == 0
    assert manifest["effective_configuration"]["bounded_working_set"][
        "maximum_session_input_rows"
    ] == 0
    station_rollups = pq.read_table(
        final / "reports" / "station_count" / "station_bin_rollups.parquet"
    ).to_pandas()
    assert len(station_rollups) == 2
    assert station_rollups["rollup_type"].eq("classroom").all()
    assert not station_rollups["available"].any()
    assert station_rollups["station_count"].isna().all()
    station_class = pd.read_csv(
        final / "reports" / "station_count" / "station_class_report.csv"
    )
    assert len(station_class) == 1
    assert pd.isna(station_class.loc[0, "peak_client_count"])


@pytest.mark.parametrize("whole_room_missing", [False, True])
@pytest.mark.filterwarnings("error::FutureWarning")
def test_missing_configured_ap_warns_before_sessions_and_publishes_unavailable_room(tmp_path, capsys, monkeypatch, whole_room_missing):
    import corrected_capture_run as runner

    captures, configured, _, mapping_path, schedule_path, frames = _fixture_inputs(tmp_path)
    if whole_room_missing:
        configured.loc[configured.Hostname.eq("EXTREME-AP-01"), "Hostname"] = "UNOBSERVED-AP"
    else:
        configured.loc[len(configured)] = ["UNOBSERVED-AP", "ROOM1", "515"]
    configured.to_csv(mapping_path, index=False)
    missing_room = "ROOM2" if whole_room_missing else "ROOM1"
    original_select = runner._select_staged_sessions

    def verify_early_warning(run, *args, **kwargs):
        details = run.effective_configuration["project_observation_warnings"]
        assert details["configured_aps_without_observations"] == [{"ap_id": "UNOBSERVED-AP", "classroom": missing_room}]
        assert details["scheduled_classrooms_without_radios"] == (["ROOM2"] if whole_room_missing else [])
        events = [json.loads(line) for line in run.workspace.log_path.read_text().splitlines()]
        assert events[-1]["event"] == "project_observation_warning"
        return original_select(run, *args, **kwargs)

    monkeypatch.setattr(runner, "_select_staged_sessions", verify_early_warning)
    final = run_corrected_capture_project(
        project_name="Unobserved classroom", capture_paths=captures,
        ap_mapping_path=mapping_path, class_schedule_path=schedule_path,
        output_root=tmp_path / "output/runs", extractor=_extractor(frames),
        code_provenance={"source": "git", "revision": "a" * 40, "dirty": False},
    )
    stderr = capsys.readouterr().err
    assert f"UNOBSERVED-AP ({missing_room})" in stderr
    assert ("scheduled classrooms have no resolved radios: ROOM2" in stderr) == whole_room_missing
    manifest = load_run_manifest(final / "manifest.json")
    assert next(stage for stage in manifest["stages"] if stage["name"] == "resolve_project_identity")["warning_count"] == (2 if whole_room_missing else 1)
    assert manifest["effective_configuration"]["project_observation_warnings"]["scheduled_classrooms_without_radios"] == (["ROOM2"] if whole_room_missing else [])
    classroom = pd.read_csv(final / "reports/cu_coverage_streak/classroom_class_report.csv").set_index("classroom")
    if whole_room_missing:
        assert classroom.loc["ROOM2", "radio_count_total"] == 0
        assert classroom.loc["ROOM2", "radios_with_valid_cu_count"] == 0
        assert pd.isna(classroom.loc["ROOM2", "radio_cu_coverage_min_percent"])
        assert pd.isna(classroom.loc["ROOM2", "high_actual_observed_max_seconds"])
    else:
        assert classroom.loc["ROOM2", "radio_count_total"] == 1
        assert classroom.loc["ROOM2", "radio_cu_coverage_min_percent"] == 100
    assert classroom.loc["ROOM1", "radio_count_total"] == 1
    assert classroom.loc["ROOM1", "radio_cu_coverage_min_percent"] == 100
    sessions = pd.read_csv(final / "reports/class_sessions/class_session_report.csv").set_index("classroom")
    assert pd.isna(sessions.loc["ROOM2", "peak_client_count"]) == whole_room_missing
    text = (final / "reports/index.html").read_text(encoding="utf-8")
    assert text.count("Measurements unavailable: no radios were resolved") == int(whole_room_missing)


def test_current_sample_class_shape_runs_with_derived_session_ids(
    tmp_path: Path,
):
    (
        captures,
        _configured,
        _sessions,
        mapping_path,
        schedule_path,
        frames,
    ) = _fixture_inputs(tmp_path)
    current_classes = pd.DataFrame(
        {
            "location": ["ROOM1", "ROOM2"],
            "course": ["COURSE-A", "COURSE-B"],
            "start_time": [
                pd.Timestamp("2026-09-26T09:00:00-06:00"),
                pd.Timestamp("2026-09-26T09:10:00-06:00"),
            ],
            "end_time": [
                pd.Timestamp("2026-09-26T09:00:02.048-06:00"),
                pd.Timestamp("2026-09-26T09:10:02.048-06:00"),
            ],
            "enrolled": [20, 30],
            "capacity": [25, 35],
        }
    )
    schedule_path = schedule_path.with_suffix(".parquet")
    current_classes.to_parquet(schedule_path)

    final = run_corrected_capture_project(
        project_name="Current sample inputs",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        class_schedule_path=schedule_path,
        output_root=tmp_path / "output" / "runs",
        extractor=_extractor(frames),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
    )

    selected = pd.read_parquet(final / "selected_observations")
    assert set(selected["class_session_id"]) == {
        "ROOM1|2026-09-26T15:00:00Z|2026-09-26T15:00:02.048000Z",
        "ROOM2|2026-09-26T15:10:00Z|2026-09-26T15:10:02.048000Z",
    }
    station_class = pd.read_csv(
        final / "reports" / "station_count" / "station_class_report.csv"
    )
    classroom_station = station_class.loc[
        station_class["rollup_type"].eq("classroom")
    ].set_index("classroom")
    assert classroom_station.loc["ROOM1", "combined_enrollment"] == 20
    assert classroom_station.loc[
        "ROOM1", "peak_client_count_per_enrolled"
    ] == pytest.approx(0.6)
    assert classroom_station.loc["ROOM2", "combined_enrollment"] == 30
    assert classroom_station.loc[
        "ROOM2", "peak_client_count_per_enrolled"
    ] == pytest.approx(0.4)
    station_aggregate = pd.read_csv(
        final
        / "reports"
        / "station_count"
        / "station_aggregate_report.csv"
    ).set_index(["population_type", "population_id"])
    for population in (
        ("overall", "all"),
        ("ssid", "eduroam"),
        ("band", "5"),
    ):
        result = station_aggregate.loc[population]
        assert result["eligible_session_count"] == 2
        assert result["peak_client_count_sum"] == 24
        assert result["combined_enrollment_sum"] == 50
        assert result["average_peak_client_count_per_enrolled"] == pytest.approx(
            0.48
        )
    run_summary = (final / "reports" / "run_summary.txt").read_text(
        encoding="utf-8"
    )
    assert "- Overall: 0.480 (eligible sessions 2; peak sum 24; " in run_summary
    assert "- SSID eduroam: 0.480" in run_summary
    assert "- Band 5 GHz: 0.480" in run_summary
    manifest = load_run_manifest(final / "manifest.json")
    assert manifest["effective_configuration"]["input_adapter"] == {
        "ap_mapping_profile": "normalized_ap_mapping",
        "class_schedule_profile": "current_sample_classes",
        "class_session_id_method": "classroom_utc_start_utc_end",
    }


@pytest.mark.parametrize("one_location,unobserved_ap", [(False, False), (True, False), (False, True)])
def test_raw_sample_csvs_are_hashed_and_expanded_inside_capture_run(
    tmp_path: Path,
    one_location: bool,
    unobserved_ap: bool,
):
    (
        captures,
        configured,
        _sessions,
        mapping_path,
        schedule_path,
        _frames,
    ) = _fixture_inputs(tmp_path)
    if unobserved_ap:
        configured.loc[configured.Hostname.eq("EXTREME-AP-01"), "Hostname"] = "UNOBSERVED-AP"
        configured.to_csv(mapping_path, index=False)
    classrooms_path = tmp_path / "Sample_classrooms.csv"
    pd.DataFrame(
        {
            "Location": ["ROOM1"] if one_location else ["ROOM1", "ROOM2"],
            "Capacity": [30] if one_location else [30, 40],
        }
    ).to_csv(classrooms_path, index=False)
    schedule_rows = []
    for room, course, period in (
        ("ROOM1", "COURSE-A", "9:00a - 9:01a"),
        ("ROOM2", "COURSE-B", "10:00a - 10:01a"),
    ):
        schedule_rows.append(
            {
                "Location": room,
                "Course": course,
                "Sec": "1",
                "LQ": "A",
                "H": "B",
                "S": "C",
                "Cr Hr": "1",
                "Sec Type": "Regular",
                "Class Period": period,
                "Days": "M",
                "Instructor": "Instructor",
                "Status": "Active",
                "Assign To": room,
                "Begin Date": "2026-09-01",
                "End Date": "2026-12-01",
                "sample": "1",
                "Size": 30,
                "Enrl": 20,
            }
        )
    pd.DataFrame(schedule_rows).to_csv(schedule_path, index=False)
    frames = {
        captures[2].name: _beacons(
            ap_name_field="wlan.vs.aruba.ap_name",
            ap_name="ARUBA-AP-01",
            bssid="aa:bb:cc:dd:ee:01",
            classroom_start="2026-09-28T15:00:00Z",
            source_frame_start=1,
        ),
        captures[1].name: _beacons(
            ap_name_field="wlan.vs.aruba.ap_name",
            ap_name="ARUBA-AP-01",
            bssid="aa:bb:cc:dd:ee:01",
            classroom_start="2026-09-28T15:00:00Z",
            source_frame_start=1,
        ),
        captures[0].name: _beacons(
            ap_name_field="wlan.vs.extreme.ap_name",
            ap_name="EXTREME-AP-01",
            bssid="aa:bb:cc:dd:ee:02",
            classroom_start="2026-09-28T16:00:00Z",
            source_frame_start=20,
        ),
    }

    final = run_corrected_capture_project(
        project_name="Raw CSV inputs",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        classroom_path=classrooms_path,
        class_schedule_path=schedule_path,
        schedule_timezone="America/Denver",
        schedule_start_date="2026-09-28",
        schedule_end_date="2026-09-28",
        output_root=tmp_path / "output" / "runs",
        extractor=_extractor(frames),
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
    )

    manifest = load_run_manifest(final / "manifest.json")
    roles = {item["role"] for item in manifest["input_set"]["files"]}
    assert roles == {
        "capture",
        "class_schedule",
        "classroom_metadata",
        "ap_classroom_map",
    }
    assert manifest["effective_configuration"]["input_adapter"] == {
        "ap_mapping_profile": "normalized_ap_mapping",
        "class_schedule_profile": "raw_sample_schedule_csv",
        "class_session_id_method": "classroom_utc_start_utc_end",
        "schedule_timezone": "America/Denver",
        "schedule_start_date": "2026-09-28",
        "schedule_end_date": "2026-09-28",
        "location_scope": {
            "source": "Sample_classrooms.csv:Location",
            "locations": ["ROOM1"] if one_location else ["ROOM1", "ROOM2"],
            "ap_mapping_and_schedule": "only_listed_locations",
        },
    }
    selected = pd.read_parquet(final / "selected_observations")
    expected_sessions = {"ROOM1|2026-09-28T15:00:00Z|2026-09-28T15:01:00Z"}
    if not one_location and not unobserved_ap:
        expected_sessions.add("ROOM2|2026-09-28T16:00:00Z|2026-09-28T16:01:00Z")
    assert set(selected["class_session_id"]) == expected_sessions
    identity = pd.read_parquet(final / "project_identity_map.parquet")
    assert set(identity.classroom) == ({"ROOM1"} if one_location or unobserved_ap else {"ROOM1", "ROOM2"})
    comparison = pd.read_csv(final / "reports/band_comparison/deployment_summary.csv")
    assert comparison.configured_ap_count.iloc[0] == (1 if one_location else 2)
    classrooms = pd.read_parquet(final / "classroom_class_summary.parquet").set_index("classroom")
    assert set(classrooms.index) == ({"ROOM1"} if one_location else {"ROOM1", "ROOM2"})
    if unobserved_ap:
        assert classrooms.loc["ROOM2", "radio_count_total"] == 0
        assert pd.isna(classrooms.loc["ROOM2", "high_actual_observed_max_us"])
    assert not list(tmp_path.glob("sample_*.parquet"))


def test_native_clock_anomaly_is_logged_published_and_does_not_retime(tmp_path, capsys):
    captures, _, sessions, mapping, schedule, frames = _fixture_inputs(tmp_path)
    sessions.loc[sessions.class_session_id.eq("C2"), "class_end"] = "2026-09-26T15:10:10.240Z"
    sessions.to_csv(schedule, index=False)
    frames["c-extreme.pcapng"]["tsf_us"] = [1_000_000, 5_000_000]
    final = run_corrected_capture_project(project_name="Native clock anomaly", capture_paths=captures,
        ap_mapping_path=mapping, class_schedule_path=schedule, output_root=tmp_path / "output/runs",
        extractor=_extractor(frames), code_provenance={"source": "git", "revision": "a" * 40, "dirty": False})
    assert "native TSF anomaly" in capsys.readouterr().err
    selected = pd.read_parquet(final / "selected_observations")
    native = selected.loc[selected.class_session_id.eq("C2")]
    assert native.bin_number.tolist() == [0, 1]
    assert native.timing_method.eq("observation_time").all()
    assert native.tsf_derived_time.isna().all()
    assert native.tsf_status.eq("discontinuous").all()
    manifest = load_run_manifest(final / "manifest.json")
    assert manifest["metric_id"] == "beacon.metric/corrected-cu/v2"
    assert manifest["effective_configuration"]["beacon_quality"]["tsf_anomaly_stream_count"] == 1
    assert next(stage for stage in manifest["stages"] if stage["name"] == "select_class_observations")["warning_count"] == 1
    events = [json.loads(line) for line in (final.parent / "_logs" / (final.name + ".jsonl")).read_text().splitlines()]
    anomaly = next(event for event in events if event["event"] == "native_tsf_anomaly")
    assert anomaly["streams"][0]["tsf_anomaly_source_frame"] == "21"
    cadence = pd.read_csv(final / "reports/beacon_quality/beacon_cadence_sessions.csv")
    assert cadence.loc[cadence.capture_family.eq("native"), "native_timing_excluded_rows"].eq(0).all()
    assert "Native TSF clock anomalies were detected in 1" in (final / "reports/index.html").read_text(encoding="utf-8")


@pytest.mark.requires_external_tools
def test_owner_aruba_and_extreme_captures_publish_one_corrected_run(
    tmp_path: Path,
    monkeypatch,
):
    captures = [
        Path(
            "data/captures_test/"
            "Aruba_ERM_Capture_635_type0_2026-09-14.pcapng"
        ),
        Path(
            "data/captures_test/"
            "Beacons_LVES_fr5220_00004_20260905195309.pcapng"
        ),
    ]
    missing = [str(path) for path in captures if not path.is_file()]
    if missing:
        pytest.skip("Owner captures are not available: " + ", ".join(missing))
    portable_tshark = Path(
        r"C:\temp\WiresharkPortable64\App\Wireshark\tshark.exe"
    )
    if portable_tshark.is_file():
        monkeypatch.setattr(pp, "TSHARK_EXECUTABLE", str(portable_tshark))

    configured = pd.DataFrame(
        {
            "Hostname": [
                "P-HBLL-3410-AP-02-3330D",
                "P-LVES-B105R22S30-AP-01-WN",
            ],
            "Building-Room": ["ROOM1", "ROOM2"],
        }
    )
    sessions = pd.DataFrame(
        {
            "class_session_id": ["ARUBA-C1", "EXTREME-C1"],
            "classroom": ["ROOM1", "ROOM2"],
            "class_start": [
                "2026-09-14T21:41:12Z",
                "2026-09-06T01:53:10Z",
            ],
            "class_end": [
                "2026-09-14T21:41:14.048Z",
                "2026-09-06T01:53:12.048Z",
            ],
            "bin_width_us": [1_024_000, 1_024_000],
        }
    )
    mapping_path = tmp_path / "Sample_APs.csv"
    schedule_path = tmp_path / "classes.csv"
    configured.to_csv(mapping_path, index=False)
    sessions.to_csv(schedule_path, index=False)

    final = run_corrected_capture_project(
        project_name="Owner capture integration",
        capture_paths=captures,
        ap_mapping_path=mapping_path,
        class_schedule_path=schedule_path,
        output_root=tmp_path / "output" / "runs",
        code_provenance={
            "source": "git",
            "revision": "a" * 40,
            "dirty": False,
        },
    )

    manifest = load_run_manifest(final / "manifest.json")
    assert {plan["capture_profile"] for plan in manifest["capture_plans"]} == {
        "aruba_erm_type0",
        "native_80211",
    }
    selected = pd.read_parquet(final / "selected_observations")
    assert set(selected["class_session_id"]) == {"ARUBA-C1", "EXTREME-C1"}
    identity = pq.read_table(final / "project_identity_map.parquet").to_pandas()
    assert set(identity["ap_name"]) == {
        "P-HBLL-3410-AP-02-3330D",
        "P-LVES-B105R22S30-AP-01-WN",
    }
    assert identity["radio_group_id"].notna().all()
