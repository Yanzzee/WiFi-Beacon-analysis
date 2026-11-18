from __future__ import annotations

from pathlib import Path

from benchmark_corrected_outputs import (
    BenchmarkWorkload,
    build_synthetic_inputs,
    run_boundary_once,
)


def _tiny_workload() -> BenchmarkWorkload:
    return BenchmarkWorkload(
        name="test_tiny",
        class_count=1,
        radio_count=2,
        bssids_per_radio=2,
        class_duration_seconds=2,
        bin_width_us=1_000_000,
    )


def test_synthetic_inputs_have_expected_grains_and_missing_patterns():
    bins, identity, selected = build_synthetic_inputs(_tiny_workload())

    assert len(bins) == 2
    assert len(identity) == 4
    assert selected[["class_session_id", "bssid", "bin_number"]].duplicated().sum() == 0
    assert selected["qbss_cu_raw"].isna().any()


def test_tiny_boundary_publishes_all_five_logical_artifacts(tmp_path: Path):
    result = run_boundary_once(_tiny_workload(), workspace_root=tmp_path)

    assert result["correctness"]["published"]
    assert result["correctness"]["artifact_names"] == [
        "classroom_class_summary",
        "project_identity_map",
        "radio_class_summary",
        "selected_observations",
    ]
    assert result["correctness"]["stage_names"] == [
        "write_project_identity_map",
        "write_selected_observations",
        "write_corrected_summaries",
        "write_compact_csv_exports",
        "write_corrected_cu_report_package",
        "write_run_report_overview",
    ]
    assert result["measurement"]["published_run_bytes"] > 0
    assert set(result["measurement"]["stage_wall_seconds"]) == {
        "write_project_identity_map",
        "write_selected_observations",
        "write_corrected_summaries",
        "write_compact_csv_exports",
        "write_corrected_cu_report_package",
        "write_run_report_overview",
    }
    assert result["input_shape"]["radio_class_rows"] == 2
    assert result["input_shape"]["classroom_class_rows"] == 1
