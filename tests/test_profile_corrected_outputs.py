from __future__ import annotations

from benchmark_corrected_outputs import BenchmarkWorkload
from profile_corrected_outputs import profile_workload


def test_narrow_profile_separates_four_correct_output_phases():
    workload = BenchmarkWorkload(
        name="profile_test_tiny",
        class_count=1,
        radio_count=2,
        bssids_per_radio=2,
        class_duration_seconds=2,
        bin_width_us=1_000_000,
    )

    report = profile_workload(workload, allocation_profile=True)

    assert [phase["name"] for phase in report["phases"]] == [
        "selected_validation_and_logical_build",
        "selected_partition_arrow_and_parquet_write",
        "adr0004_summary_calculation",
        "adr0004_arrow_and_parquet_write",
    ]
    assert all(phase["wall_seconds"] >= 0 for phase in report["phases"])
    assert all(
        phase["python_traced_peak_bytes"] is not None
        for phase in report["phases"]
    )
    assert report["correctness"]["selected_rows"] == report["correctness"][
        "expected_selected_rows"
    ]
    assert report["correctness"]["radio_class_rows"] == 2
    assert report["correctness"]["classroom_class_rows"] == 1
    assert report["correctness"]["summary_files_exist"]
