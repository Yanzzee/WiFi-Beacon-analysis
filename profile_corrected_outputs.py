"""Narrow CPU/allocation profile for corrected output hot stages.

This diagnostic harness starts with the same synthetic, already-selected
inputs as ``benchmark_corrected_outputs.py``.  It separates the work currently
performed by the public selected-observation and ADR-0004 summary writers into
their calculation/validation and serialization phases.  It does not change
production behavior and it is not an optimization benchmark.
"""

from __future__ import annotations

import argparse
import cProfile
from dataclasses import asdict
from datetime import datetime, timezone
import gc
import json
import os
from pathlib import Path
import pstats
import shutil
import sys
import tempfile
import threading
import time
import tracemalloc
from typing import Any, Callable
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from benchmark_corrected_outputs import (
    BenchmarkWorkload,
    WORKLOADS,
    build_synthetic_inputs,
)
from corrected_summary_output import (
    CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
    CLASSROOM_CLASS_SUMMARY_FILENAME,
    RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
    RADIO_CLASS_SUMMARY_FILENAME,
    _arrow_table,
    build_corrected_summaries,
)
from selected_observation_output import (
    SELECTED_OBSERVATION_ARROW_SCHEMA,
    SELECTED_OBSERVATIONS_DIRECTORY,
    _build_selected_observation_artifact,
    _class_date_partition_slices,
    read_selected_observations,
)

try:
    import psutil
except ImportError:  # pragma: no cover - optional OS-memory evidence
    psutil = None


PROFILE_SMALL = BenchmarkWorkload(
    name="profile_small_5min",
    class_count=1,
    radio_count=3,
    bssids_per_radio=4,
    class_duration_seconds=300,
    bin_width_us=1_024_000,
)
PROFILE_WORKLOADS = {**WORKLOADS, PROFILE_SMALL.name: PROFILE_SMALL}


def _project_function(function: str) -> bool:
    normalized = function.replace("\\", "/")
    return any(
        name in normalized
        for name in (
            "selected_observation_output.py",
            "corrected_summary_output.py",
            "profile_corrected_outputs.py",
        )
    )


def _profile_rows(
    profile: cProfile.Profile,
    *,
    limit: int = 18,
    project_only: bool = False,
) -> list[dict[str, Any]]:
    stats = pstats.Stats(profile)
    rows = []
    for (filename, line, function), values in stats.stats.items():
        primitive_calls, total_calls, self_seconds, cumulative_seconds, _ = values
        label = f"{Path(filename).name}:{line}({function})"
        row = {
            "function": label,
            "primitive_calls": int(primitive_calls),
            "total_calls": int(total_calls),
            "self_seconds": float(self_seconds),
            "cumulative_seconds": float(cumulative_seconds),
            "project_function": _project_function(filename),
        }
        if not project_only or row["project_function"]:
            rows.append(row)
    rows.sort(key=lambda row: (row["cumulative_seconds"], row["self_seconds"]), reverse=True)
    return rows[:limit]


def _allocation_rows(snapshot: tracemalloc.Snapshot, limit: int = 15) -> list[dict[str, Any]]:
    rows = []
    for statistic in snapshot.statistics("lineno"):
        frame = statistic.traceback[0]
        if not _project_function(frame.filename):
            continue
        rows.append(
            {
                "location": f"{Path(frame.filename).name}:{frame.lineno}",
                "allocated_bytes_retained": int(statistic.size),
                "allocation_count_retained": int(statistic.count),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _run_phase(
    name: str,
    operation: Callable[[], Any],
    *,
    allocation_profile: bool,
) -> tuple[Any, dict[str, Any]]:
    gc.collect()
    process = psutil.Process() if psutil is not None else None
    baseline_rss = int(process.memory_info().rss) if process is not None else None
    peak_rss = baseline_rss
    stop_sampling = threading.Event()

    def sample_rss() -> None:
        nonlocal peak_rss
        while not stop_sampling.is_set():
            try:
                rss = int(process.memory_info().rss) if process is not None else None
            except psutil.Error:
                rss = None
            if rss is not None:
                peak_rss = max(peak_rss or 0, rss)
            stop_sampling.wait(0.002)

    # A Python sampling thread contaminates cProfile's call tree on this
    # platform.  Allocation mode owns peak-RSS sampling; CPU mode records only
    # the phase boundary RSS and keeps its call tree clean.
    sampler = None
    if allocation_profile:
        sampler = threading.Thread(target=sample_rss, daemon=True)
        sampler.start()
    if allocation_profile:
        tracemalloc.start(10)
        tracemalloc.reset_peak()
    arrow_before = int(pa.total_allocated_bytes())
    profiler = None if allocation_profile else cProfile.Profile()
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    try:
        if profiler is not None:
            profiler.enable()
        result = operation()
        if profiler is not None:
            profiler.disable()
    finally:
        if profiler is not None:
            profiler.disable()
        cpu_seconds = time.process_time() - cpu_started
        wall_seconds = time.perf_counter() - wall_started
        arrow_after = int(pa.total_allocated_bytes())
        if allocation_profile:
            current_python_bytes, peak_python_bytes = tracemalloc.get_traced_memory()
            snapshot = tracemalloc.take_snapshot()
            allocations = _allocation_rows(snapshot)
            tracemalloc.stop()
        else:
            current_python_bytes = None
            peak_python_bytes = None
            allocations = []
        if sampler is not None:
            stop_sampling.set()
            sampler.join()
    if process is not None:
        peak_rss = max(peak_rss or 0, int(process.memory_info().rss))
    return result, {
        "name": name,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "baseline_rss_bytes": baseline_rss,
        "peak_rss_bytes": peak_rss,
        "peak_incremental_rss_bytes": (
            None
            if baseline_rss is None or peak_rss is None
            else max(peak_rss - baseline_rss, 0)
        ),
        "python_traced_current_bytes": current_python_bytes,
        "python_traced_peak_bytes": peak_python_bytes,
        "arrow_allocated_before_bytes": arrow_before,
        "arrow_allocated_after_bytes": arrow_after,
        "arrow_allocated_delta_bytes": arrow_after - arrow_before,
        "top_cpu_functions": [] if profiler is None else _profile_rows(profiler),
        "top_project_cpu_functions": (
            []
            if profiler is None
            else _profile_rows(profiler, project_only=True)
        ),
        "top_retained_project_allocations": allocations,
    }


def _write_selected_table(
    table: pa.Table,
    partition_slices: list[tuple[Any, int, int]],
    destination: Path,
) -> Path:
    """Run the serialization half of the production selected writer."""

    target = destination / SELECTED_OBSERVATIONS_DIRECTORY
    temporary = destination / f".{SELECTED_OBSERVATIONS_DIRECTORY}.{uuid.uuid4().hex}.tmp"
    temporary.mkdir()
    try:
        if table.num_rows == 0:
            pq.write_table(table, temporary / "part-00000.parquet")
        else:
            partition_columns = [
                name
                for name in SELECTED_OBSERVATION_ARROW_SCHEMA.names
                if name != "class_date"
            ]
            for class_date, offset, length in partition_slices:
                partition_directory = temporary / f"class_date={class_date.isoformat()}"
                partition_directory.mkdir()
                partition_table = table.slice(offset, length).select(
                    partition_columns
                )
                pq.write_table(
                    partition_table,
                    partition_directory / "part-00000.parquet",
                    compression="zstd",
                )
        os.replace(temporary, target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return target


def _write_summary_logical(summaries: Any, destination: Path) -> tuple[Path, Path]:
    """Run the serialization half of the production ADR-0004 writer."""

    radio_path = destination / RADIO_CLASS_SUMMARY_FILENAME
    classroom_path = destination / CLASSROOM_CLASS_SUMMARY_FILENAME
    pq.write_table(
        _arrow_table(summaries.radio_class, RADIO_CLASS_SUMMARY_ARROW_SCHEMA),
        radio_path,
    )
    pq.write_table(
        _arrow_table(summaries.classroom_class, CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA),
        classroom_path,
    )
    return radio_path, classroom_path


def profile_workload(
    workload: BenchmarkWorkload,
    *,
    allocation_profile: bool,
) -> dict[str, Any]:
    """Profile four narrow phases without changing production semantics."""

    class_bins, identity, selected = build_synthetic_inputs(workload)
    run_id = "profile-run"
    phases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as temporary_name:
        destination = Path(temporary_name)
        built, phase = _run_phase(
            "selected_validation_and_logical_build",
            lambda: _build_selected_observation_artifact(
                selected, run_id=run_id
            ),
            allocation_profile=allocation_profile,
        )
        phases.append(phase)
        selected_table = built.table
        partition_slices = _class_date_partition_slices(built.logical)
        del built
        selected_path, phase = _run_phase(
            "selected_partition_arrow_and_parquet_write",
            lambda: _write_selected_table(
                selected_table, partition_slices, destination
            ),
            allocation_profile=allocation_profile,
        )
        phases.append(phase)
        summaries, phase = _run_phase(
            "adr0004_summary_calculation",
            lambda: build_corrected_summaries(
                class_bins,
                identity,
                selected,
                run_id=run_id,
            ),
            allocation_profile=allocation_profile,
        )
        phases.append(phase)
        summary_paths, phase = _run_phase(
            "adr0004_arrow_and_parquet_write",
            lambda: _write_summary_logical(summaries, destination),
            allocation_profile=allocation_profile,
        )
        phases.append(phase)

        selected_rows = read_selected_observations(selected_path).num_rows
        output_bytes = sum(
            path.stat().st_size for path in destination.rglob("*") if path.is_file()
        )
        correctness = {
            "selected_rows": selected_rows,
            "expected_selected_rows": len(selected),
            "radio_class_rows": len(summaries.radio_class),
            "expected_radio_class_rows": workload.class_count * workload.radio_count,
            "classroom_class_rows": len(summaries.classroom_class),
            "expected_classroom_class_rows": workload.class_count,
            "summary_files_exist": all(path.is_file() for path in summary_paths),
            "output_bytes": output_bytes,
        }

    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "narrow_corrected_output_cpu_allocation_profile",
        "workload": asdict(workload),
        "input_rows": {
            "class_bins": len(class_bins),
            "identity": len(identity),
            "selected_observations": len(selected),
        },
        "allocation_profile_enabled": allocation_profile,
        "phases": phases,
        "correctness": correctness,
        "notes": [
            "cProfile instrumentation is included in phase wall/CPU time",
            "tracemalloc is optional and materially slows allocation-profile runs",
            "RSS increments are per phase and can retain memory from earlier phases",
            "the two write-only phases reproduce production serialization after logical build",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile selected-observation and ADR-0004 output stages."
    )
    parser.add_argument(
        "--workload",
        choices=sorted(PROFILE_WORKLOADS),
        default=PROFILE_SMALL.name,
    )
    parser.add_argument(
        "--allocations",
        action="store_true",
        help="enable tracemalloc (use the small workload first)",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = profile_workload(
        PROFILE_WORKLOADS[args.workload],
        allocation_profile=args.allocations,
    )
    rendered = json.dumps(report, indent=2)
    if args.output is None:
        print(rendered)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
