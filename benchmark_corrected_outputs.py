"""Resource benchmark for the complete corrected five-artifact boundary.

This manual harness begins with already selected in-memory observations. It
measures corrected-run lifecycle creation, all four Parquet calculations and
writes, manifest creation, and atomic whole-run publication. Capture
extraction, vendor normalization, identity resolution, and beacon selection
are intentionally outside this boundary.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any

import pandas as pd

from corrected_beacon_selection import build_class_bins
from corrected_run import (
    begin_corrected_run,
    complete_corrected_run,
    write_core_corrected_artifacts,
    write_corrected_summary_artifacts,
)
from run_lifecycle import AnalysisInput
from run_manifest import build_capture_plan_record, load_run_manifest
from tshark_field_planner import TsharkExtractionPlan

try:
    import psutil
except ImportError:  # pragma: no cover - optional resource measurements
    psutil = None


@dataclass(frozen=True)
class BenchmarkWorkload:
    name: str
    class_count: int
    radio_count: int
    bssids_per_radio: int
    class_duration_seconds: int
    bin_width_us: int

    def validate(self) -> None:
        for name in (
            "class_count",
            "radio_count",
            "bssids_per_radio",
            "class_duration_seconds",
            "bin_width_us",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


WORKLOADS = {
    workload.name: workload
    for workload in (
        BenchmarkWorkload(
            name="one_class_1024ms",
            class_count=1,
            radio_count=3,
            bssids_per_radio=4,
            class_duration_seconds=3_000,
            bin_width_us=1_024_000,
        ),
        BenchmarkWorkload(
            name="ten_classes_1024ms",
            class_count=10,
            radio_count=3,
            bssids_per_radio=4,
            class_duration_seconds=3_000,
            bin_width_us=1_024_000,
        ),
        BenchmarkWorkload(
            name="one_class_102ms_projection",
            class_count=1,
            radio_count=3,
            bssids_per_radio=4,
            class_duration_seconds=3_000,
            bin_width_us=102_400,
        ),
    )
}

_BANDS = ("2.4", "5", "6")
_CHANNELS = (6, 36, 5)
_FREQUENCIES = (2437, 5180, 5975)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            # A staging path can be atomically renamed between enumeration and
            # stat while the sampler is running.
            continue
    return total


def _artifact_sizes(final_directory: Path) -> dict[str, int]:
    selected = final_directory / "selected_observations"
    return {
        "manifest": (final_directory / "manifest.json").stat().st_size,
        "project_identity_map": (
            final_directory / "project_identity_map.parquet"
        ).stat().st_size,
        "selected_observations": _tree_size(selected),
        "radio_class_summary": (
            final_directory / "radio_class_summary.parquet"
        ).stat().st_size,
        "classroom_class_summary": (
            final_directory / "classroom_class_summary.parquet"
        ).stat().st_size,
    }


def _identity_frame(workload: BenchmarkWorkload) -> pd.DataFrame:
    rows = []
    for radio in range(workload.radio_count):
        band_index = radio % len(_BANDS)
        for member in range(workload.bssids_per_radio):
            rows.append(
                {
                    "bssid": f"02:00:{radio:02x}:00:00:{member:02x}",
                    "ssid": f"benchmark-{member}",
                    "radio_group_id": f"benchmark-radio-{radio:03d}",
                    "radio_number": (radio % 3) + 1,
                    "ap_id": f"benchmark-ap-{radio // 3:03d}",
                    "ap_name": f"BENCHMARK-AP-{radio // 3:03d}",
                    "classroom": "BENCHMARK-ROOM",
                    "band": _BANDS[band_index],
                    "channels": [_CHANNELS[band_index]],
                    "frequencies_mhz": [_FREQUENCIES[band_index]],
                    "ap_identity_method": "benchmark_synthetic",
                    "radio_grouping_method": "benchmark_band_channel",
                    "result_grain": "ap_radio_group",
                    "mapping_status": "resolved",
                    "mapping_diagnostics": [],
                }
            )
    return pd.DataFrame(rows)


def _class_and_selected_frames(
    workload: BenchmarkWorkload,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    class_frames = []
    selected_frames = []
    base_start = pd.Timestamp("2026-01-12T15:00:00Z")
    source_frame_offset = 0
    for class_number in range(workload.class_count):
        class_start = base_start + pd.Timedelta(days=class_number)
        class_end = class_start + pd.Timedelta(
            seconds=workload.class_duration_seconds
        )
        session_id = f"benchmark-class-{class_number:04d}"
        bins = build_class_bins(
            class_start,
            class_end,
            workload.bin_width_us,
        )
        bins["class_session_id"] = session_id
        bins["classroom"] = "BENCHMARK-ROOM"
        class_frames.append(bins)

        product = pd.MultiIndex.from_product(
            [
                range(workload.radio_count),
                range(workload.bssids_per_radio),
                bins["bin_number"].tolist(),
            ],
            names=["radio_slot", "bssid_slot", "bin_number"],
        ).to_frame(index=False)
        rows = product.merge(bins, on="bin_number", how="left", validate="many_to_one")
        no_beacon = (
            (rows["bin_number"] + rows["radio_slot"] * 3) % 29
        ).eq(0)
        rows = rows.loc[~no_beacon].copy()

        rows["class_session_id"] = session_id
        rows["classroom"] = "BENCHMARK-ROOM"
        rows["bssid"] = [
            f"02:00:{radio:02x}:00:00:{member:02x}"
            for radio, member in zip(rows["radio_slot"], rows["bssid_slot"])
        ]
        rows["ssid"] = rows["bssid_slot"].map(
            lambda value: f"benchmark-{value}"
        )
        rows["radio_group_id"] = rows["radio_slot"].map(
            lambda value: f"benchmark-radio-{value:03d}"
        )
        rows["ap_id"] = rows["radio_slot"].map(
            lambda value: f"benchmark-ap-{value // 3:03d}"
        )
        rows["band"] = rows["radio_slot"].map(
            lambda value: _BANDS[value % len(_BANDS)]
        )
        rows["channel"] = rows["radio_slot"].map(
            lambda value: _CHANNELS[value % len(_CHANNELS)]
        )
        offset = pd.to_timedelta(rows["bssid_slot"] + 1, unit="ms")
        rows["observation_time"] = rows["bin_start"] + offset
        rows["observation_time_source"] = "frame_time_epoch"
        rows["tsf_us"] = (
            rows["bin_number"] * workload.bin_width_us
            + rows["bssid_slot"] * 100
        )
        rows["tsf_origin"] = class_start
        rows["tsf_derived_time"] = rows["bin_start"] + offset
        rows["timing_method"] = "tsf_derived"
        rows["tsf_status"] = "usable_advancing"
        rows["unusable_tsf_reason"] = pd.NA
        rows["source_capture"] = "benchmark-synthetic.pcapng"
        rows["source_frame"] = pd.RangeIndex(
            source_frame_offset + 1,
            source_frame_offset + len(rows) + 1,
        ).astype(str)
        source_frame_offset += len(rows)
        rows["source_rank"] = 0
        rows["duplicate_views_collapsed"] = 0
        rows["skipped_intervals_before"] = pd.NA
        cu = (
            rows["bin_number"] * 17 + rows["radio_slot"] * 31
        ).mod(256).astype("Int64")
        all_missing = (
            (rows["bin_number"] + rows["radio_slot"] * 5) % 17
        ).eq(0)
        preferred_missing = rows["bssid_slot"].eq(0) & rows[
            "bin_number"
        ].mod(11).eq(0)
        cu.loc[all_missing | preferred_missing] = pd.NA
        rows["qbss_cu_raw"] = cu
        rows["qbss_station_count"] = (
            rows["bin_number"] + rows["bssid_slot"]
        ).mod(256)
        rows["qbss_admission_capacity"] = 1_000
        rows["is_selected"] = True
        selected_frames.append(rows)

    class_bins = pd.concat(class_frames, ignore_index=True)
    selected = pd.concat(selected_frames, ignore_index=True)
    return class_bins, selected


def build_synthetic_inputs(
    workload: BenchmarkWorkload,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build deterministic already-selected inputs outside the measured span."""

    workload.validate()
    class_bins, selected = _class_and_selected_frames(workload)
    return class_bins, _identity_frame(workload), selected


def _analysis_inputs(
    directory: Path,
    workload: BenchmarkWorkload,
) -> tuple[list[AnalysisInput], dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=True)
    capture = directory / "synthetic_capture_descriptor.json"
    schedule = directory / "synthetic_schedule_descriptor.json"
    mapping = directory / "synthetic_mapping_descriptor.json"
    capture.write_text(json.dumps(asdict(workload), sort_keys=True), encoding="utf-8")
    schedule.write_text(
        json.dumps(
            {
                "class_count": workload.class_count,
                "duration_seconds": workload.class_duration_seconds,
                "bin_width_us": workload.bin_width_us,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    mapping.write_text(
        json.dumps(
            {
                "classroom": "BENCHMARK-ROOM",
                "radio_count": workload.radio_count,
                "bssids_per_radio": workload.bssids_per_radio,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    inputs = [
        AnalysisInput("capture", capture, "benchmark/synthetic-capture.json"),
        AnalysisInput("class_schedule", schedule, "benchmark/schedule.json"),
        AnalysisInput("ap_classroom_map", mapping, "benchmark/mapping.json"),
    ]
    plan = TsharkExtractionPlan(
        capture_profile="benchmark_preselected",
        fields=("benchmark.selected_observation",),
        decode_args=(),
        display_filter="benchmark_preselected_input",
        adapter_versions=(("beacon.benchmark/synthetic", 1),),
    )
    capture_plan = build_capture_plan_record(
        source_path="benchmark/synthetic-capture.json",
        source_bytes=capture.stat().st_size,
        source_sha256=_sha256(capture),
        capture_profile=plan.capture_profile,
        profile_detection="benchmark_generated",
        erm_type=None,
        tshark_version="not-invoked",
        plan=plan,
    )
    return inputs, capture_plan


def _io_record() -> dict[str, int] | None:
    if psutil is None:
        return None
    counters = psutil.Process().io_counters()
    return {
        "read_bytes": int(counters.read_bytes),
        "write_bytes": int(counters.write_bytes),
        "read_count": int(counters.read_count),
        "write_count": int(counters.write_count),
    }


def _io_delta(
    before: dict[str, int] | None,
    after: dict[str, int] | None,
) -> dict[str, int] | None:
    if before is None or after is None:
        return None
    return {name: after[name] - before[name] for name in before}


def run_boundary_once(
    workload: BenchmarkWorkload,
    *,
    workspace_root: Path | None = None,
) -> dict[str, Any]:
    """Measure one fresh-process-compatible five-artifact boundary run."""

    workload.validate()
    generation_started = time.perf_counter()
    class_bins, identity, selected = build_synthetic_inputs(workload)
    generation_seconds = time.perf_counter() - generation_started
    dataframe_bytes = {
        "class_bins": int(class_bins.memory_usage(index=True, deep=True).sum()),
        "identity_map": int(identity.memory_usage(index=True, deep=True).sum()),
        "selected_observations": int(
            selected.memory_usage(index=True, deep=True).sum()
        ),
    }
    own_temporary = workspace_root is None
    temporary = tempfile.TemporaryDirectory() if own_temporary else None
    root = Path(temporary.name) if temporary is not None else Path(workspace_root)
    inputs, capture_plan = _analysis_inputs(root / "inputs", workload)
    output_root = root / "output" / "runs"

    gc.collect()
    process = psutil.Process() if psutil is not None else None
    baseline_rss = int(process.memory_info().rss) if process is not None else None
    peak_rss = baseline_rss
    peak_output_tree_bytes = _tree_size(output_root)
    stop_sampling = threading.Event()

    def sample_resources() -> None:
        nonlocal peak_rss, peak_output_tree_bytes
        next_disk_sample = 0.0
        while not stop_sampling.is_set():
            if process is not None:
                try:
                    rss = int(process.memory_info().rss)
                except psutil.Error:
                    rss = None
                if rss is not None:
                    peak_rss = max(peak_rss or 0, rss)
            now = time.perf_counter()
            if now >= next_disk_sample:
                peak_output_tree_bytes = max(
                    peak_output_tree_bytes,
                    _tree_size(output_root),
                )
                next_disk_sample = now + 0.05
            stop_sampling.wait(0.005)

    io_before = _io_record()
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    sampler = threading.Thread(target=sample_resources, daemon=True)
    sampler.start()
    try:
        requested_start = class_bins["bin_start"].min().isoformat().replace(
            "+00:00", "Z"
        )
        requested_end = class_bins["bin_end"].max().isoformat().replace(
            "+00:00", "Z"
        )
        run = begin_corrected_run(
            output_root=output_root,
            project_name=f"Benchmark {workload.name}",
            analysis_inputs=inputs,
            requested_start=requested_start,
            requested_end=requested_end,
            effective_configuration={
                "benchmark_workload": asdict(workload),
                "bin_widths_us": [workload.bin_width_us],
                "capture_extraction_included": False,
            },
            source_precedence=["frame_time_epoch", "source_rank", "lineage"],
            storage={
                "timestamp": "timestamp_ns_utc",
                "duration": "int64_microseconds",
            },
            capture_plans=[capture_plan],
            code_provenance={
                "source": "benchmark_harness",
                "revision": "synthetic",
                "dirty": False,
            },
        )
        write_core_corrected_artifacts(
            run,
            identity_map=identity,
            selected_observations=selected,
        )
        write_corrected_summary_artifacts(
            run,
            class_bins=class_bins,
            identity_map=identity,
            selected_observations=selected,
        )
        final_directory = complete_corrected_run(run)
    finally:
        wall_seconds = time.perf_counter() - wall_started
        cpu_seconds = time.process_time() - cpu_started
        stop_sampling.set()
        sampler.join()
    peak_output_tree_bytes = max(peak_output_tree_bytes, _tree_size(output_root))
    if process is not None:
        peak_rss = max(peak_rss or 0, int(process.memory_info().rss))
    io_after = _io_record()

    artifact_bytes = _artifact_sizes(final_directory)
    manifest = load_run_manifest(final_directory / "manifest.json")
    stage_wall_seconds = {
        stage["name"]: (
            pd.Timestamp(stage["end"]) - pd.Timestamp(stage["start"])
        ).total_seconds()
        for stage in manifest["stages"]
    }
    result = {
        "workload": asdict(workload),
        "input_shape": {
            "class_bins": len(class_bins),
            "identity_rows": len(identity),
            "selected_observation_rows": len(selected),
            "radio_class_rows": manifest["artifacts"]["radio_class_summary"][
                "rows"
            ],
            "classroom_class_rows": manifest["artifacts"][
                "classroom_class_summary"
            ]["rows"],
            "dataframe_deep_bytes": dataframe_bytes,
            "dataframe_deep_bytes_total": sum(dataframe_bytes.values()),
        },
        "measurement": {
            "scope": "five_artifact_calculation_and_publication",
            "synthetic_input_generation_included": False,
            "capture_extraction_included": False,
            "input_generation_wall_seconds": generation_seconds,
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "baseline_rss_bytes": baseline_rss,
            "peak_rss_bytes": peak_rss,
            "peak_incremental_rss_bytes": (
                None
                if baseline_rss is None or peak_rss is None
                else max(peak_rss - baseline_rss, 0)
            ),
            "process_io": _io_delta(io_before, io_after),
            "peak_output_tree_bytes": peak_output_tree_bytes,
            "published_run_bytes": sum(artifact_bytes.values()),
            "artifact_bytes": artifact_bytes,
            "published_file_count": sum(
                1 for path in final_directory.rglob("*") if path.is_file()
            ),
            "microseconds_per_selected_row": (
                wall_seconds * 1_000_000 / len(selected) if len(selected) else None
            ),
            "stage_wall_seconds": stage_wall_seconds,
            "unattributed_lifecycle_wall_seconds": max(
                wall_seconds - sum(stage_wall_seconds.values()),
                0.0,
            ),
        },
        "correctness": {
            "published": final_directory.is_dir(),
            "artifact_names": sorted(manifest["artifacts"]),
            "stage_names": [stage["name"] for stage in manifest["stages"]],
            "schema_id": manifest["schema_id"],
            "metric_id": manifest["metric_id"],
        },
    }
    if temporary is not None:
        temporary.cleanup()
    return result


def _numeric_summary(values: list[float | int | None]) -> dict[str, Any]:
    present = [float(value) for value in values if value is not None]
    return {
        "runs": values,
        "median": statistics.median(present) if present else None,
        "minimum": min(present) if present else None,
        "maximum": max(present) if present else None,
    }


def _invoke_worker(workload_name: str) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            "--workload",
            workload_name,
        ],
        cwd=Path(__file__).resolve().parent,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"benchmark worker failed for {workload_name}: "
            f"{completed.stderr.strip()}"
        )
    return json.loads(completed.stdout)


def _aggregate_workload(runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        raise ValueError("benchmark requires at least one run")
    first = runs[0]
    measurements = [run["measurement"] for run in runs]
    stage_names = list(first["measurement"]["stage_wall_seconds"])
    return {
        "workload": first["workload"],
        "input_shape": first["input_shape"],
        "repetitions": len(runs),
        "wall_seconds": _numeric_summary(
            [value["wall_seconds"] for value in measurements]
        ),
        "cpu_seconds": _numeric_summary(
            [value["cpu_seconds"] for value in measurements]
        ),
        "peak_rss_bytes": _numeric_summary(
            [value["peak_rss_bytes"] for value in measurements]
        ),
        "peak_incremental_rss_bytes": _numeric_summary(
            [value["peak_incremental_rss_bytes"] for value in measurements]
        ),
        "peak_output_tree_bytes": _numeric_summary(
            [value["peak_output_tree_bytes"] for value in measurements]
        ),
        "published_run_bytes": _numeric_summary(
            [value["published_run_bytes"] for value in measurements]
        ),
        "microseconds_per_selected_row": _numeric_summary(
            [value["microseconds_per_selected_row"] for value in measurements]
        ),
        "stage_wall_seconds": {
            stage_name: _numeric_summary(
                [
                    value["stage_wall_seconds"][stage_name]
                    for value in measurements
                ]
            )
            for stage_name in stage_names
        },
        "unattributed_lifecycle_wall_seconds": _numeric_summary(
            [
                value["unattributed_lifecycle_wall_seconds"]
                for value in measurements
            ]
        ),
        "process_write_bytes": _numeric_summary(
            [
                None
                if value["process_io"] is None
                else value["process_io"]["write_bytes"]
                for value in measurements
            ]
        ),
        "artifact_bytes": first["measurement"]["artifact_bytes"],
        "published_file_count": first["measurement"]["published_file_count"],
        "correctness": first["correctness"],
    }


def build_benchmark_report(
    workload_names: list[str],
    *,
    repeats: int,
) -> dict[str, Any]:
    if repeats < 1:
        raise ValueError("repeats must be positive")
    unknown = sorted(set(workload_names).difference(WORKLOADS))
    if unknown:
        raise ValueError("unknown workloads: " + ", ".join(unknown))
    total_memory = None
    if psutil is not None:
        total_memory = int(psutil.virtual_memory().total)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "corrected_five_artifact_boundary_baseline",
        "environment": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "logical_cpu_count": os.cpu_count(),
            "total_memory_bytes": total_memory,
            "psutil_measurement_available": psutil is not None,
        },
        "scope": {
            "included": [
                "run lifecycle and input descriptor hashing",
                "identity-map Parquet write",
                "selected-observation partitioned Parquet write",
                "ADR-0004 radio/class and classroom/class calculation/write",
                "manifest write and atomic run publication",
            ],
            "excluded": [
                "capture extraction and raw-capture hashing at representative size",
                "vendor normalization and AP/radio identity resolution",
                "per-BSSID timing and selection",
                "plots, CSV exports, and user-interface latency",
                "synthetic input generation",
            ],
        },
        "workloads": [
            _aggregate_workload(
                [_invoke_worker(workload_name) for _ in range(repeats)]
            )
            for workload_name in workload_names
        ],
        "interpretation": {
            "release_budget": None,
            "supported_scale_claim": None,
            "optimization_authorized": False,
            "note": (
                "Observed synthetic baseline only. Use the results to locate "
                "costs; do not infer capture-to-run performance or accept "
                "102.4 ms production bins from this evidence alone."
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark corrected five-artifact calculation/publication."
    )
    parser.add_argument(
        "--workload",
        action="append",
        choices=sorted(WORKLOADS),
        help="Workload to run; repeat for multiple. Defaults to all.",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    workloads = args.workload or list(WORKLOADS)
    if args.worker:
        if len(workloads) != 1:
            parser.error("worker mode requires exactly one workload")
        report = run_boundary_once(WORKLOADS[workloads[0]])
    else:
        report = build_benchmark_report(workloads, repeats=args.repeats)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
