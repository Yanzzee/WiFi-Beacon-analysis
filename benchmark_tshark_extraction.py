"""Reproducible resource baseline for the planned full TShark extraction.

This is a manual benchmark harness, not an ordinary CI test or release gate.
It measures the exact production command after capture-profile detection and
writes JSON to standard output unless ``--output`` is supplied.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import threading
import time
from typing import Any

import process_pcapng as pp

try:
    import psutil
except ImportError:  # pragma: no cover - optional measurement support
    psutil = None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_count(csv_bytes: bytes) -> int:
    """Count emitted lines after the one-row TShark CSV header."""

    if not csv_bytes:
        return 0
    line_count = csv_bytes.count(b"\n")
    if not csv_bytes.endswith((b"\n", b"\r")):
        line_count += 1
    return max(line_count - 1, 0)


def _run_measured(command: list[str]) -> dict[str, Any]:
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    peak_rss_bytes: int | None = None
    cpu_seconds: float | None = None
    stop_sampling = threading.Event()

    def sample_process() -> None:
        nonlocal peak_rss_bytes, cpu_seconds
        if psutil is None:
            return
        try:
            measured = psutil.Process(process.pid)
        except psutil.Error:
            return
        while not stop_sampling.is_set():
            try:
                memory = measured.memory_info().rss
                cpu = measured.cpu_times()
            except psutil.Error:
                break
            peak_rss_bytes = max(peak_rss_bytes or 0, int(memory))
            cpu_seconds = float(cpu.user + cpu.system)
            stop_sampling.wait(0.005)

    sampler = threading.Thread(target=sample_process, daemon=True)
    sampler.start()
    stdout, stderr = process.communicate()
    stop_sampling.set()
    sampler.join()
    elapsed = time.perf_counter() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"TShark benchmark failed with exit code {process.returncode}: "
            f"{stderr.decode(errors='replace').strip()}"
        )
    return {
        "wall_seconds": elapsed,
        "cpu_seconds": cpu_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "output_bytes": len(stdout),
        "extracted_rows": _record_count(stdout),
    }


def _probe_invocation_count(profile: pp.CaptureProfile) -> int:
    return 4 if profile.is_aruba_erm else 1


def benchmark_capture(
    capture_path: Path,
    *,
    tshark_executable: str,
    repeats: int,
) -> dict[str, Any]:
    if repeats < 1:
        raise ValueError("repeats must be positive")
    capture_path = capture_path.resolve()
    detection_started = time.perf_counter()
    profile = pp.detect_capture_profile(capture_path, tshark_executable)
    detection_seconds = time.perf_counter() - detection_started
    if profile.is_aruba_erm:
        pp.validate_supported_aruba_tshark_version(tshark_executable)
    plan = pp.build_tshark_extraction_plan(profile)
    command = pp.build_tshark_export_command(
        tshark_executable,
        capture_path,
        plan,
    )
    runs = [_run_measured(command) for _ in range(repeats)]
    row_counts = {run["extracted_rows"] for run in runs}
    output_sizes = {run["output_bytes"] for run in runs}
    if len(row_counts) != 1 or len(output_sizes) != 1:
        raise RuntimeError("Repeated extraction produced inconsistent output")

    wall_values = [float(run["wall_seconds"]) for run in runs]
    cpu_values = [
        float(run["cpu_seconds"])
        for run in runs
        if run["cpu_seconds"] is not None
    ]
    rss_values = [
        int(run["peak_rss_bytes"])
        for run in runs
        if run["peak_rss_bytes"] is not None
    ]
    return {
        "capture": capture_path.name,
        "capture_bytes": capture_path.stat().st_size,
        "capture_sha256": _sha256(capture_path),
        "capture_profile": profile.name,
        "profile_detection_seconds": detection_seconds,
        "profile_probe_invocations": _probe_invocation_count(profile),
        "production_full_extractions_per_capture": 1,
        "benchmark_repeats": repeats,
        "field_count": len(plan.fields),
        "fields": list(plan.fields),
        "display_filter": plan.display_filter,
        "adapter_versions": [
            {"identifier": identifier, "version": version}
            for identifier, version in plan.adapter_versions
        ],
        "extracted_rows": next(iter(row_counts)),
        "output_bytes": next(iter(output_sizes)),
        "wall_seconds": {
            "runs": wall_values,
            "median": statistics.median(wall_values),
            "minimum": min(wall_values),
            "maximum": max(wall_values),
        },
        "cpu_seconds": {
            "runs": cpu_values,
            "median": statistics.median(cpu_values) if cpu_values else None,
        },
        "peak_rss_bytes": {
            "runs": rss_values,
            "maximum": max(rss_values) if rss_values else None,
        },
    }


def _tshark_version(executable: str) -> str:
    completed = subprocess.run(
        [executable, "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.splitlines()[0]


def build_benchmark_report(
    captures: list[Path],
    *,
    repeats: int,
) -> dict[str, Any]:
    executable = pp.resolve_tshark_executable()
    total_memory = None
    if psutil is not None:
        total_memory = int(psutil.virtual_memory().total)
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "dynamic_one_extraction_baseline",
        "environment": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "logical_cpu_count": os.cpu_count(),
            "total_memory_bytes": total_memory,
            "tshark": _tshark_version(executable),
            "psutil_measurement_available": psutil is not None,
        },
        "captures": [
            benchmark_capture(
                capture,
                tshark_executable=executable,
                repeats=repeats,
            )
            for capture in captures
        ],
        "interpretation": {
            "release_budget": None,
            "scalability_claim": None,
            "temporary_spill_required": None,
            "note": (
                "Initial observed baseline only; no pass/fail threshold or "
                "large-capture conclusion is implied."
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark the production one-extraction TShark plan."
    )
    parser.add_argument("captures", nargs="+", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build_benchmark_report(args.captures, repeats=args.repeats)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
