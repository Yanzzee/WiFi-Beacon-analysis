"""Manual bounded-worker benchmark for corrected capture runs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import tempfile
import threading
import time
from typing import Any, Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import process_pcapng as pp
from corrected_capture_run import run_corrected_capture_project

try:
    import psutil
except ImportError:  # pragma: no cover - optional resource measurements
    psutil = None


_TABLE_ARTIFACTS = (
    "project_identity_map.parquet",
    "selected_observations",
    "radio_class_summary.parquet",
    "classroom_class_summary.parquet",
)


def _tree_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return total
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total


class _ResourceSampler:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.stop = threading.Event()
        self.peak_rss_bytes: int | None = None
        self.peak_workspace_bytes = 0
        self.cpu_start_by_pid: dict[int, float] = {}
        self.cpu_delta_by_pid: dict[int, float] = {}
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        if psutil is None:
            return
        root = psutil.Process(os.getpid())
        while not self.stop.is_set():
            try:
                processes = [root, *root.children(recursive=True)]
            except psutil.Error:
                processes = [root]
            rss = 0
            for process in processes:
                try:
                    rss += int(process.memory_info().rss)
                    cpu = process.cpu_times()
                    total_cpu = float(cpu.user + cpu.system)
                    started_cpu = self.cpu_start_by_pid.setdefault(
                        process.pid,
                        total_cpu,
                    )
                    self.cpu_delta_by_pid[process.pid] = max(
                        self.cpu_delta_by_pid.get(process.pid, 0.0),
                        total_cpu - started_cpu,
                    )
                except psutil.Error:
                    continue
            self.peak_rss_bytes = max(self.peak_rss_bytes or 0, rss)
            self.peak_workspace_bytes = max(
                self.peak_workspace_bytes,
                _tree_size(self.workspace),
            )
            self.stop.wait(0.25)

    def __enter__(self) -> "_ResourceSampler":
        self.thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.stop.set()
        self.thread.join()

    def record(self) -> dict[str, Any]:
        return {
            "peak_process_tree_rss_bytes": self.peak_rss_bytes,
            "sampled_process_tree_cpu_seconds": (
                sum(self.cpu_delta_by_pid.values())
                if self.cpu_delta_by_pid
                else None
            ),
            "peak_workspace_bytes": self.peak_workspace_bytes,
        }


def _parquet_paths(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
    else:
        yield from sorted(path.rglob("*.parquet"))


def _logical_table_fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    row_count = 0
    columns: list[str] | None = None
    for parquet_path in _parquet_paths(path):
        parquet = pq.ParquetFile(parquet_path)
        try:
            for batch in parquet.iter_batches():
                frame = batch.to_pandas()
                if "run_id" in frame:
                    frame = frame.drop(columns=["run_id"])
                if columns is None:
                    columns = list(frame.columns)
                    digest.update(
                        json.dumps(columns, separators=(",", ":")).encode(
                            "utf-8"
                        )
                    )
                for column in frame.columns:
                    if frame[column].dtype == "object":
                        frame[column] = frame[column].map(
                            lambda value: json.dumps(
                                (
                                    value.tolist()
                                    if isinstance(value, np.ndarray)
                                    else value
                                ),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            if isinstance(value, (list, tuple, np.ndarray))
                            else value
                        )
                hashes = pd.util.hash_pandas_object(
                    frame,
                    index=False,
                    categorize=False,
                )
                digest.update(hashes.to_numpy().tobytes())
                row_count += len(frame)
        finally:
            parquet.close()
    return {
        "rows": row_count,
        "sha256": digest.hexdigest(),
        "columns": columns or [],
    }


def _artifact_fingerprints(final: Path) -> dict[str, Any]:
    return {
        name: _logical_table_fingerprint(final / name)
        for name in _TABLE_ARTIFACTS
    }


def _run_once(args: argparse.Namespace, workers: int, root: Path) -> dict[str, Any]:
    run_root = root / f"workers-{workers}"
    started = time.perf_counter()
    with _ResourceSampler(run_root) as sampler:
        final = run_corrected_capture_project(
            project_name=f"capture-worker-benchmark-{workers}",
            capture_paths=args.captures,
            ap_mapping_path=args.aps_csv,
            classroom_path=args.classrooms_csv,
            class_schedule_path=args.schedule_csv,
            schedule_timezone=args.timezone,
            schedule_start_date=args.start_date,
            schedule_end_date=args.end_date,
            output_root=run_root,
            capture_workers=workers,
            repository=Path(__file__).resolve().parent,
        )
    return {
        "capture_workers": workers,
        "wall_seconds": time.perf_counter() - started,
        "resources": sampler.record(),
        "artifacts": _artifact_fingerprints(final),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark corrected capture staging at selected worker counts."
    )
    parser.add_argument("captures", nargs="+", type=Path)
    parser.add_argument("--aps-csv", required=True, type=Path)
    parser.add_argument("--classrooms-csv", required=True, type=Path)
    parser.add_argument("--schedule-csv", required=True, type=Path)
    parser.add_argument("--timezone", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--tshark")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--workers",
        nargs="+",
        type=int,
        default=(1, 2),
        help="worker counts to benchmark in order (default: 1 2)",
    )
    args = parser.parse_args()

    if not args.workers or any(not 1 <= value <= 16 for value in args.workers):
        parser.error("--workers values must be between 1 and 16")
    if len(args.workers) != len(set(args.workers)):
        parser.error("--workers values must be unique")

    if args.tshark:
        pp.TSHARK_EXECUTABLE = args.tshark
    tshark = pp.resolve_tshark_executable()
    captures = [path.resolve() for path in args.captures]
    with tempfile.TemporaryDirectory(prefix="beacon-worker-benchmark-") as temp:
        root = Path(temp)
        runs = [_run_once(args, workers, root) for workers in args.workers]

    equivalent = all(
        run["artifacts"] == runs[0]["artifacts"] for run in runs[1:]
    )
    report = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "corrected_capture_worker_comparison",
        "environment": {
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
            "total_memory_bytes": (
                int(psutil.virtual_memory().total) if psutil is not None else None
            ),
            "tshark": pp.read_tshark_version(tshark),
        },
        "captures": [
            {"path": str(path), "bytes": path.stat().st_size}
            for path in captures
        ],
        "runs": runs,
        "logical_artifacts_equivalent": equivalent,
        "speedup_vs_first_run": {
            str(run["capture_workers"]): (
                runs[0]["wall_seconds"] / run["wall_seconds"]
            )
            for run in runs
        },
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8", newline="\n")
    if not equivalent:
        raise RuntimeError("worker-count logical artifacts differ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
