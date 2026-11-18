"""Rebuild an offline HTML overview from a completed run's saved reports."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import uuid

import pandas as pd

from corrected_cu_reports import build_radio_class_mean_cu_histogram
from corrected_station_reports import build_station_band_histogram_from_rollups
from corrected_cached_plots import refresh_saved_comparison_plots
from corrected_cu_bounds_refresh import backfill_cu_bounds
from corrected_cu_bounds import CU_BOUNDS_METRIC_ID
from corrected_cu_reports import _band_comparison_summary_lines, _affected_station_summary_lines
from corrected_report_overview import REPORT_SCHEMA_ID, write_run_overview
from run_lifecycle import detect_git_code_provenance, project_slug, sha256_file
from run_manifest import load_run_manifest


DEFAULT_REPORT_ROOT = Path(__file__).resolve().parent / "output" / "reports"


def _copy_assets(source: Path, destination: Path, *, skip_overview=False) -> tuple[int, int]:
    """Copy independent assets, rejecting links that could escape or recurse."""
    if source.is_symlink() or source.is_junction():
        raise ValueError(f"report assets must not contain filesystem links: {source}")
    files, size = 0, 0
    for parent, directories, filenames in os.walk(source, followlinks=False):
        parent = Path(parent)
        for name in [*directories, *filenames]:
            path = parent / name
            if path.is_symlink() or path.is_junction():
                raise ValueError(f"report assets must not contain filesystem links: {path}")
        relative = parent.relative_to(source)
        target = destination / relative
        target.mkdir(parents=True, exist_ok=True)
        for name in filenames:
            if skip_overview and relative == Path(".") and name == "index.html":
                continue
            path = parent / name
            shutil.copy2(path, target / name)
            files += 1
            size += path.stat().st_size
    return files, size


def refresh_run_report(source_run: str | Path, *, output_root: str | Path | None = None) -> Path:
    """Return a new report bundle; never analyze captures or modify the source."""
    started = time.perf_counter()
    source = Path(source_run).expanduser().resolve()
    manifest_path = source / "manifest.json"
    if not source.is_dir() or not manifest_path.is_file():
        raise ValueError("--from-run must name a completed run folder containing manifest.json")
    if (source / ".beacon-analysis-staging.json").exists():
        raise ValueError("--from-run must name a completed run, not a staging folder")
    manifest_hash = sha256_file(manifest_path)
    # Saved reporting needs the base run contract, not capture extraction plans.
    manifest = load_run_manifest(manifest_path, require_capture_plans=False)
    if any(stage["status"] != "completed" or stage["error_count"] for stage in manifest["stages"]):
        raise ValueError("source run contains incomplete or failed stages")
    if not (source / "reports").is_dir():
        raise ValueError("source run has no saved reports folder to refresh")

    root = Path(output_root if output_root is not None else DEFAULT_REPORT_ROOT).expanduser().resolve()
    parent = (root / project_slug(manifest["project"]["name"]) / manifest["run_id"]).resolve()
    if not parent.is_relative_to(root):
        raise ValueError("report destination escapes the output root")
    if root.is_relative_to(source) or parent.is_relative_to(source):
        raise ValueError("report output must be outside the source run folder")
    created = datetime.now(timezone.utc)
    report_id = created.strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    final = parent / report_id
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".report-", dir=parent)).resolve()
    try:
        count, size = _copy_assets(source / "reports", staging / "reports", skip_overview=True)
        if (source / "exports").is_dir():
            copied, copied_size = _copy_assets(source / "exports", staging / "exports")
            count += copied
            size += copied_size
        shutil.copy2(manifest_path, staging / "manifest.json")
        if sha256_file(staging / "manifest.json") != manifest_hash:
            raise ValueError("source manifest changed during report refresh")
        bounds_records = backfill_cu_bounds(staging, source_root=source,
                                            configuration=manifest.get("effective_configuration", {}))
        summary = staging / "reports/run_summary.txt"
        if summary.is_file():
            lines = summary.read_text(encoding="utf-8").splitlines()
            for builder in (_band_comparison_summary_lines, _affected_station_summary_lines):
                updated = builder(staging)
                if not updated:
                    continue
                if updated[0] in lines:
                    start = lines.index(updated[0])
                    end = next((i for i in range(start + 1, len(lines)) if not lines[i]), len(lines))
                    lines[start:end] = updated
                else:
                    lines.extend(["", *updated])
            summary.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
        regenerated = []
        radio_means = staging / "reports/cu_coverage_streak/radio_class_report.csv"
        if radio_means.is_file():
            radio = pd.read_csv(radio_means, dtype={"band": "string"}, float_precision="round_trip")
            plot = staging / "reports/cu_coverage_streak/plots/radio_average_cu_distribution.png"
            plot.parent.mkdir(parents=True, exist_ok=True)
            build_radio_class_mean_cu_histogram(radio).savefig(plot, dpi=140)
            regenerated.append({"path": plot.relative_to(staging).as_posix(), "source": radio_means.relative_to(staging).as_posix(),
                                "renderer_sha256": sha256_file(Path(__file__).with_name("corrected_cu_reports.py")),
                                "cu_bounds_dependencies": {name: sha256_file(Path(__file__).with_name(name)) for name in
                                    ("corrected_cu_bounds.py", "corrected_cu_plot_bounds.py")}})
        station_rollups = staging / "reports/station_count/station_bin_rollups.parquet"
        if station_rollups.is_file():
            plot = staging / "reports/station_count/classroom_bin_station_histogram_by_band.png"
            build_station_band_histogram_from_rollups(station_rollups).savefig(plot, dpi=140)
            regenerated.append({"path": plot.relative_to(staging).as_posix(), "source": station_rollups.relative_to(staging).as_posix(),
                                "renderer_sha256": sha256_file(Path(__file__).with_name("corrected_station_reports.py"))})
        regenerated.extend(refresh_saved_comparison_plots(staging))
        write_run_overview(staging, project_name=manifest["project"]["name"], run_id=manifest["run_id"])
        record = {
            "schema_id": "beacon.schema/report-refresh/v1",
            "report_id": report_id,
            "created_utc": created.isoformat().replace("+00:00", "Z"),
            "source_run_directory": str(source),
            "source_run_id": manifest["run_id"],
            "source_manifest_sha256": manifest_hash,
            "source_input_set_sha256": manifest["input_set"]["sha256"],
            "mode": "html_overview_from_saved_reports",
            "overview_schema_id": REPORT_SCHEMA_ID,
            "renderer_sha256": sha256_file(Path(__file__).with_name("corrected_report_overview.py")),
            "code_provenance": detect_git_code_provenance(Path(__file__).resolve().parent),
            "copied_asset_files": count,
            "copied_asset_bytes": size,
            "regenerated_figures": regenerated,
            "cu_bounds": {"metric_id": CU_BOUNDS_METRIC_ID, "updated_tables": bounds_records,
                          "mean_source_policy": "exact_sums_first; unrounded_persisted_means_allowed_without_inventing_integer_sums",
                          "loader_sha256": sha256_file(Path(__file__).with_name("corrected_cu_bounds_refresh.py")),
                          "math_sha256": sha256_file(Path(__file__).with_name("corrected_cu_bounds.py")),
                          "missing_components": "bounds_unavailable; never_assume_a_default_bin_width"},
            "generation_seconds": time.perf_counter() - started,
        }
        with (staging / "report_refresh.json").open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(record, stream, indent=2)
            stream.write("\n")
        if final.exists():
            raise FileExistsError(final)
        staging.rename(final)
        return final
    finally:
        if staging.exists():
            # Delete only this invocation's temporary child of the intended root.
            if staging.resolve().parent != parent or not staging.name.startswith(".report-"):
                raise RuntimeError("refusing to clean an unexpected report staging path")
            shutil.rmtree(staging)
