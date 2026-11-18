"""Minimal lifecycle runner for corrected durable outputs.

The runner connects the accepted lifecycle and all five durable artifacts.
Capture extraction, identity resolution, and per-BSSID timing/selection are
composed by ``corrected_capture_run``. Final publication is gated on all four
accepted table artifacts plus the manifest, so it cannot publish a partial
result set.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pandas as pd

from corrected_class_session_reports import (
    CLASS_SESSION_REPORT_FILENAME,
    ENROLLMENT_SCATTER_FILENAME,
    METHODOLOGY_FILENAME as CLASS_SESSION_METHODOLOGY_FILENAME,
    RATIO_HISTOGRAM_FILENAME,
    REPORT_DIRECTORY as CLASS_SESSION_REPORT_DIRECTORY,
    write_class_session_report_package,
)
from corrected_csv_exports import COMPACT_CSV_EXPORTS, write_compact_csv_exports
from corrected_admission_reports import (
    SUMMARY_FILENAME as ADMISSION_SUMMARY_FILENAME,
    CORRELATION_FILENAME as ADMISSION_CORRELATION_FILENAME,
    REPORT_SCHEMA_ID as ADMISSION_REPORT_SCHEMA_ID,
)
from corrected_admission_selection import ADMISSION_METRIC_ID, RADIO_DETAIL_SCHEMA_ID
from corrected_beacon_quality_reports import (
    BeaconQualityPart, METRIC_ID as BEACON_QUALITY_METRIC_ID,
    REPORT_SCHEMA_ID as BEACON_QUALITY_REPORT_SCHEMA_ID, write_beacon_quality_package,
)
from corrected_band_comparison_reports import (
    BandComparisonAccumulator, METRIC_ID as BAND_COMPARISON_METRIC_ID,
    REPORT_SCHEMA_ID as BAND_COMPARISON_REPORT_SCHEMA_ID,
)
from corrected_affected_station_reports import (
    AffectedStationAccumulator, METRIC_ID as AFFECTED_STATION_METRIC_ID,
    REPORT_SCHEMA_ID as AFFECTED_STATION_REPORT_SCHEMA_ID,
)
from corrected_report_overview import (
    OVERVIEW_PATH, REPORT_SCHEMA_ID as OVERVIEW_REPORT_SCHEMA_ID, write_run_overview,
)
from corrected_radio_bin_reports import (
    CORRELATION_FILENAME as RADIO_BIN_CORRELATION_FILENAME,
    METHODOLOGY_FILENAME as RADIO_BIN_METHODOLOGY_FILENAME,
    PLOT_INDEX_FILENAME as RADIO_BIN_PLOT_INDEX_FILENAME,
    RADIO_BIN_FILENAME,
    RELATIONSHIP_BAND_PLOT_FILENAME as RADIO_BIN_BAND_RELATIONSHIP_PLOT,
    RELATIONSHIP_PLOT_FILENAME as RADIO_BIN_RELATIONSHIP_PLOT,
    REPORT_DIRECTORY as RADIO_BIN_REPORT_DIRECTORY,
    write_radio_bin_report_package,
)
from corrected_cu_reports import (
    BAND_POPULATION_REPORT_FILENAME,
    CDF_BOUNDS_REPORT_FILENAME,
    CLASSROOM_REPORT_FILENAME,
    METHODOLOGY_FILENAME,
    PLOT_FILENAMES,
    RADIO_REPORT_FILENAME,
    REPORT_DIRECTORY,
    RUN_SUMMARY_PATH,
    write_corrected_cu_report_package,
)
from corrected_summary_output import (
    CorrectedSummaryTables,
    write_corrected_summaries,
    write_corrected_summary_tables,
)
from corrected_station_reports import (
    AGGREGATE_REPORT_FILENAME as STATION_AGGREGATE_REPORT_FILENAME,
    BAND_HISTOGRAM_FILENAME as STATION_BAND_HISTOGRAM_FILENAME,
    CLASS_REPORT_FILENAME as STATION_CLASS_REPORT_FILENAME,
    CLASSROOM_HISTOGRAM_FILENAME as STATION_CLASSROOM_HISTOGRAM_FILENAME,
    METHODOLOGY_FILENAME as STATION_METHODOLOGY_FILENAME,
    REPORT_DIRECTORY as STATION_REPORT_DIRECTORY,
    ROLLUP_FILENAME as STATION_ROLLUP_FILENAME,
    write_station_report_package,
)
from project_identity_output import write_project_identity_map
from run_lifecycle import (
    AnalysisInput,
    RunWorkspace,
    begin_run,
    detect_git_code_provenance,
    hash_analysis_inputs,
    publish_run,
    record_run_failure,
)
from run_manifest import (
    CORRECTED_METRIC_ID,
    RESULT_SCHEMA_ID,
    build_run_manifest,
    write_run_manifest,
)
from selected_observation_output import write_selected_observation_partitions


PROJECT_IDENTITY_ARTIFACT = "project_identity_map"
SELECTED_OBSERVATIONS_ARTIFACT = "selected_observations"
RADIO_CLASS_SUMMARY_ARTIFACT = "radio_class_summary"
CLASSROOM_CLASS_SUMMARY_ARTIFACT = "classroom_class_summary"

REQUIRED_TABLE_ARTIFACTS = frozenset(
    {
        PROJECT_IDENTITY_ARTIFACT,
        SELECTED_OBSERVATIONS_ARTIFACT,
        RADIO_CLASS_SUMMARY_ARTIFACT,
        CLASSROOM_CLASS_SUMMARY_ARTIFACT,
    }
)
class CorrectedRunError(RuntimeError):
    """The corrected output run cannot advance safely."""


@dataclass
class CorrectedRun:
    workspace: RunWorkspace
    requested_start: str
    requested_end: str
    effective_configuration: dict[str, Any]
    source_precedence: list[str]
    storage: dict[str, Any]
    capture_plans: list[dict[str, Any]]
    code_provenance: dict[str, Any]
    clock: Callable[[], datetime] = field(repr=False)
    artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    stages: list[dict[str, Any]] = field(default_factory=list)
    actual_selected_start: str | None = None
    actual_selected_end: str | None = None
    published: bool = False


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("runner clock must return timezone-aware values")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _logical_artifact_path(run: CorrectedRun, relative: str) -> str:
    return (
        f"output/runs/{run.workspace.project_slug}/{run.workspace.run_id}/"
        f"{relative}"
    )


def _report_bin_widths_us(configuration: Mapping[str, Any]) -> tuple[int, ...]:
    values = configuration.get("bin_widths_us")
    if values is None:
        value = configuration.get("bin_width_us")
        values = [] if value is None else [value]
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError(
            "effective configuration must declare bin_widths_us for reports"
        )
    widths = tuple(sorted({int(value) for value in values}))
    if any(width <= 0 for width in widths):
        raise ValueError("configured analysis bin widths must be positive")
    return widths


def _completed_stage(
    run: CorrectedRun,
    *,
    name: str,
    operation: Callable[[], Any],
) -> Any:
    started = run.clock()
    result = operation()
    ended = run.clock()
    run.stages.append(
        {
            "name": name,
            "start": _utc_text(started),
            "end": _utc_text(ended),
            "status": "completed",
            "warning_count": 0,
            "error_count": 0,
        }
    )
    return result


def execute_corrected_stage(
    run: CorrectedRun,
    *,
    name: str,
    operation: Callable[[], Any],
) -> Any:
    """Execute and record one pre-publication corrected-run stage."""

    if run.published:
        raise CorrectedRunError("run is already published")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("stage name must be non-empty")
    return _completed_stage(run, name=name.strip(), operation=operation)


def begin_corrected_run(
    *,
    output_root: str | Path,
    project_name: str,
    analysis_inputs: Iterable[AnalysisInput],
    requested_start: str,
    requested_end: str,
    effective_configuration: Mapping[str, Any],
    source_precedence: Iterable[str],
    storage: Mapping[str, Any],
    capture_plans: Iterable[Mapping[str, Any]],
    repository: str | Path | None = None,
    code_provenance: Mapping[str, Any] | None = None,
    started_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> CorrectedRun:
    """Hash inputs and begin one project-scoped corrected output run."""

    input_set = hash_analysis_inputs(analysis_inputs)
    if code_provenance is None:
        repository_root = repository or Path(__file__).resolve().parent
        code_provenance = detect_git_code_provenance(repository_root)
    normalized_configuration = dict(effective_configuration)
    normalized_precedence = list(source_precedence)
    normalized_storage = dict(storage)
    normalized_capture_plans = [dict(value) for value in capture_plans]
    workspace = begin_run(
        output_root=output_root,
        project_name=project_name,
        input_set=input_set,
        started_at=started_at,
    )
    return CorrectedRun(
        workspace=workspace,
        requested_start=requested_start,
        requested_end=requested_end,
        effective_configuration=normalized_configuration,
        source_precedence=normalized_precedence,
        storage=normalized_storage,
        capture_plans=normalized_capture_plans,
        code_provenance=dict(code_provenance),
        clock=clock or (lambda: datetime.now(timezone.utc)),
    )


def write_core_corrected_artifacts(
    run: CorrectedRun,
    *,
    identity_map: pd.DataFrame,
    selected_observations: pd.DataFrame,
) -> None:
    """Write the implemented identity and selected-observation artifacts."""

    write_bounded_core_corrected_artifacts(
        run,
        identity_map=identity_map,
        selected_observation_parts=[selected_observations],
    )


def write_bounded_core_corrected_artifacts(
    run: CorrectedRun,
    *,
    identity_map: pd.DataFrame,
    selected_observation_parts: Iterable[pd.DataFrame],
) -> None:
    """Write identity plus bounded selected-observation session parts."""

    if run.published:
        raise CorrectedRunError("run is already published")
    if run.artifacts:
        raise CorrectedRunError("core artifacts were already written")
    try:
        if "bssid" not in identity_map:
            raise ValueError("identity map requires bssid")
        identity_bssids = set(identity_map["bssid"].dropna().astype(str))

        def validated_parts() -> Iterable[pd.DataFrame]:
            for selected in selected_observation_parts:
                if "bssid" not in selected:
                    raise ValueError("selected observations require bssid")
                selected_bssids = set(selected["bssid"].dropna().astype(str))
                unresolved = sorted(selected_bssids.difference(identity_bssids))
                if unresolved:
                    raise ValueError(
                        "selected observations reference BSSIDs absent from "
                        "the project identity map: " + ", ".join(unresolved)
                    )
                yield selected

        _completed_stage(
            run,
            name="write_project_identity_map",
            operation=lambda: write_project_identity_map(
                identity_map,
                run_directory=run.workspace.staging_directory,
                run_id=run.workspace.run_id,
            ),
        )
        run.artifacts[PROJECT_IDENTITY_ARTIFACT] = {
            "path": _logical_artifact_path(
                run, "project_identity_map.parquet"
            ),
            "rows": len(identity_map),
        }

        written = _completed_stage(
            run,
            name="write_selected_observations",
            operation=lambda: write_selected_observation_partitions(
                validated_parts(),
                run_directory=run.workspace.staging_directory,
                run_id=run.workspace.run_id,
            ),
        )
        run.artifacts[SELECTED_OBSERVATIONS_ARTIFACT] = {
            "path": _logical_artifact_path(run, "selected_observations/"),
            "partition_by": ["class_date"],
            "rows": written.rows,
        }
        run.actual_selected_start = (
            None
            if written.first_observation_time is None
            else written.first_observation_time.isoformat().replace(
                "+00:00", "Z"
            )
        )
        run.actual_selected_end = (
            None
            if written.last_observation_time is None
            else written.last_observation_time.isoformat().replace(
                "+00:00", "Z"
            )
        )
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise
    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise


def _write_corrected_summary_artifact_operation(
    run: CorrectedRun,
    *,
    operation: Callable[[], Any],
) -> None:
    if run.published:
        raise CorrectedRunError("run is already published")
    if not {
        PROJECT_IDENTITY_ARTIFACT,
        SELECTED_OBSERVATIONS_ARTIFACT,
    }.issubset(run.artifacts):
        raise CorrectedRunError(
            "core corrected artifacts must be written before summaries"
        )
    if {
        RADIO_CLASS_SUMMARY_ARTIFACT,
        CLASSROOM_CLASS_SUMMARY_ARTIFACT,
    }.intersection(run.artifacts):
        raise CorrectedRunError("summary artifacts were already written")
    try:
        written = _completed_stage(
            run,
            name="write_corrected_summaries",
            operation=operation,
        )
        run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT] = {
            "path": _logical_artifact_path(run, "radio_class_summary.parquet"),
            "rows": written.radio_class_rows,
        }
        run.artifacts[CLASSROOM_CLASS_SUMMARY_ARTIFACT] = {
            "path": _logical_artifact_path(
                run, "classroom_class_summary.parquet"
            ),
            "rows": written.classroom_class_rows,
        }
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise
    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise


def write_corrected_summary_artifacts(
    run: CorrectedRun,
    *,
    class_bins: pd.DataFrame,
    identity_map: pd.DataFrame,
    selected_observations: pd.DataFrame,
) -> None:
    """Calculate and stage both accepted ADR-0004 summary artifacts."""

    _write_corrected_summary_artifact_operation(
        run,
        operation=lambda: write_corrected_summaries(
            class_bins,
            identity_map,
            selected_observations,
            run_directory=run.workspace.staging_directory,
            run_id=run.workspace.run_id,
        ),
    )


def write_precomputed_corrected_summary_artifacts(
    run: CorrectedRun,
    *,
    summaries: CorrectedSummaryTables,
) -> None:
    """Stage bounded ADR-0004 summary calculations as durable artifacts."""

    _write_corrected_summary_artifact_operation(
        run,
        operation=lambda: write_corrected_summary_tables(
            summaries,
            run_directory=run.workspace.staging_directory,
            run_id=run.workspace.run_id,
        ),
    )


def write_corrected_station_report_artifacts(
    run: CorrectedRun,
    *,
    rollup_parts: Iterable[pd.DataFrame],
    class_report: pd.DataFrame,
) -> None:
    """Stage the ADR-0010 bounded station-count report package."""

    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError(
            "all corrected table artifacts must be written before station reports"
        )
    try:
        written = _completed_stage(
            run,
            name="write_corrected_station_report_package",
            operation=lambda: write_station_report_package(
                run.workspace.staging_directory,
                rollup_parts=rollup_parts,
                class_report=class_report,
            ),
        )
        report_relative = STATION_REPORT_DIRECTORY.as_posix()
        run.artifacts[SELECTED_OBSERVATIONS_ARTIFACT].setdefault(
            "derived_exports", []
        ).extend(
            [
                {
                    "format": "parquet",
                    "role": "corrected_station_bin_rollups",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{STATION_ROLLUP_FILENAME}"
                    ),
                    "rows": written.rollup_rows,
                },
                {
                    "format": "text",
                    "role": "corrected_station_methodology",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{STATION_METHODOLOGY_FILENAME}",
                    ),
                },
                *[
                    {
                        "format": "png",
                        "role": "corrected_station_distribution_plot",
                        "path": _logical_artifact_path(
                            run, f"{report_relative}/{filename}"
                        ),
                    }
                    for filename in (
                        STATION_CLASSROOM_HISTOGRAM_FILENAME,
                        STATION_BAND_HISTOGRAM_FILENAME,
                    )
                ],
            ]
        )
        run.artifacts[CLASSROOM_CLASS_SUMMARY_ARTIFACT].setdefault(
            "derived_exports", []
        ).extend(
            [
                {
                    "format": "csv",
                    "role": "corrected_station_class_report",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{STATION_CLASS_REPORT_FILENAME}",
                    ),
                    "rows": written.class_report_rows,
                },
                {
                    "format": "csv",
                    "role": "corrected_station_aggregate_report",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{STATION_AGGREGATE_REPORT_FILENAME}",
                    ),
                    "rows": written.aggregate_report_rows,
                },
            ]
        )
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise

    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise


def write_corrected_radio_bin_report_artifacts(
    run: CorrectedRun,
    *,
    detail_parts: Iterable[pd.DataFrame],
    create_plots: bool = False,
    primary_ssid: str | None = None,
    display_timezone: str | None = None,
    class_sessions: pd.DataFrame | None = None,
    configured_aps: pd.DataFrame | None = None,
) -> None:
    """Stage the bounded radio/bin view and CU/station/admission reports."""

    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError(
            "all corrected table artifacts must be written before radio/bin reports"
        )
    try:
        written = _completed_stage(
            run,
            name="write_corrected_radio_bin_report_package",
            operation=lambda: write_radio_bin_report_package(
                run.workspace.staging_directory,
                detail_parts=detail_parts,
                create_plots=create_plots,
                primary_ssid=primary_ssid,
                display_timezone=display_timezone,
                class_sessions=class_sessions,
                configured_aps=configured_aps,
            ),
        )
        report_relative = RADIO_BIN_REPORT_DIRECTORY.as_posix()
        exports = run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT].setdefault(
            "derived_exports", []
        )
        exports.extend(
            [
                {
                    "format": "parquet",
                    "role": "corrected_radio_bin_detail",
                    "report_schema_id": RADIO_DETAIL_SCHEMA_ID,
                    "admission_metric_id": ADMISSION_METRIC_ID,
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{RADIO_BIN_FILENAME}"
                    ),
                    "rows": written.rows,
                },
                {
                    "format": "text",
                    "role": "corrected_radio_bin_methodology",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{RADIO_BIN_METHODOLOGY_FILENAME}",
                    ),
                },
                {
                    "format": "csv",
                    "role": "corrected_cu_station_correlation",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{RADIO_BIN_CORRELATION_FILENAME}"
                    ),
                    "rows": written.correlation_rows,
                },
                *[
                    {
                        "format": "png",
                        "role": "corrected_cu_station_relationship_plot",
                        "path": _logical_artifact_path(
                            run, f"{report_relative}/{filename}"
                        ),
                    }
                    for filename in (
                        RADIO_BIN_RELATIONSHIP_PLOT,
                        RADIO_BIN_BAND_RELATIONSHIP_PLOT,
                    )
                ],
            ]
        )
        for path in written.admission_files:
            export = {
                "format": {".csv": "csv", ".png": "png", ".txt": "text"}[path.suffix],
                "role": (
                    "corrected_admission_capacity_summary" if path.name == ADMISSION_SUMMARY_FILENAME
                    else "corrected_admission_capacity_correlation" if path.name == ADMISSION_CORRELATION_FILENAME
                    else "corrected_admission_capacity_methodology" if path.suffix == ".txt"
                    else "corrected_admission_capacity_plot"
                ),
                "path": _logical_artifact_path(run, path.relative_to(run.workspace.staging_directory).as_posix()),
                "report_schema_id": ADMISSION_REPORT_SCHEMA_ID,
                "metric_id": ADMISSION_METRIC_ID,
            }
            if path.name == ADMISSION_SUMMARY_FILENAME:
                export["rows"] = written.admission_summary_rows
            elif path.name == ADMISSION_CORRELATION_FILENAME:
                export["rows"] = written.admission_correlation_rows
            exports.append(export)
        if create_plots:
            exports.extend(
                [
                    {
                        "format": "csv",
                        "role": "corrected_radio_class_plot_index",
                        "path": _logical_artifact_path(
                            run,
                            f"{report_relative}/{RADIO_BIN_PLOT_INDEX_FILENAME}",
                        ),
                        "rows": written.plot_count,
                    },
                    {
                        "format": "png_collection",
                        "role": "corrected_radio_class_plots",
                        "path": _logical_artifact_path(
                            run, f"{report_relative}/plots"
                        ),
                        "files": written.plot_count,
                    },
                ]
            )
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise
    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise


def write_corrected_class_session_report_artifacts(
    run: CorrectedRun,
    *,
    class_sessions: pd.DataFrame,
    classroom_summary: pd.DataFrame,
    radio_summary: pd.DataFrame,
    station_class_report: pd.DataFrame,
    display_timezone: str | None = None,
) -> None:
    """Stage the bounded corrected class-session review package."""

    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError(
            "all corrected table artifacts must be written before class reports"
        )
    try:
        written = _completed_stage(
            run,
            name="write_corrected_class_session_report_package",
            operation=lambda: write_class_session_report_package(
                run.workspace.staging_directory,
                class_sessions=class_sessions,
                classroom_summary=classroom_summary,
                radio_summary=radio_summary,
                station_class_report=station_class_report,
                display_timezone=display_timezone,
            ),
        )
        report_relative = CLASS_SESSION_REPORT_DIRECTORY.as_posix()
        exports = run.artifacts[CLASSROOM_CLASS_SUMMARY_ARTIFACT].setdefault(
            "derived_exports", []
        )
        exports.extend(
            [
                {
                    "format": "csv",
                    "role": "corrected_class_session_report",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{CLASS_SESSION_REPORT_FILENAME}"
                    ),
                    "rows": written.rows,
                },
                *[
                    {
                        "format": "png",
                        "role": "corrected_class_session_plot",
                        "path": _logical_artifact_path(
                            run, f"{report_relative}/{filename}"
                        ),
                    }
                    for filename in (
                        RATIO_HISTOGRAM_FILENAME,
                        ENROLLMENT_SCATTER_FILENAME,
                    )
                ],
                {
                    "format": "text",
                    "role": "corrected_class_session_methodology",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{CLASS_SESSION_METHODOLOGY_FILENAME}",
                    ),
                },
            ]
        )
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise
    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise


def write_corrected_beacon_quality_report_artifacts(
    run: CorrectedRun, *, parts: Iterable[BeaconQualityPart],
) -> None:
    """Publish pre-selection exploratory diagnostics without changing core metrics."""
    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError("all corrected table artifacts must be written before quality reports")
    files = _completed_stage(
        run, name="write_corrected_beacon_quality_report_package",
        operation=lambda: write_beacon_quality_package(run.workspace.staging_directory, parts),
    )
    exports = run.artifacts[SELECTED_OBSERVATIONS_ARTIFACT].setdefault("derived_exports", [])
    exports.extend({
        "format": {".csv": "csv", ".parquet": "parquet", ".png": "png", ".txt": "text"}[path.suffix],
        "role": "exploratory_beacon_quality_" + path.stem,
        "report_schema_id": BEACON_QUALITY_REPORT_SCHEMA_ID,
        "report_metric_id": BEACON_QUALITY_METRIC_ID,
        "path": _logical_artifact_path(run, path.relative_to(run.workspace.staging_directory).as_posix()),
    } for path in files)


def write_corrected_band_comparison_artifacts(run: CorrectedRun, *, comparison: BandComparisonAccumulator) -> None:
    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError("all corrected table artifacts must be written before band comparisons")
    files = _completed_stage(run, name="write_corrected_band_comparison_package", operation=lambda: comparison.write(
        run.workspace.staging_directory, context={
            "requested_start": run.requested_start, "requested_end": run.requested_end,
            "code_provenance": run.code_provenance, "capture_plans": run.capture_plans,
            "configuration": {key: run.effective_configuration.get(key) for key in (
                "bin_widths_us", "cu_radio_bin", "station_count", "admission_capacity", "band_comparison")},
        }))
    exports = run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT].setdefault("derived_exports", [])
    exports.extend({
        "format": {".csv": "csv", ".png": "png", ".txt": "text", ".json": "json"}[path.suffix],
        "role": "corrected_band_comparison_" + path.stem,
        "report_schema_id": BAND_COMPARISON_REPORT_SCHEMA_ID, "report_metric_id": BAND_COMPARISON_METRIC_ID,
        "path": _logical_artifact_path(run, path.relative_to(run.workspace.staging_directory).as_posix()),
    } for path in files)


def write_corrected_affected_station_artifacts(run: CorrectedRun, *, affected: AffectedStationAccumulator) -> None:
    if run.published:
        raise CorrectedRunError("run is already published")
    if set(run.artifacts) != REQUIRED_TABLE_ARTIFACTS:
        raise CorrectedRunError("all corrected table artifacts must be written before affected-station reports")
    files = _completed_stage(run, name="write_corrected_affected_station_package",
                             operation=lambda: affected.write(run.workspace.staging_directory))
    exports = run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT].setdefault("derived_exports", [])
    exports.extend({
        "format": {".csv": "csv", ".png": "png", ".txt": "text", ".json": "json"}[path.suffix],
        "role": "corrected_affected_stations_" + path.stem,
        "report_schema_id": AFFECTED_STATION_REPORT_SCHEMA_ID, "report_metric_id": AFFECTED_STATION_METRIC_ID,
        "path": _logical_artifact_path(run, path.relative_to(run.workspace.staging_directory).as_posix()),
    } for path in files)


def complete_corrected_run(
    run: CorrectedRun,
    *,
    additional_stages: Iterable[Mapping[str, Any]] = (),
) -> Path:
    """Write the authoritative manifest and publish a complete artifact set."""

    if run.published:
        raise CorrectedRunError("run is already published")
    present = set(run.artifacts)
    if present != REQUIRED_TABLE_ARTIFACTS:
        missing = sorted(REQUIRED_TABLE_ARTIFACTS.difference(present))
        extra = sorted(present.difference(REQUIRED_TABLE_ARTIFACTS))
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise CorrectedRunError(
            "cannot publish an incomplete corrected run: " + "; ".join(details)
        )

    try:
        additional_stages = tuple(additional_stages)
        csv_rows = _completed_stage(
            run,
            name="write_compact_csv_exports",
            operation=lambda: write_compact_csv_exports(
                run.workspace.staging_directory
            ),
        )
        for artifact, rows in csv_rows.items():
            _parquet_name, csv_name = COMPACT_CSV_EXPORTS[artifact]
            run.artifacts[artifact].setdefault("derived_exports", []).append(
                {
                    "format": "csv",
                    "path": _logical_artifact_path(
                        run, f"exports/csv/{csv_name}"
                    ),
                    "rows": rows,
                }
            )
        input_adapter = run.effective_configuration.get("input_adapter", {})
        display_timezone = (
            input_adapter.get("schedule_timezone")
            if isinstance(input_adapter, Mapping)
            else None
        )
        if not isinstance(display_timezone, str) or not display_timezone.strip():
            display_timezone = None
        report = _completed_stage(
            run,
            name="write_corrected_cu_report_package",
            operation=lambda: write_corrected_cu_report_package(
                run.workspace.staging_directory,
                display_timezone=display_timezone,
                bin_widths_us=_report_bin_widths_us(
                    run.effective_configuration
                ),
                run_metadata={
                    "project_name": run.workspace.project_name,
                    "started_at": (
                        run.stages[0]["start"] if run.stages else None
                    ),
                    "effective_configuration": run.effective_configuration,
                    "completed_stages": [dict(stage) for stage in run.stages],
                },
                clock=run.clock,
            ),
        )
        report_relative = REPORT_DIRECTORY.as_posix()
        run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT]["derived_exports"].extend(
            [
                {"format": "csv", "role": "corrected_cu_cdf_bounds",
                 "path": _logical_artifact_path(run, f"{report_relative}/{CDF_BOUNDS_REPORT_FILENAME}"),
                 "rows": report.cdf_bounds_rows},
                {
                    "format": "csv",
                    "role": "corrected_cu_radio_class_report",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{RADIO_REPORT_FILENAME}"
                    ),
                    "rows": report.radio_class_rows,
                },
                {
                    "format": "csv",
                    "role": "corrected_cu_band_population_report",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/{BAND_POPULATION_REPORT_FILENAME}",
                    ),
                    "rows": report.band_population_rows,
                },
                *[
                    {
                        "format": "png",
                        "role": "corrected_cu_project_plot",
                        "path": _logical_artifact_path(
                            run, f"{report_relative}/plots/{filename}"
                        ),
                    }
                    for filename in (
                        *PLOT_FILENAMES[:5],
                        *PLOT_FILENAMES[6:],
                    )
                ],
                {
                    "format": "text",
                    "role": "corrected_cu_methodology",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{METHODOLOGY_FILENAME}"
                    ),
                },
                {
                    "format": "text",
                    "role": "corrected_run_summary",
                    "path": _logical_artifact_path(
                        run, RUN_SUMMARY_PATH.as_posix()
                    ),
                },
            ]
        )
        run.artifacts[CLASSROOM_CLASS_SUMMARY_ARTIFACT]["derived_exports"].extend(
            [
                {
                    "format": "csv",
                    "role": "corrected_cu_classroom_class_report",
                    "path": _logical_artifact_path(
                        run, f"{report_relative}/{CLASSROOM_REPORT_FILENAME}"
                    ),
                    "rows": report.classroom_class_rows,
                },
                {
                    "format": "png",
                    "role": "corrected_cu_project_plot",
                    "path": _logical_artifact_path(
                        run,
                        f"{report_relative}/plots/{PLOT_FILENAMES[5]}",
                    ),
                },
            ]
        )
        _completed_stage(run, name="write_run_report_overview", operation=lambda: write_run_overview(
            run.workspace.staging_directory, project_name=run.workspace.project_name,
            run_id=run.workspace.run_id, manifest_pending=True, run_context={
                "input_set": run.workspace.input_set.as_manifest_record(),
                # Include this HTML-writing stage in the final published count.
                "processing_stage_count": len(run.stages) + 1 + len(additional_stages),
                "actual_selected_observation_range": {"start": run.actual_selected_start, "end": run.actual_selected_end},
                "effective_configuration": run.effective_configuration,
            }))
        run.artifacts[RADIO_CLASS_SUMMARY_ARTIFACT]["derived_exports"].append({
            "format": "html", "role": "corrected_run_report_overview",
            "report_schema_id": OVERVIEW_REPORT_SCHEMA_ID,
            "path": _logical_artifact_path(run, OVERVIEW_PATH.as_posix()),
        })
        stages = [*run.stages, *(dict(value) for value in additional_stages)]
        manifest = build_run_manifest(
            project_name=run.workspace.project_name,
            project_slug=run.workspace.project_slug,
            run_id=run.workspace.run_id,
            input_set=run.workspace.input_set.as_manifest_record(),
            code_provenance=run.code_provenance,
            requested_start=run.requested_start,
            requested_end=run.requested_end,
            actual_selected_start=run.actual_selected_start,
            actual_selected_end=run.actual_selected_end,
            effective_configuration=run.effective_configuration,
            source_precedence=run.source_precedence,
            storage=run.storage,
            capture_plans=run.capture_plans,
            stages=stages,
            artifacts=run.artifacts,
            detailed_log=(
                f"output/runs/{run.workspace.project_slug}/_logs/"
                f"{run.workspace.run_id}.jsonl"
            ),
            schema_id=RESULT_SCHEMA_ID,
            metric_id=CORRECTED_METRIC_ID,
        )
        write_run_manifest(
            manifest,
            run_directory=run.workspace.staging_directory,
        )
        final_directory = publish_run(run.workspace)
    except (TypeError, ValueError, OSError) as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=True,
        )
        raise
    except Exception as exc:
        record_run_failure(
            run.workspace,
            reason=f"{type(exc).__name__}: {exc}",
            understood=False,
        )
        raise
    run.published = True
    return final_directory


def abandon_corrected_run(run: CorrectedRun, *, reason: str) -> None:
    """End an intentionally unfinished staged run and remove its artifacts."""

    if run.published:
        raise CorrectedRunError("published runs cannot be abandoned")
    record_run_failure(run.workspace, reason=reason, understood=True)
