"""Programmatic raw-capture-to-corrected-run orchestration.

This is the first narrow production path through the accepted corrected
contracts. Capture acquisition and user-interface concerns remain outside the
boundary. Supported capture rows are extracted once, dispatched through the
reviewed Aruba/Extreme adapters, filtered to configured AP hostnames, resolved
to project radio identity, selected per class, summarized, and atomically
published as the five durable artifacts.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
import shutil
import sys
from time import perf_counter
from typing import Callable, Iterable, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from corrected_beacon_selection import (
    DEFAULT_BEACON_INTERVAL_TU,
    build_class_bins,
    preferred_observation_times,
    select_bssid_session_observations,
    TIMING_POLICY_ID,
)
from corrected_input_adapter import (
    load_corrected_project_inputs,
    load_raw_sample_project_inputs,
)
from corrected_radio_bin_reports import build_radio_bin_detail
from corrected_beacon_quality_reports import (
    BeaconQualityPart, WINDOW_SCHEMA as BEACON_QUALITY_WINDOW_SCHEMA,
    build_beacon_quality_part, METRIC_ID as BEACON_QUALITY_METRIC_ID,
)
from corrected_band_comparison_reports import BandComparisonAccumulator, METRIC_ID as BAND_COMPARISON_METRIC_ID
from corrected_affected_station_reports import AffectedStationAccumulator, METRIC_ID as AFFECTED_STATION_METRIC_ID
from corrected_admission_selection import ADMISSION_METRIC_ID, RADIO_DETAIL_SCHEMA_ID
from corrected_run import (
    CorrectedRun,
    begin_corrected_run,
    complete_corrected_run,
    execute_corrected_stage,
    write_bounded_core_corrected_artifacts,
    write_corrected_class_session_report_artifacts,
    write_corrected_beacon_quality_report_artifacts,
    write_corrected_band_comparison_artifacts,
    write_corrected_affected_station_artifacts,
    write_corrected_radio_bin_report_artifacts,
    write_corrected_station_report_artifacts,
    write_precomputed_corrected_summary_artifacts,
)
from corrected_station_reports import (
    STATION_CLASS_REPORT_COLUMNS,
    build_station_bin_rollups,
    build_station_class_report,
)
from corrected_summary_output import (
    CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
    HIGH_CU_MINIMUM_RAW,
    RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
    VERY_HIGH_CU_MINIMUM_RAW,
    CorrectedSummaryTables,
    build_corrected_summaries,
)
from process_pcapng import (
    CaptureExtractionResult,
    CaptureProfile,
    SOURCE_CAPTURE_FIELD,
    SOURCE_RANK_FIELD,
    extract_capture_beacons,
    rank_capture_sources,
)
from project_scope import (
    ProjectScopeCounts,
    _configured_ap_mapping,
    filter_configured_project_scope,
)
from radio_resolver import (
    RADIO_IDENTITY_EVIDENCE_COLUMNS,
    RadioResolutionCounts,
    RadioResolutionResult,
    attach_project_radio_identity,
    resolve_project_radios,
)
from run_lifecycle import AnalysisInput, record_run_event, record_run_failure
from run_manifest import build_capture_plan_record
from selected_observation_output import SELECTED_OBSERVATION_ARROW_SCHEMA
from tshark_field_planner import BEACON_DISPLAY_FILTER, TsharkExtractionPlan
from vendor_adapter_registry import BEACON_ADAPTER_REGISTRY


DEFAULT_BIN_WIDTH_US = 1_024_000
MAX_CAPTURE_WORKERS = 16

CaptureExtractor = Callable[..., CaptureExtractionResult]


def _utc_text(timestamp: pd.Timestamp) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def _input_reference(prefix: str, position: int, path: Path) -> str:
    return f"{prefix}/{position:04d}-{path.name}"


def _analysis_inputs(
    captures: list[tuple[Path, int]],
    *,
    class_schedule_path: Path,
    ap_mapping_path: Path,
    classroom_path: Path | None = None,
) -> tuple[list[AnalysisInput], dict[Path, str]]:
    inputs: list[AnalysisInput] = []
    references: dict[Path, str] = {}
    for path, rank in captures:
        reference = _input_reference("captures", rank, path)
        references[path] = reference
        inputs.append(AnalysisInput("capture", path, reference))
    inputs.extend(
        [
            AnalysisInput(
                "class_schedule",
                class_schedule_path,
                f"inputs/class_schedule-{class_schedule_path.name}",
            ),
            AnalysisInput(
                "ap_classroom_map",
                ap_mapping_path,
                f"inputs/ap_classroom_map-{ap_mapping_path.name}",
            ),
        ]
    )
    if classroom_path is not None:
        inputs.append(
            AnalysisInput(
                "classroom_metadata",
                classroom_path,
                f"inputs/classroom_metadata-{classroom_path.name}",
            )
        )
    return inputs, references


def _input_provenance_by_reference(run: CorrectedRun) -> dict[str, object]:
    return {item.path: item for item in run.workspace.input_set.files}


def _dispatch_counts_record(counts: object) -> dict[str, object]:
    record = asdict(counts)
    record["rows_by_adapter"] = [
        {"adapter_id": identifier, "rows": rows}
        for identifier, rows in record["rows_by_adapter"]
    ]
    return record


@dataclass(frozen=True)
class _StagedCaptureRows:
    identity_evidence: pd.DataFrame
    observation_template: pd.DataFrame
    dispatch_counts: list[dict[str, object]]
    scope_counts: ProjectScopeCounts
    bssid_row_counts: dict[str, int]
    class_window_rows: int
    class_window_part_files: int
    session_directories: dict[str, Path]
    capture_stage_measurements: list[dict[str, object]]


@dataclass(frozen=True)
class _StagedCapturePart:
    source_rank: int
    source_reference: str
    extraction_profile: CaptureProfile
    extraction_plan: TsharkExtractionPlan
    tshark_version: str
    identity_evidence: pd.DataFrame | None
    observation_template: pd.DataFrame | None
    dispatch_counts: dict[str, object]
    scope_counts: ProjectScopeCounts
    bssid_row_counts: dict[str, int]
    claimed_rows: int
    class_window_rows: int
    class_window_part_files: int
    stage_measurement: dict[str, object]


@dataclass(frozen=True)
class _BoundedSelection:
    selected_paths: tuple[Path, ...]
    station_rollup_paths: tuple[Path, ...]
    radio_bin_detail_paths: tuple[Path, ...]
    beacon_quality_paths: tuple[Path, ...]
    beacon_quality_stream_rows: int
    beacon_quality_window_rows: int
    station_class_report: pd.DataFrame
    summaries: CorrectedSummaryTables
    selected_rows: int
    station_rollup_rows: int
    radio_bin_detail_rows: int
    maximum_session_radio_bin_detail_rows: int
    maximum_session_station_rollup_rows: int
    maximum_session_input_rows: int
    post_duplicate_handling_rows: int
    tsf_duplicate_views_collapsed: int


def _ordered_sessions(class_sessions: pd.DataFrame) -> pd.DataFrame:
    return class_sessions.sort_values(
        ["class_session_id", "classroom"], kind="stable"
    ).reset_index(drop=True)


def _session_directories(
    windows_root: Path,
    class_sessions: pd.DataFrame,
) -> dict[str, Path]:
    identifiers = class_sessions["class_session_id"].astype(str)
    if identifiers.duplicated().any():
        duplicates = sorted(identifiers.loc[identifiers.duplicated(False)].unique())
        raise ValueError(
            "class_session_id values must be unique: " + ", ".join(duplicates)
        )
    return {
        identifier: windows_root / f"session-{position:05d}"
        for position, identifier in enumerate(identifiers)
    }


def _compact_identity_evidence(observations: pd.DataFrame) -> pd.DataFrame:
    evidence_source = observations
    optional_columns = {
        "ap_identity_method": "string",
        "ap_model": "string",
    }
    missing_optional = [
        column for column in optional_columns if column not in evidence_source
    ]
    if missing_optional:
        evidence_source = evidence_source.copy(deep=False)
        for column in missing_optional:
            evidence_source[column] = pd.Series(
                pd.NA,
                index=evidence_source.index,
                dtype=optional_columns[column],
            )
    missing = sorted(
        set(RADIO_IDENTITY_EVIDENCE_COLUMNS).difference(evidence_source.columns)
    )
    if missing:
        raise ValueError(
            "observations are missing identity evidence columns: "
            + ", ".join(missing)
        )
    return evidence_source.loc[
        :, list(RADIO_IDENTITY_EVIDENCE_COLUMNS)
    ].drop_duplicates()


def _bssid_counts(observations: pd.DataFrame) -> dict[str, int]:
    values = observations["bssid"].astype("string").str.strip().str.lower()
    return {
        str(bssid): int(count)
        for bssid, count in values.dropna().value_counts().items()
        if str(bssid)
    }


def _stage_class_windows(
    observations: pd.DataFrame,
    *,
    sessions_by_classroom: Mapping[str, pd.DataFrame],
    session_directories: Mapping[str, Path],
    source_rank: int,
) -> tuple[int, int]:
    if observations.empty:
        return 0, 0
    # Adapter and scope operations may preserve duplicate source indexes. Use
    # one unique positional index before grouping so selecting one classroom
    # cannot also select another classroom's rows with the same label.
    observations = observations.reset_index(drop=True)
    observation_times = preferred_observation_times(observations)
    staged_rows = 0
    staged_parts = 0
    for classroom, indexes in observations.groupby(
        "classroom", sort=False, dropna=False
    ).groups.items():
        room_sessions = sessions_by_classroom.get(str(classroom))
        if room_sessions is None:
            continue
        room_observations = observations.loc[indexes]
        room_times = observation_times.loc[indexes]
        for session in room_sessions.itertuples(index=False):
            inside = (room_times >= session.class_start) & (
                room_times < session.class_end
            )
            if not inside.any():
                continue
            part = room_observations.loc[inside].copy()
            session_directory = session_directories[str(session.class_session_id)]
            session_directory.mkdir(parents=True, exist_ok=True)
            part.to_parquet(
                session_directory / f"capture-{source_rank:05d}.parquet",
                index=False,
            )
            staged_rows += len(part)
            staged_parts += 1
    return staged_rows, staged_parts


def _extract_stage_capture(
    *,
    capture_position: int,
    capture_count: int,
    path: Path,
    source_rank: int,
    source_reference: str,
    extractor: CaptureExtractor,
    configured_aps: pd.DataFrame,
    sessions_by_classroom: Mapping[str, pd.DataFrame],
    session_directories: Mapping[str, Path],
    capture_workers: int,
) -> _StagedCapturePart:
    """Extract, scope, and stage one capture without returning its full rows."""

    started = perf_counter()
    worker_detail = (
        f" (worker pool {capture_workers})" if capture_workers > 1 else ""
    )
    print(
        f"[capture {capture_position}/{capture_count}] {path.name}: "
        f"extracting{worker_detail}",
        flush=True,
    )
    extraction_started = perf_counter()
    extraction = extractor(path, source_rank=source_rank)
    extraction_seconds = perf_counter() - extraction_started
    if not isinstance(extraction, CaptureExtractionResult):
        raise TypeError("capture extractor must return CaptureExtractionResult")
    records = extraction.observations.copy(deep=False)
    records["capture_profile"] = extraction.profile.name
    records[SOURCE_CAPTURE_FIELD] = source_reference
    records[SOURCE_RANK_FIELD] = source_rank
    dispatch_started = perf_counter()
    dispatched = BEACON_ADAPTER_REGISTRY.dispatch(
        records,
        capture_profile=extraction.profile.name,
        source_capture=source_reference,
        source_rank=source_rank,
    )
    dispatch_seconds = perf_counter() - dispatch_started
    dispatch_counts = {
        "source_path": source_reference,
        **_dispatch_counts_record(dispatched.counts),
    }
    empty_scope = ProjectScopeCounts(0, 0, 0, 0, 0)
    if dispatched.observations.empty:
        elapsed = perf_counter() - started
        print(
            f"[capture {capture_position}/{capture_count}] {path.name}: "
            f"complete in {elapsed:.1f}s; supported=0, retained=0, "
            "class_window=0",
            flush=True,
        )
        return _StagedCapturePart(
            source_rank=source_rank,
            source_reference=source_reference,
            extraction_profile=extraction.profile,
            extraction_plan=extraction.extraction_plan,
            tshark_version=extraction.tshark_version,
            identity_evidence=None,
            observation_template=None,
            dispatch_counts=dispatch_counts,
            scope_counts=empty_scope,
            bssid_row_counts={},
            claimed_rows=0,
            class_window_rows=0,
            class_window_part_files=0,
            stage_measurement={
                "source_path": source_reference,
                "extracted_rows": len(records),
                "adapter_claimed_rows": 0,
                "retained_rows": 0,
                "class_window_rows": 0,
                "extraction_seconds": extraction_seconds,
                "adapter_dispatch_seconds": dispatch_seconds,
                "project_scope_seconds": 0.0,
                "identity_and_class_staging_seconds": 0.0,
                "total_seconds": elapsed,
            },
        )

    scope_started = perf_counter()
    scoped = filter_configured_project_scope(
        dispatched.observations,
        configured_aps,
        observed_ap_names=dispatched.observations["ap_name"],
    )
    scope_seconds = perf_counter() - scope_started
    staging_started = perf_counter()
    identity_evidence = None
    observation_template = None
    bssid_row_counts: dict[str, int] = {}
    class_window_rows = 0
    class_window_parts = 0
    if not scoped.observations.empty:
        identity_evidence = _compact_identity_evidence(scoped.observations)
        observation_template = scoped.observations.iloc[0:0].copy()
        bssid_row_counts = _bssid_counts(scoped.observations)
        class_window_rows, class_window_parts = _stage_class_windows(
            scoped.observations,
            sessions_by_classroom=sessions_by_classroom,
            session_directories=session_directories,
            source_rank=source_rank,
        )
    staging_seconds = perf_counter() - staging_started

    elapsed = perf_counter() - started
    source_filtered = (
        extraction.extraction_plan.display_filter != BEACON_DISPLAY_FILTER
    )
    scope_detail = (
        "source_scope=filtered"
        if source_filtered
        else (
            f"outside_project={scoped.counts.outside_project_scope_rows:,}, "
            f"missing_ap_name={scoped.counts.missing_ap_name_rows:,}"
        )
    )
    print(
        f"[capture {capture_position}/{capture_count}] {path.name}: "
        f"complete in {elapsed:.1f}s; "
        f"supported={len(dispatched.observations):,}, "
        f"retained={scoped.counts.retained_rows:,}, "
        f"class_window={class_window_rows:,}, {scope_detail}",
        flush=True,
    )
    return _StagedCapturePart(
        source_rank=source_rank,
        source_reference=source_reference,
        extraction_profile=extraction.profile,
        extraction_plan=extraction.extraction_plan,
        tshark_version=extraction.tshark_version,
        identity_evidence=identity_evidence,
        observation_template=observation_template,
        dispatch_counts=dispatch_counts,
        scope_counts=scoped.counts,
        bssid_row_counts=bssid_row_counts,
        claimed_rows=len(dispatched.observations),
        class_window_rows=class_window_rows,
        class_window_part_files=class_window_parts,
        stage_measurement={
            "source_path": source_reference,
            "extracted_rows": len(records),
            "adapter_claimed_rows": len(dispatched.observations),
            "retained_rows": scoped.counts.retained_rows,
            "class_window_rows": class_window_rows,
            "extraction_seconds": extraction_seconds,
            "adapter_dispatch_seconds": dispatch_seconds,
            "project_scope_seconds": scope_seconds,
            "identity_and_class_staging_seconds": staging_seconds,
            "total_seconds": elapsed,
        },
    )


def _stage_capture_parts(
    captures: list[tuple[Path, int]],
    references: Mapping[Path, str],
    extractor: CaptureExtractor,
    configured_aps: pd.DataFrame,
    sessions_by_classroom: Mapping[str, pd.DataFrame],
    session_directories: Mapping[str, Path],
    capture_workers: int,
) -> list[_StagedCapturePart]:
    """Run the complete bounded per-capture stage with limited concurrency."""

    arguments = [
        {
            "capture_position": position,
            "capture_count": len(captures),
            "path": path,
            "source_rank": source_rank,
            "source_reference": references[path],
            "extractor": extractor,
            "configured_aps": configured_aps,
            "sessions_by_classroom": sessions_by_classroom,
            "session_directories": session_directories,
            "capture_workers": capture_workers,
        }
        for position, (path, source_rank) in enumerate(captures, start=1)
    ]
    if capture_workers == 1:
        return [_extract_stage_capture(**values) for values in arguments]

    results: list[_StagedCapturePart] = []
    with ThreadPoolExecutor(
        max_workers=capture_workers,
        thread_name_prefix="capture-stage",
    ) as executor:
        futures = [
            executor.submit(_extract_stage_capture, **values)
            for values in arguments
        ]
        try:
            for future in as_completed(futures):
                results.append(future.result())
        except BaseException:
            for future in futures:
                future.cancel()
            raise
    return sorted(results, key=lambda result: result.source_rank)


def _extract_and_stage(
    run: CorrectedRun,
    captures: list[tuple[Path, int]],
    references: Mapping[Path, str],
    extractor: CaptureExtractor,
    configured_aps: pd.DataFrame,
    class_sessions: pd.DataFrame,
    working_root: Path,
    capture_workers: int,
) -> _StagedCaptureRows:
    windows_root = working_root / "class_windows"
    windows_root.mkdir(parents=True)
    ordered_sessions = _ordered_sessions(class_sessions)
    session_directories = _session_directories(windows_root, ordered_sessions)
    sessions_by_classroom = {
        str(classroom): group.reset_index(drop=True)
        for classroom, group in ordered_sessions.groupby(
            "classroom", sort=False, dropna=False
        )
    }
    evidence_parts: list[pd.DataFrame] = []
    observation_templates: list[pd.DataFrame] = []
    dispatch_counts: list[dict[str, object]] = []
    bssid_row_counts: dict[str, int] = {}
    scope_totals = {
        "input_rows": 0,
        "retained_rows": 0,
        "missing_ap_name_rows": 0,
        "outside_project_scope_rows": 0,
        "other_classroom_rows": 0,
    }
    claimed_rows = 0
    class_window_rows = 0
    class_window_part_files = 0
    capture_stage_measurements: list[dict[str, object]] = []
    provenance = _input_provenance_by_reference(run)

    staged_parts = _stage_capture_parts(
        captures,
        references,
        extractor,
        configured_aps,
        sessions_by_classroom,
        session_directories,
        capture_workers,
    )
    for part in staged_parts:
        source = provenance[part.source_reference]
        run.capture_plans.append(
            build_capture_plan_record(
                source_path=part.source_reference,
                source_bytes=source.bytes,
                source_sha256=source.sha256,
                capture_profile=part.extraction_profile.name,
                profile_detection=part.extraction_profile.detection_method,
                erm_type=part.extraction_profile.erm_type,
                tshark_version=part.tshark_version,
                plan=part.extraction_plan,
            )
        )
        dispatch_counts.append(part.dispatch_counts)
        capture_stage_measurements.append(part.stage_measurement)
        claimed_rows += part.claimed_rows
        for name in scope_totals:
            scope_totals[name] += getattr(part.scope_counts, name)
        if part.identity_evidence is not None:
            evidence_parts.append(part.identity_evidence)
        if part.observation_template is not None:
            observation_templates.append(part.observation_template)
        for bssid, count in part.bssid_row_counts.items():
            bssid_row_counts[bssid] = bssid_row_counts.get(bssid, 0) + count
        class_window_rows += part.class_window_rows
        class_window_part_files += part.class_window_part_files

    if claimed_rows == 0:
        raise ValueError("no capture rows were claimed by a supported adapter")
    if not evidence_parts:
        raise ValueError("no supported beacon rows matched configured AP hostnames")
    identity_evidence = pd.concat(
        evidence_parts, ignore_index=True, sort=False
    ).drop_duplicates(ignore_index=True)
    observation_template = pd.concat(
        observation_templates, ignore_index=True, sort=False
    )
    return _StagedCaptureRows(
        identity_evidence=identity_evidence,
        observation_template=observation_template,
        dispatch_counts=dispatch_counts,
        scope_counts=ProjectScopeCounts(**scope_totals),
        bssid_row_counts=bssid_row_counts,
        class_window_rows=class_window_rows,
        class_window_part_files=class_window_part_files,
        session_directories=session_directories,
        capture_stage_measurements=capture_stage_measurements,
    )


def _warn_unobserved_project_scope(
    run: CorrectedRun,
    configured_aps: pd.DataFrame,
    identity_map: pd.DataFrame,
    class_sessions: pd.DataFrame,
) -> None:
    """Warn once extraction establishes which configured APs were observed."""
    mapping = _configured_ap_mapping(configured_aps)
    observed = identity_map.loc[identity_map.radio_group_id.notna()]
    missing_aps = mapping.loc[~mapping.ap_id.isin(observed.ap_id), ["ap_id", "classroom"]]
    missing_rooms = sorted(set(class_sessions.classroom) - set(observed.classroom))
    if missing_aps.empty and not missing_rooms:
        return
    details = {
        "configured_aps_without_observations": missing_aps.to_dict("records"),
        "scheduled_classrooms_without_radios": missing_rooms,
    }
    run.effective_configuration["project_observation_warnings"] = details
    record_run_event(run.workspace, event="project_observation_warning", **details)
    warning_count = 0
    if not missing_aps.empty:
        names = ", ".join(f"{row.ap_id} ({row.classroom})" for row in missing_aps.itertuples())
        print(f"Warning: configured APs have no matching supported beacon observations: {names}. "
              "Check AP names and capture coverage.", file=sys.stderr, flush=True)
        warning_count += 1
    if missing_rooms:
        print("Warning: scheduled classrooms have no resolved radios: " + ", ".join(missing_rooms) +
              ". Their measurements will be reported as unavailable, not zero.", file=sys.stderr, flush=True)
        warning_count += 1
    run.stages[-1]["warning_count"] += warning_count


def _resolve_identity(staged: _StagedCaptureRows) -> RadioResolutionResult:
    evidence_resolution = resolve_project_radios(staged.identity_evidence)
    unresolved = evidence_resolution.identity_map.loc[
        evidence_resolution.identity_map["radio_group_id"].isna(), "bssid"
    ].astype(str)
    if not unresolved.empty:
        raise ValueError(
            "configured BSSIDs could not be resolved to radios: "
            + ", ".join(sorted(unresolved))
        )
    resolved_bssids = set(
        evidence_resolution.identity_map.loc[
            evidence_resolution.identity_map["radio_group_id"].notna(), "bssid"
        ].astype(str)
    )
    resolved_rows = sum(
        count
        for bssid, count in staged.bssid_row_counts.items()
        if bssid in resolved_bssids
    )
    identity_map = evidence_resolution.identity_map
    counts = RadioResolutionCounts(
        input_rows=staged.scope_counts.retained_rows,
        input_bssids=len(identity_map),
        resolved_rows=resolved_rows,
        resolved_bssids=len(resolved_bssids),
        fallback_rows=staged.scope_counts.retained_rows - resolved_rows,
        fallback_bssids=len(identity_map) - len(resolved_bssids),
        conflict_bssids=int(identity_map["mapping_status"].eq("conflict").sum()),
    )
    return RadioResolutionResult(
        observations=evidence_resolution.observations,
        identity_map=identity_map,
        counts=counts,
    )


def _empty_selected_frame(template: pd.DataFrame) -> pd.DataFrame:
    selected = template.iloc[0:0].copy()
    generated = {"run_id", "schema_id", "metric_id", "class_date"}
    for column in SELECTED_OBSERVATION_ARROW_SCHEMA.names:
        if column not in generated and column not in selected:
            selected[column] = pd.Series(dtype="object")
    return selected


def _read_session_observations(
    directory: Path,
    template: pd.DataFrame,
) -> pd.DataFrame:
    files = sorted(directory.glob("capture-*.parquet"))
    if not files:
        return template.iloc[0:0].copy()
    return pd.concat(
        (pd.read_parquet(path) for path in files),
        ignore_index=True,
        sort=False,
    )


def _select_staged_sessions(
    run: CorrectedRun,
    staged: _StagedCaptureRows,
    identity_map: pd.DataFrame,
    class_sessions: pd.DataFrame,
    working_root: Path,
    *,
    default_beacon_interval_tu: int,
    primary_ssid: str | None,
    cadence_beacon_interval_tu: int = 100,
) -> _BoundedSelection:
    selected_root = working_root / "selected_sessions"
    selected_root.mkdir()
    station_root = working_root / "station_sessions"
    station_root.mkdir()
    radio_bin_root = working_root / "radio_bin_sessions"
    radio_bin_root.mkdir()
    beacon_quality_root = working_root / "beacon_quality_sessions"
    beacon_quality_root.mkdir()
    beacon_quality_paths: list[Path] = []
    beacon_quality_stream_rows = beacon_quality_window_rows = 0
    selected_paths: list[Path] = []
    station_rollup_paths: list[Path] = []
    radio_bin_detail_paths: list[Path] = []
    station_class_parts: list[pd.DataFrame] = []
    radio_parts: list[pd.DataFrame] = []
    classroom_parts: list[pd.DataFrame] = []
    selected_rows = 0
    station_rollup_rows = 0
    radio_bin_detail_rows = 0
    maximum_session_radio_bin_detail_rows = 0
    maximum_session_station_rollup_rows = 0
    maximum_session_input_rows = 0
    post_duplicate_handling_rows = 0
    tsf_duplicate_views_collapsed = 0
    ordered_sessions = _ordered_sessions(class_sessions)
    total_sessions = len(ordered_sessions)

    for position, session in enumerate(
        ordered_sessions.itertuples(index=False), start=1
    ):
        session_id = str(session.class_session_id)
        directory = staged.session_directories[session_id]
        observations = _read_session_observations(
            directory, staged.observation_template
        )
        maximum_session_input_rows = max(
            maximum_session_input_rows, len(observations)
        )
        observations = attach_project_radio_identity(observations, identity_map)
        selection = select_bssid_session_observations(
            observations,
            class_session_id=session_id,
            class_start=session.class_start,
            class_end=session.class_end,
            bin_width_us=int(session.bin_width_us),
            default_beacon_interval_tu=default_beacon_interval_tu,
        )
        if not selection.timing.empty:
            anomalies = selection.timing.loc[
                selection.timing.capture_family.eq("native")
                & selection.timing.tsf_status.isin(["discontinuous", "reversing", "time_mismatch"])
            ]
            if not anomalies.empty:
                fields = ["bssid", "tsf_status", "tsf_clock_jump_count", "tsf_max_step_error_us",
                          "tsf_anomaly_source_capture", "tsf_anomaly_source_frame"]
                records = anomalies.reindex(columns=fields).astype(object).where(anomalies.reindex(columns=fields).notna(), None).to_dict("records")
                record_run_event(run.workspace, event="native_tsf_anomaly", class_session_id=session_id, streams=records)
                print(f"Warning: native TSF anomaly in {session_id}: "
                      + ", ".join(f"{row.bssid} ({row.tsf_status})" for row in anomalies.itertuples())
                      + "; observation timestamps retained.", file=sys.stderr, flush=True)
                config = run.effective_configuration["beacon_quality"]
                config["tsf_anomaly_stream_count"] = config.get("tsf_anomaly_stream_count", 0) + len(anomalies)
        post_duplicate_handling_rows += len(selection.observations)
        if "duplicate_views_collapsed" in selection.observations:
            tsf_duplicate_views_collapsed += int(
                pd.to_numeric(
                    selection.observations["duplicate_views_collapsed"],
                    errors="coerce",
                ).fillna(0).sum()
            )
        selected = selection.selected
        bins = build_class_bins(
            session.class_start,
            session.class_end,
            int(session.bin_width_us),
        )
        bins.insert(0, "classroom", session.classroom)
        bins.insert(0, "class_session_id", session_id)
        selected_for_summary = (
            selected
            if not selected.empty
            else _empty_selected_frame(staged.observation_template)
        )
        summaries = build_corrected_summaries(
            bins,
            identity_map,
            selected_for_summary,
            run_id=run.workspace.run_id,
            primary_ssid=primary_ssid,
        )
        radio_parts.append(summaries.radio_class)
        classroom_parts.append(summaries.classroom_class)
        station_rollups = build_station_bin_rollups(
            bins,
            selected_for_summary,
            run_id=run.workspace.run_id,
        )
        station_rollup_rows += len(station_rollups)
        maximum_session_station_rollup_rows = max(
            maximum_session_station_rollup_rows,
            len(station_rollups),
        )
        station_path = station_root / f"session-{position - 1:05d}.parquet"
        station_rollups.to_parquet(station_path, index=False)
        station_rollup_paths.append(station_path)
        if summaries.radio_bin_cu is None:
            raise AssertionError("radio/bin CU rows were not produced")
        radio_bin_detail = build_radio_bin_detail(
            summaries.radio_bin_cu,
            station_rollups,
        )
        quality = build_beacon_quality_part(
            observations, identity_map, radio_bin_detail, session,
            run_id=run.workspace.run_id, expected_interval_tu=cadence_beacon_interval_tu,
            selection=selection,
        )
        quality_path = beacon_quality_root / f"session-{position - 1:05d}"
        quality.streams.to_csv(quality_path.with_suffix(".streams.csv"), index=False)
        quality.gaps.to_csv(quality_path.with_suffix(".gaps.csv"), index=False)
        pq.write_table(pa.Table.from_pandas(quality.windows, schema=BEACON_QUALITY_WINDOW_SCHEMA,
                                          preserve_index=False), quality_path.with_suffix(".parquet"), compression="zstd")
        beacon_quality_paths.append(quality_path)
        beacon_quality_stream_rows += len(quality.streams)
        beacon_quality_window_rows += len(quality.windows)
        radio_bin_detail_rows += len(radio_bin_detail)
        maximum_session_radio_bin_detail_rows = max(
            maximum_session_radio_bin_detail_rows,
            len(radio_bin_detail),
        )
        radio_bin_path = radio_bin_root / f"session-{position - 1:05d}.parquet"
        radio_bin_detail.to_parquet(radio_bin_path, index=False)
        radio_bin_detail_paths.append(radio_bin_path)
        station_class_parts.append(
            build_station_class_report(
                station_rollups,
                {
                    "class_session_id": session_id,
                    "classroom": session.classroom,
                    "class_start": session.class_start,
                    "class_end": session.class_end,
                    "enrolled": session.enrolled,
                },
            )
        )

        if not selected.empty:
            selected_path = selected_root / f"session-{position - 1:05d}.parquet"
            selected.to_parquet(selected_path, index=False)
            selected_paths.append(selected_path)
            selected_rows += len(selected)
        shutil.rmtree(directory, ignore_errors=True)
        print(
            f"[session {position}/{total_sessions}] {session_id}: "
            f"staged={len(observations):,}, selected={len(selected):,}",
            flush=True,
        )

    shutil.rmtree(working_root / "class_windows", ignore_errors=True)
    nonempty_radio_parts = [part for part in radio_parts if not part.empty]
    radio_summary = (
        pd.concat(nonempty_radio_parts, ignore_index=True, sort=False)
        if nonempty_radio_parts
        else pd.DataFrame(columns=RADIO_CLASS_SUMMARY_ARROW_SCHEMA.names)
    )
    classroom_summary = pd.concat(
        classroom_parts, ignore_index=True, sort=False
    )
    radio_summary = radio_summary.sort_values(
        ["class_session_id", "classroom", "radio_group_id"], kind="stable"
    ).reset_index(drop=True)
    classroom_summary = classroom_summary.sort_values(
        ["class_session_id", "classroom"], kind="stable"
    ).reset_index(drop=True)
    station_class_report = pd.concat(
        station_class_parts, ignore_index=True, sort=False
    ).loc[:, list(STATION_CLASS_REPORT_COLUMNS)]
    return _BoundedSelection(
        selected_paths=tuple(selected_paths),
        station_rollup_paths=tuple(station_rollup_paths),
        radio_bin_detail_paths=tuple(radio_bin_detail_paths),
        beacon_quality_paths=tuple(beacon_quality_paths),
        beacon_quality_stream_rows=beacon_quality_stream_rows,
        beacon_quality_window_rows=beacon_quality_window_rows,
        station_class_report=station_class_report,
        summaries=CorrectedSummaryTables(
            radio_class=radio_summary.loc[
                :, RADIO_CLASS_SUMMARY_ARROW_SCHEMA.names
            ],
            classroom_class=classroom_summary.loc[
                :, CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA.names
            ],
        ),
        selected_rows=selected_rows,
        station_rollup_rows=station_rollup_rows,
        radio_bin_detail_rows=radio_bin_detail_rows,
        maximum_session_radio_bin_detail_rows=(
            maximum_session_radio_bin_detail_rows
        ),
        maximum_session_station_rollup_rows=(
            maximum_session_station_rollup_rows
        ),
        maximum_session_input_rows=maximum_session_input_rows,
        post_duplicate_handling_rows=post_duplicate_handling_rows,
        tsf_duplicate_views_collapsed=tsf_duplicate_views_collapsed,
    )


def _read_selected_parts(paths: Iterable[Path]) -> Iterable[pd.DataFrame]:
    for path in paths:
        selected = pd.read_parquet(path)
        try:
            yield selected
        finally:
            del selected
            path.unlink(missing_ok=True)


def _read_station_parts(paths: Iterable[Path], comparison=None) -> Iterable[pd.DataFrame]:
    for path in paths:
        station = pd.read_parquet(path)
        try:
            if comparison is not None:
                comparison.add_station(station)
            yield station
        finally:
            del station
            path.unlink(missing_ok=True)


def _read_radio_bin_parts(paths: Iterable[Path], comparison=None, affected=None) -> Iterable[pd.DataFrame]:
    for path in paths:
        detail = pd.read_parquet(path)
        try:
            if comparison is not None:
                comparison.add_detail(detail)
            if affected is not None:
                affected.add_detail(detail)
            yield detail
        finally:
            del detail
            path.unlink(missing_ok=True)


def _read_beacon_quality_parts(paths: Iterable[Path]) -> Iterable[BeaconQualityPart]:
    for path in paths:
        yield BeaconQualityPart(
            pd.read_csv(path.with_suffix(".streams.csv"), keep_default_na=False),
            pd.read_parquet(path.with_suffix(".parquet")),
            pd.read_csv(path.with_suffix(".gaps.csv")),
        )


def run_corrected_capture_project(
    *,
    project_name: str,
    capture_paths: Iterable[str | Path],
    ap_mapping_path: str | Path,
    class_schedule_path: str | Path,
    classroom_path: str | Path | None = None,
    schedule_timezone: str | None = None,
    schedule_start_date: str | None = None,
    schedule_end_date: str | None = None,
    output_root: str | Path,
    default_bin_width_us: int = DEFAULT_BIN_WIDTH_US,
    default_beacon_interval_tu: int = DEFAULT_BEACON_INTERVAL_TU,
    capture_workers: int = 1,
    cu_primary_ssid: str | None = "eduroam",
    radio_class_plots: bool = False,
    cadence_beacon_interval_tu: int = 100,
    extractor: CaptureExtractor = extract_capture_beacons,
    repository: str | Path | None = None,
    code_provenance: Mapping[str, object] | None = None,
    started_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> Path:
    """Run the accepted corrected path from captures to atomic publication.

    With ``classroom_path`` omitted, ``ap_mapping_path`` and
    ``class_schedule_path`` use the existing normalized CSV/Parquet adapter.
    Supplying ``classroom_path`` selects the raw three-CSV adapter and also
    requires ``schedule_timezone``, ``schedule_start_date``, and
    ``schedule_end_date``. Sample_classrooms.Location scopes both AP mappings
    and schedules before expansion; no intermediate Parquet files are created.
    """

    materialized_paths = [Path(path) for path in capture_paths]
    if not materialized_paths:
        raise ValueError("capture_paths must contain at least one capture")
    capture_identities = [
        str(path.resolve()).casefold() for path in materialized_paths
    ]
    if len(capture_identities) != len(set(capture_identities)):
        raise ValueError("capture_paths cannot contain the same file twice")
    if (
        not isinstance(default_beacon_interval_tu, int)
        or isinstance(default_beacon_interval_tu, bool)
        or default_beacon_interval_tu <= 0
    ):
        raise ValueError(
            "default_beacon_interval_tu must be a positive integer"
        )
    if (
        not isinstance(capture_workers, int)
        or isinstance(capture_workers, bool)
        or not 1 <= capture_workers <= MAX_CAPTURE_WORKERS
    ):
        raise ValueError(
            f"capture_workers must be between 1 and {MAX_CAPTURE_WORKERS}"
        )
    if cu_primary_ssid is not None:
        cu_primary_ssid = str(cu_primary_ssid).strip() or None
    if not isinstance(radio_class_plots, bool):
        raise TypeError("radio_class_plots must be boolean")
    if isinstance(cadence_beacon_interval_tu, bool) or not isinstance(cadence_beacon_interval_tu, int) or not 1 <= cadence_beacon_interval_tu <= 65535:
        raise ValueError("cadence_beacon_interval_tu must be an integer from 1 to 65535")
    ranked = [
        (Path(path), rank)
        for path, rank in rank_capture_sources(materialized_paths)
    ]
    schedule_path = Path(class_schedule_path)
    mapping_path = Path(ap_mapping_path)
    raw_values = {
        "classroom_path": classroom_path,
        "schedule_timezone": schedule_timezone,
        "schedule_start_date": schedule_start_date,
        "schedule_end_date": schedule_end_date,
    }
    raw_requested = any(value is not None for value in raw_values.values())
    if raw_requested and any(value is None for value in raw_values.values()):
        missing = sorted(
            name for name, value in raw_values.items() if value is None
        )
        raise ValueError(
            "raw sample CSV input requires: " + ", ".join(missing)
        )
    classroom_input_path = (
        Path(classroom_path) if classroom_path is not None else None
    )
    if raw_requested:
        project_inputs = load_raw_sample_project_inputs(
            ap_mapping_path=mapping_path,
            classroom_path=classroom_input_path,
            class_schedule_path=schedule_path,
            timezone=str(schedule_timezone),
            start_date=str(schedule_start_date),
            end_date=str(schedule_end_date),
            default_bin_width_us=default_bin_width_us,
        )
    else:
        project_inputs = load_corrected_project_inputs(
            ap_mapping_path=mapping_path,
            class_schedule_path=schedule_path,
            default_bin_width_us=default_bin_width_us,
        )
    configured_aps = project_inputs.configured_aps
    sessions = project_inputs.class_sessions
    configured_ap_names = tuple(
        configured_aps["Hostname"].astype("string").str.strip().tolist()
    )
    source_scope_filter_enabled = extractor is extract_capture_beacons
    effective_extractor = (
        partial(
            extractor,
            configured_ap_names=configured_ap_names,
        )
        if source_scope_filter_enabled
        else extractor
    )
    analysis_inputs, references = _analysis_inputs(
        ranked,
        class_schedule_path=schedule_path,
        ap_mapping_path=mapping_path,
        classroom_path=classroom_input_path,
    )
    requested_start = _utc_text(sessions["class_start"].min())
    requested_end = _utc_text(sessions["class_end"].max())
    widths = sorted(int(value) for value in sessions["bin_width_us"].unique())
    adapters = [
        {
            "identifier": descriptor.identifier,
            "version": descriptor.version,
        }
        for descriptor in BEACON_ADAPTER_REGISTRY.descriptors
    ]
    effective_configuration: dict[str, object] = {
        "beacon_quality": {
            "metric_id": BEACON_QUALITY_METRIC_ID,
            "expected_beacon_interval_tu": cadence_beacon_interval_tu,
            "erm_intervals_per_capture_slot": 10,
            "intervals_per_quality_window": 100,
            "erm_timing": "preferred_observation_time_no_tsf",
            "native_timing": "observation_time_with_independent_tsf_diagnostics",
            "tsf_anomaly_stream_count": 0,
            "capture_family_pooling": True,
            "observation_role": "unknown",
            "listening_exposure_known": False,
            "covariates": "previous_window_pooled_same_radio_observed_cu_and_accepted_station_count",
            "cu_imputation": False,
            "reliability_threshold_inference": False,
            "model_loss_correction": False,
        },
        "default_beacon_interval_tu": default_beacon_interval_tu,
        "nominal_raw_beacon_interval_us": default_beacon_interval_tu * 1_024,
        "useful_metadata_cadence_us": 1_024_000,
        "bin_widths_us": widths,
        "class_admission_time": "preferred_observation_time",
        "timing_policy_id": TIMING_POLICY_ID,
        "bin_placement_time": "preferred_observation_time",
        "tsf_role": "native_diagnostics_and_corroborated_duplicate_views_only",
        "tsf_trust_tolerance": "one_advertised_beacon_interval",
        "tsf_origin": "not_used",
        "representative_selection": "first_observed_per_bssid_bin",
        "rolling_minimum_spacing_gate": False,
        "high_cu_raw_minimum": HIGH_CU_MINIMUM_RAW,
        "very_high_cu_raw_minimum": VERY_HIGH_CU_MINIMUM_RAW,
        "cu_radio_bin": {
            "metric_id": "beacon.metric/corrected-radio-bin/v1",
            "grain": "class_session_radio_bin",
            "primary_ssid": cu_primary_ssid,
            "fallback": "earliest_valid_same_radio_member_bssid",
            "radio_class_plots": radio_class_plots,
            "display_timezone": schedule_timezone or "UTC",
            "cu_station_relationship": {
                "grain": "same_radio_same_bin_valid_pairs",
                "stale_assisted_station_values": True,
                "statistics": ["pearson", "spearman"],
                "inferential_statistics": False,
            },
        },
        "station_count": {
            "metric_id": "beacon.metric/corrected-station-count/v1",
            "session_membership": "selected_bssid_observed_in_session",
            "stale_limit_seconds": 30,
            "leading_backward_extension": True,
            "interior_future_fill": False,
            "partial_numeric_totals": False,
            "rollups": ["radio", "ssid", "band", "classroom"],
            "class_statistic": "maximum_available_classroom_bin",
            "enrollment": "combined_physical_session_sections",
        },
        "admission_capacity": {
            "metric_id": ADMISSION_METRIC_ID,
            "radio_detail_schema_id": RADIO_DETAIL_SCHEMA_ID,
            "grain": "class_session_radio_bin",
            "role": "radio_qos_medium_admission_allowance",
            "cu_role": "channel_conditions_sampled_per_radio",
            "usable_raw_range": [0, 31_250],
            "percent_conversion": "raw / 31250 * 100",
            "source_preference": "valid_capacity_from_unchanged_cu_source",
            "fallback": "stable_primary_ssid_or_first_valid_then_earliest_member",
            "primary_ssid": cu_primary_ssid,
            "stale_extension": False,
            "disagreement": "retain_selected_value_with_diagnostic",
            "cross_radio_sums": False,
            "stale_assisted_station_pairs": True,
            "inferential_statistics": False,
        },
        "class_session_report": {
            "grain": "physical_classroom_session",
            "course_labels": "deterministic_combined_section_list",
            "capacity_role": "descriptive_context_only",
            "capacity_conflict": "null_with_conflict_status",
        },
        "bounded_working_set": {
            "identity": "distinct_project_identity_evidence",
            "class_observations": "hidden_parquet_per_class_session",
            "selection": "one_complete_class_session_at_a_time",
            "temporary_files_published": False,
        },
        "capture_count": len(ranked),
        "capture_workers": capture_workers,
        "configured_ap_count": len(configured_aps),
        "capture_source_scope_filter": {
            "enabled": source_scope_filter_enabled,
            "basis": "configured_ap_hostname",
            "vendor_fields": [
                field
                for descriptor in BEACON_ADAPTER_REGISTRY.descriptors
                for field in descriptor.extraction_fields
            ],
            "discard_counts_available": not source_scope_filter_enabled,
        },
        "class_session_count": len(sessions),
        "adapters": adapters,
        "input_adapter": {
            "ap_mapping_profile": project_inputs.ap_mapping_profile,
            "class_schedule_profile": project_inputs.class_schedule_profile,
            "class_session_id_method": (
                project_inputs.class_session_id_method
            ),
        },
    }
    shared_room_records = [
        merge.as_record()
        for merge in project_inputs.shared_room_session_merges
    ]
    if shared_room_records:
        effective_configuration["input_adapter"][
            "shared_room_normalization"
        ] = {
            "policy": "collapse_same_classroom_start_end",
            "enrollment_policy": "sum_source_section_enrollment",
            "merged_physical_session_count": len(shared_room_records),
            "source_row_count": sum(
                int(record["source_row_count"])
                for record in shared_room_records
            ),
            "sessions": shared_room_records,
        }
    if raw_requested:
        effective_configuration["input_adapter"].update(
            {
                "schedule_timezone": schedule_timezone,
                "schedule_start_date": schedule_start_date,
                "schedule_end_date": schedule_end_date,
                "location_scope": {
                    "source": "Sample_classrooms.csv:Location",
                    "locations": list(project_inputs.location_scope),
                    "ap_mapping_and_schedule": "only_listed_locations",
                },
            }
        )
    run = begin_corrected_run(
        output_root=output_root,
        project_name=project_name,
        analysis_inputs=analysis_inputs,
        requested_start=requested_start,
        requested_end=requested_end,
        effective_configuration=effective_configuration,
        source_precedence=[
            "valid_aruba_erm_time",
            "valid_frame_time_epoch",
            "declared_capture_source_rank_within_timestamp_source",
            "earliest_preferred_observation_time",
            "stable_source_path_and_frame",
        ],
        storage={
            "timestamp": "timestamp_ns_utc",
            "tsf": "uint64_microseconds",
            "duration": "int64_microseconds",
            "qbss_cu_raw": "nullable_uint8",
            "qbss_station_count": "nullable_uint16",
            "coverage_and_average": "float64_unrounded_ratio",
            "responsible_radio_ids": "list_string",
        },
        capture_plans=[],
        repository=repository,
        code_provenance=code_provenance,
        started_at=started_at,
        clock=clock,
    )
    if shared_room_records:
        record_run_event(
            run.workspace,
            event="shared_room_sessions_normalized",
            merged_physical_session_count=len(shared_room_records),
            source_row_count=sum(
                int(record["source_row_count"])
                for record in shared_room_records
            ),
            enrollment_policy="sum_source_section_enrollment",
            sessions=shared_room_records,
        )

    try:
        working_root = run.workspace.staging_directory / ".working"
        working_root.mkdir()
        staged = execute_corrected_stage(
            run,
            name="extract_and_normalize_captures",
            operation=lambda: _extract_and_stage(
                run,
                ranked,
                references,
                effective_extractor,
                configured_aps,
                sessions,
                working_root,
                capture_workers,
            ),
        )
        resolved = execute_corrected_stage(
            run,
            name="resolve_project_identity",
            operation=lambda: _resolve_identity(staged),
        )
        _warn_unobserved_project_scope(run, configured_aps, resolved.identity_map, sessions)
        run.effective_configuration["adapter_dispatch"] = staged.dispatch_counts
        run.effective_configuration["capture_stage_measurements"] = (
            staged.capture_stage_measurements
        )
        project_scope_counts = asdict(staged.scope_counts)
        if source_scope_filter_enabled:
            project_scope_counts.update(
                {
                    "input_rows": None,
                    "missing_ap_name_rows": None,
                    "outside_project_scope_rows": None,
                }
            )
        run.effective_configuration["project_scope_counts"] = (
            project_scope_counts
        )
        run.effective_configuration["radio_resolution_counts"] = asdict(
            resolved.counts
        )
        bounded = execute_corrected_stage(
            run,
            name="select_class_observations",
            operation=lambda: _select_staged_sessions(
                run,
                staged,
                resolved.identity_map,
                sessions,
                working_root,
                default_beacon_interval_tu=default_beacon_interval_tu,
                primary_ssid=cu_primary_ssid,
                cadence_beacon_interval_tu=cadence_beacon_interval_tu,
            ),
        )
        run.stages[-1]["warning_count"] += run.effective_configuration["beacon_quality"].get("tsf_anomaly_stream_count", 0)
        run.effective_configuration["bounded_working_set"].update(
            {
                "identity_evidence_rows": len(staged.identity_evidence),
                "class_window_rows": staged.class_window_rows,
                "class_window_part_files": staged.class_window_part_files,
                "maximum_session_input_rows": (
                    bounded.maximum_session_input_rows
                ),
                "selected_rows": bounded.selected_rows,
                "station_rollup_rows": bounded.station_rollup_rows,
                "radio_bin_detail_rows": bounded.radio_bin_detail_rows,
                "maximum_session_radio_bin_detail_rows": (
                    bounded.maximum_session_radio_bin_detail_rows
                ),
                "maximum_session_station_rollup_rows": (
                    bounded.maximum_session_station_rollup_rows
                ),
                "post_duplicate_handling_rows": (
                    bounded.post_duplicate_handling_rows
                ),
                "tsf_duplicate_views_collapsed": (
                    bounded.tsf_duplicate_views_collapsed
                ),
            }
        )
        run.effective_configuration["beacon_quality"].update(
            stream_rows=bounded.beacon_quality_stream_rows,
            window_rows=bounded.beacon_quality_window_rows,
        )
        comparison = BandComparisonAccumulator(
            configured_aps=configured_aps, identity_map=resolved.identity_map, class_sessions=sessions,
            run_id=run.workspace.run_id, project_name=project_name, display_timezone=schedule_timezone or "UTC",
            radio_summary=bounded.summaries.radio_class,
        )
        affected = AffectedStationAccumulator(
            class_sessions=sessions, ap_infrastructure=comparison.ap, classroom_infrastructure=comparison.rooms,
            run_id=run.workspace.run_id, project_name=project_name, display_timezone=schedule_timezone or "UTC",
        )
        run.effective_configuration["affected_stations"] = {
            "metric_id": AFFECTED_STATION_METRIC_ID, "high_minimum_raw": 128, "very_high_minimum_raw": 192,
            "weighting": "paired_station_duration_ratio_of_sums", "stale_station_assistance": True,
            "peak_policy": "complete_pairs_same_bin_session_active_radios", "cu_filled": False,
        }
        run.effective_configuration["band_comparison"] = {
            "metric_id": BAND_COMPARISON_METRIC_ID, "enablement_column": "6GHz_Enabled",
            "default_enablement": "project_observed_6_ghz", "explicit_false_conflict": "reject",
            "cu_primary_weighting": "valid_radio_duration", "equal_entity_session_views": True,
            "adoption": "actual_station_use_ratio_of_station_duration_sums",
            "cu_relative_station": "same_radio_bin_positive_station_pairs_ratio_of_sums",
            "cross_project_comparison": "manual", "two_point_four_included_when_sampled": True,
        }
        write_bounded_core_corrected_artifacts(
            run,
            identity_map=resolved.identity_map,
            selected_observation_parts=_read_selected_parts(
                bounded.selected_paths
            ),
        )
        write_precomputed_corrected_summary_artifacts(
            run,
            summaries=bounded.summaries,
        )
        write_corrected_radio_bin_report_artifacts(
            run,
            detail_parts=_read_radio_bin_parts(
                bounded.radio_bin_detail_paths, comparison=comparison, affected=affected,
            ),
            create_plots=radio_class_plots,
            primary_ssid=cu_primary_ssid,
            display_timezone=schedule_timezone or "UTC",
            class_sessions=sessions,
            configured_aps=configured_aps,
        )
        write_corrected_station_report_artifacts(
            run,
            rollup_parts=_read_station_parts(
                bounded.station_rollup_paths, comparison=comparison,
            ),
            class_report=bounded.station_class_report,
        )
        write_corrected_class_session_report_artifacts(
            run,
            class_sessions=sessions,
            classroom_summary=bounded.summaries.classroom_class,
            radio_summary=bounded.summaries.radio_class,
            station_class_report=bounded.station_class_report,
            display_timezone=schedule_timezone or "UTC",
        )
        write_corrected_beacon_quality_report_artifacts(
            run, parts=_read_beacon_quality_parts(bounded.beacon_quality_paths),
        )
        write_corrected_band_comparison_artifacts(run, comparison=comparison)
        write_corrected_affected_station_artifacts(run, affected=affected)
        shutil.rmtree(working_root)
        return complete_corrected_run(run)
    except KeyboardInterrupt:
        if run.workspace.staging_directory.exists():
            record_run_failure(
                run.workspace,
                reason="KeyboardInterrupt: run interrupted",
                understood=True,
            )
        raise
    except (TypeError, ValueError, OSError, RuntimeError) as exc:
        if run.workspace.staging_directory.exists():
            record_run_failure(
                run.workspace,
                reason=f"{type(exc).__name__}: {exc}",
                understood=True,
            )
        raise
    except Exception as exc:
        if run.workspace.staging_directory.exists():
            record_run_failure(
                run.workspace,
                reason=f"{type(exc).__name__}: {exc}",
                understood=False,
            )
        raise
