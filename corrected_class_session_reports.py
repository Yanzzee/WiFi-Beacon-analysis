"""Bounded class-session review package from accepted corrected semantics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Any
from zoneinfo import ZoneInfo

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd

from corrected_summary_output import (
    CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
    RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
)
from corrected_station_reports import STATION_CLASS_REPORT_COLUMNS


REPORT_DIRECTORY = Path("reports") / "class_sessions"
CLASS_SESSION_REPORT_FILENAME = "class_session_report.csv"
RATIO_HISTOGRAM_FILENAME = "peak_client_count_per_enrolled_histogram.png"
ENROLLMENT_SCATTER_FILENAME = "enrollment_vs_peak_client_count.png"
METHODOLOGY_FILENAME = "methodology.txt"
CLASS_SESSION_REPORT_METRIC_ID = "beacon.metric/corrected-class-session-report/v1"

CLASS_SESSION_REPORT_COLUMNS = (
    "run_id",
    "schema_id",
    "metric_id",
    "class_session_id",
    "classroom",
    "class_start_utc",
    "class_end_utc",
    "local_date",
    "local_start",
    "local_end",
    "display_timezone",
    "course_labels",
    "source_section_count",
    "combined_enrollment",
    "room_capacity",
    "capacity_status",
    "radio_count_2_4_ghz",
    "radio_count_5_ghz",
    "radio_count_6_ghz",
    "radio_count_unknown",
    "radio_count_total",
    "radio_cu_coverage_min",
    "radio_cu_coverage_mean",
    "radios_below_full_cu_coverage",
    "high_possible_connected_max_us",
    "high_possible_connected_responsible_radio_ids",
    "high_actual_observed_max_us",
    "high_actual_observed_responsible_radio_ids",
    "very_high_possible_connected_max_us",
    "very_high_possible_connected_responsible_radio_ids",
    "very_high_actual_observed_max_us",
    "very_high_actual_observed_responsible_radio_ids",
    "available_station_bin_count",
    "unavailable_station_bin_count",
    "stale_assisted_station_bin_count",
    "peak_client_count",
    "peak_bin_number",
    "peak_bin_start_utc",
    "peak_stale_assisted",
    "peak_client_count_per_enrolled",
)


@dataclass(frozen=True)
class WrittenClassSessionReportPackage:
    directory: Path
    rows: int
    files: tuple[Path, ...]


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def _utc_series(values: pd.Series, *, label: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    if parsed.isna().any():
        raise ValueError(f"{label} contains invalid timestamps")
    return parsed


def _coverage_by_session(radio_summary: pd.DataFrame) -> pd.DataFrame:
    if radio_summary.empty:
        return pd.DataFrame(
            columns=[
                "class_session_id",
                "radio_cu_coverage_min",
                "radio_cu_coverage_mean",
                "radios_below_full_cu_coverage",
            ]
        )
    rows: list[dict[str, object]] = []
    for session_id, group in radio_summary.groupby("class_session_id", sort=True):
        coverage = pd.to_numeric(group["cu_coverage"], errors="coerce")
        valid = coverage.dropna()
        rows.append(
            {
                "class_session_id": str(session_id),
                "radio_cu_coverage_min": (
                    None if valid.empty else float(valid.min())
                ),
                "radio_cu_coverage_mean": (
                    None if valid.empty else float(valid.mean())
                ),
                "radios_below_full_cu_coverage": int(coverage.lt(1.0).sum()),
            }
        )
    return pd.DataFrame(rows)


def build_class_session_report(
    class_sessions: pd.DataFrame,
    classroom_summary: pd.DataFrame,
    radio_summary: pd.DataFrame,
    station_class_report: pd.DataFrame,
    *,
    display_timezone: str,
) -> pd.DataFrame:
    """Join accepted schedule, CU/streak, and station facts at session grain."""

    timezone = ZoneInfo(display_timezone)
    _require_columns(
        class_sessions,
        {"class_session_id", "classroom", "class_start", "class_end"},
        "class sessions",
    )
    _require_columns(
        classroom_summary,
        set(CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA.names),
        "classroom summary",
    )
    _require_columns(
        radio_summary,
        set(RADIO_CLASS_SUMMARY_ARROW_SCHEMA.names),
        "radio summary",
    )
    _require_columns(
        station_class_report,
        set(STATION_CLASS_REPORT_COLUMNS),
        "station class report",
    )

    schedule = class_sessions.copy()
    if schedule["class_session_id"].astype(str).duplicated().any():
        raise ValueError("class sessions require unique class_session_id values")
    schedule["class_session_id"] = schedule["class_session_id"].astype(str)
    schedule["class_start"] = _utc_series(schedule["class_start"], label="class_start")
    schedule["class_end"] = _utc_series(schedule["class_end"], label="class_end")
    local_start = schedule["class_start"].dt.tz_convert(timezone)
    local_end = schedule["class_end"].dt.tz_convert(timezone)
    schedule_context = pd.DataFrame(
        {
            "class_session_id": schedule["class_session_id"],
            "classroom": schedule["classroom"].astype(str),
            "class_start_utc": schedule["class_start"].map(
                lambda value: value.isoformat().replace("+00:00", "Z")
            ),
            "class_end_utc": schedule["class_end"].map(
                lambda value: value.isoformat().replace("+00:00", "Z")
            ),
            "local_date": local_start.dt.strftime("%Y-%m-%d"),
            "local_start": local_start.dt.strftime("%H:%M"),
            "local_end": local_end.dt.strftime("%H:%M"),
            "display_timezone": display_timezone,
            "course_labels": schedule.get(
                "course_labels", schedule.get("course", pd.Series(pd.NA, index=schedule.index))
            ),
            "source_section_count": pd.to_numeric(
                schedule.get(
                    "source_section_count",
                    pd.Series(1, index=schedule.index),
                ),
                errors="coerce",
            ).fillna(1).astype(int),
            "combined_enrollment": pd.to_numeric(
                schedule.get("enrolled", pd.Series(pd.NA, index=schedule.index)),
                errors="coerce",
            ),
            "room_capacity": pd.to_numeric(
                schedule.get("capacity", pd.Series(pd.NA, index=schedule.index)),
                errors="coerce",
            ),
            "capacity_status": schedule.get(
                "capacity_status",
                pd.Series(pd.NA, index=schedule.index),
            ),
        }
    )
    inferred_capacity_status = np.where(
        schedule_context["room_capacity"].notna(), "available", "missing"
    )
    schedule_context["capacity_status"] = (
        schedule_context["capacity_status"]
        .astype("string")
        .fillna(pd.Series(inferred_capacity_status, index=schedule_context.index))
    )
    schedule_context.loc[
        schedule_context["capacity_status"].eq("conflict"), "room_capacity"
    ] = np.nan

    classroom = classroom_summary.copy()
    if classroom["class_session_id"].astype(str).duplicated().any():
        raise ValueError("classroom summary requires one row per class session")
    classroom["class_session_id"] = classroom["class_session_id"].astype(str)
    classroom_columns = [
        "run_id",
        "schema_id",
        "class_session_id",
        "radio_count_2_4_ghz",
        "radio_count_5_ghz",
        "radio_count_6_ghz",
        "radio_count_unknown",
        "radio_count_total",
        "high_possible_connected_max_us",
        "high_possible_connected_responsible_radio_ids",
        "high_actual_observed_max_us",
        "high_actual_observed_responsible_radio_ids",
        "very_high_possible_connected_max_us",
        "very_high_possible_connected_responsible_radio_ids",
        "very_high_actual_observed_max_us",
        "very_high_actual_observed_responsible_radio_ids",
    ]

    station = station_class_report.loc[
        station_class_report["rollup_type"].astype(str).eq("classroom")
    ].copy()
    if station["class_session_id"].astype(str).duplicated().any():
        raise ValueError("station report requires one classroom row per session")
    station["class_session_id"] = station["class_session_id"].astype(str)
    station = station.rename(
        columns={
            "available_bin_count": "available_station_bin_count",
            "unavailable_bin_count": "unavailable_station_bin_count",
            "stale_assisted_bin_count": "stale_assisted_station_bin_count",
        }
    )
    station_columns = [
        "class_session_id",
        "available_station_bin_count",
        "unavailable_station_bin_count",
        "stale_assisted_station_bin_count",
        "peak_client_count",
        "peak_bin_number",
        "peak_bin_start_utc",
        "peak_stale_assisted",
        "peak_client_count_per_enrolled",
    ]

    coverage = _coverage_by_session(radio_summary)
    result = (
        schedule_context.merge(
            classroom.loc[:, classroom_columns],
            on="class_session_id",
            how="left",
            validate="one_to_one",
        )
        .merge(coverage, on="class_session_id", how="left", validate="one_to_one")
        .merge(
            station.loc[:, station_columns],
            on="class_session_id",
            how="left",
            validate="one_to_one",
        )
    )
    if result["run_id"].isna().any():
        raise ValueError("every configured class session requires a classroom summary")
    result.insert(2, "metric_id", CLASS_SESSION_REPORT_METRIC_ID)
    return result.loc[:, CLASS_SESSION_REPORT_COLUMNS].sort_values(
        ["class_start_utc", "classroom", "class_session_id"], kind="stable"
    ).reset_index(drop=True)


def _write_ratio_histogram(report: pd.DataFrame, path: Path) -> None:
    values = pd.to_numeric(
        report["peak_client_count_per_enrolled"], errors="coerce"
    ).dropna()
    figure = Figure(figsize=(16, 10), constrained_layout=True)
    FigureCanvasAgg(figure)
    axis = figure.subplots()
    if values.empty:
        axis.text(0.5, 0.5, "No eligible sessions", transform=axis.transAxes, ha="center")
    else:
        bins = min(60, max(10, int(np.ceil(2 * np.sqrt(len(values))))))
        axis.hist(values, bins=bins, color="#4c78a8", edgecolor="white")
    axis.set_title("Peak client count per enrolled")
    axis.set_xlabel("Peak client count per enrolled")
    axis.set_ylabel("Class sessions")
    axis.grid(axis="y", alpha=0.25)
    figure.savefig(path, dpi=140)


def _write_enrollment_scatter(report: pd.DataFrame, path: Path) -> None:
    enrolled = pd.to_numeric(report["combined_enrollment"], errors="coerce")
    peak = pd.to_numeric(report["peak_client_count"], errors="coerce")
    valid = enrolled.gt(0) & peak.notna()
    stale = report["peak_stale_assisted"].astype("boolean").fillna(False)
    figure = Figure(figsize=(16, 12), constrained_layout=True)
    FigureCanvasAgg(figure)
    axis = figure.subplots()
    fresh = valid & ~stale
    assisted = valid & stale
    if fresh.any():
        axis.scatter(enrolled[fresh], peak[fresh], s=22, color="#4c78a8", label="Measured peak")
    if assisted.any():
        axis.scatter(
            enrolled[assisted],
            peak[assisted],
            s=28,
            facecolors="none",
            edgecolors="#4c78a8",
            label="Stale-assisted peak",
        )
    if not valid.any():
        axis.text(0.5, 0.5, "No eligible sessions", transform=axis.transAxes, ha="center")
    axis.set_title("Enrollment and peak classroom client count")
    axis.set_xlabel("Combined enrollment")
    axis.set_ylabel("Peak client count")
    axis.set_xlim(left=0)
    axis.set_ylim(bottom=0)
    axis.grid(alpha=0.25)
    if fresh.any() or assisted.any():
        axis.legend()
    figure.savefig(path, dpi=140)


def _methodology(display_timezone: str) -> str:
    return "\n".join(
        [
            "Corrected class-session report methodology",
            "",
            "Grain and schedule context",
            "- One row represents one physical classroom session.",
            "- Simultaneous source sections are combined; course labels remain",
            "  traceable, source_section_count records the number of sections,",
            "  and combined_enrollment is their accepted sum.",
            "- room_capacity is descriptive schedule context only. Missing values",
            "  remain null; conflicting source values are null with status conflict.",
            "- No room-capacity utilization or QBSS admission-capacity calculation",
            "  is included.",
            f"- Local labels use {display_timezone}; UTC endpoints remain present.",
            "",
            "CU and station semantics",
            "- CU coverage and high/very-high streak columns come from the accepted",
            "  corrected per-radio and classroom summaries.",
            "- Peak client count is the maximum available complete classroom/bin",
            "  total, including permitted stale-assisted values.",
            "- Peak client count per enrolled is null when enrollment is zero or",
            "  missing. Plots exclude those ineligible sessions.",
            "- A hollow scatter marker identifies a stale-assisted peak.",
            "",
            "Files",
            f"- {CLASS_SESSION_REPORT_FILENAME}: joined session review table.",
            f"- {RATIO_HISTOGRAM_FILENAME}: eligible session ratio distribution.",
            f"- {ENROLLMENT_SCATTER_FILENAME}: enrollment versus peak clients.",
            f"- {METHODOLOGY_FILENAME}: this file.",
            "",
        ]
    )


def write_class_session_report_package(
    run_directory: str | Path,
    *,
    class_sessions: pd.DataFrame,
    classroom_summary: pd.DataFrame,
    radio_summary: pd.DataFrame,
    station_class_report: pd.DataFrame,
    display_timezone: str | None = None,
) -> WrittenClassSessionReportPackage:
    """Write one bounded human-readable class-session package."""

    timezone_name = display_timezone.strip() if display_timezone else "UTC"
    ZoneInfo(timezone_name)
    destination = Path(run_directory) / REPORT_DIRECTORY
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
    destination.mkdir(parents=True)
    try:
        report = build_class_session_report(
            class_sessions,
            classroom_summary,
            radio_summary,
            station_class_report,
            display_timezone=timezone_name,
        )
        report.to_csv(
            destination / CLASS_SESSION_REPORT_FILENAME,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            na_rep="",
        )
        _write_ratio_histogram(report, destination / RATIO_HISTOGRAM_FILENAME)
        _write_enrollment_scatter(report, destination / ENROLLMENT_SCATTER_FILENAME)
        (destination / METHODOLOGY_FILENAME).write_text(
            _methodology(timezone_name), encoding="utf-8", newline="\n"
        )
    except BaseException:
        shutil.rmtree(destination, ignore_errors=True)
        raise
    files = tuple(sorted(path for path in destination.iterdir() if path.is_file()))
    return WrittenClassSessionReportPackage(
        directory=destination,
        rows=len(report),
        files=files,
    )
