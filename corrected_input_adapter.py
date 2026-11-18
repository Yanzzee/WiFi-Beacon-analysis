"""Adapters from current sample metadata into corrected runner inputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from sample_processing import build_sample_tables
from band_comparison_inputs import validate_enablement_column


CURRENT_AP_PROFILE = "current_sample_aps"
CURRENT_CLASS_PROFILE = "current_sample_classes"
NORMALIZED_AP_PROFILE = "normalized_ap_mapping"
NORMALIZED_CLASS_PROFILE = "normalized_class_sessions"
RAW_SAMPLE_SCHEDULE_PROFILE = "raw_sample_schedule_csv"
CLASS_SESSION_ID_METHOD = "classroom_utc_start_utc_end"
PROVIDED_CLASS_SESSION_ID_METHOD = "provided"


@dataclass(frozen=True)
class CorrectedProjectInputs:
    """Canonical project tables plus the detected source-table shapes."""

    configured_aps: pd.DataFrame
    class_sessions: pd.DataFrame
    ap_mapping_profile: str
    class_schedule_profile: str
    class_session_id_method: str
    shared_room_session_merges: tuple["SharedRoomSessionMerge", ...] = ()
    location_scope: tuple[str, ...] | None = None


@dataclass(frozen=True)
class SharedRoomSessionMerge:
    """One set of source sections collapsed into a physical room session."""

    classroom: str
    class_start: str
    class_end: str
    courses: tuple[str, ...]
    source_row_count: int
    enrolled_total: int

    def as_record(self) -> dict[str, object]:
        return {
            "classroom": self.classroom,
            "class_start": self.class_start,
            "class_end": self.class_end,
            "courses": list(self.courses),
            "source_row_count": self.source_row_count,
            "enrolled_total": self.enrolled_total,
        }


def _read_table(path: Path, *, name: str) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"{name} is not a file: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"{name} must be CSV or Parquet: {path}")


def _required_text(value: Any, *, name: str) -> str:
    if value is None or value is pd.NA or pd.isna(value):
        raise ValueError(f"{name} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _utc_timestamp(value: Any, *, name: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _utc_text(timestamp: pd.Timestamp) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def _bin_width(value: Any, *, default_bin_width_us: int) -> int:
    if value is None or pd.isna(value):
        return default_bin_width_us
    if isinstance(value, bool):
        raise ValueError("bin_width_us must be a positive integer")
    try:
        width = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("bin_width_us must be a positive integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError("bin_width_us must be a positive integer")
    if width <= 0:
        raise ValueError("bin_width_us must be a positive integer")
    return width


def _optional_nonnegative_integer(value: Any, *, name: str) -> int | None:
    if (
        value is None
        or value is pd.NA
        or pd.isna(value)
        or str(value).strip() == ""
    ):
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a non-negative integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be a non-negative integer")
    if result < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return result


def _optional_text(value: Any) -> str | None:
    if value is None or value is pd.NA or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _collapse_shared_room_sessions(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, tuple[SharedRoomSessionMerge, ...]]:
    """Collapse raw schedule sections that occupy one physical room interval."""

    physical_key = ["location", "start_time", "end_time"]
    if not set(physical_key).issubset(frame.columns):
        return frame, ()

    frame = frame.copy()
    if "course" not in frame:
        frame["course"] = pd.NA
    if "capacity" not in frame:
        frame["capacity"] = pd.NA
    frame["source_section_count"] = 1
    frame["course_labels"] = frame.get(
        "course", pd.Series(index=frame.index, dtype="object")
    ).map(lambda value: _optional_text(value))
    frame["capacity_status"] = frame.get(
        "capacity", pd.Series(index=frame.index, dtype="object")
    ).map(lambda value: "missing" if _optional_text(value) is None else "available")
    collapsed_rows: list[pd.Series] = []
    merges: list[SharedRoomSessionMerge] = []
    for _key, group in frame.groupby(
        physical_key,
        sort=False,
        dropna=False,
    ):
        row = group.iloc[0].copy()
        row["source_section_count"] = len(group)
        courses = tuple(
            dict.fromkeys(
                text
                for value in group["course"].tolist()
                if (text := _optional_text(value)) is not None
            )
        )
        row["course_labels"] = " | ".join(courses) or None
        capacities = sorted(
            {
                float(value)
                for value in pd.to_numeric(group["capacity"], errors="coerce")
                if not pd.isna(value)
            }
        )
        if len(capacities) > 1:
            row["capacity"] = None
            row["capacity_status"] = "conflict"
        elif len(capacities) == 1:
            row["capacity"] = capacities[0]
            row["capacity_status"] = "available"
        else:
            row["capacity"] = None
            row["capacity_status"] = "missing"
        if len(group) > 1:
            enrolled = pd.to_numeric(group["enrolled"], errors="raise")
            enrolled_total = int(enrolled.sum())
            row["enrolled"] = enrolled_total
            row["course"] = " + ".join(courses)
            start = _utc_timestamp(
                row["start_time"],
                name="shared-room session start",
            )
            end = _utc_timestamp(
                row["end_time"],
                name="shared-room session end",
            )
            merges.append(
                SharedRoomSessionMerge(
                    classroom=str(row["location"]).strip(),
                    class_start=_utc_text(start),
                    class_end=_utc_text(end),
                    courses=courses,
                    source_row_count=len(group),
                    enrolled_total=enrolled_total,
                )
            )
        collapsed_rows.append(row)

    collapsed = pd.DataFrame(collapsed_rows, columns=frame.columns)
    return collapsed.reset_index(drop=True), tuple(merges)


def adapt_ap_mapping(frame: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Expose current or normalized AP mapping columns to project scope."""

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("AP mapping must be a pandas DataFrame")
    result = frame.copy(deep=True)
    if "Hostname" not in result and result.index.name == "Hostname":
        result = result.reset_index()
    required = {"Hostname", "Building-Room"}
    missing = sorted(required.difference(result.columns))
    if missing:
        raise ValueError(
            "AP mapping is missing required columns: " + ", ".join(missing)
        )
    profile = (
        CURRENT_AP_PROFILE
        if {"Building", "Room"}.intersection(result.columns)
        else NORMALIZED_AP_PROFILE
    )
    return validate_enablement_column(result), profile


def adapt_class_sessions(
    frame: pd.DataFrame,
    *,
    default_bin_width_us: int,
) -> tuple[pd.DataFrame, str, str]:
    """Normalize current or corrected class rows and assign stable IDs."""

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("class schedule must be a pandas DataFrame")
    if (
        not isinstance(default_bin_width_us, int)
        or isinstance(default_bin_width_us, bool)
        or default_bin_width_us <= 0
    ):
        raise ValueError("default_bin_width_us must be a positive integer")

    corrected_columns = {"classroom", "class_start", "class_end"}
    current_columns = {"location", "start_time", "end_time"}
    if corrected_columns.issubset(frame.columns):
        classroom_column = "classroom"
        start_column = "class_start"
        end_column = "class_end"
        profile = NORMALIZED_CLASS_PROFILE
    elif current_columns.issubset(frame.columns):
        classroom_column = "location"
        start_column = "start_time"
        end_column = "end_time"
        profile = CURRENT_CLASS_PROFILE
    else:
        raise ValueError(
            "class schedule must contain either classroom/class_start/"
            "class_end or location/start_time/end_time"
        )

    rows: list[dict[str, object]] = []
    explicit_ids = "class_session_id" in frame.columns
    for position, record in enumerate(frame.to_dict(orient="records")):
        classroom = _required_text(
            record[classroom_column],
            name=f"class schedule row {position} classroom",
        )
        start = _utc_timestamp(
            record[start_column],
            name=f"class schedule row {position} start",
        )
        end = _utc_timestamp(
            record[end_column],
            name=f"class schedule row {position} end",
        )
        if end <= start:
            raise ValueError(
                f"class schedule row {position} end must be after start"
            )
        derived_id = (
            f"{classroom}|{_utc_text(start)}|{_utc_text(end)}"
        )
        session_id = (
            _required_text(
                record["class_session_id"],
                name=f"class schedule row {position} class_session_id",
            )
            if explicit_ids
            else derived_id
        )
        rows.append(
            {
                "class_session_id": session_id,
                "classroom": classroom,
                "class_start": start,
                "class_end": end,
                "bin_width_us": _bin_width(
                    record.get("bin_width_us"),
                    default_bin_width_us=default_bin_width_us,
                ),
                "course": _optional_text(record.get("course")),
                "enrolled": _optional_nonnegative_integer(
                    record.get("enrolled"),
                    name=f"class schedule row {position} enrolled",
                ),
                "capacity": _optional_nonnegative_integer(
                    record.get("capacity"),
                    name=f"class schedule row {position} capacity",
                ),
                "source_section_count": _optional_nonnegative_integer(
                    record.get("source_section_count", 1),
                    name=f"class schedule row {position} source_section_count",
                ) or 1,
                "course_labels": _optional_text(
                    record.get("course_labels", record.get("course"))
                ),
                "capacity_status": _optional_text(
                    record.get("capacity_status")
                ) or (
                    "missing"
                    if _optional_text(record.get("capacity")) is None
                    else "available"
                ),
            }
        )

    result = pd.DataFrame(
        rows,
        columns=[
            "class_session_id",
            "classroom",
            "class_start",
            "class_end",
            "bin_width_us",
            "course",
            "enrolled",
            "capacity",
            "source_section_count",
            "course_labels",
            "capacity_status",
        ],
    )
    if result.empty:
        raise ValueError("class schedule must contain at least one session")
    physical_key = ["classroom", "class_start", "class_end"]
    duplicate_physical = result.duplicated(physical_key, keep=False)
    if duplicate_physical.any():
        duplicate = result.loc[duplicate_physical, physical_key].iloc[0]
        raise ValueError(
            "class schedule contains duplicate rows for one classroom and "
            "time interval; combine shared-room sections before analysis: "
            f"{duplicate['classroom']} {_utc_text(duplicate['class_start'])} "
            f"to {_utc_text(duplicate['class_end'])}"
        )
    duplicate_ids = result["class_session_id"].duplicated(keep=False)
    if duplicate_ids.any():
        values = sorted(result.loc[duplicate_ids, "class_session_id"].unique())
        raise ValueError(
            "class_session_id values must be unique: " + ", ".join(values)
        )
    return (
        result.sort_values(
            ["class_start", "classroom", "class_session_id"],
            kind="stable",
        ).reset_index(drop=True),
        profile,
        (
            PROVIDED_CLASS_SESSION_ID_METHOD
            if explicit_ids
            else CLASS_SESSION_ID_METHOD
        ),
    )


def load_corrected_project_inputs(
    *,
    ap_mapping_path: str | Path,
    class_schedule_path: str | Path,
    default_bin_width_us: int,
) -> CorrectedProjectInputs:
    """Load and adapt the two current project metadata inputs."""

    mapping, mapping_profile = adapt_ap_mapping(
        _read_table(Path(ap_mapping_path), name="ap_mapping_path")
    )
    sessions, schedule_profile, session_id_method = adapt_class_sessions(
        _read_table(Path(class_schedule_path), name="class_schedule_path"),
        default_bin_width_us=default_bin_width_us,
    )
    return CorrectedProjectInputs(
        configured_aps=mapping,
        class_sessions=sessions,
        ap_mapping_profile=mapping_profile,
        class_schedule_profile=schedule_profile,
        class_session_id_method=session_id_method,
    )


def load_raw_sample_project_inputs(
    *,
    ap_mapping_path: str | Path,
    classroom_path: str | Path,
    class_schedule_path: str | Path,
    timezone: str,
    start_date: str,
    end_date: str,
    default_bin_width_us: int,
) -> CorrectedProjectInputs:
    """Load the three CSVs using Sample_classrooms.Location as project scope."""

    paths = {
        "ap_mapping_path": Path(ap_mapping_path),
        "classroom_path": Path(classroom_path),
        "class_schedule_path": Path(class_schedule_path),
    }
    for name, path in paths.items():
        if path.suffix.lower() != ".csv":
            raise ValueError(f"{name} must be a CSV file: {path}")

    tables = build_sample_tables(
        _read_table(paths["ap_mapping_path"], name="ap_mapping_path"),
        _read_table(paths["classroom_path"], name="classroom_path"),
        _read_table(paths["class_schedule_path"], name="class_schedule_path"),
        timezone,
        start_date,
        end_date,
        reject_malformed_periods=True,
    )
    mapping, mapping_profile = adapt_ap_mapping(tables.sample_aps)
    normalized_classes, shared_room_merges = _collapse_shared_room_sessions(
        tables.sample_classes
    )
    sessions, _profile, session_id_method = adapt_class_sessions(
        normalized_classes,
        default_bin_width_us=default_bin_width_us,
    )
    return CorrectedProjectInputs(
        configured_aps=mapping,
        class_sessions=sessions,
        ap_mapping_profile=mapping_profile,
        class_schedule_profile=RAW_SAMPLE_SCHEDULE_PROFILE,
        class_session_id_method=session_id_method,
        shared_room_session_merges=shared_room_merges,
        location_scope=tuple(sorted(tables.sample_classrooms["Location"].unique())),
    )
