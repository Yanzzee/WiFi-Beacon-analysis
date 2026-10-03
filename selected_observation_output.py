"""Durable selected-observation Parquet dataset.

The logical artifact contains at most one selected beacon per
``(class_session_id, bssid, bin_number)``.  It is stored below a run directory
and partitioned by the UTC date of ``bin_start``.  Missing QBSS values remain
null; this module does not calculate summaries or synthesize missing beacons.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
from pathlib import Path
import re
import shutil
import uuid
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


RESULT_SCHEMA_ID = "beacon.schema/analysis-results/v1"
CORRECTED_METRIC_ID = "beacon.metric/corrected-cu/v1"
SELECTED_OBSERVATIONS_DIRECTORY = "selected_observations"

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_UTC_TIMESTAMP = pa.timestamp("ns", tz="UTC")

SELECTED_OBSERVATION_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("class_date", pa.date32(), nullable=False),
        pa.field("class_session_id", pa.string(), nullable=False),
        pa.field("classroom", pa.string(), nullable=False),
        pa.field("bin_number", pa.int64(), nullable=False),
        pa.field("bin_start", _UTC_TIMESTAMP, nullable=False),
        pa.field("bin_end", _UTC_TIMESTAMP, nullable=False),
        pa.field("bin_duration_us", pa.int64(), nullable=False),
        pa.field("width_status", pa.string(), nullable=False),
        pa.field("bssid", pa.string(), nullable=False),
        pa.field("ssid", pa.string()),
        pa.field("radio_group_id", pa.string()),
        pa.field("ap_id", pa.string()),
        pa.field("band", pa.string()),
        pa.field("channel", pa.int32()),
        pa.field("observation_time", _UTC_TIMESTAMP, nullable=False),
        pa.field("observation_time_source", pa.string(), nullable=False),
        pa.field("tsf_us", pa.uint64()),
        pa.field("tsf_origin", _UTC_TIMESTAMP),
        pa.field("tsf_derived_time", _UTC_TIMESTAMP),
        pa.field("timing_method", pa.string(), nullable=False),
        pa.field("tsf_status", pa.string(), nullable=False),
        pa.field("unusable_tsf_reason", pa.string()),
        pa.field("source_capture", pa.string(), nullable=False),
        pa.field("source_frame", pa.string(), nullable=False),
        pa.field("duplicate_views_collapsed", pa.uint32(), nullable=False),
        pa.field("skipped_intervals_before", pa.int64()),
        pa.field("qbss_cu_raw", pa.uint8()),
        pa.field("qbss_station_count", pa.uint16()),
        pa.field("qbss_admission_capacity", pa.uint16()),
    ]
)

_GENERATED_COLUMNS = {"run_id", "schema_id", "metric_id", "class_date"}
_REQUIRED_INPUT_COLUMNS = set(SELECTED_OBSERVATION_ARROW_SCHEMA.names).difference(
    _GENERATED_COLUMNS
)
_PARTITION_SCHEMA = pa.schema([pa.field("class_date", pa.date32())])
_PARTITION_FILE_SCHEMA = SELECTED_OBSERVATION_ARROW_SCHEMA.remove(
    SELECTED_OBSERVATION_ARROW_SCHEMA.get_field_index("class_date")
)
_PARTITION_FILE_COLUMNS = _PARTITION_FILE_SCHEMA.names


@dataclass(frozen=True)
class _BuiltSelectedObservations:
    logical: pd.DataFrame
    table: pa.Table


@dataclass(frozen=True)
class WrittenSelectedObservations:
    path: Path
    rows: int
    first_observation_time: pd.Timestamp | None
    last_observation_time: pd.Timestamp | None


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if hasattr(missing, "__len__"):
        return False
    return bool(missing)


def _required_text(value: Any, *, name: str) -> str:
    if _is_missing(value):
        raise ValueError(f"selected observations require {name}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"selected observations require {name}")
    return text


def _optional_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _integer(
    value: Any,
    *,
    name: str,
    minimum: int,
    maximum: int,
    nullable: bool,
) -> int | None:
    if _is_missing(value) or str(value).strip() == "":
        if nullable:
            return None
        raise ValueError(f"selected observations require {name}")
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    if number < minimum or number > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return number


def _utc_timestamp(value: Any, *, name: str, nullable: bool) -> pd.Timestamp | None:
    if _is_missing(value) or str(value).strip() == "":
        if nullable:
            return None
        raise ValueError(f"selected observations require {name}")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return timestamp.tz_convert("UTC")


def _rows_to_table(
    rows: list[dict[str, Any]],
    *,
    schema: pa.Schema = SELECTED_OBSERVATION_ARROW_SCHEMA,
) -> pa.Table:
    # A logical pandas frame represents nullable timestamps as ``NaT`` and
    # nullable integers as ``NaN`` when converted back to records. Arrow
    # expects Python ``None`` for those nulls.
    normalized = [
        {
            name: None if _is_missing(value) else value
            for name, value in row.items()
        }
        for row in rows
    ]
    return pa.Table.from_pylist(normalized, schema=schema)


def _class_date_partition_slices(
    logical: pd.DataFrame,
) -> list[tuple[date, int, int]]:
    """Return contiguous date slices for the already date-ordered rows."""

    slices = []
    offset = 0
    partition_counts = logical.groupby("class_date", sort=False).size()
    for class_date, count in partition_counts.items():
        length = int(count)
        slices.append((class_date, offset, length))
        offset += length
    if offset != len(logical):
        raise AssertionError("selected-observation date partitions lost rows")
    return slices


def _build_selected_observation_artifact(
    selected: pd.DataFrame,
    *,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> _BuiltSelectedObservations:
    """Validate once and retain both logical and physical representations."""

    if not isinstance(selected, pd.DataFrame):
        raise TypeError("selected must be a pandas DataFrame")
    if not _SAFE_RUN_ID.fullmatch(str(run_id)):
        raise ValueError("run_id must be one safe path segment")
    missing = sorted(_REQUIRED_INPUT_COLUMNS.difference(selected.columns))
    if missing:
        raise ValueError(
            "selected observations are missing durable columns: "
            + ", ".join(missing)
        )
    if (
        "is_selected" in selected
        and not selected["is_selected"].fillna(False).all()
    ):
        raise ValueError("selected observations cannot contain unselected rows")

    rows: list[dict[str, Any]] = []
    for values in selected.to_dict(orient="records"):
        bin_start = _utc_timestamp(
            values["bin_start"], name="bin_start", nullable=False
        )
        bin_end = _utc_timestamp(
            values["bin_end"], name="bin_end", nullable=False
        )
        observation_time = _utc_timestamp(
            values["observation_time"],
            name="observation_time",
            nullable=False,
        )
        bin_duration_us = _integer(
            values["bin_duration_us"],
            name="bin_duration_us",
            minimum=1,
            maximum=2**63 - 1,
            nullable=False,
        )
        assert bin_start is not None
        assert bin_end is not None
        assert observation_time is not None
        assert bin_duration_us is not None
        duration_ns = int(bin_end.value) - int(bin_start.value)
        if duration_ns <= 0 or duration_ns % 1_000 != 0:
            raise ValueError("bin interval must have a positive whole-microsecond duration")
        if duration_ns // 1_000 != bin_duration_us:
            raise ValueError("bin_duration_us must equal bin_end minus bin_start")

        timing_method = _required_text(
            values["timing_method"], name="timing_method"
        )
        tsf_us = _integer(
            values["tsf_us"],
            name="tsf_us",
            minimum=0,
            maximum=2**64 - 1,
            nullable=True,
        )
        tsf_origin = _utc_timestamp(
            values["tsf_origin"], name="tsf_origin", nullable=True
        )
        tsf_derived_time = _utc_timestamp(
            values["tsf_derived_time"],
            name="tsf_derived_time",
            nullable=True,
        )
        unusable_tsf_reason = _optional_text(values["unusable_tsf_reason"])
        if timing_method == "tsf_derived":
            if tsf_us is None or tsf_origin is None or tsf_derived_time is None:
                raise ValueError(
                    "tsf_derived rows require tsf_us, tsf_origin, and tsf_derived_time"
                )
            if unusable_tsf_reason is not None:
                raise ValueError("tsf_derived rows cannot have an unusable TSF reason")
        elif timing_method == "observation_time_fallback":
            if tsf_origin is not None or tsf_derived_time is not None:
                raise ValueError(
                    "observation-time fallback rows cannot have mapped TSF times"
                )
            if unusable_tsf_reason is None:
                raise ValueError(
                    "observation-time fallback rows require an unusable TSF reason"
                )
        else:
            raise ValueError(f"unsupported timing_method: {timing_method}")

        width_status = _required_text(
            values["width_status"], name="width_status"
        )
        if width_status not in {"complete", "partial"}:
            raise ValueError("width_status must be complete or partial")

        rows.append(
            {
                "run_id": str(run_id),
                "schema_id": str(schema_id),
                "metric_id": str(metric_id),
                "class_date": bin_start.date(),
                "class_session_id": _required_text(
                    values["class_session_id"], name="class_session_id"
                ),
                "classroom": _required_text(
                    values["classroom"], name="classroom"
                ),
                "bin_number": _integer(
                    values["bin_number"],
                    name="bin_number",
                    minimum=0,
                    maximum=2**63 - 1,
                    nullable=False,
                ),
                "bin_start": bin_start,
                "bin_end": bin_end,
                "bin_duration_us": bin_duration_us,
                "width_status": width_status,
                "bssid": _required_text(values["bssid"], name="bssid"),
                "ssid": _optional_text(values["ssid"]),
                "radio_group_id": _optional_text(values["radio_group_id"]),
                "ap_id": _optional_text(values["ap_id"]),
                "band": _optional_text(values["band"]),
                "channel": _integer(
                    values["channel"],
                    name="channel",
                    minimum=0,
                    maximum=2**31 - 1,
                    nullable=True,
                ),
                "observation_time": observation_time,
                "observation_time_source": _required_text(
                    values["observation_time_source"],
                    name="observation_time_source",
                ),
                "tsf_us": tsf_us,
                "tsf_origin": tsf_origin,
                "tsf_derived_time": tsf_derived_time,
                "timing_method": timing_method,
                "tsf_status": _required_text(
                    values["tsf_status"], name="tsf_status"
                ),
                "unusable_tsf_reason": unusable_tsf_reason,
                "source_capture": _required_text(
                    values["source_capture"], name="source_capture"
                ),
                "source_frame": _required_text(
                    values["source_frame"], name="source_frame"
                ),
                "duplicate_views_collapsed": _integer(
                    values["duplicate_views_collapsed"],
                    name="duplicate_views_collapsed",
                    minimum=0,
                    maximum=2**32 - 1,
                    nullable=False,
                ),
                "skipped_intervals_before": _integer(
                    values["skipped_intervals_before"],
                    name="skipped_intervals_before",
                    minimum=0,
                    maximum=2**63 - 1,
                    nullable=True,
                ),
                "qbss_cu_raw": _integer(
                    values["qbss_cu_raw"],
                    name="qbss_cu_raw",
                    minimum=0,
                    maximum=255,
                    nullable=True,
                ),
                "qbss_station_count": _integer(
                    values["qbss_station_count"],
                    name="qbss_station_count",
                    minimum=0,
                    maximum=65_535,
                    nullable=True,
                ),
                "qbss_admission_capacity": _integer(
                    values["qbss_admission_capacity"],
                    name="qbss_admission_capacity",
                    minimum=0,
                    maximum=65_535,
                    nullable=True,
                ),
            }
        )

    logical = pd.DataFrame(rows, columns=SELECTED_OBSERVATION_ARROW_SCHEMA.names)
    if not logical.empty:
        duplicate = logical.duplicated(
            subset=["class_session_id", "bssid", "bin_number"], keep=False
        )
        if duplicate.any():
            raise ValueError(
                "selected observations require one row per class/BSSID/bin"
            )
        logical = logical.sort_values(
            ["class_date", "class_session_id", "bssid", "bin_number"],
            kind="stable",
        ).reset_index(drop=True)

    # Arrow construction is the final range/nullability check for the physical
    # contract, including unsigned QBSS and TSF fields.
    table = _rows_to_table(logical.to_dict(orient="records"))
    return _BuiltSelectedObservations(logical=logical, table=table)


def build_selected_observations(
    selected: pd.DataFrame,
    *,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> pd.DataFrame:
    """Validate and return the durable logical selected-observation rows."""

    return _build_selected_observation_artifact(
        selected,
        run_id=run_id,
        schema_id=schema_id,
        metric_id=metric_id,
    ).logical


def read_selected_observations(path: str | Path) -> pa.Table:
    """Read a writer-produced dataset with the exact durable Arrow schema."""

    dataset = ds.dataset(
        Path(path),
        format="parquet",
        partitioning=ds.partitioning(_PARTITION_SCHEMA, flavor="hive"),
    )
    table = dataset.to_table()
    table = table.select(SELECTED_OBSERVATION_ARROW_SCHEMA.names)
    return table.cast(SELECTED_OBSERVATION_ARROW_SCHEMA)


def write_selected_observations(
    selected: pd.DataFrame,
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> Path:
    """Atomically write ``selected_observations/`` partitioned by class date."""

    return write_selected_observation_partitions(
        [selected],
        output_root=output_root,
        run_directory=run_directory,
        run_id=run_id,
        schema_id=schema_id,
        metric_id=metric_id,
    ).path


def write_selected_observation_partitions(
    selected_parts: Iterable[pd.DataFrame],
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> WrittenSelectedObservations:
    """Write bounded class-session parts as one class-date dataset.

    A class session must occur in at most one non-empty input part. This keeps
    duplicate-key validation bounded while allowing the capture runner to
    validate and release one selected session at a time.
    """

    if (output_root is None) == (run_directory is None):
        raise ValueError("provide exactly one of output_root or run_directory")
    destination = (
        Path(run_directory)
        if run_directory is not None
        else Path(output_root) / run_id
    )
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / SELECTED_OBSERVATIONS_DIRECTORY
    if target.exists():
        raise FileExistsError(
            f"selected-observation artifact already exists: {target}"
        )

    temporary = destination / (
        f".{SELECTED_OBSERVATIONS_DIRECTORY}.{uuid.uuid4().hex}.tmp"
    )
    temporary.mkdir()
    total_rows = 0
    first_observation_time: pd.Timestamp | None = None
    last_observation_time: pd.Timestamp | None = None
    seen_sessions: set[str] = set()
    partition_writers: dict[date, pq.ParquetWriter] = {}
    try:
        try:
            for selected in selected_parts:
                built = _build_selected_observation_artifact(
                    selected,
                    run_id=run_id,
                    schema_id=schema_id,
                    metric_id=metric_id,
                )
                if built.table.num_rows == 0:
                    continue
                sessions = set(
                    built.logical["class_session_id"].dropna().astype(str)
                )
                repeated = sorted(seen_sessions.intersection(sessions))
                if repeated:
                    raise ValueError(
                        "selected class session occurs in more than one bounded "
                        "part: " + ", ".join(repeated)
                    )
                seen_sessions.update(sessions)

                observed = built.logical["observation_time"]
                part_first = observed.min()
                part_last = observed.max()
                if (
                    first_observation_time is None
                    or part_first < first_observation_time
                ):
                    first_observation_time = part_first
                if (
                    last_observation_time is None
                    or part_last > last_observation_time
                ):
                    last_observation_time = part_last

                partition_slices = _class_date_partition_slices(built.logical)
                table = built.table
                for class_date, offset, length in partition_slices:
                    partition_directory = (
                        temporary / f"class_date={class_date.isoformat()}"
                    )
                    partition_directory.mkdir(exist_ok=True)
                    # Logical rows are already ordered by class_date. Arrow
                    # slices select each row group without copying buffers.
                    partition_table = table.slice(offset, length).select(
                        _PARTITION_FILE_COLUMNS
                    )
                    if partition_table.schema != _PARTITION_FILE_SCHEMA:
                        raise AssertionError(
                            "selected-observation partition schema changed "
                            "unexpectedly"
                        )
                    writer = partition_writers.get(class_date)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            partition_directory / "part-00000.parquet",
                            _PARTITION_FILE_SCHEMA,
                            compression="zstd",
                        )
                        partition_writers[class_date] = writer
                    writer.write_table(partition_table)
                total_rows += table.num_rows
                del built, table
        finally:
            close_error: BaseException | None = None
            for writer in partition_writers.values():
                try:
                    writer.close()
                except BaseException as exc:
                    if close_error is None:
                        close_error = exc
            if close_error is not None:
                raise close_error

        if total_rows == 0:
            # Preserve a readable, typed zero-row artifact. Non-empty runs use
            # Hive partitions and omit the partition column from each file.
            pq.write_table(
                pa.Table.from_pylist([], schema=SELECTED_OBSERVATION_ARROW_SCHEMA),
                temporary / "part-00000.parquet",
            )
        os.replace(temporary, target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return WrittenSelectedObservations(
        path=target,
        rows=total_rows,
        first_observation_time=first_observation_time,
        last_observation_time=last_observation_time,
    )
