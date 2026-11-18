"""ADR-0010 bounded station-count rollups and review exports."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure


RESULT_SCHEMA_ID = "beacon.schema/analysis-results/v1"
STATION_METRIC_ID = "beacon.metric/corrected-station-count/v1"
STALE_LIMIT_SECONDS = 30

REPORT_DIRECTORY = Path("reports") / "station_count"
ROLLUP_FILENAME = "station_bin_rollups.parquet"
CLASS_REPORT_FILENAME = "station_class_report.csv"
AGGREGATE_REPORT_FILENAME = "station_aggregate_report.csv"
CLASSROOM_HISTOGRAM_FILENAME = "classroom_bin_station_histogram.png"
BAND_HISTOGRAM_FILENAME = "classroom_bin_station_histogram_by_band.png"
METHODOLOGY_FILENAME = "methodology.txt"
ENROLLMENT_WEIGHTED_METHOD = "ratio_of_sums_enrollment_weighted"

_BIN_REQUIRED = {
    "class_session_id",
    "classroom",
    "bin_number",
    "bin_start",
    "bin_end",
    "bin_duration_us",
}
_SELECTED_REQUIRED = {
    "class_session_id",
    "classroom",
    "bin_number",
    "bssid",
    "ssid",
    "radio_group_id",
    "band",
    "qbss_station_count",
}
_ROLLUP_ORDER = {"radio": 0, "ssid": 1, "band": 2, "classroom": 3}

STATION_BIN_ROLLUP_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("class_session_id", pa.string(), nullable=False),
        pa.field("classroom", pa.string(), nullable=False),
        pa.field("bin_number", pa.int64(), nullable=False),
        pa.field("bin_start", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("bin_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("bin_duration_us", pa.int64(), nullable=False),
        pa.field("rollup_type", pa.string(), nullable=False),
        pa.field("rollup_id", pa.string(), nullable=False),
        pa.field("station_count", pa.uint64()),
        pa.field("available", pa.bool_(), nullable=False),
        pa.field("stale_assisted", pa.bool_(), nullable=False),
    ]
)

STATION_CLASS_REPORT_COLUMNS = (
    "run_id",
    "schema_id",
    "metric_id",
    "class_session_id",
    "classroom",
    "class_start_utc",
    "class_end_utc",
    "rollup_type",
    "rollup_id",
    "available_bin_count",
    "unavailable_bin_count",
    "stale_assisted_bin_count",
    "peak_client_count",
    "peak_bin_number",
    "peak_bin_start_utc",
    "peak_stale_assisted",
    "combined_enrollment",
    "peak_client_count_per_enrolled",
)

# Use the same nullable types for every session, including empty/all-null
# reports. Do not let concat infer types from whichever session has values.
STATION_CLASS_REPORT_DTYPES = {
    "available_bin_count": "Int64",
    "unavailable_bin_count": "Int64",
    "stale_assisted_bin_count": "Int64",
    "peak_client_count": "UInt64",
    "peak_bin_number": "Int64",
    "peak_stale_assisted": "boolean",
    "combined_enrollment": "Int64",
    "peak_client_count_per_enrolled": "Float64",
}

STATION_AGGREGATE_REPORT_COLUMNS = (
    "run_id",
    "schema_id",
    "metric_id",
    "aggregation_method",
    "population_type",
    "population_id",
    "eligible_session_count",
    "peak_client_count_sum",
    "combined_enrollment_sum",
    "stale_assisted_peak_session_count",
    "average_peak_client_count_per_enrolled",
)


@dataclass(frozen=True)
class WrittenStationReportPackage:
    directory: Path
    rollup_rows: int
    class_report_rows: int
    aggregate_report_rows: int
    files: tuple[Path, ...]


def _missing(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(result) if not hasattr(result, "__len__") else False


def _required_text(value: Any, *, name: str) -> str:
    if _missing(value):
        raise ValueError(f"{name} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: Any) -> str | None:
    if _missing(value):
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp(value: Any, *, name: str) -> pd.Timestamp:
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid timestamp") from exc
    if result.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return result.tz_convert("UTC")


def _integer(
    value: Any,
    *,
    name: str,
    minimum: int = 0,
    maximum: int = 2**63 - 1,
) -> int:
    if isinstance(value, bool) or _missing(value):
        raise ValueError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, (float, np.floating)) and not float(value).is_integer():
        raise ValueError(f"{name} must be an integer")
    if result < minimum or result > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _station_value(value: Any) -> int | None:
    if _missing(value) or str(value).strip() == "":
        return None
    return _integer(
        value,
        name="qbss_station_count",
        minimum=0,
        maximum=2**16 - 1,
    )


def _normalize_bins(class_bins: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(class_bins, pd.DataFrame):
        raise TypeError("class_bins must be a pandas DataFrame")
    missing = sorted(_BIN_REQUIRED.difference(class_bins.columns))
    if missing:
        raise ValueError("class bins are missing columns: " + ", ".join(missing))
    rows: list[dict[str, Any]] = []
    for record in class_bins.to_dict(orient="records"):
        start = _utc_timestamp(record["bin_start"], name="bin_start")
        end = _utc_timestamp(record["bin_end"], name="bin_end")
        duration = _integer(
            record["bin_duration_us"], name="bin_duration_us", minimum=1
        )
        if end <= start or (end.value - start.value) // 1_000 != duration:
            raise ValueError("bin duration must exactly match bin_start/bin_end")
        rows.append(
            {
                "class_session_id": _required_text(
                    record["class_session_id"], name="class_session_id"
                ),
                "classroom": _required_text(
                    record["classroom"], name="classroom"
                ),
                "bin_number": _integer(
                    record["bin_number"], name="bin_number"
                ),
                "bin_start": start,
                "bin_end": end,
                "bin_duration_us": duration,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("class_bins must contain at least one bin")
    if result["class_session_id"].nunique() != 1 or result["classroom"].nunique() != 1:
        raise ValueError("station rollups must be built one class session at a time")
    result = result.sort_values("bin_number", kind="stable").reset_index(drop=True)
    if result["bin_number"].tolist() != list(range(len(result))):
        raise ValueError("class bin numbers must be contiguous and start at zero")
    if result["bin_start"].tolist()[1:] != result["bin_end"].tolist()[:-1]:
        raise ValueError("class bins must be contiguous")
    return result


def _one_metadata_value(
    rows: pd.DataFrame,
    column: str,
    *,
    bssid: str,
) -> str | None:
    values = sorted(
        {
            text
            for value in rows[column]
            if (text := _optional_text(value)) is not None
        }
    )
    if len(values) > 1:
        raise ValueError(f"BSSID {bssid} has multiple {column} values: {values}")
    return values[0] if values else None


def _normalize_selected(
    selected: pd.DataFrame,
    *,
    session_id: str,
    classroom: str,
    valid_bins: set[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(selected, pd.DataFrame):
        raise TypeError("selected_observations must be a pandas DataFrame")
    missing = sorted(_SELECTED_REQUIRED.difference(selected.columns))
    if missing:
        raise ValueError(
            "selected observations are missing columns: " + ", ".join(missing)
        )
    rows: list[dict[str, Any]] = []
    metadata: list[dict[str, Any]] = []
    for record in selected.to_dict(orient="records"):
        record_session = _required_text(
            record["class_session_id"], name="class_session_id"
        )
        record_room = _required_text(record["classroom"], name="classroom")
        if record_session != session_id or record_room != classroom:
            raise ValueError("selected observation is outside the class session")
        bin_number = _integer(record["bin_number"], name="bin_number")
        if bin_number not in valid_bins:
            raise ValueError("selected observation references an unknown class bin")
        rows.append(
            {
                "bssid": _required_text(record["bssid"], name="bssid"),
                "bin_number": bin_number,
                "station_count": _station_value(record["qbss_station_count"]),
            }
        )
    observations = pd.DataFrame(
        rows, columns=["bssid", "bin_number", "station_count"]
    )
    if observations.duplicated(["bssid", "bin_number"]).any():
        raise ValueError("selected observations require one row per BSSID/bin")
    for bssid, group in selected.groupby("bssid", sort=True, dropna=False):
        bssid_text = _required_text(bssid, name="bssid")
        metadata.append(
            {
                "bssid": bssid_text,
                "radio_group_id": _one_metadata_value(
                    group, "radio_group_id", bssid=bssid_text
                ),
                "ssid": _one_metadata_value(group, "ssid", bssid=bssid_text),
                "band": _one_metadata_value(group, "band", bssid=bssid_text)
                or "unknown",
            }
        )
    return observations, pd.DataFrame(
        metadata,
        columns=["bssid", "radio_group_id", "ssid", "band"],
    )


def _supplement_bssid(group: pd.DataFrame) -> pd.DataFrame:
    result = group.sort_values("bin_number", kind="stable").copy()
    values = pd.array(result["station_count"], dtype="Int64")
    result["station_count"] = values
    fresh = result["station_count"].notna()
    stale = pd.Series(False, index=result.index, dtype="bool")
    if fresh.any():
        source_times = result["bin_start"].where(fresh).ffill()
        previous = result["station_count"].ffill()
        previous_age = (result["bin_start"] - source_times).dt.total_seconds()
        use_previous = (
            result["station_count"].isna()
            & source_times.notna()
            & previous_age.le(STALE_LIMIT_SECONDS)
        )
        result.loc[use_previous, "station_count"] = previous.loc[use_previous]
        stale.loc[use_previous] = True

        first_fresh_position = int(np.flatnonzero(fresh.to_numpy())[0])
        leading = pd.Series(
            np.arange(len(result)) < first_fresh_position,
            index=result.index,
        )
        next_times = result["bin_start"].where(fresh).bfill()
        following = pd.Series(values, index=result.index).bfill()
        following_age = (next_times - result["bin_start"]).dt.total_seconds()
        use_following = (
            result["station_count"].isna()
            & leading
            & next_times.notna()
            & following_age.le(STALE_LIMIT_SECONDS)
        )
        result.loc[use_following, "station_count"] = following.loc[use_following]
        stale.loc[use_following] = True
    result["stale_assisted"] = stale
    return result


def _rollup_rows(
    base: pd.DataFrame,
    *,
    rollup_type: str,
    id_column: str,
) -> list[dict[str, Any]]:
    source = base.loc[base[id_column].notna()].copy()
    if source.empty:
        return []
    rows: list[dict[str, Any]] = []
    group_columns = [
        "class_session_id",
        "classroom",
        "bin_number",
        "bin_start",
        "bin_end",
        "bin_duration_us",
        id_column,
    ]
    for key, group in source.groupby(group_columns, sort=True, dropna=False):
        available = bool(group["station_count"].notna().all())
        station_count = (
            int(group["station_count"].sum()) if available else None
        )
        rows.append(
            {
                "class_session_id": str(key[0]),
                "classroom": str(key[1]),
                "bin_number": int(key[2]),
                "bin_start": key[3],
                "bin_end": key[4],
                "bin_duration_us": int(key[5]),
                "rollup_type": rollup_type,
                "rollup_id": str(key[6]),
                "station_count": station_count,
                "available": available,
                "stale_assisted": bool(
                    available and group["stale_assisted"].any()
                ),
            }
        )
    return rows


def build_station_bin_rollups(
    class_bins: pd.DataFrame,
    selected_observations: pd.DataFrame,
    *,
    run_id: str,
) -> pd.DataFrame:
    """Build one bounded session's ADR-0010 station-count rollups."""

    run_id = _required_text(run_id, name="run_id")
    bins = _normalize_bins(class_bins)
    session_id = str(bins.loc[0, "class_session_id"])
    classroom = str(bins.loc[0, "classroom"])
    selected, metadata = _normalize_selected(
        selected_observations,
        session_id=session_id,
        classroom=classroom,
        valid_bins=set(bins["bin_number"].astype(int)),
    )

    rows: list[dict[str, Any]] = []
    if metadata.empty:
        for record in bins.to_dict(orient="records"):
            rows.append(
                {
                    **record,
                    "rollup_type": "classroom",
                    "rollup_id": classroom,
                    "station_count": None,
                    "available": False,
                    "stale_assisted": False,
                }
            )
    else:
        grid = pd.MultiIndex.from_product(
            [metadata["bssid"].tolist(), bins["bin_number"].tolist()],
            names=["bssid", "bin_number"],
        ).to_frame(index=False)
        base = (
            grid.merge(selected, on=["bssid", "bin_number"], how="left")
            .merge(metadata, on="bssid", how="left", validate="many_to_one")
            .merge(bins, on="bin_number", how="left", validate="many_to_one")
        )
        base = pd.concat(
            [
                _supplement_bssid(group)
                for _bssid, group in base.groupby("bssid", sort=True)
            ],
            ignore_index=True,
        )
        base["classroom_rollup_id"] = classroom
        rows.extend(
            _rollup_rows(
                base, rollup_type="radio", id_column="radio_group_id"
            )
        )
        rows.extend(_rollup_rows(base, rollup_type="ssid", id_column="ssid"))
        rows.extend(_rollup_rows(base, rollup_type="band", id_column="band"))
        rows.extend(
            _rollup_rows(
                base,
                rollup_type="classroom",
                id_column="classroom_rollup_id",
            )
        )

    result = pd.DataFrame(rows)
    result.insert(0, "metric_id", STATION_METRIC_ID)
    result.insert(0, "schema_id", RESULT_SCHEMA_ID)
    result.insert(0, "run_id", run_id)
    result["station_count"] = pd.array(result["station_count"], dtype="UInt64")
    result["rollup_order"] = result["rollup_type"].map(_ROLLUP_ORDER)
    result = result.sort_values(
        ["class_session_id", "bin_number", "rollup_order", "rollup_id"],
        kind="stable",
    ).drop(columns="rollup_order").reset_index(drop=True)
    table = pa.Table.from_pandas(
        result,
        schema=STATION_BIN_ROLLUP_ARROW_SCHEMA,
        preserve_index=False,
        safe=True,
    )
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _optional_enrollment(value: Any) -> int | None:
    if _missing(value) or str(value).strip() == "":
        return None
    return _integer(value, name="enrolled", maximum=2**63 - 1)


def build_station_class_report(
    rollups: pd.DataFrame,
    session: Mapping[str, Any],
) -> pd.DataFrame:
    """Reduce one session's rollups to peak review rows by rollup entity."""

    if not isinstance(rollups, pd.DataFrame):
        raise TypeError("rollups must be a pandas DataFrame")
    required = set(STATION_BIN_ROLLUP_ARROW_SCHEMA.names)
    missing = sorted(required.difference(rollups.columns))
    if missing:
        raise ValueError("station rollups are missing columns: " + ", ".join(missing))
    session_id = _required_text(session.get("class_session_id"), name="class_session_id")
    classroom = _required_text(session.get("classroom"), name="classroom")
    class_start = _utc_timestamp(session.get("class_start"), name="class_start")
    class_end = _utc_timestamp(session.get("class_end"), name="class_end")
    enrollment = _optional_enrollment(session.get("enrolled"))
    rows: list[dict[str, Any]] = []
    for (rollup_type, rollup_id), group in rollups.groupby(
        ["rollup_type", "rollup_id"], sort=True
    ):
        available = group.loc[group["available"].astype(bool)].copy()
        peak = None
        if not available.empty:
            maximum = int(pd.to_numeric(available["station_count"]).max())
            peak = (
                available.loc[
                    pd.to_numeric(available["station_count"]).eq(maximum)
                ]
                .sort_values(
                    ["stale_assisted", "bin_number"], kind="stable"
                )
                .iloc[0]
            )
        is_classroom = str(rollup_type) == "classroom"
        ratio = (
            float(int(peak["station_count"]) / enrollment)
            if peak is not None and is_classroom and enrollment not in {None, 0}
            else None
        )
        rows.append(
            {
                "run_id": str(group.iloc[0]["run_id"]),
                "schema_id": RESULT_SCHEMA_ID,
                "metric_id": STATION_METRIC_ID,
                "class_session_id": session_id,
                "classroom": classroom,
                "class_start_utc": class_start.isoformat().replace("+00:00", "Z"),
                "class_end_utc": class_end.isoformat().replace("+00:00", "Z"),
                "rollup_type": str(rollup_type),
                "rollup_id": str(rollup_id),
                "available_bin_count": len(available),
                "unavailable_bin_count": int((~group["available"].astype(bool)).sum()),
                "stale_assisted_bin_count": int(
                    group["stale_assisted"].astype(bool).sum()
                ),
                "peak_client_count": (
                    None if peak is None else int(peak["station_count"])
                ),
                "peak_bin_number": (
                    None if peak is None else int(peak["bin_number"])
                ),
                "peak_bin_start_utc": (
                    None
                    if peak is None
                    else pd.Timestamp(peak["bin_start"])
                    .isoformat()
                    .replace("+00:00", "Z")
                ),
                "peak_stale_assisted": (
                    None if peak is None else bool(peak["stale_assisted"])
                ),
                "combined_enrollment": enrollment if is_classroom else None,
                "peak_client_count_per_enrolled": ratio,
            }
        )
    # Construct typed columns directly so missing counts never force an
    # intermediate float conversion (which could lose integer precision).
    return pd.DataFrame({
        column: pd.Series(
            [row[column] for row in rows],
            dtype=STATION_CLASS_REPORT_DTYPES.get(column, "object"),
        )
        for column in STATION_CLASS_REPORT_COLUMNS
    })


def build_station_aggregate_report(class_report: pd.DataFrame) -> pd.DataFrame:
    """Aggregate class-session peaks using enrollment-weighted ratios of sums."""

    if not isinstance(class_report, pd.DataFrame):
        raise TypeError("class_report must be a pandas DataFrame")
    missing = sorted(
        set(STATION_CLASS_REPORT_COLUMNS).difference(class_report.columns)
    )
    if missing:
        raise ValueError(
            "station class report is missing columns: " + ", ".join(missing)
        )
    if class_report.empty:
        return pd.DataFrame(columns=STATION_AGGREGATE_REPORT_COLUMNS)

    classroom = class_report.loc[
        class_report["rollup_type"].eq("classroom"),
        [
            "run_id",
            "class_session_id",
            "classroom",
            "combined_enrollment",
        ],
    ].copy()
    if classroom["class_session_id"].duplicated().any():
        raise ValueError("class report requires one classroom row per session")
    if classroom.empty:
        raise ValueError("class report requires classroom rows for enrollment")

    population_parts = [
        class_report.loc[class_report["rollup_type"].eq("classroom")].assign(
            population_type="overall",
            population_id="all",
        )
    ]
    for rollup_type in ("ssid", "band"):
        part = class_report.loc[
            class_report["rollup_type"].eq(rollup_type)
        ].copy()
        part = part.drop(columns=["combined_enrollment"]).merge(
            classroom,
            on=["run_id", "class_session_id", "classroom"],
            how="left",
            validate="many_to_one",
        )
        part["population_type"] = rollup_type
        part["population_id"] = part["rollup_id"].astype(str)
        population_parts.append(part)

    source = pd.concat(population_parts, ignore_index=True, sort=False)
    enrollment = pd.to_numeric(source["combined_enrollment"], errors="raise")
    peaks = pd.to_numeric(source["peak_client_count"], errors="raise")
    eligible = enrollment.gt(0) & peaks.notna()
    source["_enrollment"] = enrollment
    source["_peak"] = peaks
    source["_eligible"] = eligible

    rows: list[dict[str, Any]] = []
    for (population_type, population_id), group in source.groupby(
        ["population_type", "population_id"], sort=True
    ):
        included = group.loc[group["_eligible"]]
        peak_sum = int(included["_peak"].sum()) if not included.empty else 0
        enrollment_sum = (
            int(included["_enrollment"].sum()) if not included.empty else 0
        )
        stale_count = int(
            included["peak_stale_assisted"].fillna(False).astype(bool).sum()
        )
        rows.append(
            {
                "run_id": str(group.iloc[0]["run_id"]),
                "schema_id": RESULT_SCHEMA_ID,
                "metric_id": STATION_METRIC_ID,
                "aggregation_method": ENROLLMENT_WEIGHTED_METHOD,
                "population_type": str(population_type),
                "population_id": str(population_id),
                "eligible_session_count": len(included),
                "peak_client_count_sum": peak_sum,
                "combined_enrollment_sum": enrollment_sum,
                "stale_assisted_peak_session_count": stale_count,
                "average_peak_client_count_per_enrolled": (
                    float(peak_sum / enrollment_sum)
                    if enrollment_sum > 0
                    else None
                ),
            }
        )
    result = pd.DataFrame(rows, columns=STATION_AGGREGATE_REPORT_COLUMNS)
    result["_population_order"] = result["population_type"].map(
        {"overall": 0, "ssid": 1, "band": 2}
    )
    return result.sort_values(
        ["_population_order", "population_id"], kind="stable"
    ).drop(columns="_population_order").reset_index(drop=True)


def _methodology() -> str:
    return "\n".join(
        [
            "Corrected station-count report methodology",
            "",
            "Scope",
            "- Totals include only configured APs and sampled BSSIDs, SSIDs,",
            "  radios, and bands represented in the analyzed class-session data.",
            "- A BSSID seen elsewhere in the project but absent for the complete",
            "  session is not session-active and is excluded for that session.",
            "",
            "Stale assistance",
            "- Missing station count uses the same BSSID's previous valid value for",
            "  at most 30 seconds.",
            "- Leading missing bins may use the first later valid value for at most",
            "  30 seconds. Interior gaps never use a future value.",
            "- Values never cross a physical class-session boundary.",
            "",
            "Rollups",
            "- Radio, SSID/classroom, band/classroom, and all-classroom totals",
            "  sum supplemented BSSID values directly.",
            "- A total is null when any expected session-active BSSID remains",
            "  unavailable. Partial numeric totals are not published.",
            "- stale_assisted is true when any contributing BSSID value was",
            "  extended.",
            "- The primary distribution uses one complete all-band/all-SSID",
            "  classroom total per bin. Per-band panels use one complete",
            "  band/classroom total per bin. Unavailable bins are excluded.",
            "- Stale-assisted values participate and are shown as an outline",
            "  overlay on each station-count histogram.",
            "- Station-count histograms use one-client buckets.",
            "",
            "Enrollment",
            "- Peak client count is the maximum available classroom/bin total,",
            "  including stale-assisted bins.",
            "- Enrollment combines all sections in the physical session.",
            "- Zero or missing enrollment produces no ratio.",
            "- The ratio label is peak client count per enrolled.",
            "- Run-wide overall, SSID, and band averages use the Original-",
            "  compatible enrollment-weighted ratio of sums: eligible peak",
            "  counts summed and divided by their corresponding enrollment",
            "  sum. Sessions do not receive equal weight.",
            "- The maximum remains sensitive to genuine or erroneous single-bin",
            "  peaks; this limitation is accepted for the current statistic.",
            "",
            "Files",
            f"- {ROLLUP_FILENAME}: derived bin totals at radio, SSID, band, and",
            "  classroom grain; selected_observations/ remains authoritative.",
            f"- {CLASS_REPORT_FILENAME}: class-session peaks and availability",
            "  counts for every rollup entity.",
            f"- {AGGREGATE_REPORT_FILENAME}: enrollment-weighted overall, SSID,",
            "  and band peak-client-count-per-enrolled results with component",
            "  sums and eligible-session counts.",
            f"- {CLASSROOM_HISTOGRAM_FILENAME}: all-band classroom/bin station",
            "  distribution.",
            f"- {BAND_HISTOGRAM_FILENAME}: per-band classroom/bin distributions.",
            f"- {METHODOLOGY_FILENAME}: this file.",
            "",
        ]
    )


def _add_distribution_counts(
    part: pd.DataFrame,
    counts: dict[str, Counter[int]],
    stale_counts: dict[str, Counter[int]],
) -> None:
    for row in part.itertuples(index=False):
        rollup_type = str(row.rollup_type)
        if rollup_type == "classroom":
            key = "all"
        elif rollup_type == "band":
            key = f"band:{row.rollup_id}"
        else:
            continue
        counts[key]
        stale_counts[key]
        if not bool(row.available) or pd.isna(row.station_count):
            continue
        value = int(row.station_count)
        counts[key][value] += 1
        if bool(row.stale_assisted):
            stale_counts[key][value] += 1


def _histogram_axis(
    axis: Any,
    counts: Counter[int],
    stale_counts: Counter[int],
    *,
    title: str,
) -> None:
    axis.set_title(title)
    axis.set_xlabel("Station count")
    axis.set_ylabel("Classroom/bin observations")
    axis.grid(axis="y", alpha=0.25)
    if not counts:
        axis.text(
            0.5,
            0.5,
            "No available totals",
            transform=axis.transAxes,
            ha="center",
        )
        return
    values = np.asarray(sorted(counts), dtype=float)
    weights = np.asarray([counts[int(value)] for value in values], dtype=float)
    maximum = float(values.max())
    edges = np.arange(-0.5, maximum + 1.5, 1.0)
    axis.hist(
        values,
        bins=edges,
        weights=weights,
        color="#4c78a8",
        alpha=0.8,
        label="Available",
    )
    if stale_counts:
        stale_weights = np.asarray(
            [stale_counts.get(int(value), 0) for value in values], dtype=float
        )
        axis.hist(
            values,
            bins=edges,
            weights=stale_weights,
            histtype="step",
            color="#f58518",
            linewidth=1.5,
            label="Stale-assisted",
        )
    axis.set_xlim(left=-0.5)
    axis.legend()


def build_station_band_histogram(
    counts: dict[str, Counter[int]],
    stale_counts: dict[str, Counter[int]],
) -> Figure:
    """Plot the existing classroom/bin counts with a common vertical scale."""
    bands = sorted(
        key.split(":", 1)[1] for key in counts if key.startswith("band:")
    )
    panel_count = max(1, len(bands))
    band_figure = Figure(
        figsize=(12 * panel_count, 10),
        constrained_layout=True,
    )
    FigureCanvasAgg(band_figure)
    axes = np.atleast_1d(band_figure.subplots(1, panel_count, sharey=True))
    if not bands:
        bands = ["unknown"]
    for axis, band in zip(axes, bands):
        _histogram_axis(
            axis,
            counts.get(f"band:{band}", Counter()),
            stale_counts.get(f"band:{band}", Counter()),
            title=f"{band} GHz" if band != "unknown" else "Unknown band",
        )
    band_figure.suptitle("Classroom/bin associated-station histograms by band")
    return band_figure


def build_station_band_histogram_from_rollups(path: str | Path) -> Figure:
    """Read only saved band totals, one bounded batch at a time."""
    counts: dict[str, Counter[int]] = defaultdict(Counter)
    stale_counts: dict[str, Counter[int]] = defaultdict(Counter)
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=65_536, columns=["rollup_type", "rollup_id", "available", "station_count", "stale_assisted"]
    ):
        frame = batch.to_pandas(types_mapper=pd.ArrowDtype)
        _add_distribution_counts(frame.loc[frame["rollup_type"].eq("band")], counts, stale_counts)
    return build_station_band_histogram(counts, stale_counts)


def _write_distribution_plots(
    destination: Path,
    counts: dict[str, Counter[int]],
    stale_counts: dict[str, Counter[int]],
) -> tuple[Path, Path]:
    classroom_path = destination / CLASSROOM_HISTOGRAM_FILENAME
    figure = Figure(figsize=(16, 10), constrained_layout=True)
    FigureCanvasAgg(figure)
    _histogram_axis(
        figure.subplots(), counts["all"], stale_counts["all"], title="All-band classroom station count by bin",
    )
    figure.savefig(classroom_path, dpi=140)
    band_path = destination / BAND_HISTOGRAM_FILENAME
    build_station_band_histogram(counts, stale_counts).savefig(band_path, dpi=140)
    return classroom_path, band_path


def write_station_report_package(
    run_directory: str | Path,
    *,
    rollup_parts: Iterable[pd.DataFrame],
    class_report: pd.DataFrame,
) -> WrittenStationReportPackage:
    """Stream bounded station parts into one run-local derived package."""

    root = Path(run_directory)
    destination = root / REPORT_DIRECTORY
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
    destination.mkdir(parents=True)
    rollup_path = destination / ROLLUP_FILENAME
    writer: pq.ParquetWriter | None = None
    rollup_rows = 0
    distribution_counts: dict[str, Counter[int]] = defaultdict(Counter)
    stale_distribution_counts: dict[str, Counter[int]] = defaultdict(Counter)
    try:
        writer = pq.ParquetWriter(
            rollup_path,
            STATION_BIN_ROLLUP_ARROW_SCHEMA,
            compression="zstd",
        )
        for part in rollup_parts:
            _add_distribution_counts(
                part,
                distribution_counts,
                stale_distribution_counts,
            )
            table = pa.Table.from_pandas(
                part,
                schema=STATION_BIN_ROLLUP_ARROW_SCHEMA,
                preserve_index=False,
                safe=True,
            )
            writer.write_table(table)
            rollup_rows += table.num_rows
        if rollup_rows == 0:
            writer.write_table(
                pa.Table.from_pylist([], schema=STATION_BIN_ROLLUP_ARROW_SCHEMA)
            )
        writer.close()
        writer = None

        missing = sorted(set(STATION_CLASS_REPORT_COLUMNS).difference(class_report.columns))
        if missing:
            raise ValueError(
                "station class report is missing columns: " + ", ".join(missing)
            )
        class_report.loc[:, list(STATION_CLASS_REPORT_COLUMNS)].to_csv(
            destination / CLASS_REPORT_FILENAME,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            na_rep="",
        )
        aggregate_report = build_station_aggregate_report(class_report)
        aggregate_report.to_csv(
            destination / AGGREGATE_REPORT_FILENAME,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            na_rep="",
        )
        distribution_paths = _write_distribution_plots(
            destination,
            distribution_counts,
            stale_distribution_counts,
        )
        (destination / METHODOLOGY_FILENAME).write_text(
            _methodology(), encoding="utf-8", newline="\n"
        )
    except BaseException:
        if writer is not None:
            writer.close()
        shutil.rmtree(destination, ignore_errors=True)
        parent = destination.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        raise

    files = (
        rollup_path,
        destination / CLASS_REPORT_FILENAME,
        destination / AGGREGATE_REPORT_FILENAME,
        *distribution_paths,
        destination / METHODOLOGY_FILENAME,
    )
    return WrittenStationReportPackage(
        directory=destination,
        rollup_rows=rollup_rows,
        class_report_rows=len(class_report),
        aggregate_report_rows=len(aggregate_report),
        files=files,
    )
