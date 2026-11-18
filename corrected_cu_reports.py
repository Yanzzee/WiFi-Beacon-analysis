"""Bounded human-readable reports derived from corrected durable artifacts.

The report package is deliberately presentation-only.  It reads the compact
Parquet outputs, converts units and labels for review, and never recalculates
packet-level selection or ADR-0004 metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from numbers import Real
from pathlib import Path
import shutil
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd

from corrected_cu_bounds import (
    CU_BOUNDS_METRIC_COLUMN, CU_BOUNDS_METRIC_ID,
    add_radio_summary_cu_bounds, cu_mean_bounds_fields, cu_cdf_summary_rows,
)
from corrected_cu_plot_bounds import draw_cdf_bounds, draw_histogram_bounds

from corrected_station_reports import (
    AGGREGATE_REPORT_FILENAME as STATION_AGGREGATE_REPORT_FILENAME,
    REPORT_DIRECTORY as STATION_REPORT_DIRECTORY,
    STATION_AGGREGATE_REPORT_COLUMNS,
)


REPORT_DIRECTORY = Path("reports") / "cu_coverage_streak"
RADIO_REPORT_FILENAME = "radio_class_report.csv"
CLASSROOM_REPORT_FILENAME = "classroom_class_report.csv"
BAND_POPULATION_REPORT_FILENAME = "band_population_report.csv"
CDF_BOUNDS_REPORT_FILENAME = "cu_cdf_bounds.csv"
RUN_SUMMARY_FILENAME = "run_summary.txt"
RUN_SUMMARY_PATH = Path("reports") / RUN_SUMMARY_FILENAME
METHODOLOGY_FILENAME = "methodology.txt"
PLOT_FILENAMES = (
    "radio_average_cu_distribution.png",
    "radio_cu_vs_coverage.png",
    "radio_coverage_and_missingness.png",
    "radio_bin_cu_histogram.png",
    "radio_bin_cu_cdf.png",
    "classroom_streak_distributions.png",
    "radio_bin_cu_histogram_by_band.png",
    "radio_bin_cu_cdf_by_band.png",
)

_RADIO_REQUIRED = {
    "run_id", "schema_id", "metric_id", "class_session_id", "classroom",
    "class_start", "class_end", "radio_group_id", "band", "channel",
    "member_bssids", "preferred_bssid", "class_duration_us",
    "valid_cu_duration_us", "missing_cu_duration_us", "no_beacon_duration_us",
    "cu_coverage", "average_cu_ratio", "high_duration_us",
    "valid_cu_raw_counts",
    "high_possible_connected_streak_us", "high_actual_observed_streak_us",
    "very_high_duration_us", "very_high_possible_connected_streak_us",
    "very_high_actual_observed_streak_us",
}
_CLASSROOM_REQUIRED = {
    "run_id", "schema_id", "metric_id", "class_session_id", "classroom",
    "class_start", "class_end", "class_duration_us", "radio_count_2_4_ghz",
    "radio_count_5_ghz", "radio_count_6_ghz", "radio_count_unknown",
    "radio_count_total", "high_possible_connected_max_us",
    "high_possible_connected_responsible_radio_ids",
    "high_actual_observed_max_us", "high_actual_observed_responsible_radio_ids",
    "very_high_possible_connected_max_us",
    "very_high_possible_connected_responsible_radio_ids",
    "very_high_actual_observed_max_us",
    "very_high_actual_observed_responsible_radio_ids",
}
_IDENTITY_REQUIRED = {"radio_group_id", "ap_id", "band"}


@dataclass(frozen=True)
class CorrectedCuReportTables:
    radio_class: pd.DataFrame
    classroom_class: pd.DataFrame
    band_population: pd.DataFrame
    valid_cu_raw_counts: np.ndarray
    band_valid_cu_raw_counts: dict[str, np.ndarray]


@dataclass(frozen=True)
class WrittenCorrectedCuReportPackage:
    directory: Path
    radio_class_rows: int
    classroom_class_rows: int
    band_population_rows: int
    files: tuple[Path, ...]
    cdf_bounds_rows: int = 0


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def _utc_text(series: pd.Series) -> pd.Series:
    values = pd.to_datetime(series, utc=True, errors="raise")
    return values.map(lambda value: value.isoformat().replace("+00:00", "Z"))


def _local_text(series: pd.Series, timezone_name: str | None) -> pd.Series:
    if timezone_name is None:
        return pd.Series([None] * len(series), index=series.index, dtype="object")
    timezone = ZoneInfo(timezone_name)
    values = pd.to_datetime(series, utc=True, errors="raise")
    return values.map(lambda value: value.tz_convert(timezone).isoformat())


def _seconds(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce") / 1_000_000.0


def _percent(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce") * 100.0


def _band_text(value: Any) -> str:
    if pd.isna(value) or not str(value).strip():
        return "unknown"
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _capture_cohort(
    *,
    ap_model: str | None,
    band: str,
    ap_band_radio_count: int,
) -> str:
    model = ap_model or "unknown model"
    if model.isdigit():
        model = f"AP-{model}"
    band_label = f"{band} GHz" if band != "unknown" else "unknown band"
    noun = "radio" if ap_band_radio_count == 1 else "radios"
    return f"{model} / {ap_band_radio_count}x {band_label} {noun}"


def _radio_metadata(identity: pd.DataFrame) -> pd.DataFrame:
    _require_columns(identity, _IDENTITY_REQUIRED, "project identity map")
    source = identity.copy()
    if "ap_model" not in source.columns:
        source["ap_model"] = pd.Series(
            pd.NA, index=source.index, dtype="string"
        )
    rows_by_radio: list[dict[str, Any]] = []
    for radio_id, rows in source.groupby(
        "radio_group_id", sort=True, dropna=False
    ):
        if pd.isna(radio_id):
            continue
        ap_ids = sorted(
            {str(value) for value in rows["ap_id"] if pd.notna(value) and str(value)}
        )
        models = sorted(
            {
                str(value).strip()
                for value in rows["ap_model"]
                if pd.notna(value) and str(value).strip()
            }
        )
        bands = sorted({_band_text(value) for value in rows["band"]})
        if len(ap_ids) > 1:
            raise ValueError(
                f"radio_group_id {radio_id!r} maps to multiple AP IDs: {ap_ids}"
            )
        if len(models) > 1:
            raise ValueError(
                f"radio_group_id {radio_id!r} maps to multiple AP models: {models}"
            )
        if len(bands) > 1:
            raise ValueError(
                f"radio_group_id {radio_id!r} maps to multiple bands: {bands}"
            )
        rows_by_radio.append(
            {
                "radio_group_id": str(radio_id),
                "ap_id": ap_ids[0] if ap_ids else None,
                "ap_model": models[0] if models else None,
                "identity_band": bands[0] if bands else "unknown",
            }
        )

    metadata = pd.DataFrame(rows_by_radio)
    if metadata.empty:
        return pd.DataFrame(
            columns=[
                "radio_group_id",
                "ap_id",
                "ap_model",
                "identity_band",
                "ap_band_radio_count",
                "capture_cohort",
                "ap_has_6_ghz",
            ]
        ).set_index("radio_group_id")
    counts = metadata.groupby(
        ["ap_id", "identity_band"], dropna=False
    )["radio_group_id"].transform("nunique")
    metadata["ap_band_radio_count"] = counts.astype(int)
    aps_with_6_ghz = set(
        metadata.loc[
            metadata["identity_band"].eq("6") & metadata["ap_id"].notna(),
            "ap_id",
        ]
    )
    metadata["ap_has_6_ghz"] = metadata["ap_id"].isin(aps_with_6_ghz)
    metadata["capture_cohort"] = metadata.apply(
        lambda row: _capture_cohort(
            ap_model=row["ap_model"],
            band=row["identity_band"],
            ap_band_radio_count=int(row["ap_band_radio_count"]),
        ),
        axis=1,
    )
    return metadata.set_index("radio_group_id")


def build_corrected_cu_report_tables(
    radio_summary: pd.DataFrame,
    classroom_summary: pd.DataFrame,
    identity_map: pd.DataFrame,
    *,
    display_timezone: str | None = None,
    radio_bin_counts: pd.DataFrame | None = None,
) -> CorrectedCuReportTables:
    """Build presentation tables without changing corrected metric semantics."""

    _require_columns(radio_summary, _RADIO_REQUIRED, "radio/class summary")
    _require_columns(
        classroom_summary, _CLASSROOM_REQUIRED, "classroom/class summary"
    )
    if display_timezone is not None:
        ZoneInfo(display_timezone)

    radio_metadata = _radio_metadata(identity_map)
    radios = add_radio_summary_cu_bounds(radio_summary)
    missing_radios = sorted(
        set(radios["radio_group_id"].astype(str)).difference(
            radio_metadata.index
        )
    )
    if missing_radios:
        raise ValueError(
            "summary radios are absent from the project identity map: "
            + ", ".join(missing_radios)
        )

    radio_report = pd.DataFrame(
        {
            "run_id": radios["run_id"],
            "schema_id": radios["schema_id"],
            "metric_id": radios["metric_id"],
            "class_session_id": radios["class_session_id"],
            "classroom": radios["classroom"],
            "class_start_utc": _utc_text(radios["class_start"]),
            "class_end_utc": _utc_text(radios["class_end"]),
            "class_start_local": _local_text(radios["class_start"], display_timezone),
            "class_end_local": _local_text(radios["class_end"], display_timezone),
            "display_timezone": display_timezone,
            "ap_id": radios["radio_group_id"].astype(str).map(
                radio_metadata["ap_id"]
            ),
            "ap_model": radios["radio_group_id"].astype(str).map(
                radio_metadata["ap_model"]
            ),
            "ap_band_radio_count": radios["radio_group_id"].astype(str).map(
                radio_metadata["ap_band_radio_count"]
            ),
            "capture_cohort": radios["radio_group_id"].astype(str).map(
                radio_metadata["capture_cohort"]
            ),
            "ap_has_6_ghz": radios["radio_group_id"].astype(str).map(
                radio_metadata["ap_has_6_ghz"]
            ),
            "radio_group_id": radios["radio_group_id"],
            "band": radios["band"],
            "channel": radios["channel"],
            "member_bssids": radios["member_bssids"],
            "preferred_bssid": radios["preferred_bssid"],
            "class_duration_seconds": _seconds(radios["class_duration_us"]),
            "valid_cu_seconds": _seconds(radios["valid_cu_duration_us"]),
            "missing_cu_seconds": _seconds(radios["missing_cu_duration_us"]),
            "no_beacon_seconds": _seconds(radios["no_beacon_duration_us"]),
            "cu_coverage_percent": _percent(radios["cu_coverage"]),
            "average_cu_percent": _percent(radios["average_cu_ratio"]),
            "high_duration_seconds": _seconds(radios["high_duration_us"]),
            "high_possible_connected_streak_seconds": _seconds(
                radios["high_possible_connected_streak_us"]
            ),
            "high_actual_observed_streak_seconds": _seconds(
                radios["high_actual_observed_streak_us"]
            ),
            "very_high_duration_seconds": _seconds(
                radios["very_high_duration_us"]
            ),
            "very_high_possible_connected_streak_seconds": _seconds(
                radios["very_high_possible_connected_streak_us"]
            ),
            "very_high_actual_observed_streak_seconds": _seconds(
                radios["very_high_actual_observed_streak_us"]
            ),
        }
    )

    for name in ("lower", "upper"):
        radio_report[f"average_cu_percent_{name}_bound"] = radios[f"average_cu_ratio_{name}_bound"] * 100
    radio_report[CU_BOUNDS_METRIC_COLUMN] = CU_BOUNDS_METRIC_ID
    radio_report["expected_radio_bin_count"] = pd.Series(pd.NA, index=radios.index, dtype="Int64")
    if radio_bin_counts is not None:
        _require_columns(radio_bin_counts, {"class_session_id", "radio_group_id", "radio_bin_rows"}, "radio/bin counts")
        if radio_bin_counts.duplicated(["class_session_id", "radio_group_id"]).any():
            raise ValueError("duplicate radio/session bin counts")
        lookup = radio_bin_counts.set_index(["class_session_id", "radio_group_id"]).radio_bin_rows
        expected = []
        for row in radios.itertuples(index=False):
            key = (row.class_session_id, row.radio_group_id)
            if key not in lookup.index:
                raise ValueError("missing expected radio/session bin count")
            value = lookup.loc[key]
            if (pd.isna(value) or isinstance(value, (bool, np.bool_))
                    or not isinstance(value, Real) or not np.isfinite(value)
                    or int(value) != value or value <= 0
                    or value < sum(int(v) for v in row.valid_cu_raw_counts)):
                raise ValueError("invalid expected radio/session bin count")
            expected.append(int(value))
        radio_report["expected_radio_bin_count"] = pd.Series(expected, index=radios.index, dtype="Int64")
    radios["expected_radio_bin_count"] = radio_report["expected_radio_bin_count"]

    coverage = radios.assign(
        _coverage_percent=_percent(radios["cu_coverage"]),
        _has_valid=pd.to_numeric(radios["valid_cu_duration_us"], errors="raise").gt(0),
    )
    coverage_rollup = coverage.groupby(
        ["class_session_id", "classroom"], sort=True
    ).agg(
        radio_cu_coverage_min_percent=("_coverage_percent", "min"),
        radio_cu_coverage_median_percent=("_coverage_percent", "median"),
        radio_cu_coverage_max_percent=("_coverage_percent", "max"),
        radios_with_valid_cu_count=("_has_valid", "sum"),
        _radio_rows=("radio_group_id", "size"),
    )
    coverage_rollup["radios_without_valid_cu_count"] = (
        coverage_rollup["_radio_rows"]
        - coverage_rollup["radios_with_valid_cu_count"]
    )

    classrooms = classroom_summary.copy()
    classroom_keys = pd.MultiIndex.from_frame(
        classrooms[["class_session_id", "classroom"]]
    )
    missing_rows = ~classroom_keys.isin(coverage_rollup.index)
    zero_radios = pd.to_numeric(classrooms["radio_count_total"], errors="raise").eq(0).to_numpy()
    # A scheduled classroom can have no project-observed radios. Its counts
    # are zero, but coverage and streak measurements remain unavailable.
    # Missing rows for a classroom that does have radios are still an error.
    if np.any(missing_rows & ~zero_radios):
        raise ValueError("classroom summaries have no matching radio summaries")
    if "classroom" in identity_map:
        observed_rooms = identity_map.loc[
            identity_map["radio_group_id"].notna(), "classroom"
        ]
        if np.any(missing_rows & classrooms["classroom"].isin(observed_rooms).to_numpy()):
            raise ValueError("classroom summaries have no matching radio summaries")
    rollup = coverage_rollup.reindex(classroom_keys).reset_index(drop=True)
    count_columns = ["_radio_rows", "radios_with_valid_cu_count", "radios_without_valid_cu_count"]
    rollup.loc[missing_rows, count_columns] = 0
    rollup[count_columns] = rollup[count_columns].astype("int64")
    if not np.array_equal(
        rollup["_radio_rows"].to_numpy(),
        classrooms["radio_count_total"].to_numpy(),
    ):
        raise ValueError("classroom radio counts disagree with radio summaries")

    classroom_report = pd.DataFrame(
        {
            "run_id": classrooms["run_id"],
            "schema_id": classrooms["schema_id"],
            "metric_id": classrooms["metric_id"],
            "class_session_id": classrooms["class_session_id"],
            "classroom": classrooms["classroom"],
            "class_start_utc": _utc_text(classrooms["class_start"]),
            "class_end_utc": _utc_text(classrooms["class_end"]),
            "class_start_local": _local_text(
                classrooms["class_start"], display_timezone
            ),
            "class_end_local": _local_text(classrooms["class_end"], display_timezone),
            "display_timezone": display_timezone,
            "class_duration_seconds": _seconds(classrooms["class_duration_us"]),
            "radio_count_2_4_ghz": classrooms["radio_count_2_4_ghz"],
            "radio_count_5_ghz": classrooms["radio_count_5_ghz"],
            "radio_count_6_ghz": classrooms["radio_count_6_ghz"],
            "radio_count_unknown": classrooms["radio_count_unknown"],
            "radio_count_total": classrooms["radio_count_total"],
            "radio_cu_coverage_min_percent": rollup[
                "radio_cu_coverage_min_percent"
            ],
            "radio_cu_coverage_median_percent": rollup[
                "radio_cu_coverage_median_percent"
            ],
            "radio_cu_coverage_max_percent": rollup[
                "radio_cu_coverage_max_percent"
            ],
            "radios_with_valid_cu_count": rollup["radios_with_valid_cu_count"],
            "radios_without_valid_cu_count": rollup[
                "radios_without_valid_cu_count"
            ],
            "high_possible_connected_max_seconds": _seconds(
                classrooms["high_possible_connected_max_us"]
            ),
            "high_possible_connected_responsible_radio_ids": classrooms[
                "high_possible_connected_responsible_radio_ids"
            ],
            "high_actual_observed_max_seconds": _seconds(
                classrooms["high_actual_observed_max_us"]
            ),
            "high_actual_observed_responsible_radio_ids": classrooms[
                "high_actual_observed_responsible_radio_ids"
            ],
            "very_high_possible_connected_max_seconds": _seconds(
                classrooms["very_high_possible_connected_max_us"]
            ),
            "very_high_possible_connected_responsible_radio_ids": classrooms[
                "very_high_possible_connected_responsible_radio_ids"
            ],
            "very_high_actual_observed_max_seconds": _seconds(
                classrooms["very_high_actual_observed_max_us"]
            ),
            "very_high_actual_observed_responsible_radio_ids": classrooms[
                "very_high_actual_observed_responsible_radio_ids"
            ],
        }
    )
    count_vectors: list[np.ndarray] = []
    for position, value in enumerate(radios["valid_cu_raw_counts"]):
        counts = np.asarray(value, dtype=np.int64)
        if counts.shape != (256,) or np.any(counts < 0):
            raise ValueError(
                "radio/class valid_cu_raw_counts must contain 256 "
                f"nonnegative values; row {position} is invalid"
            )
        count_vectors.append(counts)
    aggregate_counts = (
        np.sum(np.stack(count_vectors), axis=0, dtype=np.int64)
        if count_vectors
        else np.zeros(256, dtype=np.int64)
    )
    normalized_bands = radios["band"].map(_band_text)
    band_counts = {
        band: np.zeros(256, dtype=np.int64)
        for band in ("2.4", "5", "6", "unknown")
    }
    for band, counts in zip(normalized_bands, count_vectors):
        band_counts.setdefault(band, np.zeros(256, dtype=np.int64))
        band_counts[band] += counts

    population_source = radios.copy()
    radio_keys = population_source["radio_group_id"].astype(str)
    population_source["_ap_id"] = radio_keys.map(radio_metadata["ap_id"])
    population_source["_ap_has_6_ghz"] = radio_keys.map(
        radio_metadata["ap_has_6_ghz"]
    ).fillna(False)
    population_source["_band"] = normalized_bands
    population_source["_valid_bin_values"] = [
        int(counts.sum()) for counts in count_vectors
    ]
    population_specs = (
        (
            "all_5_ghz",
            "All 5 GHz radios",
            population_source["_band"].eq("5"),
        ),
        (
            "5_ghz_on_6_ghz_ap",
            "5 GHz radios on APs with observed 6 GHz",
            population_source["_band"].eq("5")
            & population_source["_ap_has_6_ghz"],
        ),
        (
            "6_ghz_on_6_ghz_ap",
            "6 GHz radios on APs with observed 6 GHz",
            population_source["_band"].eq("6")
            & population_source["_ap_has_6_ghz"],
        ),
    )
    population_provenance: dict[str, str | None] = {}
    for column in ("run_id", "schema_id", "metric_id"):
        values = sorted(set(population_source[column].dropna().astype(str)))
        if len(values) > 1:
            raise ValueError(
                f"radio/class summary contains multiple {column} values"
            )
        population_provenance[column] = values[0] if values else None
    population_rows: list[dict[str, Any]] = []
    for population_id, label, mask in population_specs:
        members = population_source.loc[mask]
        class_duration_us = int(
            pd.to_numeric(
                members["class_duration_us"], errors="raise"
            ).sum()
        )
        valid_duration_us = int(
            pd.to_numeric(
                members["valid_cu_duration_us"], errors="raise"
            ).sum()
        )
        average_raw = pd.to_numeric(
            members["average_cu_raw"], errors="coerce"
        )
        valid_durations = pd.to_numeric(
            members["valid_cu_duration_us"], errors="raise"
        )
        weighted_cu = float((average_raw * valid_durations).sum())
        high_duration_us = float(
            pd.to_numeric(
                members["high_duration_us"], errors="coerce"
            ).fillna(0).sum()
        )
        very_high_duration_us = float(
            pd.to_numeric(
                members["very_high_duration_us"], errors="coerce"
            ).fillna(0).sum()
        )
        population_rows.append(
            {
                **population_provenance,
                "population_id": population_id,
                "population_label": label,
                "status": "observed" if len(members) else "no_matching_radios",
                "radio_class_rows": int(len(members)),
                "radio_count": int(members["radio_group_id"].nunique()),
                "ap_count": int(members["_ap_id"].dropna().nunique()),
                "class_session_count": int(
                    members["class_session_id"].nunique()
                ),
                "class_radio_hours": class_duration_us / 3_600_000_000,
                "valid_cu_radio_hours": valid_duration_us / 3_600_000_000,
                "valid_cu_radio_time_coverage_percent": (
                    valid_duration_us / class_duration_us * 100.0
                    if class_duration_us
                    else None
                ),
                "average_cu_percent": (
                    weighted_cu / valid_duration_us * (100.0 / 255.0)
                    if valid_duration_us
                    else None
                ),
                **cu_mean_bounds_fields(weighted_cu, valid_duration_us, class_duration_us,
                                       prefix="average_cu_percent"),
                "high_cu_percent_of_valid_time": (
                    high_duration_us / valid_duration_us * 100.0
                    if valid_duration_us
                    else None
                ),
                "very_high_cu_percent_of_valid_time": (
                    very_high_duration_us / valid_duration_us * 100.0
                    if valid_duration_us
                    else None
                ),
                "valid_radio_bin_values": int(
                    members["_valid_bin_values"].sum()
                ),
                "expected_radio_bin_values": (sum(int(value) for value in members.expected_radio_bin_count)
                                               if members.expected_radio_bin_count.notna().all() else None),
            }
        )
    band_population = pd.DataFrame(population_rows)
    return CorrectedCuReportTables(
        radio_report,
        classroom_report,
        band_population,
        aggregate_counts,
        band_counts,
    )


def _csv_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    output = frame.copy()
    for column in output.columns:
        if output[column].dtype == "object":
            output[column] = output[column].map(_csv_value)
    output.to_csv(
        path, index=False, encoding="utf-8", lineterminator="\n", na_rep=""
    )


def _new_figure(*, width: float = 9.0, height: float = 5.0) -> Figure:
    figure = Figure(figsize=(width * 2.0, height * 2.0), layout="constrained")
    FigureCanvasAgg(figure)
    return figure


def _empty_axis(axis: Any, title: str) -> None:
    axis.set_title(title)
    axis.text(0.5, 0.5, "No values available", ha="center", va="center")
    axis.set_axis_off()


def _histogram_interval_us(bin_widths_us: tuple[int, ...]) -> int:
    widths = sorted(set(bin_widths_us))
    if not widths or any(width <= 0 for width in widths):
        raise ValueError("at least one positive analysis bin width is required")
    interval = widths[0]
    if any(width % interval for width in widths):
        raise ValueError(
            "combined time histograms require analysis bin widths that are "
            "integer multiples of the smallest width"
        )
    return interval


def _average_cu_edges_percent() -> np.ndarray:
    """Return 128 fixed buckets, each spanning two raw CU units."""

    return np.arange(0, 257, 2, dtype=float) * (100.0 / 255.0)


def _time_histogram_edges(
    maximum_seconds: float,
    *,
    interval_us: int,
) -> np.ndarray:
    """Return zero-anchored edges aligned to the analysis-bin interval."""

    interval_seconds = interval_us / 1_000_000.0
    upper_slots = int(np.ceil(maximum_seconds / interval_seconds))
    return np.arange(upper_slots + 2, dtype=float) * interval_seconds


def _cdf_below_raw_threshold(
    counts: np.ndarray,
    raw_threshold: int,
) -> float | None:
    """Return the share strictly below one raw CU threshold."""

    total = int(counts.sum())
    if not total:
        return None
    return float(counts[:raw_threshold].sum() / total)


def _annotate_cdf_thresholds(axis: Any, counts: np.ndarray, total_weight=None) -> None:
    """Mark the cumulative shares at the 50% and 75% CU boundaries."""

    rows = cu_cdf_summary_rows(counts, total_weight, scope="", population="", weighting="")
    for row, color in zip(rows, ("#777777", "#333333")):
        x_value, raw_threshold = row["cu_threshold_percent"], row["cu_raw_exclusive_threshold"]
        cumulative = _cdf_below_raw_threshold(counts, raw_threshold)
        if cumulative is None and row["cdf_lower_bound_percent"] is None:
            continue
        # Keep high cumulative values below their marker so their labels do
        # not collide with the axes title. Lower values remain above it.
        vertical_offset = -7 if cumulative is not None and cumulative >= 0.9 else 7
        if cumulative is not None:
            axis.scatter([x_value], [cumulative], color=color, s=24, zorder=4)
        observed = f"Observed {cumulative * 100:.1f}%" if cumulative is not None else "Observed unavailable"
        interval = (f"Min {row['cdf_lower_bound_percent']:.1f}% / max {row['cdf_upper_bound_percent']:.1f}%"
                    if row["cdf_lower_bound_percent"] is not None else "Bounds unavailable")
        axis.annotate(
            observed + "\n" + interval,
            xy=(x_value, cumulative if cumulative is not None else .5),
            xytext=(6, vertical_offset),
            textcoords="offset points",
            color=color,
            fontsize="small",
            verticalalignment=("top" if vertical_offset < 0 else "bottom"),
        )


def build_radio_class_mean_cu_histogram(radio: pd.DataFrame) -> Figure:
    """Render saved radio/class-session means with the existing histogram bins."""
    colors = {
        "2.4": "#4c78a8",
        "5": "#f58518",
        "6": "#54a24b",
        "unknown": "#777777",
    }

    figure = _new_figure()
    axis = figure.subplots()
    plotted = False
    ranged = False
    null_average_count = int(
        pd.to_numeric(radio["average_cu_percent"], errors="coerce").isna().sum()
    )
    for band in ("2.4", "5", "6", "unknown"):
        mask = radio["band"].fillna("unknown").astype(str).eq(band)
        values = pd.to_numeric(
            radio.loc[mask, "average_cu_percent"], errors="coerce"
        ).dropna()
        if not values.empty:
            axis.hist(
                values,
                bins=_average_cu_edges_percent(),
                histtype="step",
                linewidth=2,
                label=f"{band} GHz" if band != "unknown" else band,
                color=colors[band],
            )
            plotted = True
        lower, upper = "average_cu_percent_lower_bound", "average_cu_percent_upper_bound"
        if {lower, upper}.issubset(radio.columns):
            has_range = draw_histogram_bounds(axis, radio.loc[mask, lower], radio.loc[mask, upper],
                                              _average_cu_edges_percent(), color=colors[band])
            ranged |= has_range
            if has_range and values.empty:
                axis.plot([], [], color=colors[band], label=f"{band} GHz (no observed means)")
            plotted |= has_range
    if plotted:
        axis.set(
            title="Radio/class-session mean CU histogram" + (
                f"\n{null_average_count} observed means unavailable" if null_average_count else ""
            ),
            xlabel="Mean CU (%) - bin width approximately 0.78 percentage points",
            ylabel="Radio/class-sessions",
            xlim=(0, 100),
        )
        axis.legend()
        if ranged:
            axis.text(.02, .98, "Solid: observed means. Shading: min/max possible counts per bin.\n"
                      "Bucket maxima need not occur together.", transform=axis.transAxes, va="top", fontsize=9,
                      bbox={"facecolor": "white", "edgecolor": "none", "alpha": .85})
    else:
        _empty_axis(
            axis,
            "Radio/class-session mean CU histogram (no usable means)",
        )
    return figure


def build_radio_coverage_histograms(radio: pd.DataFrame) -> Figure:
    """Compare radio/class-session availability distributions on one count scale."""
    figure = _new_figure(width=12.0, height=4.2)
    axes = figure.subplots(1, 3, sharey=True)
    shares = (("cu_coverage_percent", "Valid CU coverage"),
              ("missing_cu_seconds", "Beacon present, CU missing"), ("no_beacon_seconds", "No beacon"))
    class_seconds = pd.to_numeric(radio["class_duration_seconds"], errors="coerce")
    for axis, (column, title) in zip(axes, shares):
        values = pd.to_numeric(radio[column], errors="coerce")
        if column != "cu_coverage_percent":
            values = values.div(class_seconds).mul(100.0)
        values = values.dropna()
        if values.empty:
            _empty_axis(axis, title)
        else:
            axis.hist(values, bins=np.linspace(0, 100, 41), color="#4c78a8", edgecolor="none", linewidth=0)
            axis.set(title=title, xlabel="Share of class (%)", ylabel="Radio/class rows", xlim=(0, 100))
    return figure


def expected_radio_bin_totals(radio):
    """Read exact saved counts; an unknown count never becomes zero coverage."""
    def total(frame):
        if "expected_radio_bin_count" not in frame or frame.expected_radio_bin_count.isna().any():
            return None if len(frame) else 0
        return sum(int(value) for value in frame.expected_radio_bin_count)
    grouped = radio.assign(_bounds_band=radio.band.map(_band_text)).groupby("_bounds_band", sort=False)
    return total(radio), {band: total(group) for band, group in grouped}


def build_radio_bin_cu_cdf(counts, expected_count=None):
    counts = np.asarray(counts, dtype=np.int64)
    total = sum(int(value) for value in counts)
    figure = _new_figure(width=10, height=5.5)
    axis = figure.subplots()
    if total:
        axis.step(np.arange(256) * (100 / 255), np.cumsum(counts, dtype=np.float64) / total,
                  where="post", color="#f58518", linewidth=2, label="Observed")
    bounded = draw_cdf_bounds(axis, counts, expected_count, color="#f58518")
    if total or bounded:
        axis.axvline(50, color="#777777", linestyle="--", linewidth=1)
        axis.axvline(75, color="#333333", linestyle="--", linewidth=1)
        _annotate_cdf_thresholds(axis, counts, expected_count)
        axis.set(title="CDF of selected radio/bin channel utilization", xlabel="Channel utilization (%)",
                 ylabel="Cumulative fraction", xlim=(0, 100), ylim=(0, 1.01))
        axis.grid(alpha=.3)
        axis.legend(loc="lower right", fontsize="small")
        if not total:
            axis.text(.02, .98, "No observed CU; missing-bin range only", transform=axis.transAxes, va="top")
    else:
        _empty_axis(axis, "CDF of selected radio/bin channel utilization")
    return figure


def cu_cdf_report_table(tables):
    total, band_totals = expected_radio_bin_totals(tables.radio_class)
    rows = cu_cdf_summary_rows(tables.valid_cu_raw_counts, total, scope="overall", population="all", weighting="radio_bins")
    for band, counts in sorted(tables.band_valid_cu_raw_counts.items()):
        rows.extend(cu_cdf_summary_rows(counts, band_totals.get(band, 0), scope="band", population=band, weighting="radio_bins"))
    return pd.DataFrame(rows)


def build_radio_bin_cu_by_band(band_counts, *, kind="histogram", expected_counts=None) -> Figure:
    """Keep count weighting; CDFs also show known, entirely missing populations."""
    if kind not in {"histogram", "cdf"}:
        raise ValueError("CU band plot kind must be histogram or cdf")
    expected_counts = expected_counts or {}
    bands = [band for band in ("2.4", "5", "6") if np.asarray(band_counts.get(band, [])).sum() > 0
             or (kind == "cdf" and (expected_counts.get(band) or 0) > 0)]
    figure = _new_figure(width=5.0 * max(1, len(bands)), height=4.8)
    axes = np.atleast_1d(figure.subplots(1, max(1, len(bands)), sharey=True))
    values = np.arange(256) * (100.0 / 255.0)
    colors = {"2.4": "#4c78a8", "5": "#f58518", "6": "#54a24b"}
    for axis, band in zip(axes, bands):
        counts = np.asarray(band_counts.get(band, np.zeros(256, dtype=np.int64)), dtype=np.int64)
        total = int(counts.sum())
        if kind == "histogram":
            axis.bar(values, counts, width=100.0 / 255.0, color=colors[band], linewidth=0)
            axis.set_ylabel("Selected radio/bin values")
        else:
            if total:
                axis.step(values, np.cumsum(counts, dtype=np.float64) / total, where="post", color=colors[band], linewidth=2, label="Observed")
            draw_cdf_bounds(axis, counts, expected_counts.get(band), color=colors[band])
            axis.axvline(50.0, color="#777777", linestyle="--", linewidth=1)
            axis.axvline(75.0, color="#333333", linestyle="--", linewidth=1)
            _annotate_cdf_thresholds(axis, counts, expected_counts.get(band))
            axis.set(ylabel="Cumulative share", ylim=(0, 1.01))
            axis.grid(alpha=0.3)
            axis.legend(loc="lower right", fontsize="small")
        count_label = "observed values" if kind == "cdf" else "values"
        axis.set(title=f"{band} GHz ({total:,} {count_label})", xlabel="Channel utilization (%)", xlim=(0, 100))
    if not bands:
        _empty_axis(axes[0], "No valid CU for sampled bands")
    unknown = int(np.asarray(band_counts.get("unknown", [])).sum())
    title = "Channel utilization histograms by band" if kind == "histogram" else "CDF of channel utilization by band"
    if unknown:
        title += f" ({unknown:,} unknown-band values omitted)"
    figure.suptitle(title, fontsize=12)
    return figure


def build_radio_mean_cu_vs_coverage(radio):
    null_average_count = int(pd.to_numeric(radio["average_cu_percent"], errors="coerce").isna().sum())
    ordered_bands = [
        band
        for band in ("2.4", "5", "6", "unknown")
        if radio["band"].fillna("unknown").astype(str).eq(band).any()
    ] or ["unknown"]
    figure = _new_figure(width=max(7.0, 5.5 * len(ordered_bands)))
    axes_value = figure.subplots(1, len(ordered_bands))
    axes = [axes_value] if len(ordered_bands) == 1 else list(axes_value)
    palette = (
        "#4c78a8", "#f58518", "#54a24b", "#e45756", "#72b7b2",
        "#b279a2", "#ff9da6", "#9d755d", "#bab0ac",
    )
    cohorts = sorted(radio["capture_cohort"].dropna().astype(str).unique())
    cohort_colors = {
        cohort: palette[position % len(palette)]
        for position, cohort in enumerate(cohorts)
    }
    for axis, band in zip(axes, ordered_bands):
        band_rows = radio.loc[
            radio["band"].fillna("unknown").astype(str).eq(band)
        ]
        plotted = False
        for cohort, cohort_rows in band_rows.groupby(
            "capture_cohort", sort=True, dropna=False
        ):
            label = str(cohort) if pd.notna(cohort) else "unknown cohort"
            points = cohort_rows.loc[
                :, ["cu_coverage_percent", "average_cu_percent"]
            ].apply(pd.to_numeric, errors="coerce").dropna()
            if points.empty:
                continue
            axis.scatter(
                points["cu_coverage_percent"],
                points["average_cu_percent"],
                s=18,
                alpha=0.65,
                label=label,
                color=cohort_colors.get(label, "#777777"),
            )
            plotted = True
        band_label = f"{band} GHz" if band != "unknown" else "Unknown band"
        if plotted:
            axis.set(
                title=f"{band_label} radio/class rows",
                xlabel="Valid-CU coverage (%)",
                ylabel="Observed mean CU (%)",
                xlim=(0, 100),
                ylim=(0, 100),
            )
            if plotted:
                axis.legend(fontsize="small")
        else:
            _empty_axis(axis, f"{band_label} radio/class rows")
    figure.suptitle(
        "Observed mean CU vs valid-CU coverage\n"
        "by AP model and radio configuration "
        f"({null_average_count} null omitted)",
        fontsize=12,
    )
    return figure


def _save_plots(
    tables: CorrectedCuReportTables,
    directory: Path,
    *,
    bin_widths_us: tuple[int, ...],
) -> None:
    directory.mkdir()
    radio, classroom = tables.radio_class, tables.classroom_class
    expected_total, expected_bands = expected_radio_bin_totals(radio)
    build_radio_class_mean_cu_histogram(radio).savefig(directory / PLOT_FILENAMES[0], dpi=140)
    build_radio_mean_cu_vs_coverage(radio).savefig(directory / PLOT_FILENAMES[1], dpi=140)

    build_radio_coverage_histograms(radio).savefig(directory / PLOT_FILENAMES[2], dpi=140)

    raw_counts = tables.valid_cu_raw_counts
    raw_values = np.arange(256, dtype=float)
    cu_percent = raw_values * (100.0 / 255.0)
    total_values = int(raw_counts.sum())

    figure = _new_figure(width=10.0, height=5.5)
    axis = figure.subplots()
    if total_values:
        axis.bar(
            cu_percent,
            raw_counts,
            width=(100.0 / 255.0),
            color="#4c78a8",
            linewidth=0,
        )
        axis.set(
            title=(
                "Channel utilization for selected radio/bin values "
                f"({total_values:,} valid values)"
            ),
            xlabel="Channel utilization (%) - one bucket per raw CU value",
            ylabel="Selected radio/bin values",
            xlim=(0, 100),
        )
    else:
        _empty_axis(axis, "Channel utilization for selected radio/bin values")
    figure.savefig(directory / PLOT_FILENAMES[3], dpi=140)

    build_radio_bin_cu_cdf(raw_counts, expected_total).savefig(directory / PLOT_FILENAMES[4], dpi=140)

    figure = _new_figure(width=11.0, height=4.5)
    axes = figure.subplots(1, 2, sharex=True, sharey=True)
    interval_us = _histogram_interval_us(bin_widths_us)
    interval_seconds = interval_us / 1_000_000.0
    streak_panels = []
    for prefix, title in (
        ("high", "High CU (>50%) longest streak"),
        ("very_high", "Very high CU (>75%) longest streak"),
    ):
        actual = pd.to_numeric(
            classroom[f"{prefix}_actual_observed_max_seconds"],
            errors="coerce",
        ).dropna()
        possible = pd.to_numeric(
            classroom[f"{prefix}_possible_connected_max_seconds"],
            errors="coerce",
        ).dropna()
        values = pd.concat([actual, possible], ignore_index=True)
        omitted = int(len(classroom) * 2 - len(actual) - len(possible))
        streak_panels.append((title, actual, possible, values, omitted))
    all_streak_values = pd.concat(
        [panel[3] for panel in streak_panels],
        ignore_index=True,
    )
    common_bins = (
        _time_histogram_edges(
            float(all_streak_values.max()),
            interval_us=interval_us,
        )
        if not all_streak_values.empty
        else None
    )
    for axis, (title, actual, possible, values, omitted) in zip(
        axes,
        streak_panels,
    ):
        if values.empty:
            _empty_axis(axis, f"{title} ({omitted} null omitted)")
            continue
        axis.hist(
            possible,
            bins=common_bins,
            histtype="step",
            linewidth=2,
            label="Possible connected",
        )
        axis.hist(
            actual,
            bins=common_bins,
            histtype="step",
            linewidth=2,
            label="Actual observed",
        )
        axis.set(
            title=f"{title} ({omitted} null omitted)",
            xlabel=(
                f"Seconds ({interval_seconds:g}-second analysis-bin buckets)"
            ),
            ylabel="Classroom/class rows",
        )
        axis.legend()
    if common_bins is not None:
        for axis in axes:
            axis.set_xlim(float(common_bins[0]), float(common_bins[-1]))
    figure.savefig(directory / PLOT_FILENAMES[5], dpi=140)

    build_radio_bin_cu_by_band(tables.band_valid_cu_raw_counts).savefig(directory / PLOT_FILENAMES[6], dpi=140)
    build_radio_bin_cu_by_band(tables.band_valid_cu_raw_counts, kind="cdf", expected_counts=expected_bands).savefig(directory / PLOT_FILENAMES[7], dpi=140)


def _summary_value(value: object) -> str:
    if value is None:
        return "not recorded"
    if isinstance(value, (int, np.integer)):
        return f"{int(value):,}"
    return str(value)


def _elapsed_text(started_at: object, generated_at: datetime) -> str:
    if not isinstance(started_at, str) or not started_at.strip():
        return "not recorded"
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return "not recorded"
    elapsed_seconds = max(0, int((generated_at - started).total_seconds()))
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:d}:{minutes:02d}:{seconds:02d}"


def _station_enrollment_summary_lines(root: Path) -> list[str]:
    report_path = (
        root / STATION_REPORT_DIRECTORY / STATION_AGGREGATE_REPORT_FILENAME
    )
    if not report_path.is_file():
        return []

    report = pd.read_csv(report_path)
    _require_columns(
        report,
        set(STATION_AGGREGATE_REPORT_COLUMNS),
        "station aggregate report",
    )
    lines = [
        "Enrollment-weighted peak client count per enrolled",
        "- Method: sum of eligible class-session peaks divided by the "
        "corresponding enrollment sum.",
    ]
    for row in report.itertuples(index=False):
        population_type = str(row.population_type)
        population_id = str(row.population_id)
        if population_type == "overall":
            label = "Overall"
        elif population_type == "ssid":
            label = f"SSID {population_id}"
        elif population_type == "band":
            band = _band_text(population_id)
            label = f"Band {band} GHz" if band != "unknown" else "Band unknown"
        else:
            label = f"{population_type} {population_id}"

        ratio = row.average_peak_client_count_per_enrolled
        ratio_text = "unavailable" if pd.isna(ratio) else f"{float(ratio):.3f}"
        lines.append(
            f"- {label}: {ratio_text} "
            f"(eligible sessions {int(row.eligible_session_count):,}; "
            f"peak sum {int(row.peak_client_count_sum):,}; "
            f"enrollment sum {int(row.combined_enrollment_sum):,}; "
            "stale-assisted peaks "
            f"{int(row.stale_assisted_peak_session_count):,})"
        )
    return lines


def _cu_station_summary_lines(root: Path) -> list[str]:
    path = (
        root
        / "reports"
        / "radio_class_detail"
        / "cu_station_correlation.csv"
    )
    if not path.is_file():
        return []
    report = pd.read_csv(path)
    required = {
        "population_type",
        "population_id",
        "total_radio_bin_rows",
        "valid_pair_rows",
        "valid_pair_coverage",
        "stale_assisted_pair_rows",
        "pearson_correlation",
        "spearman_correlation",
    }
    if not required.issubset(report.columns):
        return []
    lines = ["Radio/bin CU and station-count relationship"]
    for row in report.itertuples(index=False):
        if str(row.population_type) == "overall":
            label = "Overall"
        elif str(row.population_type) == "band":
            band = _band_text(row.population_id)
            label = f"Band {band} GHz" if band != "unknown" else "Band unknown"
        else:
            continue
        pearson = (
            "unavailable"
            if pd.isna(row.pearson_correlation)
            else f"{float(row.pearson_correlation):.3f}"
        )
        spearman = (
            "unavailable"
            if pd.isna(row.spearman_correlation)
            else f"{float(row.spearman_correlation):.3f}"
        )
        coverage = (
            "unavailable"
            if pd.isna(row.valid_pair_coverage)
            else f"{float(row.valid_pair_coverage):.1%}"
        )
        lines.append(
            f"- {label}: Pearson {pearson}; Spearman {spearman}; "
            f"valid pairs {int(row.valid_pair_rows):,}/"
            f"{int(row.total_radio_bin_rows):,} ({coverage}); "
            f"stale-assisted pairs {int(row.stale_assisted_pair_rows):,}"
        )
    return lines


def _bounded_summary(row, field, suffix="%"):
    def number(value):
        return "unavailable" if value is None or pd.isna(value) or not np.isfinite(float(value)) else f"{float(value):.2f}{suffix}"
    observed = number(row.get(field))
    lower, upper = (number(row.get(f"{field}_{bound}_bound")) for bound in ("lower", "upper"))
    interval = "bounds unavailable" if "unavailable" in (lower, upper) else f"min {lower}; max {upper}"
    return f"observed {observed}; {interval}"


def _affected_station_summary_lines(root: Path) -> list[str]:
    path = root / "reports/affected_stations/population_exposure.csv"
    if not path.exists():
        return []
    frame = pd.read_csv(path, float_precision="round_trip")
    row = frame.loc[frame.population_scope.eq("overall") & frame.population_id.eq("all")].iloc[0]
    def percent(value):
        return "unavailable" if pd.isna(value) else f"{value:.2f}%"
    return ["Potentially affected station associations (not proven impairment or unique clients)",
            f"- Station-time with high CU: {_bounded_summary(row, 'high_exposure_percent')}; very high: {_bounded_summary(row, 'very_high_exposure_percent')} (included in high)",
            "- Observed percentages use paired CU/station-time; bounds use all available station-time. Unknown station counts are outside these bounds.",
            f"- CU/station pair coverage, pooled radio time: {percent(row.pair_coverage_percent)}; stale-assisted paired time: {percent(row.stale_pair_percent)}",
            "- Session peaks require complete same-bin radio totals; see coverage and distributions in reports/affected_stations/",
            "- Numeric results: reports/affected_stations/population_exposure.csv and entity_session_exposure.csv"]


def _band_comparison_summary_lines(root: Path) -> list[str]:
    report = root / "reports" / "band_comparison"
    if not (report / "population_summary.csv").exists():
        return []
    if not all((report / name).is_file() for name in ("deployment_summary.csv", "station_adoption_summary.csv")):
        return []
    population = pd.read_csv(report / "population_summary.csv", float_precision="round_trip")
    deployment = pd.read_csv(report / "deployment_summary.csv", float_precision="round_trip").iloc[0]
    adoption = pd.read_csv(report / "station_adoption_summary.csv", float_precision="round_trip")
    overall = population.loc[population.population_scope.eq("overall")].iloc[0]
    station = adoption.loc[adoption.population_scope.eq("overall")].iloc[0]
    def number(value, suffix=""):
        return "unavailable" if pd.isna(value) else f"{float(value):.2f}{suffix}"
    cohort_lines = []
    for population_id, label in (("5_on_without_6", "5 GHz on APs without 6 GHz"),
                                 ("5_on_with_6", "5 GHz on APs with 6 GHz"),
                                 ("6_on_with_6", "6 GHz on APs with 6 GHz")):
        selected = population.loc[population.population_scope.eq("ap_cohort") & population.population_id.eq(population_id)]
        if len(selected):
            row = selected.iloc[0]
            cohort_lines.append(f"- {label}: mean CU {_bounded_summary(row, 'mean_cu_percent')}; "
                                f"mean stations/radio {number(row.mean_radio_station_count)}; CU coverage {number(row.cu_coverage_percent, '%')}")
    return [
        "Band comparison and longitudinal metrics",
        f"- 6 GHz enabled APs in configured sample: {int(deployment.six_ghz_enabled_ap_count)}/{int(deployment.configured_ap_count)} "
        f"({number(deployment.six_ghz_enabled_ap_percent, '%')}); enabled but unobserved: {int(deployment.enabled_6_ghz_not_observed_ap_count)}",
        f"- Overall mean CU: {_bounded_summary(overall, 'mean_cu_percent')}; CU coverage: {number(overall.cu_coverage_percent, '%')}",
        "- CU mean bounds assign missing radio time 0%/100% CU; they are not confidence intervals or filled measurements.",
        f"- Overall mean stations/radio, available time: {number(overall.mean_radio_station_count)}",
        *cohort_lines,
        f"- Overall CU relative to associated station count: {number(overall.cu_relative_to_associated_station_count)} percentage points/station",
        f"- 6 GHz station association-time share: {number(station['5_6_six_station_share_percent'], '%')} of 5+6 GHz; "
        f"{number(station['all_bands_six_station_share_percent'], '%')} of all sampled bands",
        "- Detailed populations, component sums, coverage and manual year-over-year context: reports/band_comparison/",
        "- Calculated overall/band/cohort metrics: reports/band_comparison/population_summary.csv",
    ]


def _run_summary(
    *,
    root: Path,
    tables: CorrectedCuReportTables,
    identity: pd.DataFrame,
    run_metadata: Mapping[str, Any],
    generated_at: datetime,
) -> str:
    configuration = run_metadata.get("effective_configuration", {})
    if not isinstance(configuration, Mapping):
        configuration = {}
    bounded = configuration.get("bounded_working_set", {})
    if not isinstance(bounded, Mapping):
        bounded = {}
    radio_bin_configuration = configuration.get("cu_radio_bin", {})
    if not isinstance(radio_bin_configuration, Mapping):
        radio_bin_configuration = {}
    scope = configuration.get("project_scope_counts", {})
    if not isinstance(scope, Mapping):
        scope = {}
    input_adapter = configuration.get("input_adapter", {})
    if not isinstance(input_adapter, Mapping):
        input_adapter = {}
    normalization = input_adapter.get("shared_room_normalization", {})
    if not isinstance(normalization, Mapping):
        normalization = {}
    normalized_sessions = normalization.get("sessions", ())
    if not isinstance(normalized_sessions, (list, tuple)):
        normalized_sessions = ()
    stages = run_metadata.get("completed_stages", ())
    if not isinstance(stages, (list, tuple)):
        stages = ()
    warning_count = sum(
        int(stage.get("warning_count", 0))
        for stage in stages
        if isinstance(stage, Mapping)
    )
    error_count = sum(
        int(stage.get("error_count", 0))
        for stage in stages
        if isinstance(stage, Mapping)
    )
    run_ids = sorted(set(tables.radio_class["run_id"].astype(str)))
    run_id = run_ids[0] if len(run_ids) == 1 else ", ".join(run_ids)
    identity_bands = identity.assign(
        _band=identity["band"].map(_band_text)
    )
    radio_counts = {
        band: int(
            identity_bands.loc[
                identity_bands["_band"].eq(band), "radio_group_id"
            ].nunique()
        )
        for band in ("2.4", "5", "6", "unknown")
    }
    selected_parts = len(list((root / "selected_observations").rglob("*.parquet")))
    normalization_lines = [
        "Input normalization",
        "- Shared-room physical sessions merged: "
        + _summary_value(normalization.get("merged_physical_session_count", 0)),
        "- Source schedule rows represented by those merges: "
        + _summary_value(normalization.get("source_row_count", 0)),
    ]
    for record in normalized_sessions:
        if not isinstance(record, Mapping):
            continue
        courses = record.get("courses", ())
        if isinstance(courses, (list, tuple)):
            course_text = " + ".join(str(course) for course in courses)
        else:
            course_text = str(courses)
        normalization_lines.append(
            "- "
            f"{_summary_value(record.get('classroom'))} "
            f"{_summary_value(record.get('class_start'))} to "
            f"{_summary_value(record.get('class_end'))}: "
            f"{course_text}; enrolled total "
            f"{_summary_value(record.get('enrolled_total'))}"
        )
    station_enrollment_lines = _station_enrollment_summary_lines(root)
    cu_station_lines = _cu_station_summary_lines(root)
    band_comparison_lines = _band_comparison_summary_lines(root)
    affected_station_lines = _affected_station_summary_lines(root)
    return "\n".join(
        [
            "Corrected beacon-analysis run summary",
            "",
            "Start here: reports/index.html (offline run overview)",
            "",
            f"Project: {_summary_value(run_metadata.get('project_name'))}",
            f"Run ID: {run_id}",
            "Status: completed inside the atomic publication gate",
            "Generated at: "
            + generated_at.astimezone(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "Elapsed through report generation: "
            + _elapsed_text(run_metadata.get("started_at"), generated_at),
            "",
            "Processing",
            f"- Capture files: {_summary_value(configuration.get('capture_count'))}",
            f"- Capture workers: {_summary_value(configuration.get('capture_workers'))}",
            "- Configured APs: "
            + _summary_value(configuration.get("configured_ap_count")),
            "- Expanded physical class sessions: "
            + _summary_value(configuration.get("class_session_count")),
            "- In-scope retained beacon rows: "
            + _summary_value(scope.get("retained_rows")),
            "- Class-window observations staged: "
            + _summary_value(bounded.get("class_window_rows")),
            "- Selected observations: "
            + _summary_value(bounded.get("selected_rows")),
            "- Station rollup rows: "
            + _summary_value(bounded.get("station_rollup_rows")),
            "- Radio/bin detail rows: "
            + _summary_value(bounded.get("radio_bin_detail_rows")),
            "- Largest per-session radio/bin detail part: "
            + _summary_value(
                bounded.get("maximum_session_radio_bin_detail_rows")
            ),
            "- Largest per-session station rollup part: "
            + _summary_value(
                bounded.get("maximum_session_station_rollup_rows")
            ),
            f"- Selected-observation date partitions: {selected_parts:,}",
            "- Radio/class detail plots: "
            + (
                "enabled"
                if radio_bin_configuration.get("radio_class_plots") is True
                else "disabled"
            ),
            "",
            *normalization_lines,
            "",
            "Resolved identity",
            f"- BSSIDs: {identity['bssid'].nunique():,}",
            f"- APs observed: {identity['ap_id'].nunique():,}",
            f"- Radios observed: {identity['radio_group_id'].nunique():,}",
            f"- Classrooms observed: {identity['classroom'].nunique():,}",
            f"- 2.4 GHz radios: {radio_counts['2.4']:,}",
            f"- 5 GHz radios: {radio_counts['5']:,}",
            f"- 6 GHz radios: {radio_counts['6']:,}",
            f"- Unknown-band radios: {radio_counts['unknown']:,}",
            "",
            *station_enrollment_lines,
            *([""] if station_enrollment_lines else []),
            *cu_station_lines,
            *([""] if cu_station_lines else []),
            *band_comparison_lines,
            *([""] if band_comparison_lines else []),
            *affected_station_lines,
            *([""] if affected_station_lines else []),
            "Summary outputs",
            f"- Radio/class rows: {len(tables.radio_class):,}",
            f"- Classroom/class rows: {len(tables.classroom_class):,}",
            f"- Band-population rows: {len(tables.band_population):,}",
            "- Completed stages including this report package: "
            f"{len(stages) + 1:,}",
            f"- Stage warnings: {warning_count:,}",
            f"- Stage errors: {error_count:,}",
            "",
            "See each report family's methodology.txt for metric definitions "
            "and descriptions of its derived output files.",
            "",
        ]
    )


def _methodology(
    tables: CorrectedCuReportTables,
    display_timezone: str | None,
    bin_widths_us: tuple[int, ...],
    *,
    station_report_present: bool = False,
    radio_bin_report_present: bool = False,
) -> str:
    radio = tables.radio_class
    schema_ids = sorted(set(radio["schema_id"].astype(str)))
    metric_ids = sorted(set(radio["metric_id"].astype(str)))
    station_guide = (
        [
            "- ../station_count/station_bin_rollups.parquet: derived ADR-0010 "
            "radio, SSID, band, and classroom/bin station totals.",
            "- ../station_count/station_class_report.csv: per-session station "
            "availability, peaks, enrollment, and peak-client-count ratio.",
            "- ../station_count/station_aggregate_report.csv: enrollment-"
            "weighted overall, SSID, and band peak-client-count-per-enrolled "
            "results.",
            "- ../station_count/classroom_bin_station_histogram*.png: complete "
            "classroom/bin all-band and per-band distributions.",
            "- ../station_count/methodology.txt: station membership, stale-"
            "assistance, completeness, and enrollment rules.",
        ]
        if station_report_present
        else []
    )
    radio_bin_guide = (
        [
            "- ../radio_class_detail/radio_bin_detail.parquet: one corrected "
            "CU state/value and the accepted station total per physical "
            "radio/class-session/bin.",
            "- ../radio_class_detail/cu_station_correlation.csv and "
            "cu_station_hexbin*.png: same-radio/same-bin valid-pair coverage, "
            "descriptive correlations, and density plots.",
            "- ../radio_class_detail/methodology.txt: CU source selection, "
            "lineage, station join, and optional plot interpretation.",
        ]
        if radio_bin_report_present
        else []
    )
    return "\n".join(
        [
            "Corrected CU, coverage, and streak report package",
            "",
            "Source artifacts: project_identity_map.parquet, "
            "radio_class_summary.parquet, and classroom_class_summary.parquet.",
            "This package converts units and adds presentation rollups; "
            "it does not recalculate packet-level metrics.",
            f"Schema ID: {', '.join(schema_ids)}",
            f"Metric ID: {', '.join(metric_ids)}",
            "Display timezone: "
            f"{display_timezone or 'not configured; UTC columns remain authoritative'}",
            "",
            "High CU means raw QBSS channel utilization >= 128 "
            "(>50% of the 0-255 scale).",
            "Very high CU means raw QBSS channel utilization >= 192 "
            "(>75% of the 0-255 scale).",
            "Coverage is valid-CU bin duration divided by total class duration.",
            "Missing CU means a beacon was selected but its CU value was "
            "absent or invalid; no beacon is tracked separately.",
            "Actual observed streaks stop at missing-CU or no-beacon bins.",
            "Possible connected streaks may connect high bins across an "
            "empty intervening bin for comparison.",
            "Missing values remain blank. Measured zero remains zero. "
            "Terminal partial-bin durations are preserved.",
            "Average CU excludes missing-CU and no-beacon bins. "
            "No combined classroom CU average is reported.",
            f"Ordinary mean bounds metric: {CU_BOUNDS_METRIC_ID}. Existing mean "
            "columns remain observed. Companion *_lower_bound and *_upper_bound "
            "columns assign missing-CU and no-beacon time 0% and 100% CU, "
            "respectively, using total expected radio duration. A nonempty "
            "all-missing population has bounds 0-100%; an absent population "
            "has unavailable bounds. These are missing-bin scenarios, not "
            "confidence intervals or filled measurements. The bounds do not "
            "cover missed variation within otherwise observed bins. HTML/text "
            "displays keep the observed point value alongside minimum/maximum "
            "scenarios. Historical refresh derives only supported companions "
            "from saved components and leaves unsupported bounds unavailable.",
            "CU CDFs keep their observed lines and show minimum/maximum curves "
            "with a shaded range. For cumulative valid weight A, total expected "
            "weight T and missing weight M, the range is A/T to (A+M)/T below "
            "100% CU; at 100% both endpoints are one. These CDFs give each "
            "radio/bin equal weight. Expected counts come from the comparison "
            "radio/session table, or ceiling(class duration / bin width) only "
            "when one explicit width applies to every session. Mixed widths "
            "without per-radio counts leave bounds unavailable. Absent bands "
            "are omitted; present bands with all CU missing show only a range.",
            "Each point in the CU-versus-coverage plot is one radio/class "
            "row. Panels separate bands; color identifies AP model and the "
            "observed count of same-band radios on that AP. This scatter "
            "shows observed means only; bounds remain in the numeric tables.",
            "AP model is optional source metadata. Same-band radio count is "
            "derived from the project identity map, not inferred from model. "
            "Neither field changes CU, coverage, or weighting.",
            "Classroom streak maxima name every radio tied for the maximum.",
            "Average-CU histograms use two raw CU units per bucket.",
            "Mean-histogram shading bounds each bucket's possible count: the "
            "minimum counts intervals wholly in that bucket; the maximum "
            "counts all intersecting intervals. Bucket maxima need not be "
            "jointly attainable. Observed histogram counts remain unchanged.",
            "Coverage and missingness histograms use 2.5-percentage-point buckets.",
            "The all-value CU histogram and CDF use one valid selected value "
            "per radio/analysis bin, with one bucket per raw CU value.",
            "Band histogram and CDF panels use that same radio/bin population "
            "and separate 2.4, 5, and 6 GHz without reweighting.",
            "The band-population CSV reports all 5 GHz radios, the subset of "
            "5 GHz radios on APs with a resolved 6 GHz radio, and the 6 GHz "
            "radios on those APs. The all-5 population therefore overlaps "
            "the paired-AP 5 GHz subset by design.",
            "Here ap_has_6_ghz means observed 6 GHz in the identity map. "
            "Enablement-aware AP/classroom comparisons, including explicit "
            "6GHz_Enabled input declarations, are in ../band_comparison/.",
            "Population coverage is summed valid-CU radio time divided by "
            "summed class radio time. Population average CU is duration-"
            "weighted across valid CU time. Empty populations remain explicit "
            "with null CU/coverage values.",
            "Time histograms use "
            f"{_histogram_interval_us(bin_widths_us) / 1_000_000.0:g}-second "
            "buckets aligned to the smallest compatible configured analysis "
            "interval.",
            "",
            "Output file guide",
            "- ../run_summary.txt: concise processing, input-normalization, "
            "identity, band, row-count, stage, warning, and error results "
            "for this run.",
            "- methodology.txt: metric definitions, interpretation rules, and "
            "this output-file guide.",
            "- radio_class_report.csv: one human-readable row per stable "
            "radio/class session, including coverage, CU, missingness, and "
            "both streak interpretations.",
            "- classroom_class_report.csv: one row per physical classroom "
            "session with per-band radio counts and classroom maximum streaks "
            "plus responsible radio IDs.",
            "- band_population_report.csv: all-5-GHz, paired-AP 5-GHz, and "
            "6-GHz population counts, coverage, average CU, and high/very-high "
            "time shares.",
            "- cu_cdf_bounds.csv: observed/minimum/maximum cumulative percentages "
            "at the existing raw CU <128 and <192 cutoffs (50%/75% markers), "
            "with exact valid/expected bin counts. The similarly named file "
            "in ../band_comparison/ uses radio-duration weights instead.",
            "- plots/radio_average_cu_distribution.png: radio/class average-CU "
            "distribution using two raw CU values per bucket.",
            "- plots/radio_cu_vs_coverage.png: radio/class average CU versus "
            "valid-CU coverage, separated by band and review cohort.",
            "- plots/radio_coverage_and_missingness.png: distributions of "
            "valid CU, beacon-with-missing-CU, and no-beacon class shares.",
            "- plots/radio_bin_cu_histogram.png: all valid selected radio/bin "
            "CU values, one bucket per raw 0-255 value.",
            "- plots/radio_bin_cu_cdf.png: CDF of the same radio/bin CU values "
            "with cumulative shares labeled at 50% and 75% CU.",
            "- plots/classroom_streak_distributions.png: classroom maximum "
            "actual and possible-connected streak distributions at both CU "
            "thresholds on common axes.",
            "- plots/radio_bin_cu_histogram_by_band.png: the radio/bin CU "
            "histogram split into 2.4, 5, and 6 GHz panels.",
            "- plots/radio_bin_cu_cdf_by_band.png: band-separated CDF panels "
            "with cumulative shares labeled at 50% and 75% CU.",
            "- ../../project_identity_map.parquet: authoritative BSSID to "
            "radio to AP to classroom identity and band/channel evidence.",
            "- ../../selected_observations/: authoritative date-partitioned "
            "selected beacon rows, at most one per BSSID/class bin.",
            "- ../../radio_class_summary.parquet: authoritative ADR-0004 "
            "radio/class coverage, CU, missingness, and streak metrics.",
            "- ../../classroom_class_summary.parquet: authoritative physical "
            "classroom-session radio counts and maximum streak metrics.",
            "- ../../exports/csv/: compact CSV copies of the identity and two "
            "summary Parquet tables for review.",
            *station_guide,
            *radio_bin_guide,
            "- ../../manifest.json: input hashes, code provenance, effective "
            "configuration, schemas, stages, and artifact references.",
            "- ../../../_logs/<run_id>.jsonl: project-level lifecycle events, "
            "including input normalization and publication status.",
            "",
        ]
    )


def write_corrected_cu_report_package(
    run_directory: str | Path,
    *,
    display_timezone: str | None = None,
    bin_widths_us: tuple[int, ...],
    run_metadata: Mapping[str, Any] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> WrittenCorrectedCuReportPackage:
    """Write the bounded report package inside an unpublished run directory."""

    root = Path(run_directory)
    destination = root / REPORT_DIRECTORY
    summary_path = root / RUN_SUMMARY_PATH
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
    if summary_path.exists():
        raise FileExistsError(f"run summary already exists: {summary_path}")
    sources = {
        "radio": root / "radio_class_summary.parquet",
        "classroom": root / "classroom_class_summary.parquet",
        "identity": root / "project_identity_map.parquet",
    }
    for path in sources.values():
        if not path.is_file():
            raise FileNotFoundError(f"authoritative Parquet table is missing: {path}")

    destination.mkdir(parents=True)
    try:
        identity = pd.read_parquet(sources["identity"])
        radio_summary = pd.read_parquet(sources["radio"])
        counts_path = root / "reports/band_comparison/radio_class_comparison.csv"
        radio_bin_counts = None
        if counts_path.is_file():
            radio_bin_counts = pd.read_csv(counts_path, usecols=["class_session_id", "radio_group_id", "radio_bin_rows"],
                                           dtype={"class_session_id": "string", "radio_group_id": "string"})
        elif len(set(bin_widths_us)) == 1:
            # One explicitly configured width determines every expected count,
            # including terminal partial bins. Never choose among mixed widths.
            width = _histogram_interval_us(bin_widths_us)
            radio_bin_counts = radio_summary[["class_session_id", "radio_group_id"]].copy()
            radio_bin_counts["radio_bin_rows"] = [(int(duration) + width - 1) // width
                                                  for duration in radio_summary.class_duration_us]
        tables = build_corrected_cu_report_tables(
            radio_summary,
            pd.read_parquet(sources["classroom"]),
            identity,
            display_timezone=display_timezone,
            radio_bin_counts=radio_bin_counts,
        )
        _write_csv(tables.radio_class, destination / RADIO_REPORT_FILENAME)
        _write_csv(tables.classroom_class, destination / CLASSROOM_REPORT_FILENAME)
        _write_csv(
            tables.band_population,
            destination / BAND_POPULATION_REPORT_FILENAME,
        )
        cdf_table = cu_cdf_report_table(tables)
        _write_csv(cdf_table, destination / CDF_BOUNDS_REPORT_FILENAME)
        (destination / METHODOLOGY_FILENAME).write_text(
            _methodology(
                tables,
                display_timezone,
                bin_widths_us,
                station_report_present=(
                    root / "reports" / "station_count"
                ).is_dir(),
                radio_bin_report_present=(
                    root / "reports" / "radio_class_detail"
                ).is_dir(),
            ),
            encoding="utf-8",
            newline="\n",
        )
        _save_plots(
            tables,
            destination / "plots",
            bin_widths_us=bin_widths_us,
        )
        generated_at = (clock or (lambda: datetime.now(timezone.utc)))()
        if generated_at.tzinfo is None:
            raise ValueError("report clock must return a timezone-aware value")
        summary_path.write_text(
            _run_summary(
                root=root,
                tables=tables,
                identity=identity,
                run_metadata=run_metadata or {},
                generated_at=generated_at,
            ),
            encoding="utf-8",
            newline="\n",
        )
    except BaseException:
        summary_path.unlink(missing_ok=True)
        shutil.rmtree(destination, ignore_errors=True)
        parent = destination.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        raise

    files = (
        destination / RADIO_REPORT_FILENAME,
        destination / CLASSROOM_REPORT_FILENAME,
        destination / BAND_POPULATION_REPORT_FILENAME,
        destination / CDF_BOUNDS_REPORT_FILENAME,
        summary_path,
        destination / METHODOLOGY_FILENAME,
        *(destination / "plots" / name for name in PLOT_FILENAMES),
    )
    return WrittenCorrectedCuReportPackage(
        directory=destination,
        radio_class_rows=len(tables.radio_class),
        classroom_class_rows=len(tables.classroom_class),
        band_population_rows=len(tables.band_population),
        files=files,
        cdf_bounds_rows=len(cdf_table),
    )
