"""Bounded human-readable reports derived from corrected durable artifacts.

The report package is deliberately presentation-only.  It reads the compact
Parquet outputs, converts units and labels for review, and never recalculates
packet-level selection or ADR-0004 metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd


REPORT_DIRECTORY = Path("reports") / "cu_coverage_streak"
RADIO_REPORT_FILENAME = "radio_class_report.csv"
CLASSROOM_REPORT_FILENAME = "classroom_class_report.csv"
BAND_POPULATION_REPORT_FILENAME = "band_population_report.csv"
RUN_SUMMARY_FILENAME = "run_summary.txt"
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
) -> CorrectedCuReportTables:
    """Build presentation tables without changing corrected metric semantics."""

    _require_columns(radio_summary, _RADIO_REQUIRED, "radio/class summary")
    _require_columns(
        classroom_summary, _CLASSROOM_REQUIRED, "classroom/class summary"
    )
    if display_timezone is not None:
        ZoneInfo(display_timezone)

    radio_metadata = _radio_metadata(identity_map)
    radios = radio_summary.copy()
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
    missing_keys = classroom_keys.difference(coverage_rollup.index)
    if len(missing_keys):
        raise ValueError("classroom summaries have no matching radio summaries")
    rollup = coverage_rollup.loc[classroom_keys].reset_index(drop=True)
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
            "5 GHz radios on APs with 6 GHz",
            population_source["_band"].eq("5")
            & population_source["_ap_has_6_ghz"],
        ),
        (
            "6_ghz_on_6_ghz_ap",
            "6 GHz radios on APs with 6 GHz",
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
    figure = Figure(figsize=(width, height), layout="constrained")
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
    """Return 64 fixed buckets, each spanning four raw CU units."""

    return np.arange(0, 257, 4, dtype=float) * (100.0 / 255.0)


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


def _annotate_cdf_thresholds(axis: Any, counts: np.ndarray) -> None:
    """Mark the cumulative shares at the 50% and 75% CU boundaries."""

    for x_value, raw_threshold, color in (
        (50.0, 128, "#777777"),
        (75.0, 192, "#333333"),
    ):
        cumulative = _cdf_below_raw_threshold(counts, raw_threshold)
        if cumulative is None:
            continue
        # Keep high cumulative values below their marker so their labels do
        # not collide with the axes title. Lower values remain above it.
        vertical_offset = -7 if cumulative >= 0.9 else 7
        axis.scatter(
            [x_value],
            [cumulative],
            color=color,
            s=24,
            zorder=4,
        )
        axis.annotate(
            f"{cumulative * 100.0:.1f}%",
            xy=(x_value, cumulative),
            xytext=(6, vertical_offset),
            textcoords="offset points",
            color=color,
            fontsize="small",
            verticalalignment=("top" if vertical_offset < 0 else "bottom"),
        )


def _save_plots(
    tables: CorrectedCuReportTables,
    directory: Path,
    *,
    bin_widths_us: tuple[int, ...],
) -> None:
    directory.mkdir()
    radio = tables.radio_class
    classroom = tables.classroom_class
    colors = {
        "2.4": "#4c78a8",
        "5": "#f58518",
        "6": "#54a24b",
        "unknown": "#777777",
    }

    figure = _new_figure()
    axis = figure.subplots()
    plotted = False
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
    if plotted:
        axis.set(
            title=(
                "Average channel utilization by radio/class "
                f"({null_average_count} null omitted)"
            ),
            xlabel="Average CU (%) - four raw CU units per bucket",
            ylabel="Radio/class rows",
            xlim=(0, 100),
        )
        axis.legend()
    else:
        _empty_axis(
            axis,
            "Average channel utilization by radio/class "
            f"({null_average_count} null omitted)",
        )
    figure.savefig(directory / PLOT_FILENAMES[0], dpi=140)

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
                ylabel="Average CU (%)",
                xlim=(0, 100),
                ylim=(0, 100),
            )
            axis.legend(fontsize="small")
        else:
            _empty_axis(axis, f"{band_label} radio/class rows")
    figure.suptitle(
        "Average CU vs valid-CU coverage\n"
        "by AP model and radio configuration "
        f"({null_average_count} null omitted)",
        fontsize=12,
    )
    figure.savefig(directory / PLOT_FILENAMES[1], dpi=140)

    figure = _new_figure(width=12.0, height=4.2)
    axes = figure.subplots(1, 3)
    shares = (
        ("cu_coverage_percent", "Valid CU coverage"),
        ("missing_cu_seconds", "Beacon present, CU missing"),
        ("no_beacon_seconds", "No beacon"),
    )
    class_seconds = pd.to_numeric(radio["class_duration_seconds"], errors="coerce")
    for axis, (column, title) in zip(axes, shares):
        values = pd.to_numeric(radio[column], errors="coerce")
        if column != "cu_coverage_percent":
            values = values.div(class_seconds).mul(100.0)
        values = values.dropna()
        if values.empty:
            _empty_axis(axis, title)
        else:
            axis.hist(
                values,
                bins=np.linspace(0, 100, 21),
                color="#4c78a8",
                edgecolor="none",
                linewidth=0,
            )
            axis.set(
                title=title,
                xlabel="Share of class (%)",
                ylabel="Radio/class rows",
                xlim=(0, 100),
            )
    figure.savefig(directory / PLOT_FILENAMES[2], dpi=140)

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

    figure = _new_figure(width=10.0, height=5.5)
    axis = figure.subplots()
    if total_values:
        cdf = np.cumsum(raw_counts, dtype=np.float64) / total_values
        axis.step(cu_percent, cdf, where="post", color="#f58518", linewidth=2)
        axis.axvline(
            50.0,
            color="#777777",
            linestyle="--",
            linewidth=1,
            label="50% CU (high begins at raw 128)",
        )
        axis.axvline(
            75.0,
            color="#333333",
            linestyle="--",
            linewidth=1,
            label="75% CU (very high begins at raw 192)",
        )
        _annotate_cdf_thresholds(axis, raw_counts)
        axis.set(
            title="CDF of selected radio/bin channel utilization",
            xlabel="Channel utilization (%)",
            ylabel="Cumulative share of valid radio/bin values",
            xlim=(0, 100),
            ylim=(0, 1.01),
        )
        axis.grid(alpha=0.3)
        axis.legend()
    else:
        _empty_axis(axis, "CDF of selected radio/bin channel utilization")
    figure.savefig(directory / PLOT_FILENAMES[4], dpi=140)

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

    plotted_bands = ["2.4", "5", "6"]
    unknown_count = int(
        tables.band_valid_cu_raw_counts.get(
            "unknown", np.zeros(256, dtype=np.int64)
        ).sum()
    )
    figure = _new_figure(width=15.0, height=4.8)
    axes = figure.subplots(1, 3)
    for axis, band in zip(axes, plotted_bands):
        counts = tables.band_valid_cu_raw_counts.get(
            band, np.zeros(256, dtype=np.int64)
        )
        total = int(counts.sum())
        if not total:
            _empty_axis(axis, f"{band} GHz (no valid values)")
            continue
        axis.bar(
            cu_percent,
            counts,
            width=(100.0 / 255.0),
            color=colors[band],
            linewidth=0,
        )
        axis.set(
            title=f"{band} GHz ({total:,} values)",
            xlabel="Channel utilization (%)",
            ylabel="Selected radio/bin values",
            xlim=(0, 100),
        )
    figure.suptitle(
        "Channel utilization by band - one valid value per radio/bin"
        + (
            f" ({unknown_count:,} unknown-band values omitted)"
            if unknown_count
            else ""
        ),
        fontsize=12,
    )
    figure.savefig(directory / PLOT_FILENAMES[6], dpi=140)

    figure = _new_figure(width=15.0, height=4.8)
    axes = figure.subplots(1, 3)
    for axis, band in zip(axes, plotted_bands):
        counts = tables.band_valid_cu_raw_counts.get(
            band, np.zeros(256, dtype=np.int64)
        )
        total = int(counts.sum())
        if not total:
            _empty_axis(axis, f"{band} GHz (no valid values)")
            continue
        cdf = np.cumsum(counts, dtype=np.float64) / total
        axis.step(
            cu_percent,
            cdf,
            where="post",
            color=colors[band],
            linewidth=2,
        )
        axis.axvline(
            50.0,
            color="#777777",
            linestyle="--",
            linewidth=1,
        )
        axis.axvline(
            75.0,
            color="#333333",
            linestyle="--",
            linewidth=1,
        )
        _annotate_cdf_thresholds(axis, counts)
        axis.set(
            title=f"{band} GHz ({total:,} values)",
            xlabel="Channel utilization (%)",
            ylabel="Cumulative share",
            xlim=(0, 100),
            ylim=(0, 1.01),
        )
        axis.grid(alpha=0.3)
    figure.suptitle(
        "CDF of channel utilization by band"
        + (
            f" ({unknown_count:,} unknown-band values omitted)"
            if unknown_count
            else ""
        ),
        fontsize=12,
    )
    figure.savefig(directory / PLOT_FILENAMES[7], dpi=140)


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
    return "\n".join(
        [
            "Corrected beacon-analysis run summary",
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
            f"- Selected-observation date partitions: {selected_parts:,}",
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
            "Summary outputs",
            f"- Radio/class rows: {len(tables.radio_class):,}",
            f"- Classroom/class rows: {len(tables.classroom_class):,}",
            f"- Band-population rows: {len(tables.band_population):,}",
            "- Completed stages including this report package: "
            f"{len(stages) + 1:,}",
            f"- Stage warnings: {warning_count:,}",
            f"- Stage errors: {error_count:,}",
            "",
            "See methodology.txt for metric definitions and a description of "
            "every authoritative and derived output file.",
            "",
        ]
    )


def _methodology(
    tables: CorrectedCuReportTables,
    display_timezone: str | None,
    bin_widths_us: tuple[int, ...],
) -> str:
    radio = tables.radio_class
    schema_ids = sorted(set(radio["schema_id"].astype(str)))
    metric_ids = sorted(set(radio["metric_id"].astype(str)))
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
            "Each point in the CU-versus-coverage plot is one radio/class "
            "row. Panels separate bands; color identifies AP model and the "
            "observed count of same-band radios on that AP.",
            "AP model is optional source metadata. Same-band radio count is "
            "derived from the project identity map, not inferred from model. "
            "Neither field changes CU, coverage, or weighting.",
            "Classroom streak maxima name every radio tied for the maximum.",
            "Average-CU histograms use four raw CU units per bucket.",
            "The all-value CU histogram and CDF use one valid selected value "
            "per radio/analysis bin, with one bucket per raw CU value.",
            "Band histogram and CDF panels use that same radio/bin population "
            "and separate 2.4, 5, and 6 GHz without reweighting.",
            "The band-population CSV reports all 5 GHz radios, the subset of "
            "5 GHz radios on APs with a resolved 6 GHz radio, and the 6 GHz "
            "radios on those APs. The all-5 population therefore overlaps "
            "the paired-AP 5 GHz subset by design.",
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
            "- run_summary.txt: concise processing, input-normalization, "
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
            "- plots/radio_average_cu_distribution.png: radio/class average-CU "
            "distribution using four raw CU values per bucket.",
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
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
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
        tables = build_corrected_cu_report_tables(
            pd.read_parquet(sources["radio"]),
            pd.read_parquet(sources["classroom"]),
            identity,
            display_timezone=display_timezone,
        )
        _write_csv(tables.radio_class, destination / RADIO_REPORT_FILENAME)
        _write_csv(tables.classroom_class, destination / CLASSROOM_REPORT_FILENAME)
        _write_csv(
            tables.band_population,
            destination / BAND_POPULATION_REPORT_FILENAME,
        )
        (destination / METHODOLOGY_FILENAME).write_text(
            _methodology(tables, display_timezone, bin_widths_us), encoding="utf-8", newline="\n"
        )
        _save_plots(
            tables,
            destination / "plots",
            bin_widths_us=bin_widths_us,
        )
        generated_at = (clock or (lambda: datetime.now(timezone.utc)))()
        if generated_at.tzinfo is None:
            raise ValueError("report clock must return a timezone-aware value")
        (destination / RUN_SUMMARY_FILENAME).write_text(
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
        shutil.rmtree(destination, ignore_errors=True)
        parent = destination.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        raise

    files = (
        destination / RADIO_REPORT_FILENAME,
        destination / CLASSROOM_REPORT_FILENAME,
        destination / BAND_POPULATION_REPORT_FILENAME,
        destination / RUN_SUMMARY_FILENAME,
        destination / METHODOLOGY_FILENAME,
        *(destination / "plots" / name for name in PLOT_FILENAMES),
    )
    return WrittenCorrectedCuReportPackage(
        directory=destination,
        radio_class_rows=len(tables.radio_class),
        classroom_class_rows=len(tables.classroom_class),
        band_population_rows=len(tables.band_population),
        files=files,
    )
