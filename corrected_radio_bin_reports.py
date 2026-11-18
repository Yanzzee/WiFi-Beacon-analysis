"""Bounded radio/bin CU, radio admission-capacity and station reporting."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from math import ceil
from pathlib import Path
import re
import shutil
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from corrected_summary_output import RADIO_BIN_CU_COLUMNS
from corrected_admission_selection import RADIO_DETAIL_SCHEMA_ID
from corrected_admission_reports import (
    AdmissionAccumulator, REPORT_DIRECTORY as ADMISSION_REPORT_DIRECTORY,
    write_admission_outputs,
)


REPORT_DIRECTORY = Path("reports") / "radio_class_detail"
RADIO_BIN_FILENAME = "radio_bin_detail.parquet"
PLOT_INDEX_FILENAME = "radio_class_plot_index.csv"
CORRELATION_FILENAME = "cu_station_correlation.csv"
RELATIONSHIP_PLOT_FILENAME = "cu_station_hexbin.png"
RELATIONSHIP_BAND_PLOT_FILENAME = "cu_station_hexbin_by_band.png"
METHODOLOGY_FILENAME = "methodology.txt"

CORRELATION_COLUMNS = (
    "run_id",
    "schema_id",
    "metric_id",
    "population_type",
    "population_id",
    "total_radio_bin_rows",
    "valid_cu_rows",
    "available_station_rows",
    "valid_pair_rows",
    "valid_pair_coverage",
    "stale_assisted_pair_rows",
    "radio_count",
    "class_session_count",
    "pearson_correlation",
    "spearman_correlation",
)

RADIO_BIN_DETAIL_COLUMNS = (
    *RADIO_BIN_CU_COLUMNS,
    "station_count",
    "station_available",
    "station_stale_assisted",
)

RADIO_BIN_DETAIL_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("class_session_id", pa.string(), nullable=False),
        pa.field("classroom", pa.string(), nullable=False),
        pa.field("radio_group_id", pa.string(), nullable=False),
        pa.field("ap_id", pa.string()),
        pa.field("band", pa.string()),
        pa.field("channel", pa.int32()),
        pa.field("bin_number", pa.int64(), nullable=False),
        pa.field("bin_start", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("bin_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("bin_duration_us", pa.int64(), nullable=False),
        pa.field("width_status", pa.string(), nullable=False),
        pa.field("cu_state", pa.string(), nullable=False),
        pa.field("qbss_cu_raw", pa.uint8()),
        pa.field("source_ssid", pa.string()),
        pa.field("source_bssid", pa.string()),
        pa.field("source_selection_method", pa.string()),
        pa.field("source_observation_time", pa.timestamp("ns", tz="UTC")),
        pa.field("observed_member_bssid_count", pa.uint32(), nullable=False),
        pa.field("valid_cu_candidate_count", pa.uint32(), nullable=False),
        pa.field("admission_metric_id", pa.string(), nullable=False),
        pa.field("admission_state", pa.string(), nullable=False),
        pa.field("qbss_admission_capacity", pa.uint16()),
        pa.field("admission_source_ssid", pa.string()),
        pa.field("admission_source_bssid", pa.string()),
        pa.field("admission_source_selection_method", pa.string()),
        pa.field("admission_source_observation_time", pa.timestamp("ns", tz="UTC")),
        pa.field("valid_admission_candidate_count", pa.uint32(), nullable=False),
        pa.field("invalid_admission_candidate_count", pa.uint32(), nullable=False),
        pa.field("admission_disagreement", pa.bool_(), nullable=False),
        pa.field("admission_source_matches_cu", pa.bool_(), nullable=False),
        pa.field("station_count", pa.uint64()),
        pa.field("station_available", pa.bool_(), nullable=False),
        pa.field("station_stale_assisted", pa.bool_(), nullable=False),
    ]
).with_metadata({b"report_schema_id": RADIO_DETAIL_SCHEMA_ID.encode("utf-8")})


@dataclass(frozen=True)
class WrittenRadioBinReportPackage:
    directory: Path
    rows: int
    plot_count: int
    correlation_rows: int
    files: tuple[Path, ...]
    admission_files: tuple[Path, ...]
    admission_summary_rows: int
    admission_correlation_rows: int


class _RelationshipAccumulator:
    """Bounded exact counts for radio/bin CU-station relationships."""

    def __init__(self) -> None:
        self.counts: dict[str, Counter[tuple[int, int]]] = defaultdict(Counter)
        self.total_rows: Counter[str] = Counter()
        self.valid_cu_rows: Counter[str] = Counter()
        self.available_station_rows: Counter[str] = Counter()
        self.stale_pair_rows: Counter[str] = Counter()
        self.radios: dict[str, set[str]] = defaultdict(set)
        self.sessions: dict[str, set[str]] = defaultdict(set)
        self.run_id: str | None = None
        self.schema_id: str | None = None
        self.metric_id: str | None = None

    def add(self, part: pd.DataFrame) -> None:
        if part.empty:
            return
        for field in ("run_id", "schema_id", "metric_id"):
            values = part[field].dropna().astype(str).unique().tolist()
            if len(values) != 1:
                raise ValueError(f"radio/bin detail requires one {field} per part")
            existing = getattr(self, field)
            if existing is not None and existing != values[0]:
                raise ValueError(f"radio/bin detail contains multiple {field} values")
            setattr(self, field, values[0])

        bands = part["band"].astype("string").fillna("unknown").astype(str)
        valid_cu = part["cu_state"].eq("valid") & part["qbss_cu_raw"].notna()
        available_station = (
            part["station_available"].astype(bool) & part["station_count"].notna()
        )
        valid_pair = valid_cu & available_station
        populations = [("all", pd.Series(True, index=part.index)), *[
            (f"band:{band}", bands.eq(band)) for band in sorted(bands.unique())
        ]]
        for population, mask in populations:
            self.total_rows[population] += int(mask.sum())
            self.valid_cu_rows[population] += int((mask & valid_cu).sum())
            self.available_station_rows[population] += int(
                (mask & available_station).sum()
            )
            paired = part.loc[mask & valid_pair]
            if paired.empty:
                continue
            self.stale_pair_rows[population] += int(
                paired["station_stale_assisted"].astype(bool).sum()
            )
            self.radios[population].update(paired["radio_group_id"].astype(str))
            self.sessions[population].update(
                paired["class_session_id"].astype(str)
            )
            for cu_raw, station_count in zip(
                pd.to_numeric(paired["qbss_cu_raw"]).astype(int),
                pd.to_numeric(paired["station_count"]).astype(int),
            ):
                self.counts[population][(int(cu_raw), int(station_count))] += 1


def _weighted_correlation(
    counts: Counter[tuple[float, float]],
) -> float | None:
    total = float(sum(counts.values()))
    if total <= 1:
        return None
    sum_x = sum(x * weight for (x, _y), weight in counts.items())
    sum_y = sum(y * weight for (_x, y), weight in counts.items())
    mean_x = sum_x / total
    mean_y = sum_y / total
    covariance = sum(
        weight * (x - mean_x) * (y - mean_y)
        for (x, y), weight in counts.items()
    )
    variance_x = sum(
        weight * (x - mean_x) ** 2 for (x, _y), weight in counts.items()
    )
    variance_y = sum(
        weight * (y - mean_y) ** 2 for (_x, y), weight in counts.items()
    )
    denominator = float(np.sqrt(variance_x * variance_y))
    return None if denominator == 0 else float(covariance / denominator)


def _midranks(frequencies: Counter[int]) -> dict[int, float]:
    cumulative = 0
    ranks: dict[int, float] = {}
    for value in sorted(frequencies):
        frequency = int(frequencies[value])
        ranks[value] = cumulative + (frequency + 1.0) / 2.0
        cumulative += frequency
    return ranks


def _weighted_spearman(
    counts: Counter[tuple[int, int]],
) -> float | None:
    x_frequency: Counter[int] = Counter()
    y_frequency: Counter[int] = Counter()
    for (x, y), weight in counts.items():
        x_frequency[x] += weight
        y_frequency[y] += weight
    x_ranks = _midranks(x_frequency)
    y_ranks = _midranks(y_frequency)
    ranked: Counter[tuple[float, float]] = Counter()
    for (x, y), weight in counts.items():
        ranked[(x_ranks[x], y_ranks[y])] += weight
    return _weighted_correlation(ranked)


def _relationship_report(accumulator: _RelationshipAccumulator) -> pd.DataFrame:
    populations = sorted(
        accumulator.total_rows,
        key=lambda value: (value != "all", value),
    )
    rows: list[dict[str, object]] = []
    for population in populations:
        counts = accumulator.counts[population]
        pair_count = int(sum(counts.values()))
        total_count = int(accumulator.total_rows[population])
        population_type, population_id = (
            ("overall", "all")
            if population == "all"
            else ("band", population.split(":", 1)[1])
        )
        rows.append(
            {
                "run_id": accumulator.run_id,
                "schema_id": accumulator.schema_id,
                "metric_id": accumulator.metric_id,
                "population_type": population_type,
                "population_id": population_id,
                "total_radio_bin_rows": total_count,
                "valid_cu_rows": int(accumulator.valid_cu_rows[population]),
                "available_station_rows": int(
                    accumulator.available_station_rows[population]
                ),
                "valid_pair_rows": pair_count,
                "valid_pair_coverage": (
                    pair_count / total_count if total_count else None
                ),
                "stale_assisted_pair_rows": int(
                    accumulator.stale_pair_rows[population]
                ),
                "radio_count": len(accumulator.radios[population]),
                "class_session_count": len(accumulator.sessions[population]),
                "pearson_correlation": _weighted_correlation(counts),
                "spearman_correlation": _weighted_spearman(counts),
            }
        )
    return pd.DataFrame(rows, columns=CORRELATION_COLUMNS)


def _relationship_axis(
    axis: Any,
    counts: Counter[tuple[int, int]],
    *,
    title: str,
    station_limit: int,
) -> Any | None:
    axis.set_title(title)
    axis.set_xlabel("Channel utilization (%)")
    axis.set_ylabel("Station count")
    axis.set_xlim(0, 100)
    axis.set_ylim(0, station_limit)
    axis.grid(alpha=0.2)
    if not counts:
        axis.text(
            0.5,
            0.5,
            "No valid CU/station pairs",
            transform=axis.transAxes,
            ha="center",
            va="center",
        )
        return None
    pairs = list(counts)
    x = np.asarray([pair[0] / 255.0 * 100.0 for pair in pairs])
    y = np.asarray([pair[1] for pair in pairs])
    weights = np.asarray([counts[pair] for pair in pairs])
    return axis.hexbin(
        x,
        y,
        C=weights,
        reduce_C_function=np.sum,
        # Counts are integer-valued; a fine vertical lattice creates artificial
        # striping between occupied rows. Coarsen display bins, not source data.
        gridsize=(40, 30),
        extent=(0, 100, 0, station_limit),
        mincnt=1,
        bins="log",
        cmap="viridis",
    )


def _write_relationship_plots(
    accumulator: _RelationshipAccumulator,
    destination: Path,
) -> tuple[Path, Path]:
    maximum = max(
        (station for counts in accumulator.counts.values() for _cu, station in counts),
        default=0,
    )
    station_limit = 100 if maximum <= 100 else int(ceil(maximum / 10.0) * 10)

    overall_path = destination / RELATIONSHIP_PLOT_FILENAME
    figure = Figure(figsize=(16, 12), constrained_layout=True)
    FigureCanvasAgg(figure)
    axis = figure.subplots()
    artist = _relationship_axis(
        axis,
        accumulator.counts["all"],
        title="Radio/bin channel utilization and station count",
        station_limit=station_limit,
    )
    if artist is not None:
        figure.colorbar(artist, ax=axis, label="Radio/bin pairs")
    figure.savefig(overall_path, dpi=140)

    band_path = destination / RELATIONSHIP_BAND_PLOT_FILENAME
    bands = [
        population.split(":", 1)[1]
        for population in sorted(accumulator.total_rows)
        if population.startswith("band:")
    ]
    panel_count = max(1, len(bands))
    band_figure = Figure(
        figsize=(12 * panel_count, 10),
        constrained_layout=True,
    )
    FigureCanvasAgg(band_figure)
    axes = np.atleast_1d(band_figure.subplots(1, panel_count))
    if not bands:
        bands = ["unknown"]
    for axis, band in zip(axes, bands):
        artist = _relationship_axis(
            axis,
            accumulator.counts[f"band:{band}"],
            title=f"{band} GHz" if band != "unknown" else "Unknown band",
            station_limit=station_limit,
        )
        if artist is not None:
            band_figure.colorbar(artist, ax=axis, label="Radio/bin pairs")
    band_figure.savefig(band_path, dpi=140)
    return overall_path, band_path


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def build_radio_bin_detail(
    radio_bin_cu: pd.DataFrame,
    station_rollups: pd.DataFrame,
) -> pd.DataFrame:
    """Join one selected CU value with the accepted radio station total."""

    if not isinstance(radio_bin_cu, pd.DataFrame):
        raise TypeError("radio_bin_cu must be a pandas DataFrame")
    if not isinstance(station_rollups, pd.DataFrame):
        raise TypeError("station_rollups must be a pandas DataFrame")
    _require_columns(radio_bin_cu, set(RADIO_BIN_CU_COLUMNS), "radio/bin CU")
    station_required = {
        "class_session_id",
        "classroom",
        "bin_number",
        "rollup_type",
        "rollup_id",
        "station_count",
        "available",
        "stale_assisted",
    }
    _require_columns(station_rollups, station_required, "station rollups")

    key = ["class_session_id", "classroom", "bin_number", "radio_group_id"]
    if radio_bin_cu.duplicated(key).any():
        raise ValueError("radio/bin CU requires one row per radio/bin")
    radio_station = station_rollups.loc[
        station_rollups["rollup_type"].astype(str).eq("radio"),
        [
            "class_session_id",
            "classroom",
            "bin_number",
            "rollup_id",
            "station_count",
            "available",
            "stale_assisted",
        ],
    ].rename(columns={"rollup_id": "radio_group_id"})
    if radio_station.duplicated(key).any():
        raise ValueError("station rollups require one radio row per radio/bin")

    result = radio_bin_cu.merge(
        radio_station,
        on=key,
        how="left",
        validate="one_to_one",
    ).rename(
        columns={
            "available": "station_available",
            "stale_assisted": "station_stale_assisted",
        }
    )
    result["station_available"] = (
        result["station_available"].astype("boolean").fillna(False).astype(bool)
    )
    result["station_stale_assisted"] = (
        result["station_stale_assisted"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )
    result.loc[~result["station_available"], "station_count"] = pd.NA
    return result.loc[:, RADIO_BIN_DETAIL_COLUMNS].sort_values(
        ["class_session_id", "classroom", "radio_group_id", "bin_number"],
        kind="stable",
    ).reset_index(drop=True)


def _safe_segment(value: object) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value)).strip("-.")
    return text[:80] or "unknown"


def _unknown_runs(valid: np.ndarray) -> list[tuple[int, int]]:
    runs: list[tuple[int, int]] = []
    start: int | None = None
    for index, is_valid in enumerate(valid):
        if not is_valid and start is None:
            start = index
        elif is_valid and start is not None:
            runs.append((start, index - 1))
            start = None
    if start is not None:
        runs.append((start, len(valid) - 1))
    return runs


def _station_axis_limit(station: pd.Series) -> int:
    observed = pd.to_numeric(station, errors="coerce").dropna()
    if observed.empty or float(observed.max()) <= 100:
        return 100
    return int(ceil(float(observed.max()) / 10.0) * 10)


def _radio_ap_id(first: pd.Series) -> str:
    ap_id = str(first["ap_id"]).strip()
    if not ap_id or ap_id.lower() in {"nan", "none", "<na>"}:
        ap_id = str(first["radio_group_id"]).split("/radio/", 1)[0]
    return ap_id


def _plot_value(value: object) -> str:
    if value is None or pd.isna(value):
        return "Unknown"
    if isinstance(value, (int, float, np.integer, np.floating)):
        return f"{value:g}"
    return str(value).strip() or "Unknown"


def _radio_plot_title(ordered: pd.DataFrame, timezone: ZoneInfo) -> str:
    first = ordered.iloc[0]
    ap_id = _radio_ap_id(first)
    radio_id = str(first["radio_group_id"])
    radio_label = (
        radio_id.rsplit("/radio/", 1)[1]
        if "/radio/" in radio_id
        else radio_id
    )
    local_start = pd.Timestamp(ordered.iloc[0]["bin_start"]).tz_convert(timezone)
    local_end = pd.Timestamp(ordered.iloc[-1]["bin_end"]).tz_convert(timezone)
    if local_start.date() == local_end.date():
        date_label = local_start.strftime("%Y-%m-%d")
    else:
        date_label = (
            f"{local_start:%Y-%m-%d}–{local_end:%Y-%m-%d}"
        )
    return (
        f"{ap_id} | Radio {radio_label} | {date_label} | "
        f"{local_start:%H:%M}–{local_end:%H:%M}"
    )


def _write_radio_plot(
    group: pd.DataFrame,
    path: Path,
    *,
    display_timezone: str,
    plot_context: dict[str, object] | None = None,
) -> None:
    ordered = group.sort_values("bin_number", kind="stable")
    timezone = ZoneInfo(display_timezone)
    centers = pd.to_datetime(ordered["bin_start"], utc=True) + (
        pd.to_datetime(ordered["bin_end"], utc=True)
        - pd.to_datetime(ordered["bin_start"], utc=True)
    ) / 2
    cu_raw = pd.to_numeric(ordered["qbss_cu_raw"], errors="coerce")
    cu_percent = cu_raw / 255.0 * 100.0
    valid = ordered["cu_state"].eq("valid").to_numpy(dtype=bool)

    figure = Figure(figsize=(24, 10), constrained_layout=True)
    FigureCanvasAgg(figure)
    cu_axis = figure.subplots()
    station_axis = cu_axis.twinx()
    cu_axis.fill_between(
        centers,
        0.0,
        cu_percent,
        where=valid,
        color="#b9d7ea",
        linewidth=0,
        label=None,
        zorder=1,
    )
    cu_axis.plot(
        centers,
        cu_percent,
        color="#1f77b4",
        linewidth=1.4,
        label="Measured CU",
        zorder=2,
    )
    unknown_runs = _unknown_runs(valid)
    interpolation_labeled = False
    for start, end in unknown_runs:
        previous = start - 1
        following = end + 1
        if previous >= 0 and following < len(ordered):
            cu_axis.plot(
                [centers.iloc[previous], centers.iloc[following]],
                [cu_percent.iloc[previous], cu_percent.iloc[following]],
                color="#1f77b4",
                linestyle="--",
                linewidth=1.2,
                alpha=0.75,
                label="Unknown CU"
                if not interpolation_labeled
                else None,
            )
            interpolation_labeled = True

    station = pd.to_numeric(ordered["station_count"], errors="coerce").where(
        ordered["station_available"].astype(bool)
    )
    station_axis.plot(
        centers,
        station,
        color="#f58518",
        linewidth=1.2,
        alpha=0.85,
        drawstyle="steps-mid",
        label="Station count",
    )
    station_bridge_labeled = False
    for start, end in _unknown_runs(station.notna().to_numpy(dtype=bool)):
        previous = start - 1
        following = end + 1
        if previous >= 0 and following < len(ordered):
            station_axis.plot(
                [centers.iloc[previous], centers.iloc[following]],
                [station.iloc[previous], station.iloc[following]],
                color="#f58518",
                linestyle="--",
                linewidth=1.2,
                alpha=0.75,
                label="Unknown station count"
                if not station_bridge_labeled
                else None,
            )
            station_bridge_labeled = True
    stale = ordered["station_stale_assisted"].astype(bool) & station.notna()
    if stale.any():
        station_axis.scatter(
            centers.loc[stale],
            station.loc[stale],
            s=13,
            facecolors="none",
            edgecolors="#f58518",
            linewidths=0.8,
            label="Stale-assisted station count",
        )

    context = plot_context or {}
    first = ordered.iloc[0]
    band = _plot_value(first.get("band"))
    band_label = f"{band} GHz" if band.lower() != "unknown" else "Unknown"
    station_axis.text(
        0.01,
        0.98,
        "\n".join(
            [
                f"Building-room: {_plot_value(first['classroom'])}",
                f"Subject: {_plot_value(context.get('subject'))}",
                f"Seats: {_plot_value(context.get('seats'))} | "
                f"Enrollment: {_plot_value(context.get('enrollment'))}",
                f"Room AP count (configured): {_plot_value(context.get('room_ap_count'))}",
                f"AP model: {_plot_value(context.get('ap_model'))}",
                f"Radio band: {band_label}",
            ]
        ),
        transform=station_axis.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "white",
              "edgecolor": "#aaaaaa", "alpha": 0.9},
        zorder=5,
    )

    cu_axis.set_title(_radio_plot_title(ordered, timezone))
    locator = mdates.AutoDateLocator(minticks=4, maxticks=10, tz=timezone)
    cu_axis.xaxis.set_major_locator(locator)
    cu_axis.xaxis.set_major_formatter(
        mdates.DateFormatter("%H:%M", tz=timezone)
    )
    cu_axis.set_xlabel(f"Time ({display_timezone})")
    cu_axis.set_ylabel("Channel utilization (%)", color="#1f77b4")
    cu_axis.set_ylim(0, 100)
    station_axis.set_ylabel("Station count", color="#f58518")
    station_axis.set_ylim(0, _station_axis_limit(station))
    cu_axis.margins(x=0.025)
    cu_axis.grid(alpha=0.25)
    handles_a, labels_a = cu_axis.get_legend_handles_labels()
    handles_b, labels_b = station_axis.get_legend_handles_labels()
    unique: dict[str, Any] = {}
    for handle, label in zip(handles_a + handles_b, labels_a + labels_b):
        if label:
            unique.setdefault(label, handle)
    cu_axis.legend(unique.values(), unique.keys(), loc="upper right")
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=140)


def _methodology(
    *,
    plots_enabled: bool,
    primary_ssid: str | None,
    display_timezone: str,
) -> str:
    return "\n".join(
        [
            "Corrected bounded radio/bin detail methodology",
            "",
            "Grain and authority",
            "- One row represents one physical radio in one class-session bin.",
            "- selected_observations/ remains the authoritative beacon evidence.",
            "- This manifest-registered Parquet file is a bounded derived view.",
            "",
            "CU selection",
            f"- Configured primary SSID: {primary_ssid or 'none'}.",
            "- When available, valid CU from the primary SSID supplies the bin.",
            "- Otherwise, the earliest valid same-radio member BSSID supplies",
            "  one fallback value. Multiple SSIDs never create duplicate rows.",
            "- missing_cu means member beacons exist but none has valid CU.",
            "- no_beacon means no member BSSID has a selected beacon in the bin.",
            "- CU describes channel conditions, sampled separately for each radio.",
            "",
            "Admission capacity (ADR-0013)",
            "- Admission capacity describes the radio's advertised QoS medium allowance,",
            "  not general channel utilization or a sum across radios/classrooms.",
            "- A valid capacity from the unchanged CU source is preferred; otherwise",
            "  use the stable primary-SSID/first-valid capacity source, then fallback.",
            "- Capacity has separate source lineage and independent availability.",
            "- Valid raw range is 0..31250; percent is raw / 31250 * 100.",
            "  Zero remains zero; missing, invalid, and no-beacon remain distinct.",
            "- No stale extension, clamping, averaging of disagreements, or 100-CU inference.",
            "- Disagreement and invalid-candidate diagnostics accompany selected values.",
            "- Tables, distributions and relationships: ../admission_capacity/.",
            "",
            "Station count",
            "- station_count is the accepted complete-only ADR-0010 radio/bin",
            "  total, including permitted 30-second stale assistance.",
            "- Unavailable radio totals remain null; measured zero remains zero.",
            "",
            "CU and station-count relationship",
            "- Correlations pair CU and station count only within the same",
            "  physical radio/bin. Unknown CU and unavailable station totals",
            "  are excluded; no value is imputed.",
            "- Stale-assisted station values participate and their pair count is",
            "  reported separately.",
            "- Pearson and tie-aware Spearman coefficients are descriptive.",
            "  No p-values or independence claims are made because adjacent",
            "  bins from one radio/session are repeated observations.",
            "- Overall and per-band hexbin plots show valid-pair density.",
            "- Hexbin density uses a 40 by 30 display grid to reduce striping "
            "from discrete station counts; source pairs and correlations are unchanged.",
            "",
            "Plots",
            f"- Radio/class plots enabled for this run: {str(plots_enabled).lower()}.",
            f"- Plot display timezone: {display_timezone}.",
            "- Solid blue lines and light-blue fill are measured CU. Unknown CU",
            "  remains blank. A dashed blue line between surrounding measurements",
            "  is visual interpolation and is not data or an imputed metric value.",
            "- Orange station count uses the secondary axis; hollow markers denote",
            "  stale-assisted station totals.",
            "- A dashed orange bridge spans unavailable station bins bounded by",
            "  available measured or stale-assisted counts. It is visual only and",
            "  does not fill null values or participate in calculations.",
            "- The upper-left box shows schedule subject, seats, combined enrollment,",
            "  configured room AP count, AP model, building-room, and radio band.",
            "  Missing metadata is labeled Unknown.",
            "- The station axis is 0-100 unless an observed count exceeds 100;",
            "  larger maxima expand the axis upward to the next multiple of 10.",
            "",
            "Files",
            f"- {RADIO_BIN_FILENAME}: combined CU, admission capacity and radio station-count view.",
            f"- {CORRELATION_FILENAME}: overall and per-band pair coverage and",
            "  descriptive correlations.",
            f"- {RELATIONSHIP_PLOT_FILENAME}: overall valid-pair density.",
            f"- {RELATIONSHIP_BAND_PLOT_FILENAME}: per-band valid-pair density.",
            f"- {PLOT_INDEX_FILENAME}: present only when plots are enabled.",
            f"- {METHODOLOGY_FILENAME}: this file.",
            "",
        ]
    )


def write_radio_bin_report_package(
    run_directory: str | Path,
    *,
    detail_parts: Iterable[pd.DataFrame],
    create_plots: bool = False,
    primary_ssid: str | None = None,
    display_timezone: str | None = None,
    class_sessions: pd.DataFrame | None = None,
    configured_aps: pd.DataFrame | None = None,
) -> WrittenRadioBinReportPackage:
    """Stream bounded session parts and optionally render radio/class plots."""

    root = Path(run_directory)
    timezone_name = (
        display_timezone.strip()
        if isinstance(display_timezone, str) and display_timezone.strip()
        else "UTC"
    )
    ZoneInfo(timezone_name)
    session_context: dict[str, dict[str, object]] = {}
    ap_models: dict[str, object] = {}
    room_ap_counts: dict[str, int] = {}
    if create_plots:
        if class_sessions is not None:
            for record in class_sessions.to_dict(orient="records"):
                subject = record.get("course_labels")
                if _plot_value(subject) == "Unknown":
                    subject = record.get("course")
                session_context[str(record["class_session_id"])] = {
                    "subject": subject,
                    "seats": None if _plot_value(record.get("capacity_status")) == "conflict"
                    else record.get("capacity"),
                    "enrollment": record.get("enrolled"),
                }
        if configured_aps is not None:
            aps = configured_aps.copy()
            aps["Hostname"] = aps["Hostname"].astype(str).str.strip()
            aps["Building-Room"] = aps["Building-Room"].astype(str).str.strip()
            room_ap_counts = aps.groupby("Building-Room")["Hostname"].nunique().to_dict()
            if "Model" in aps.columns:
                ap_models = aps.set_index("Hostname")["Model"].to_dict()
    destination = root / REPORT_DIRECTORY
    admission_destination = root / ADMISSION_REPORT_DIRECTORY
    if admission_destination.exists():
        raise FileExistsError(f"report directory already exists: {admission_destination}")
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
    destination.mkdir(parents=True)
    parquet_path = destination / RADIO_BIN_FILENAME
    writer: pq.ParquetWriter | None = None
    rows = 0
    plot_rows: list[dict[str, object]] = []
    relationship = _RelationshipAccumulator()
    admission = AdmissionAccumulator()
    try:
        writer = pq.ParquetWriter(
            parquet_path,
            RADIO_BIN_DETAIL_ARROW_SCHEMA,
            compression="zstd",
        )
        plot_number = 0
        for part in detail_parts:
            _require_columns(part, set(RADIO_BIN_DETAIL_COLUMNS), "radio/bin detail")
            relationship.add(part)
            admission.add(part)
            table = pa.Table.from_pandas(
                part.loc[:, RADIO_BIN_DETAIL_COLUMNS],
                schema=RADIO_BIN_DETAIL_ARROW_SCHEMA,
                preserve_index=False,
                safe=True,
            )
            writer.write_table(table)
            rows += table.num_rows
            if create_plots:
                for keys, group in part.groupby(
                    ["class_session_id", "classroom", "radio_group_id"],
                    sort=True,
                ):
                    session_id, classroom, radio_id = map(str, keys)
                    filename = (
                        f"{plot_number:05d}-"
                        f"{_safe_segment(classroom)}-"
                        f"{_safe_segment(radio_id)}.png"
                    )
                    relative = Path("plots") / filename
                    _write_radio_plot(
                        group,
                        destination / relative,
                        display_timezone=timezone_name,
                        plot_context={
                            **session_context.get(session_id, {}),
                            "room_ap_count": room_ap_counts.get(classroom),
                            "ap_model": ap_models.get(_radio_ap_id(group.iloc[0])),
                        },
                    )
                    plot_rows.append(
                        {
                            "class_session_id": session_id,
                            "classroom": classroom,
                            "radio_group_id": radio_id,
                            "path": relative.as_posix(),
                        }
                    )
                    plot_number += 1
        if rows == 0:
            writer.write_table(
                pa.Table.from_pylist([], schema=RADIO_BIN_DETAIL_ARROW_SCHEMA)
            )
        writer.close()
        writer = None
        correlation = _relationship_report(relationship)
        correlation.to_csv(
            destination / CORRELATION_FILENAME,
            index=False,
            encoding="utf-8",
            lineterminator="\n",
            na_rep="",
        )
        relationship_paths = _write_relationship_plots(
            relationship,
            destination,
        )
        admission_files = write_admission_outputs(admission, parquet_path, root)
        if create_plots:
            pd.DataFrame(
                plot_rows,
                columns=[
                    "class_session_id",
                    "classroom",
                    "radio_group_id",
                    "path",
                ],
            ).to_csv(
                destination / PLOT_INDEX_FILENAME,
                index=False,
                encoding="utf-8",
                lineterminator="\n",
            )
        (destination / METHODOLOGY_FILENAME).write_text(
            _methodology(
                plots_enabled=create_plots,
                primary_ssid=primary_ssid,
                display_timezone=timezone_name,
            ),
            encoding="utf-8",
            newline="\n",
        )
    except BaseException:
        if writer is not None:
            writer.close()
        shutil.rmtree(destination, ignore_errors=True)
        shutil.rmtree(admission_destination, ignore_errors=True)
        parent = destination.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        raise

    files = [
        parquet_path,
        destination / CORRELATION_FILENAME,
        *relationship_paths,
        destination / METHODOLOGY_FILENAME,
        *admission_files,
    ]
    if create_plots:
        files.append(destination / PLOT_INDEX_FILENAME)
        files.extend(sorted(destination.glob("plots/*.png")))
    return WrittenRadioBinReportPackage(
        directory=destination,
        rows=rows,
        plot_count=len(plot_rows),
        correlation_rows=len(correlation),
        files=tuple(files),
        admission_files=admission_files,
        admission_summary_rows=len(admission.diagnostics),
        admission_correlation_rows=2 * len(admission.diagnostics),
    )
