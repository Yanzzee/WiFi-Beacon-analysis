"""Bounded ADR-0013 distributions and same-radio/bin descriptive relationships.

Only marginal frequencies and fixed display grids survive between session
parts. A second narrow read of the derived Parquet computes exact correlations
and hexbin counts without retaining project-wide observations or joint pairs.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from corrected_admission_selection import ADMISSION_MAXIMUM_RAW, ADMISSION_METRIC_ID


REPORT_DIRECTORY = Path("reports") / "admission_capacity"
SUMMARY_FILENAME = "admission_capacity_summary.csv"
CORRELATION_FILENAME = "admission_capacity_correlation.csv"
METHODOLOGY_FILENAME = "methodology.txt"
PLOT_FILENAMES = tuple(
    f"admission_capacity_{kind}{suffix}.png"
    for kind in ("histogram", "cdf", "cu_hexbin", "station_hexbin")
    for suffix in ("", "_by_band")
)
REPORT_SCHEMA_ID = "beacon.schema/admission-reports/v1"
READ_COLUMNS = [
    "band", "cu_state", "qbss_cu_raw", "admission_state",
    "qbss_admission_capacity", "station_available", "station_count",
    "station_stale_assisted", "admission_disagreement",
    "admission_source_matches_cu",
]


class _HexDensity:
    """Matplotlib's two-lattice 40x30 hex geometry, with fixed-size counts."""

    def __init__(self, y_max: float):
        self.nx, self.ny = 40, 30
        self.y_max = y_max
        padding = 1e-9 * ADMISSION_MAXIMUM_RAW
        self.x_min = -padding
        self.sx = (ADMISSION_MAXIMUM_RAW + 2 * padding) / self.nx
        self.sy = y_max / self.ny
        self.first_size = (self.nx + 1) * (self.ny + 1)
        self.counts = np.zeros(self.first_size + self.nx * self.ny, dtype=np.int64)

    def add(self, x: np.ndarray, y: np.ndarray) -> None:
        ix, iy = (x - self.x_min) / self.sx, y / self.sy
        ix1, iy1 = np.rint(ix).astype(int), np.rint(iy).astype(int)
        ix2, iy2 = np.floor(ix).astype(int), np.floor(iy).astype(int)
        distance1 = (ix - ix1) ** 2 + 3 * (iy - iy1) ** 2
        distance2 = (ix - ix2 - 0.5) ** 2 + 3 * (iy - iy2 - 0.5) ** 2
        indices = np.where(
            distance1 < distance2,
            ix1 * (self.ny + 1) + iy1,
            self.first_size + ix2 * self.ny + iy2,
        )
        self.counts += np.bincount(indices, minlength=len(self.counts))

    def points(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        x1, y1 = np.meshgrid(np.arange(self.nx + 1), np.arange(self.ny + 1), indexing="ij")
        x2, y2 = np.meshgrid(np.arange(self.nx) + 0.5, np.arange(self.ny) + 0.5, indexing="ij")
        x = np.concatenate([x1.ravel(), x2.ravel()]) * self.sx + self.x_min
        y = np.concatenate([y1.ravel(), y2.ravel()]) * self.sy
        occupied = self.counts > 0
        return x[occupied], y[occupied], self.counts[occupied]


def _frequency(values: np.ndarray) -> Counter[int]:
    unique, counts = np.unique(values, return_counts=True)
    return Counter(dict(zip(map(int, unique), map(int, counts))))


def _mean(frequency: Counter[int]) -> float:
    return sum(value * count for value, count in frequency.items()) / sum(frequency.values())


def _ranks(values: np.ndarray, frequency: Counter[int]) -> np.ndarray:
    keys = np.asarray(sorted(frequency))
    counts = np.asarray([frequency[int(key)] for key in keys], dtype=np.int64)
    ranks = np.cumsum(counts) - (counts - 1) / 2
    return ranks[np.searchsorted(keys, values)]


class _PairStats:
    def __init__(self) -> None:
        self.x: Counter[int] = Counter()
        self.y: Counter[int] = Counter()
        self.stale = self.disagreement = self.different_source = 0
        self.radios: set[str] = set()
        self.sessions: set[str] = set()
        self.moments = np.zeros((2, 3), dtype=np.float64)
        self.density: _HexDensity | None = None
        self.means = (0.0, 0.0)
        self.rank_tables: list[tuple[np.ndarray, np.ndarray]] = []
        self.rank_mean = 0.0

    @property
    def count(self) -> int:
        return sum(self.x.values())

    def observe(self, paired: pd.DataFrame, kind: str) -> None:
        x = paired["qbss_admission_capacity"].to_numpy(dtype=float)
        y = paired["qbss_cu_raw" if kind == "cu" else "station_count"].to_numpy(dtype=float)
        self.x.update(_frequency(x))
        self.y.update(_frequency(y))
        self.stale += int(paired["station_stale_assisted"].sum()) if kind == "station" else 0
        self.disagreement += int(paired["admission_disagreement"].sum())
        self.different_source += int((~paired["admission_source_matches_cu"]).sum()) if kind == "cu" else 0
        self.radios.update(paired["radio_group_id"].astype(str))
        self.sessions.update(paired["class_session_id"].astype(str))

    def finish_part(self, x: np.ndarray, y: np.ndarray) -> None:
        if not len(x):
            return
        x_keys, x_ranks = self.rank_tables[0]
        y_keys, y_ranks = self.rank_tables[1]
        for index, (a, b) in enumerate((
            (x - self.means[0], y - self.means[1]),
            (x_ranks[np.searchsorted(x_keys, x)] - self.rank_mean,
             y_ranks[np.searchsorted(y_keys, y)] - self.rank_mean),
        )):
            self.moments[index] += [np.dot(a, b), np.dot(a, a), np.dot(b, b)]
        assert self.density is not None
        self.density.add(x, y)

    def correlation(self, index: int) -> float | None:
        if self.count < 2 or len(self.x) < 2 or len(self.y) < 2:
            return None
        cross, xx, yy = self.moments[index]
        return float(np.clip(cross / np.sqrt(xx * yy), -1, 1))


def _populations(part: pd.DataFrame):
    yield "all", part
    for band, group in part.groupby(part["band"].astype("string").fillna("unknown"), sort=True):
        yield f"band:{band}", group


def _valid(part: pd.DataFrame) -> pd.Series:
    return part["admission_state"].eq("valid") & part["qbss_admission_capacity"].between(0, ADMISSION_MAXIMUM_RAW)


def _paired(part: pd.DataFrame, kind: str) -> pd.DataFrame:
    other = (
        part["cu_state"].eq("valid") & part["qbss_cu_raw"].notna()
        if kind == "cu" else part["station_available"] & part["station_count"].notna()
    )
    return part.loc[_valid(part) & other]


class AdmissionAccumulator:
    def __init__(self) -> None:
        self.counts: dict[str, Counter[int]] = {}
        self.diagnostics: dict[str, Counter[str]] = {}
        self.pairs: dict[tuple[str, str], _PairStats] = {}
        self.run_id: str | None = None

    def add(self, part: pd.DataFrame) -> None:
        if part.empty:
            return
        self.run_id = str(part.iloc[0]["run_id"])
        for population, group in _populations(part):
            counts = self.counts.setdefault(population, Counter())
            diagnostics = self.diagnostics.setdefault(population, Counter())
            valid = _valid(group)
            counts.update(_frequency(group.loc[valid, "qbss_admission_capacity"].to_numpy(dtype=float)))
            diagnostics["total_radio_bin_rows"] += len(group)
            for state in ("valid", "missing_capacity", "invalid_capacity", "no_beacon"):
                diagnostics[f"{state}_rows"] += int(group["admission_state"].eq(state).sum())
            diagnostics["invalid_candidate_rows"] += int(group["invalid_admission_candidate_count"].gt(0).sum())
            diagnostics["invalid_candidate_count"] += int(group["invalid_admission_candidate_count"].sum())
            diagnostics["disagreement_rows"] += int(group["admission_disagreement"].sum())
            diagnostics["different_cu_source_rows"] += int((valid & group["cu_state"].eq("valid") & ~group["admission_source_matches_cu"]).sum())
            for kind in ("cu", "station"):
                self.pairs.setdefault((population, kind), _PairStats()).observe(_paired(group, kind), kind)

    def finish(self, parquet_path: Path) -> None:
        if not self.diagnostics:
            self.counts["all"] = Counter()
            self.diagnostics["all"] = Counter()
            for kind in ("cu", "station"):
                self.pairs[("all", kind)] = _PairStats()
        station_max = max((max(pair.y, default=0) for (pop, kind), pair in self.pairs.items() if kind == "station"), default=0)
        station_limit = max(100, int(np.ceil(station_max / 10) * 10))
        for (_population, kind), pair in self.pairs.items():
            pair.density = _HexDensity(255 if kind == "cu" else station_limit)
            if pair.count:
                pair.means = (_mean(pair.x), _mean(pair.y))
                pair.rank_mean = (pair.count + 1) / 2
                for frequency in (pair.x, pair.y):
                    keys = np.asarray(sorted(frequency))
                    pair.rank_tables.append((keys, _ranks(keys, frequency)))
        parquet = pq.ParquetFile(parquet_path)
        for index in range(parquet.num_row_groups):
            part = parquet.read_row_group(index, columns=READ_COLUMNS).to_pandas()
            for population, group in _populations(part):
                for kind in ("cu", "station"):
                    paired = _paired(group, kind)
                    self.pairs[(population, kind)].finish_part(
                        paired["qbss_admission_capacity"].to_numpy(dtype=float),
                        paired["qbss_cu_raw" if kind == "cu" else "station_count"].to_numpy(dtype=float),
                    )


def _identity(accumulator: AdmissionAccumulator, population: str) -> dict[str, Any]:
    return {
        "run_id": accumulator.run_id, "schema_id": REPORT_SCHEMA_ID,
        "metric_id": ADMISSION_METRIC_ID,
        "population_type": "overall" if population == "all" else "band",
        "population_id": "all" if population == "all" else population.split(":", 1)[1],
    }


def admission_tables(accumulator: AdmissionAccumulator) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary, correlations = [], []
    for population in sorted(accumulator.diagnostics, key=lambda p: (p != "all", p)):
        counts, diagnostics = accumulator.counts[population], accumulator.diagnostics[population]
        total, valid = diagnostics["total_radio_bin_rows"], sum(counts.values())
        summary.append({
            **_identity(accumulator, population),
            **{name: diagnostics[name] for name in (
                "total_radio_bin_rows", "valid_rows", "missing_capacity_rows",
                "invalid_capacity_rows", "no_beacon_rows", "invalid_candidate_rows",
                "invalid_candidate_count", "disagreement_rows", "different_cu_source_rows",
            )},
            "valid_coverage": valid / total if total else None,
            "zero_rows": counts[0], "constant_value_raw": next(iter(counts)) if len(counts) == 1 else None,
            "minimum_percent": min(counts) / 31250 * 100 if counts else None,
            "maximum_percent": max(counts) / 31250 * 100 if counts else None,
            "mean_percent": _mean(counts) / 31250 * 100 if counts else None,
            "warning": "constant_zero_advertisement" if valid and len(counts) == 1 and 0 in counts else "constant_advertisement" if valid and len(counts) == 1 else "no_valid_capacity" if not valid else "",
        })
        for kind in ("cu", "station"):
            pair = accumulator.pairs[(population, kind)]
            correlations.append({
                **_identity(accumulator, population), "relationship": f"capacity_vs_{kind}",
                "total_radio_bin_rows": total, "valid_capacity_rows": valid,
                "valid_pair_rows": pair.count,
                "valid_pair_coverage": pair.count / total if total else None,
                "stale_assisted_pair_rows": pair.stale,
                "disagreement_pair_rows": pair.disagreement,
                "different_beacon_pair_rows": pair.different_source if kind == "cu" else None,
                "radio_count": len(pair.radios), "class_session_count": len(pair.sessions),
                "pearson_correlation": pair.correlation(0),
                "spearman_correlation": pair.correlation(1),
            })
    return pd.DataFrame(summary), pd.DataFrame(correlations)


def _draw(axis: Any, accumulator: AdmissionAccumulator, population: str, kind: str) -> Any:
    label = "Advertised available admission capacity (%)"
    axis.set_xlabel(label)
    axis.set_xlim(0, 100)
    axis.grid(alpha=0.2)
    counts = accumulator.counts[population]
    total = sum(counts.values())
    if kind in {"histogram", "cdf"}:
        axis.set_ylabel("Valid radio/bin readings (%)" if kind == "histogram" else "Cumulative proportion")
        if kind == "cdf":
            axis.set_ylim(0, 1)
        if total:
            values = np.asarray(sorted(counts))
            weights = np.asarray([counts[int(value)] for value in values])
            if kind == "histogram":
                # Advertisements commonly lie just below integer percentages
                # (e.g. 30937 = 98.9984%). Center display bins on integers so
                # adjacent percentages do not collapse into alternating bars.
                bins = np.arange(-0.5, 101, 1) * 31250 / 100
                heights, _ = np.histogram(values, bins=bins, weights=weights)
                axis.bar(np.arange(101), heights / total * 100, width=1, color="#9467bd")
            else:
                # Include both endpoints: a zero observation jumps at zero,
                # and the final plateau reaches the right edge of the axis.
                x = np.r_[0, values / 31250 * 100, 100]
                y = np.r_[0, np.cumsum(weights) / total, 1]
                axis.step(x, y, where="post", color="#9467bd", linewidth=2)
        else:
            axis.text(0.5, 0.5, "No valid admission-capacity readings", transform=axis.transAxes, ha="center")
        return None
    pair = accumulator.pairs[(population, "cu" if kind == "cu_hexbin" else "station")]
    assert pair.density is not None
    y_scale = 100 / 255 if kind == "cu_hexbin" else 1
    axis.set_ylabel("Channel utilization (%)" if kind == "cu_hexbin" else "Station count")
    axis.set_ylim(0, pair.density.y_max * y_scale)
    x, y, weights = pair.density.points()
    if not len(weights):
        axis.text(0.5, 0.5, "No valid capacity/metric pairs", transform=axis.transAxes, ha="center")
        return None
    return axis.hexbin(
        x / 31250 * 100, y * y_scale, C=weights, reduce_C_function=np.sum,
        gridsize=(40, 30), extent=(0, 100, 0, pair.density.y_max * y_scale),
        mincnt=1, bins="log", cmap="viridis",
    )


def build_admission_figure(accumulator, kind, populations, *, by_band=False):
    figure = Figure(figsize=(12 * len(populations), 10) if by_band else (16, 12), constrained_layout=True)
    FigureCanvasAgg(figure)
    axes = np.atleast_1d(figure.subplots(1, len(populations), sharey=kind in {"histogram", "cdf"}))
    for axis, population in zip(axes, populations):
        title = "All sampled radios" if population == "all" else population.split(":", 1)[1] + " GHz"
        purpose = {"histogram": "histogram", "cdf": "CDF", "cu_hexbin": "versus CU", "station_hexbin": "versus station count"}[kind]
        axis.set_title(f"Admission capacity {purpose} — {title}")
        artist = _draw(axis, accumulator, population, kind)
        if artist is not None:
            figure.colorbar(artist, ax=axis, label="Radio/bin pairs")
    return figure


def build_admission_band_histogram_from_detail(path: str | Path):
    """Rebuild the existing histogram counts with a bounded three-column read."""
    counts = defaultdict(Counter)
    for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536, columns=["band", "admission_state", "qbss_admission_capacity"]):
        frame = batch.to_pandas(types_mapper=pd.ArrowDtype)
        for population, part in _populations(frame):
            if population == "all":
                continue
            counts[population].update({int(value): int(count) for value, count in part.loc[_valid(part), "qbss_admission_capacity"].value_counts().items()})
    populations = sorted(counts) or ["all"]
    return build_admission_figure(SimpleNamespace(counts=counts), "histogram", populations, by_band=True)


def write_admission_outputs(accumulator: AdmissionAccumulator, parquet_path: Path, root: Path) -> tuple[Path, ...]:
    destination = root / REPORT_DIRECTORY
    if destination.exists():
        raise FileExistsError(f"report directory already exists: {destination}")
    # Finish before creating the directory so early failures leave no report.
    accumulator.finish(parquet_path)
    destination.mkdir(parents=True)
    summary, correlations = admission_tables(accumulator)
    for filename, table in ((SUMMARY_FILENAME, summary), (CORRELATION_FILENAME, correlations)):
        table.to_csv(destination / filename, index=False, encoding="utf-8", lineterminator="\n", na_rep="")
    bands = [population for population in accumulator.diagnostics if population != "all"]
    for kind in ("histogram", "cdf", "cu_hexbin", "station_hexbin"):
        for suffix, populations in (("", ["all"]), ("_by_band", sorted(bands) or ["all"])):
            figure = build_admission_figure(accumulator, kind, populations, by_band=bool(suffix))
            figure.savefig(destination / f"admission_capacity_{kind}{suffix}.png", dpi=140)
    (destination / METHODOLOGY_FILENAME).write_text(
        "ADR-0013 radio admission-capacity reporting\n"
        "Source: ../radio_class_detail/radio_bin_detail.parquet; selected_observations/ is authoritative.\n"
        "CU describes channel conditions; admission capacity describes a radio's advertised QoS admission allowance.\n"
        "No admission capacities are summed across BSSIDs, radios, bands or classrooms.\n"
        "One unchanged CU-source beacon is preferred; otherwise a stable primary-SSID/first-valid admission source, then earliest valid member fallback.\n"
        "Availability is independent of CU. Separate source BSSID/SSID/method/time identifies the selected BSSID/session/bin row.\n"
        "Raw units: 32 microseconds per second; percent = raw / 31250 * 100.\n"
        "Valid range: 0..31250 inclusive. Zero remains zero, not missing. Invalid raw uint16 values remain in selected observations.\n"
        "missing_capacity, invalid_capacity and no_beacon remain distinct; no stale extension, clamping or interpolation.\n"
        "Valid member disagreements retain the deterministic selected value and are diagnosed, not averaged.\n"
        "Constant-zero/constant populations are flagged; constant correlations are null.\n"
        "Histograms use 1-percentage-point display bins centered on integer percentages; raw values are not rounded.\n"
        "The exact CDF retains every discrete advertised value.\n"
        "Every eligible radio/bin contributes once, not once per beacon/BSSID or weighted by enrollment.\n"
        "mean_percent is the mean of valid radio/bin readings, not a duration-weighted class average.\n"
        "Same-radio/same-bin valid pairs only; stale-assisted station totals participate and are counted.\n"
        "CU/capacity pairs may use different beacons in the same bin; those pairs are counted explicitly.\n"
        "Pearson and tie-aware Spearman are descriptive only: no p-values, independence claims or regression.\n"
        "Coverage denominators are all expected sampled radio/bins, including unknown bins.\n"
        "Hexbins use a 40x30 display grid. Fixed grids and marginal frequencies are retained between session parts.\n"
        "A second narrow derived-Parquet read computes exact centered/rank correlations and display counts; no capture re-decode.\n"
        "Not a client limit, throughput estimate, 100-CU calculation, or guarantee that a request will be admitted.\n",
        encoding="utf-8", newline="\n",
    )
    return tuple(destination / filename for filename in (SUMMARY_FILENAME, CORRELATION_FILENAME, *PLOT_FILENAMES, METHODOLOGY_FILENAME))
