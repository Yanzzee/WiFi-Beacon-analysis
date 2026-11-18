"""Conservative bounds for missing CU bins; never fills an observed value."""
from __future__ import annotations

import math
from numbers import Integral, Real

import pandas as pd
import numpy as np


CU_BOUNDS_METRIC_ID = "beacon.metric/cu-missing-bin-bounds/v1"
CU_BOUNDS_METRIC_COLUMN = "cu_bounds_metric_id"


def bounded_mean_bounds(observed_sum, valid_weight, total_weight, *, maximum=100):
    """Return mean bounds in the supplied units, assigning missing weight 0/max.

    Weights are integer counts or durations. Integer sums stay integers until
    division, avoiding NumPy int64 overflow. Full-precision persisted means
    may also supply a floating-point weighted sum; displayed means must not.
    An empty population has no bounds; an all-missing population has 0/max.
    """
    for name, value in (("valid_weight", valid_weight), ("total_weight", total_weight)):
        if isinstance(value, bool) or not isinstance(value, Integral) or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
    valid_weight, total_weight = int(valid_weight), int(total_weight)
    if valid_weight > total_weight:
        raise ValueError("valid_weight exceeds total_weight")
    if isinstance(maximum, bool) or not isinstance(maximum, Real) or not math.isfinite(maximum) or maximum <= 0:
        raise ValueError("maximum must be positive and finite")
    if isinstance(maximum, Integral):
        maximum = int(maximum)
    if isinstance(observed_sum, bool) or not isinstance(observed_sum, Real) or not math.isfinite(observed_sum):
        raise ValueError("observed_sum must be finite")
    # Converting numpy integers before arithmetic keeps large duration products
    # exact. Allow only floating-point roundoff at the support boundary.
    if isinstance(observed_sum, Integral):
        observed_sum = int(observed_sum)
    limit = maximum * valid_weight
    if observed_sum < 0 or observed_sum > limit:
        if (isinstance(observed_sum, Integral) or observed_sum < 0
                or not math.isclose(observed_sum, limit, rel_tol=4 * math.ulp(1.0), abs_tol=0)):
            raise ValueError("observed_sum is outside the valid-weight support")
        observed_sum = limit
    if not total_weight:
        return None, None
    lower = observed_sum / total_weight
    upper = (observed_sum + maximum * (total_weight - valid_weight)) / total_weight
    return min(float(lower), float(maximum)), min(float(upper), float(maximum))


def cu_mean_bounds_fields(raw_cu_sum, valid_weight, total_weight, *, prefix="mean_cu_percent"):
    """Return additive percent-bound fields from raw CU times exact weights."""
    lower, upper = bounded_mean_bounds(raw_cu_sum, valid_weight, total_weight, maximum=255)
    return {
        f"{prefix}_lower_bound": None if lower is None else lower * (100 / 255),
        f"{prefix}_upper_bound": None if upper is None else upper * (100 / 255),
        CU_BOUNDS_METRIC_COLUMN: CU_BOUNDS_METRIC_ID,
    }


def station_time_cu_bounds_fields(available, paired, high, very_high):
    """Bound CU exposure only over association-time with known station counts.

    Existing observed shares use paired CU/station time. These companions use
    all available station-time; they cannot bound periods with unknown counts.
    Integer association-microseconds stay exact, including above int64.
    """
    values = (available, paired, high, very_high)
    if any(isinstance(value, bool) or not isinstance(value, Integral) or value < 0 for value in values):
        raise ValueError("station-time components must be nonnegative integers")
    available, paired, high, very_high = map(int, values)
    if not very_high <= high <= paired <= available:
        raise ValueError("station-time components must satisfy very_high <= high <= paired <= available")
    missing = available - paired
    result = {"missing_cu_station_time_us": missing, CU_BOUNDS_METRIC_COLUMN: CU_BOUNDS_METRIC_ID}
    for name, observed in (("high", high), ("very_high", very_high)):
        lower, upper = bounded_mean_bounds(observed, paired, available, maximum=1)
        result.update({f"{name}_station_time_us_lower_bound": observed,
                       f"{name}_station_time_us_upper_bound": observed + missing,
                       f"{name}_exposure_percent_lower_bound": lower * 100 if lower is not None else None,
                       f"{name}_exposure_percent_upper_bound": upper * 100 if upper is not None else None})
    return result


def add_radio_summary_cu_bounds(frame: pd.DataFrame) -> pd.DataFrame:
    """Add raw/ratio bounds to a review copy; leave canonical means untouched.

    Historical canonical summaries retain a full-precision raw mean rather
    than an integer weighted sum. Use that persisted mean and exact durations
    directly, without rounding or inferring a new exact integer numerator.
    """
    required = {"average_cu_raw", "class_duration_us", "valid_cu_duration_us"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError("radio summary is missing bounds components: " + ", ".join(missing))
    bounds = []
    for mean, valid, total in frame.loc[:, ["average_cu_raw", "valid_cu_duration_us", "class_duration_us"]].itertuples(index=False, name=None):
        weighted = mean * int(valid) if valid else 0
        lower, upper = bounded_mean_bounds(weighted, valid, total, maximum=255)
        if total and valid == total:
            # Undoing mean*duration by division can introduce a rounding bit.
            # A fully observed population must retain its stored mean exactly.
            lower = upper = min(float(mean), 255.)
        bounds.append((lower, upper))
    result = frame.copy()
    for index, name in enumerate(("lower", "upper")):
        values = [pair[index] for pair in bounds]
        result[f"average_cu_raw_{name}_bound"] = pd.Series(values, index=frame.index, dtype="float64")
        result[f"average_cu_ratio_{name}_bound"] = result[f"average_cu_raw_{name}_bound"] / 255
    result[CU_BOUNDS_METRIC_COLUMN] = CU_BOUNDS_METRIC_ID
    return result


def cu_cdf_bounds(counts, total_weight):
    """Return lower/upper CDFs at raw CU 0..255; None means unknown population.

    Counts may represent bins or integer microseconds. Python integer sums
    retain exact weights; no reading is added to the observed distribution.
    At 100% CU even missing mass is at/below the threshold, so both bounds end
    at one. At 0% CU the upper bound includes missing values assigned zero.
    """
    values = list(counts)
    if len(values) != 256 or any(isinstance(value, bool) or not isinstance(value, Integral) or value < 0 for value in values):
        raise ValueError("CU CDF requires 256 nonnegative integer weights")
    valid = sum(int(value) for value in values)
    if total_weight is None:
        return None, None
    bounded_mean_bounds(valid, valid, total_weight, maximum=1)
    total_weight = int(total_weight)
    if not total_weight:
        return None, None
    cumulative, running = [], 0
    for value in values:
        running += int(value)
        cumulative.append(running)
    missing = total_weight - valid
    lower = np.array([value / total_weight for value in cumulative])
    upper = np.array([(value + missing) / total_weight for value in cumulative])
    lower[-1] = upper[-1] = 1.
    return lower, upper


def cu_cdf_summary_rows(counts, total_weight, *, scope, population, weighting):
    """Numeric observed/min/max shares at the existing raw 128/192 cutoffs."""
    lower, upper = cu_cdf_bounds(counts, total_weight)
    valid = sum(int(value) for value in counts)
    return [{"population_scope": scope, "population_id": population, "weighting": weighting,
             "cu_threshold_percent": percent, "cu_raw_exclusive_threshold": raw,
             "valid_weight": valid, "expected_weight": total_weight,
             "cdf_observed_percent": sum(int(value) for value in counts[:raw]) / valid * 100 if valid else None,
             "cdf_lower_bound_percent": float(lower[raw - 1] * 100) if lower is not None else None,
             "cdf_upper_bound_percent": float(upper[raw - 1] * 100) if upper is not None else None,
             CU_BOUNDS_METRIC_COLUMN: CU_BOUNDS_METRIC_ID}
            for percent, raw in ((50, 128), (75, 192))]


def interval_histogram_bounds(lower, upper, edges):
    """Bound each bucket's count from closed mean intervals, not endpoints.

    A bucket's minimum counts intervals wholly inside it; its maximum counts
    intervals that intersect it. Follow NumPy's half-open/last-closed buckets.
    Individual bucket maxima need not be jointly attainable.
    """
    lower, upper, edges = np.asarray(lower, dtype=float), np.asarray(upper, dtype=float), np.asarray(edges, dtype=float)
    if lower.shape != upper.shape or lower.ndim != 1 or edges.ndim != 1 or len(edges) < 2 or not np.isfinite(edges).all() or not np.all(np.diff(edges) > 0):
        raise ValueError("invalid histogram intervals or edges")
    known = np.isfinite(lower) & np.isfinite(upper)
    lower, upper = lower[known], upper[known]
    if np.any(lower > upper):
        raise ValueError("histogram lower bound exceeds upper bound")
    minimum, maximum = [], []
    for index, (left, right) in enumerate(zip(edges, edges[1:])):
        final = index == len(edges) - 2
        minimum.append(int(np.sum((lower >= left) & ((upper <= right) if final else (upper < right)))))
        maximum.append(int(np.sum((upper >= left) & ((lower <= right) if final else (lower < right)))))
    return np.asarray(minimum), np.asarray(maximum)
