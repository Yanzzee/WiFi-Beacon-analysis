"""Shared display primitives for conservative CU ranges, not confidence bands."""
from __future__ import annotations

import numpy as np

from corrected_cu_bounds import cu_cdf_bounds, interval_histogram_bounds


def draw_cdf_bounds(axis, counts, total_weight, *, color, legend=True):
    lower, upper = cu_cdf_bounds(counts, total_weight)
    if lower is None:
        return False
    x = np.arange(256) * (100 / 255)
    axis.fill_between(x, lower, upper, step="post", color=color, alpha=.12,
                      label="Missing-bin range" if legend else "_nolegend_")
    axis.step(x, lower, where="post", color=color, linestyle="--", linewidth=1,
              label="Minimum" if legend else "_nolegend_")
    axis.step(x, upper, where="post", color=color, linestyle=":", linewidth=1.2,
              label="Maximum" if legend else "_nolegend_")
    return True


def draw_histogram_bounds(axis, lower, upper, edges, *, color):
    lower, upper = np.asarray(lower, dtype=float), np.asarray(upper, dtype=float)
    if not (np.isfinite(lower) & np.isfinite(upper)).any():
        return False
    minimum, maximum = interval_histogram_bounds(lower, upper, edges)
    if not np.array_equal(minimum, maximum):
        lo, hi = np.r_[minimum, minimum[-1]], np.r_[maximum, maximum[-1]]
        axis.fill_between(edges, lo, hi, step="post", color=color, alpha=.12)
        axis.step(edges, lo, where="post", color=color, linestyle="--", linewidth=.8)
        axis.step(edges, hi, where="post", color=color, linestyle=":", linewidth=1)
    return True


def draw_vertical_mean_bounds(axis, x, lower, upper, *, color, cap_width=0):
    x, lower, upper = (np.asarray(values, dtype=float) for values in (x, lower, upper))
    valid = np.isfinite(x) & np.isfinite(lower) & np.isfinite(upper)
    if valid.any():
        axis.vlines(x[valid], lower[valid], upper[valid], color=color, linewidth=1, alpha=.35, zorder=1)
        if cap_width:
            axis.hlines(lower[valid], x[valid] - cap_width, x[valid] + cap_width, color=color, linewidth=1, alpha=.5)
            axis.hlines(upper[valid], x[valid] - cap_width, x[valid] + cap_width, color=color, linewidth=1, alpha=.5)
    return bool(valid.any())
