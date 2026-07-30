"""Shared numerical helpers for analysis stages."""

import numpy as np


def compute_pdf_cdf(data, bins):
    """Compute histogram bin centers, probability density, and cumulative density."""
    counts, bin_edges = np.histogram(data, bins=bins, density=True)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    cdf = np.cumsum(counts) * np.diff(bin_edges)
    return bin_centers, counts, cdf
