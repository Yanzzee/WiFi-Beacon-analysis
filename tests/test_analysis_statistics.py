from __future__ import annotations

import numpy as np
import pytest

from analysis_statistics import compute_pdf_cdf


def test_compute_pdf_cdf_returns_expected_arrays():
    bin_centers, pdf, cdf = compute_pdf_cdf(
        np.array([0.0, 1.0, 2.0, 3.0]),
        bins=2,
    )

    np.testing.assert_allclose(bin_centers, np.array([0.75, 2.25]))
    np.testing.assert_allclose(pdf, np.array([1 / 3, 1 / 3]))
    np.testing.assert_allclose(cdf, np.array([0.5, 1.0]))
    assert np.issubdtype(bin_centers.dtype, np.floating)
    assert np.issubdtype(pdf.dtype, np.floating)
    assert np.issubdtype(cdf.dtype, np.floating)


def test_compute_pdf_cdf_preserves_single_value_behavior():
    bin_centers, pdf, cdf = compute_pdf_cdf(np.array([4.0]), bins=2)

    np.testing.assert_allclose(bin_centers, np.array([3.75, 4.25]))
    np.testing.assert_allclose(pdf, np.array([0.0, 2.0]))
    np.testing.assert_allclose(cdf, np.array([0.0, 1.0]))


def test_compute_pdf_cdf_preserves_empty_input_behavior():
    with pytest.warns(RuntimeWarning):
        bin_centers, pdf, cdf = compute_pdf_cdf(np.array([]), bins=2)

    np.testing.assert_allclose(bin_centers, np.array([0.25, 0.75]))
    np.testing.assert_allclose(pdf, np.array([np.nan, np.nan]), equal_nan=True)
    np.testing.assert_allclose(cdf, np.array([np.nan, np.nan]), equal_nan=True)
