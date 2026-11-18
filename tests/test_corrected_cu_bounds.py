import math

import numpy as np
import pandas as pd
import pytest

from corrected_cu_bounds import (
    CU_BOUNDS_METRIC_ID, add_radio_summary_cu_bounds,
    bounded_mean_bounds, cu_mean_bounds_fields,
)


@pytest.mark.parametrize("total,valid,weighted,expected", [
    (100, 80, 3200, (32, 52)),
    (100, 100, 4000, (40, 40)),
    (100, 0, 0, (0, 100)),
    (100, 80, 0, (0, 20)),
    (100, 80, 8000, (80, 100)),
    (0, 0, 0, (None, None)),
])
def test_missing_weight_extremes_preserve_measured_zero_and_empty_population(total, valid, weighted, expected):
    assert bounded_mean_bounds(weighted, valid, total) == expected


@pytest.mark.parametrize("weighted,valid,total,maximum", [
    (0, -1, 1, 100), (0, 1, -1, 100), (0, 2, 1, 100),
    (0, 1.0, 2, 100), (0, 1, 2.0, 100), (0, True, 2, 100),
    (0, 1, True, 100), (0, None, 2, 100), (0, 1, math.nan, 100),
    (math.nan, 1, 2, 100), (math.inf, 1, 2, 100), (-1, 1, 2, 100),
    (101, 1, 2, 100), (1, 0, 1, 100), (True, 1, 2, 100),
    (0, 1, 2, 0), (0, 1, 2, -1), (0, 1, 2, math.nan),
    (0, 1, 2, math.inf), (0, 1, 2, True),
])
def test_invalid_components_do_not_produce_plausible_bounds(weighted, valid, total, maximum):
    with pytest.raises(ValueError):
        bounded_mean_bounds(weighted, valid, total, maximum=maximum)


def test_large_integer_duration_products_do_not_overflow_int64():
    valid = np.int64(2**62)
    # Both the raw weighted sum and expected duration exceed int64 capacity.
    result = bounded_mean_bounds(255 * int(valid), valid, 2 * int(valid), maximum=np.int64(255))
    assert result == (127.5, 255)


def test_float_support_roundoff_is_tolerated_but_real_out_of_range_values_are_not():
    assert bounded_mean_bounds(np.nextafter(100., math.inf), 1, 2) == (50, 100)
    with pytest.raises(ValueError):
        bounded_mean_bounds(100.000001, 1, 2)


def test_unequal_terminal_bin_duration_is_not_count_weighted():
    # Three bins: 1s at 0 CU, 1s missing CU, final 0.5s at raw CU 255.
    fields = cu_mean_bounds_fields(255 * 500000, 1500000, 2500000)
    assert fields == {"mean_cu_percent_lower_bound": 20., "mean_cu_percent_upper_bound": 60.,
                      "cu_bounds_metric_id": CU_BOUNDS_METRIC_ID}


def test_core_review_copy_preserves_observed_columns_and_full_precision():
    raw_mean = 153.123456789012
    frame = pd.DataFrame({"average_cu_raw": [raw_mean, math.nan, 0., 255.],
        "average_cu_ratio": [raw_mean / 255, math.nan, 0., 1.],
        "valid_cu_duration_us": [1500000, 0, 1000000, 1000000],
        "class_duration_us": [2500000, 2500000, 1000000, 1000000]}, index=[7, 2, 4, 9])
    original = frame.copy(deep=True)
    result = add_radio_summary_cu_bounds(frame)
    pd.testing.assert_frame_equal(frame, original)
    pd.testing.assert_frame_equal(result[frame.columns], original)
    assert result.loc[7, "average_cu_raw_lower_bound"] == pytest.approx(raw_mean * .6, abs=1e-12)
    assert result.loc[7, "average_cu_raw_upper_bound"] == pytest.approx(raw_mean * .6 + 255 * .4, abs=1e-12)
    assert result.loc[2, "average_cu_raw_lower_bound"] == 0
    assert result.loc[2, "average_cu_raw_upper_bound"] == 255
    assert result.loc[4, "average_cu_ratio_lower_bound"] == result.loc[4, "average_cu_ratio_upper_bound"] == 0
    assert result.loc[9, "average_cu_ratio_lower_bound"] == result.loc[9, "average_cu_ratio_upper_bound"] == 1
    assert result.cu_bounds_metric_id.eq(CU_BOUNDS_METRIC_ID).all()


def test_core_review_empty_rows_keep_bound_columns_and_missing_components_fail():
    source = pd.DataFrame(columns=["average_cu_raw", "valid_cu_duration_us", "class_duration_us"])
    result = add_radio_summary_cu_bounds(source)
    assert result.empty
    assert "average_cu_ratio_upper_bound" in result
    with pytest.raises(ValueError, match="missing bounds components"):
        add_radio_summary_cu_bounds(source.drop(columns="class_duration_us"))


def test_complete_core_review_bounds_equal_stored_means_exactly():
    means = np.linspace(0, 255, 101)
    source = pd.DataFrame({"average_cu_raw": means, "average_cu_ratio": means / 255,
                          "class_duration_us": 1000001, "valid_cu_duration_us": 1000001})
    review = add_radio_summary_cu_bounds(source)
    for field in ("average_cu_raw", "average_cu_ratio"):
        for bound in ("lower", "upper"):
            np.testing.assert_array_equal(review[f"{field}_{bound}_bound"], source[field])
