from __future__ import annotations

import pytest

from radio_summary_processing import (
    compute_beacon_rate_distributions,
    normalize_beacon_rate_series,
    prepare_radio_summary_frames,
    print_beacon_rate_null_counts,
    report_radio_summary_statistics,
    sort_radio_summary,
)
from radio_summary_plotting import (
    plot_beacon_rate_channel_utilization_scatter,
    plot_radio_regressions,
)
from script_sections import script_cell_source


def _radio_summary_cell_source(cell_number: int) -> str:
    source, _ = script_cell_source("analyze_radio_summary.py", cell_number)
    return source


def test_radio_summary_filters_missing_or_nonpositive_ssid_count(tmp_path):
    pd = pytest.importorskip("pandas")
    matplotlib = pytest.importorskip("matplotlib")

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    output_folder = tmp_path / "Output"
    (output_folder / "radio_summary").mkdir(parents=True)
    logged_messages: list[str] = []

    radio_summary_df = pd.DataFrame(
        {
            "total_beacons": [400, 500, 600],
            "start_time": pd.to_datetime(
                [
                    "2025-01-01 08:00:00",
                    "2025-01-01 09:00:00",
                    "2025-01-01 10:00:00",
                ]
            ),
            "end_time": pd.to_datetime(
                [
                    "2025-01-01 08:01:40",
                    "2025-01-01 09:01:40",
                    "2025-01-01 10:01:40",
                ]
            ),
            "highest_cu": [10.0, 20.0, 30.0],
            "ssid_count": [2, 0, None],
            "radio_count": [1, 2, 2],
        }
    )
    radio_summary_df = sort_radio_summary(radio_summary_df)
    (
        filtered_radio_summary_df,
        _df_radio_1,
        _df_radio_2,
        _combined_df,
    ) = prepare_radio_summary_frames(
        radio_summary_df,
        logged_messages.append,
    )

    namespace = {
        "filtered_radio_summary_df": filtered_radio_summary_df,
        "plot_beacon_rate_channel_utilization_scatter": (
            plot_beacon_rate_channel_utilization_scatter
        ),
        "plt": plt,
        "OUTPUT_FOLDER": str(output_folder),
    }

    exec(compile(_radio_summary_cell_source(2), "analyze_radio_summary.py::cell_2", "exec"), namespace)

    filtered = namespace["filtered_radio_summary_df"]
    assert filtered["ssid_count"].tolist() == [2.0]
    assert filtered["beacons_per_second"].tolist() == [4.0]
    assert any(
        "Excluding 2 rows with missing or non-positive ssid_count." in message
        for message in logged_messages
    )
    assert (output_folder / "radio_summary" / "Beacons_per_second_class_cu.png").is_file()
    plt.close("all")


def test_prepare_radio_summary_frames_preserves_complete_frames():
    pd = pytest.importorskip("pandas")

    start_time = pd.Timestamp("2025-01-01 08:00:00")
    source_df = pd.DataFrame(
        {
            "row_id": [
                "valid-radio-1",
                "below-threshold",
                "zero-duration",
                "zero-ssid",
                "missing-ssid",
                "bad-ssid",
                "valid-radio-2",
                "valid-radio-3",
            ],
            "total_beacons": [500, 299, 400, 300, 600, 700, 800, 900],
            "start_time": [start_time] * 8,
            "end_time": [
                start_time + pd.Timedelta(seconds=100),
                start_time + pd.Timedelta(seconds=100),
                start_time,
                start_time + pd.Timedelta(seconds=60),
                start_time + pd.Timedelta(seconds=120),
                start_time + pd.Timedelta(seconds=100),
                start_time + pd.Timedelta(seconds=200),
                start_time + pd.Timedelta(seconds=300),
            ],
            "ssid_count": ["2", 2, 2, "0", None, "bad", "4", 1],
            "highest_cu": [50, 10, 20, 30, 60, 70, 80, 90],
            "radio_count": [1, 1, 2, 1, 2, 2, 2, 3],
        },
        index=[40, 10, 30, 20, 50, 60, 70, 80],
    )
    original_df = source_df.copy(deep=True)
    logged_messages: list[str] = []

    sorted_df = sort_radio_summary(source_df)
    (
        filtered_df,
        radio_1_df,
        radio_2_df,
        combined_df,
    ) = prepare_radio_summary_frames(
        sorted_df,
        logged_messages.append,
    )

    pd.testing.assert_frame_equal(source_df, original_df)

    expected_sorted_df = original_df.loc[
        [10, 20, 30, 40, 50, 60, 70, 80]
    ].copy()
    expected_sorted_df["class_duration"] = [
        100.0,
        60.0,
        0.0,
        100.0,
        120.0,
        100.0,
        200.0,
        300.0,
    ]
    pd.testing.assert_frame_equal(sorted_df, expected_sorted_df)

    expected_filtered_df = expected_sorted_df.loc[[40, 70, 80]].copy()
    expected_filtered_df["ssid_count"] = pd.Series(
        [2.0, 4.0, 1.0],
        index=[40, 70, 80],
        dtype="float64",
    )
    expected_filtered_df["beacons_per_second"] = [5.0, 4.0, 3.0]
    pd.testing.assert_frame_equal(filtered_df, expected_filtered_df)
    pd.testing.assert_frame_equal(
        radio_1_df,
        expected_filtered_df.loc[[40]],
    )
    pd.testing.assert_frame_equal(
        radio_2_df,
        expected_filtered_df.loc[[70]],
    )
    pd.testing.assert_frame_equal(
        combined_df,
        expected_filtered_df.loc[[40, 70]],
    )
    assert logged_messages == [
        "Excluding 3 rows with missing or non-positive ssid_count."
    ]


def test_report_radio_summary_statistics_preserves_messages_and_correlation():
    pd = pytest.importorskip("pandas")

    radio_summary_df = pd.DataFrame(
        {
            "total_beacons": [100, 300, 500],
            "longest_duration_high_cu": [0.0, 2.0, 8.0],
            "highest_cu": [40.0, 60.0, 80.0],
            "high_scount": [10.0, 20.0, 30.0],
            "median_cu": [20.0, 40.0, 60.0],
        }
    )
    messages: list[str] = []

    correlation = report_radio_summary_statistics(
        radio_summary_df,
        messages.append,
    )

    assert correlation == pytest.approx(1.0)
    assert messages == [
        "rows in the radio_summary_df represent a single 5GHz radio per "
        "class session",
        "total number of rows: 3",
        "median number of beacons: 300.0",
        "median row of high channel utilization duration: 2.0",
        "(channel utilization is in percentage: 80.0)",
        "number of rows with total beacons less than 200: 1",
        "number of rows with some very high channel use (>75%): 1",
        "number of rows with some high channel use (>50%): 2",
        "median number of devices per radio (peak per class session): 20.0",
        "average number of devices per radio (peak per class session): 20.00",
        "The correlation between high_scount and median channel "
        "utilization is: 1.0",
    ]


def test_normalize_and_compute_beacon_rate_distributions(monkeypatch):
    pd = pytest.importorskip("pandas")

    radio_1_df = pd.DataFrame(
        {
            "beacons_per_second": [4.0, None, 12.0],
            "ssid_count": [2.0, 2.0, 3.0],
        },
        index=[10, 11, 12],
    )
    radio_2_df = pd.DataFrame(
        {
            "beacons_per_second": [9.0, 20.0],
            "ssid_count": [3.0, 4.0],
        },
        index=[20, 21],
    )
    combined_df = pd.concat([radio_1_df, radio_2_df])

    radio_1_rate, radio_2_rate, combined_rate = (
        normalize_beacon_rate_series(
            radio_1_df,
            radio_2_df,
            combined_df,
        )
    )

    pd.testing.assert_series_equal(
        radio_1_rate,
        pd.Series([2.0, 4.0], index=[10, 12]),
    )
    pd.testing.assert_series_equal(
        radio_2_rate,
        pd.Series([3.0, 5.0], index=[20, 21]),
    )
    pd.testing.assert_series_equal(
        combined_rate,
        pd.Series([2.0, 4.0, 3.0, 5.0], index=[10, 12, 20, 21]),
    )

    calls = []

    def fake_compute_pdf_cdf(values, *, bins):
        calls.append((values, bins))
        return (values.index, values, values.cumsum())

    monkeypatch.setattr(
        "radio_summary_processing.compute_pdf_cdf",
        fake_compute_pdf_cdf,
    )
    distributions = compute_beacon_rate_distributions(
        radio_1_rate,
        radio_2_rate,
        combined_rate,
    )

    assert len(distributions) == 3
    assert [bins for _values, bins in calls] == [50, 50, 50]
    for (actual, _bins), expected in zip(
        calls,
        [radio_1_rate, radio_2_rate, combined_rate],
        strict=True,
    ):
        pd.testing.assert_series_equal(actual, expected)


def test_print_beacon_rate_null_counts_preserves_output(capsys):
    pd = pytest.importorskip("pandas")

    print_beacon_rate_null_counts(
        pd.Series([1.0, 2.0]),
        pd.Series([3.0]),
    )

    assert capsys.readouterr().out == (
        "Radio 1 - Count: 2 NaNs: 0\n"
        "Radio 2 - Count: 1 NaNs: 0\n"
    )


def test_radio_summary_regression_skips_missing_radio_count(tmp_path):
    pd = pytest.importorskip("pandas")
    sm = pytest.importorskip("statsmodels.api")
    matplotlib = pytest.importorskip("matplotlib")

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    radio_summary_folder = tmp_path / "radio_summary"
    radio_summary_folder.mkdir()
    logged_messages: list[str] = []

    df_radio_1 = pd.DataFrame(
        {
            "highest_cu": [10.0, 30.0, 60.0],
            "beacons_per_second": [2.0, 4.0, 8.0],
            "ssid_count": [2, 2, 2],
        }
    )
    df_radio_2 = pd.DataFrame(
        columns=["highest_cu", "beacons_per_second", "ssid_count"]
    )

    namespace = {
        "df_radio_1": df_radio_1,
        "df_radio_2": df_radio_2,
        "log_and_print": logged_messages.append,
        "plot_radio_regressions": plot_radio_regressions,
        "plt": plt,
        "RADIO_SUMMARY_FOLDER": str(radio_summary_folder),
        "sm": sm,
    }

    exec(compile(_radio_summary_cell_source(4), "analyze_radio_summary.py::cell_4", "exec"), namespace)

    assert namespace["model_1"] is not None
    assert namespace["model_2"] is None
    assert any(
        "Skipping linear regression for radio_count 2" in message
        for message in logged_messages
    )
    assert (radio_summary_folder / "Beacons_per_second_class_linear_regression.png").is_file()
    plt.close("all")
