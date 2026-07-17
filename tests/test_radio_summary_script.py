from __future__ import annotations

import pytest

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
            "class_duration": [100.0, 100.0, 100.0],
            "highest_cu": [10.0, 20.0, 30.0],
            "ssid_count": [2, 0, None],
        }
    )

    namespace = {
        "radio_summary_df": radio_summary_df,
        "log_and_print": logged_messages.append,
        "plt": plt,
        "pd": pd,
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
