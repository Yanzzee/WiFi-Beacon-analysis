from __future__ import annotations

import json
from pathlib import Path

import pytest


def _radio_summary_cell_source(cell_index: int) -> str:
    notebook_path = Path(__file__).resolve().parents[1] / "analyze_radio_summary.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    source = notebook["cells"][cell_index]["source"]
    return "".join(source) if isinstance(source, list) else source


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

    exec(compile(_radio_summary_cell_source(3), "analyze_radio_summary.ipynb::cell_4", "exec"), namespace)

    assert namespace["model_1"] is not None
    assert namespace["model_2"] is None
    assert any(
        "Skipping linear regression for radio_count 2" in message
        for message in logged_messages
    )
    assert (radio_summary_folder / "Beacons_per_second_class_linear_regression.png").is_file()
    plt.close("all")
