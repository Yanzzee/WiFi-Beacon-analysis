from __future__ import annotations

import ast
import io
import json
from pathlib import Path

import pytest


def _analyze_data_code_source() -> str:
    notebook_path = Path(__file__).resolve().parents[1] / "analyze_data.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell.get("source", ""))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )


def _find_top_level_function_source(code_source: str, function_name: str) -> str:
    module = ast.parse(code_source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            function_source = ast.get_source_segment(code_source, node)
            assert function_source is not None
            return function_source

    raise AssertionError(f"{function_name}() was not found")


def _load_analyze_data_first_cell(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    notebook_path = Path(__file__).resolve().parents[1] / "analyze_data.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    source = "".join(notebook["cells"][0]["source"])

    data_folder = tmp_path / "Data"
    env_values = {
        "DATA_FOLDER": str(data_folder),
        "OUTPUT_FOLDER": str(tmp_path / "Output"),
        "PLOTS_FOLDER": str(tmp_path / "Output" / "plots"),
        "PLOT_TIMEZONE": "America/Denver",
        "TIMEZONE": "Etc/GMT+7",
        "SAMPLED_FOLDER": str(tmp_path / "custom_sampled"),
        "SAMPLE_FOLDER": str(tmp_path / "Sample"),
        "GROUPED_DATA_FOLDER": str(data_folder / "grouped_data"),
        "BEACON_RATE": "1.024",
    }
    for key, value in env_values.items():
        monkeypatch.setenv(key, value)

    namespace = {"__name__": "not_main"}
    exec(compile(source, str(notebook_path), "exec"), namespace)
    return namespace


def test_analyze_data_append_to_parquet_uses_configured_sampled_folder():
    function_source = _find_top_level_function_source(
        _analyze_data_code_source(),
        "append_to_parquet",
    )

    assert "Data/sampled/{AP_name}_class_beacons.parquet" not in function_source
    assert "os.path.join(SAMPLED_FOLDER" in function_source


def test_analyze_data_rejects_existing_generated_outputs(monkeypatch, tmp_path):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)

    namespace["validate_no_existing_analysis_outputs"]()

    sampled_folder = Path(namespace["SAMPLED_FOLDER"])
    stale_sampled_file = sampled_folder / "P-TEST-AP-01_class_beacons.parquet"
    stale_sampled_file.write_text("stale output", encoding="utf-8")

    with pytest.raises(FileExistsError, match="Existing analyze_data outputs"):
        namespace["validate_no_existing_analysis_outputs"]()


def test_analyze_data_combine_all_class_beacons_requires_sampled_files(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)

    with pytest.raises(
        FileNotFoundError,
        match="No sampled class beacon parquet files found",
    ):
        namespace["combine_all_class_beacons"]()


def test_analyze_data_graph_handles_three_radio_groups(monkeypatch, tmp_path):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    namespace["log_file"] = io.StringIO()
    namespace["all_class_beacons_df"] = pd.DataFrame()
    namespace["plot_channel_stations"] = (
        lambda radio_df, _eduroam_df, _byu_wifi_df, _ap_name, _start_time, _end_time:
        f"plot_R{int(radio_df.iloc[0]['radio_number'])}.png"
    )

    start_time = pd.Timestamp("2025-01-01 10:00:00", tz=namespace["TIMEZONE"])
    end_time = pd.Timestamp("2025-01-01 10:01:00", tz=namespace["TIMEZONE"])
    ap_df = pd.DataFrame(
        {
            "wlan.ta": [
                "00:00:00:00:00:00",
                "00:00:00:00:00:01",
                "00:00:00:00:00:10",
                "00:00:00:00:00:11",
                "00:00:00:00:00:20",
                "00:00:00:00:00:21",
            ],
            "wlan.vs.aruba.ap_name": ["P-TEST-AP-01"] * 6,
            "wlan.ssid": [
                "eduroam",
                "BYU-WiFi",
                "eduroam",
                "BYU-WiFi",
                "eduroam",
                "BYU-WiFi",
            ],
            "wlan.qbss.scount": [10, 20, 30, 40, 50, 60],
            "wlan.qbss.cu": [10, 20, 30, 40, 50, 60],
            "wlan.qbss.adc": [1, 2, 3, 4, 5, 6],
        },
        index=pd.date_range(start_time, periods=6, freq="5s"),
    )
    ap_df.index.name = "aruba_erm.time"

    radio_summary_df = pd.DataFrame(
        {
            "subject": pd.Series(dtype=str),
            "location": pd.Series(dtype=str),
            "AP_hostname": pd.Series(dtype=str),
            "radio_number": pd.Series(dtype=int),
            "start_time": pd.Series(dtype="datetime64[ns]"),
            "end_time": pd.Series(dtype="datetime64[ns]"),
            "capacity": pd.Series(dtype=int),
            "enrolled": pd.Series(dtype=int),
            "AP_count": pd.Series(dtype=int),
            "radio_count": pd.Series(dtype=int),
            "bssid_count": pd.Series(dtype=int),
            "ssid_count": pd.Series(dtype=int),
            "total_beacons": pd.Series(dtype=int),
            "high_cu_beacons": pd.Series(dtype=int),
            "percent_high_cu": pd.Series(dtype=float),
            "highest_cu": pd.Series(dtype=int),
            "median_cu": pd.Series(dtype=int),
            "lowest_cu": pd.Series(dtype=int),
            "high_scount": pd.Series(dtype=int),
            "median_scount": pd.Series(dtype=int),
            "low_scount": pd.Series(dtype=int),
            "longest_duration_high_cu": pd.Series(dtype=float),
            "graph_filename": pd.Series(dtype=str),
        }
    )

    namespace["graph"](
        "P-TEST-AP-01",
        start_time,
        end_time,
        "TEST-101",
        "TEST 101",
        10,
        20,
        1,
        ap_df,
        radio_summary_df,
    )

    assert radio_summary_df["radio_number"].tolist() == [1, 2, 3]
    assert radio_summary_df["radio_count"].tolist() == [3, 3, 3]
    assert radio_summary_df["high_scount"].tolist() == [30, 70, 110]

    all_beacons = namespace["all_class_beacons_df"]
    assert sorted(all_beacons["radio_number"].unique().tolist()) == [1, 2, 3]
    max_totals_by_radio = all_beacons.groupby("radio_number")["total_scount"].max()
    assert max_totals_by_radio.to_dict() == {1: 30, 2: 70, 3: 110}
