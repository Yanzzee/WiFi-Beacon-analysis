from __future__ import annotations

import ast
import io
import json
from pathlib import Path

import pytest


CONFIG_ENV_KEYS = (
    "DATA_FOLDER",
    "OUTPUT_FOLDER",
    "PLOTS_FOLDER",
    "PLOT_TIMEZONE",
    "TIMEZONE",
    "SAMPLED_FOLDER",
    "SAMPLE_FOLDER",
    "GROUPED_DATA_FOLDER",
    "ALL_CLASS_BEACONS_FOLDER",
    "CLASS_SESSIONS_FOLDER",
    "BEACON_RATE",
    "AP_PLATFORM",
    "SSID1",
    "SSID2",
)


def _notebook_code_cell(notebook_name: str, cell_index: int) -> tuple[str, Path]:
    notebook_path = Path(__file__).resolve().parents[1] / notebook_name
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    return "".join(notebook["cells"][cell_index]["source"]), notebook_path


def _clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in CONFIG_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _env_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _write_config_env(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )


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
    source, notebook_path = _notebook_code_cell("analyze_data.ipynb", 0)

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
        "AP_PLATFORM": "aruba",
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


def test_analyze_data_preflight_reports_missing_grouped_sample_hostname(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    grouped_folder = Path(namespace["GROUPED_DATA_FOLDER"])
    (grouped_folder / "P-TEST-AP-01.parquet").write_text(
        "placeholder",
        encoding="utf-8",
    )
    missing_path = grouped_folder / "P-TEST-AP-02.parquet"
    out_of_scope_path = grouped_folder / "P-OUT-OF-SCOPE.parquet"

    sample_aps_df = pd.DataFrame(
        {
            "Hostname": [
                "P-TEST-AP-01",
                "P-TEST-AP-02",
                "P-OUT-OF-SCOPE",
            ],
            "Building-Room": [
                "TEST-101",
                "TEST-101",
                "TEST-999",
            ],
        }
    )
    sample_classes_df = pd.DataFrame({"location": ["TEST-101"]})

    with pytest.raises(FileNotFoundError) as exc_info:
        namespace["validate_grouped_files_for_sample_aps"](
            sample_aps_df,
            sample_classes_df,
        )

    message = str(exc_info.value)
    assert str(missing_path) in message
    assert str(out_of_scope_path) not in message
    assert "BSSID fallback files cannot be reliably associated" in message


def test_analyze_data_grouped_schema_accepts_aruba_time_index(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    ap_df = pd.DataFrame(
        {
            "wlan.ta": ["00:00:00:00:00:00"],
            "wlan.vs.aruba.ap_name": ["P-TEST-AP-01"],
            "wlan.ssid": ["eduroam"],
            "wlan.qbss.scount": [10],
            "wlan.qbss.cu": [20],
            "wlan.qbss.adc": [30],
        },
        index=pd.date_range("2025-01-01 10:00:00", periods=1, tz="UTC"),
    )
    ap_df.index.name = "aruba_erm.time"

    namespace["validate_grouped_ap_schema"](
        ap_df,
        "Data/grouped_data/P-TEST-AP-01.parquet",
        "P-TEST-AP-01",
    )


def test_analyze_data_grouped_schema_rejects_naive_aruba_time_index(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    ap_df = pd.DataFrame(
        {
            "wlan.ta": ["00:00:00:00:00:00"],
            "wlan.vs.aruba.ap_name": ["P-TEST-AP-01"],
            "wlan.ssid": ["eduroam"],
            "wlan.qbss.scount": [10],
            "wlan.qbss.cu": [20],
            "wlan.qbss.adc": [30],
        },
        index=pd.date_range("2025-01-01 10:00:00", periods=1),
    )
    ap_df.index.name = "aruba_erm.time"

    with pytest.raises(ValueError, match="timezone-naive"):
        namespace["validate_grouped_ap_schema"](
            ap_df,
            "Data/grouped_data/P-TEST-AP-01.parquet",
            "P-TEST-AP-01",
        )


def test_analyze_data_grouped_schema_rejects_string_timestamps(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    ap_df = pd.DataFrame(
        {
            "aruba_erm.time": ["2025-01-01 10:00:00-07:00"],
            "wlan.ta": ["00:00:00:00:00:00"],
            "wlan.vs.aruba.ap_name": ["P-TEST-AP-01"],
            "wlan.ssid": ["eduroam"],
            "wlan.qbss.scount": [10],
            "wlan.qbss.cu": [20],
            "wlan.qbss.adc": [30],
        }
    )

    with pytest.raises(ValueError, match="timezone-aware datetime dtype"):
        namespace["validate_grouped_ap_schema"](
            ap_df,
            "Data/grouped_data/P-TEST-AP-01.parquet",
            "P-TEST-AP-01",
        )


def test_analyze_data_grouped_schema_rejects_native_conversion_output(
    monkeypatch,
    tmp_path,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    native_df = pd.DataFrame(
        {
            "wlan.ta": ["00:00:00:00:00:00"],
            "wlan.bssid": ["00:00:00:00:00:00"],
            "wlan.ssid": ["eduroam"],
            "wlan.qbss.scount": [10],
            "wlan.qbss.cu": [20],
            "wlan.qbss.adc": [30],
        },
        index=pd.date_range("2025-01-01 10:00:00", periods=1, tz="UTC"),
    )
    native_df.index.name = "frame.time_epoch"

    with pytest.raises(
        ValueError,
        match=r"Native/non-Aruba captures are conversion-only",
    ):
        namespace["validate_grouped_ap_schema"](
            native_df,
            "Data/grouped_data/00_00_00_00_00_00.parquet",
            "P-TEST-AP-01",
        )


@pytest.mark.parametrize("configured_platform", [None, "extreme", "none"])
def test_analyze_data_rejects_unsupported_radio_inference_platform(
    monkeypatch,
    tmp_path,
    configured_platform,
):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    namespace["AP_PLATFORM"] = configured_platform

    with pytest.raises(
        ValueError,
        match=r"radio inference is supported only when AP_PLATFORM=aruba",
    ):
        namespace["validate_radio_inference_platform"]()

    namespace["AP_PLATFORM"] = " Aruba "
    namespace["validate_radio_inference_platform"]()

    main_source = _find_top_level_function_source(
        _analyze_data_code_source(),
        "main",
    )
    assert main_source.index("validate_radio_inference_platform()") < main_source.index(
        "validate_no_existing_analysis_outputs()"
    )


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


def test_analyze_data_rejects_missing_cu_before_zero_fill(monkeypatch, tmp_path):
    namespace = _load_analyze_data_first_cell(monkeypatch, tmp_path)
    pd = namespace["pd"]

    start_time = pd.Timestamp("2025-01-01 10:00:00", tz=namespace["TIMEZONE"])
    end_time = pd.Timestamp("2025-01-01 10:01:00", tz=namespace["TIMEZONE"])
    rows = pd.DataFrame(
        {
            "aruba_erm.time": [start_time, start_time + pd.Timedelta(seconds=1)],
            "wlan.ta": ["00:00:00:00:00:00", "00:00:00:00:00:01"],
            "wlan.bssid": ["00:00:00:00:00:00", "00:00:00:00:00:01"],
            "wlan.ssid": ["eduroam", "eduroam"],
            "wlan.qbss.cu": [pd.NA, 0],
        }
    )

    with pytest.raises(
        ValueError,
        match=r"1 of 2 deduplicated in-scope rows .* missing wlan.qbss.cu",
    ):
        namespace["validate_no_missing_cu"](
            rows,
            "P-TEST-AP-01",
            start_time,
            end_time,
        )

    namespace["validate_no_missing_cu"](
        rows.assign(**{"wlan.qbss.cu": [0, 1]}),
        "P-TEST-AP-01",
        start_time,
        end_time,
    )

    graph_source = _find_top_level_function_source(
        _analyze_data_code_source(),
        "graph",
    )
    assert graph_source.index("validate_no_missing_cu(") < graph_source.index(
        'filtered_df["wlan.qbss.cu"] = filtered_df["wlan.qbss.cu"].fillna(0)'
    )


def test_analyze_all_class_beacons_filters_configured_ssids(monkeypatch, tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    _clear_config_env(monkeypatch)
    monkeypatch.chdir(tmp_path)

    data_dir = tmp_path / "Data"
    output_dir = tmp_path / "Output"
    sample_dir = tmp_path / "Sample"
    all_class_beacons_dir = output_dir / "all_class_beacons"
    data_dir.mkdir()
    output_dir.mkdir()
    sample_dir.mkdir()

    _write_config_env(
        tmp_path / "config.env",
        {
            "DATA_FOLDER": _env_path(data_dir),
            "OUTPUT_FOLDER": _env_path(output_dir),
            "SAMPLE_FOLDER": _env_path(sample_dir),
            "ALL_CLASS_BEACONS_FOLDER": _env_path(all_class_beacons_dir),
            "SSID1": "CampusSecure",
            "SSID2": "CampusGuest",
        },
    )
    pd.DataFrame(
        {
            "wlan.ssid": [
                "CampusSecure",
                "CampusGuest",
                "eduroam",
                "OtherNetwork",
            ]
        }
    ).to_parquet(data_dir / "all_class_beacons.parquet")
    pd.DataFrame({"Hostname": []}).to_parquet(sample_dir / "sample_aps.parquet")

    cell0, notebook_path = _notebook_code_cell("analyze_all_class_beacons.ipynb", 0)
    cell2, _ = _notebook_code_cell("analyze_all_class_beacons.ipynb", 2)
    namespace = {"__name__": "not_main"}
    try:
        exec(compile(cell0, f"{notebook_path}::cell_0", "exec"), namespace)
        exec(compile(cell2, f"{notebook_path}::cell_2", "exec"), namespace)
    finally:
        log_file = namespace.get("log_file")
        if log_file is not None:
            log_file.close()

    filtered_ssids = namespace["all_class_beacons_df"]["wlan.ssid"].tolist()
    assert filtered_ssids == ["CampusSecure", "CampusGuest"]


def test_analyze_class_sessions_counts_configured_primary_ssid_radios(
    monkeypatch,
    tmp_path,
):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    _clear_config_env(monkeypatch)
    monkeypatch.chdir(tmp_path)

    data_dir = tmp_path / "Data"
    output_dir = tmp_path / "Output"
    sample_dir = tmp_path / "Sample"
    class_sessions_dir = output_dir / "class_sessions"
    data_dir.mkdir()
    output_dir.mkdir()
    sample_dir.mkdir()

    _write_config_env(
        tmp_path / "config.env",
        {
            "DATA_FOLDER": _env_path(data_dir),
            "OUTPUT_FOLDER": _env_path(output_dir),
            "SAMPLE_FOLDER": _env_path(sample_dir),
            "CLASS_SESSIONS_FOLDER": _env_path(class_sessions_dir),
            "BEACON_RATE": "1.024",
            "SSID1": "CampusSecure",
            "SSID2": "CampusGuest",
        },
    )

    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    end = pd.Timestamp("2025-01-01 10:05:00", tz="UTC")
    pd.DataFrame(
        {
            "location": ["BLDG-101"],
            "start_time": [start],
            "end_time": [end],
            "enrolled": [10],
            "capacity": [20],
        }
    ).to_parquet(sample_dir / "sample_classes.parquet")
    pd.DataFrame({"Building-Room": ["BLDG-101"]}).to_parquet(
        sample_dir / "sample_aps.parquet"
    )
    pd.DataFrame({"Location": ["BLDG-101"], "Group": ["A"]}).to_parquet(
        sample_dir / "sample_classrooms.parquet"
    )
    pd.DataFrame(
        {
            "aruba_erm.time": [
                start + pd.Timedelta(seconds=1),
                start + pd.Timedelta(seconds=2),
                start + pd.Timedelta(seconds=3),
                start + pd.Timedelta(seconds=4),
            ],
            "location": ["BLDG-101"] * 4,
            "room_scount": [5, 6, 7, 8],
            "wlan.qbss.cu": [200, 100, 100, 100],
            "wlan.qbss.scount": [1, 2, 3, 4],
            "wlan.ssid": [
                "CampusSecure",
                "CampusSecure",
                "eduroam",
                "CampusGuest",
            ],
            "wlan.ta": [
                "00:00:00:00:00:01",
                "00:00:00:00:00:02",
                "00:00:00:00:00:03",
                "00:00:00:00:00:04",
            ],
        }
    ).to_parquet(data_dir / "all_class_beacons_processed.parquet")

    cell0, notebook_path = _notebook_code_cell("analyze_class_sessions.ipynb", 0)
    cell1, _ = _notebook_code_cell("analyze_class_sessions.ipynb", 1)
    namespace = {"__name__": "not_main"}
    try:
        exec(compile(cell0, f"{notebook_path}::cell_0", "exec"), namespace)
        exec(compile(cell1, f"{notebook_path}::cell_1", "exec"), namespace)
    finally:
        log_file = namespace.get("log_file")
        if log_file is not None:
            log_file.close()

    assert int(namespace["class_sessions_df"].loc[0, "number_radios"]) == 2
