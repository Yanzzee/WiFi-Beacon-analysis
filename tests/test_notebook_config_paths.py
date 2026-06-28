from __future__ import annotations

import ast
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
