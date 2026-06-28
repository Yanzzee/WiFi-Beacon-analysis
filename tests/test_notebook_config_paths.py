from __future__ import annotations

import ast
import json
from pathlib import Path


def test_analyze_data_append_to_parquet_uses_configured_sampled_folder():
    notebook_path = Path(__file__).resolve().parents[1] / "analyze_data.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    code_source = "\n".join(
        "".join(cell.get("source", ""))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )

    module = ast.parse(code_source)
    function_source = None
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "append_to_parquet":
            function_source = ast.get_source_segment(code_source, node)
            break

    assert function_source is not None, "append_to_parquet() was not found"
    assert "Data/sampled/{AP_name}_class_beacons.parquet" not in function_source
    assert "os.path.join(SAMPLED_FOLDER" in function_source
