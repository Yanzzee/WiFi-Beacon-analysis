from __future__ import annotations

import difflib
import json
from pathlib import Path

import pytest

from pipeline_harness import (
    build_fingerprints,
    missing_expected_outputs,
    missing_expected_plots,
    missing_required_columns,
    write_fingerprints,
)


def assert_analysis_golden(
    workspace: Path,
    golden_file: Path,
) -> None:
    missing_outputs = missing_expected_outputs(workspace)
    assert not missing_outputs, "missing expected output files: " + ", ".join(
        missing_outputs
    )

    missing_plots = missing_expected_plots(workspace)
    assert not missing_plots, "missing expected plot files: " + ", ".join(
        missing_plots
    )

    missing_columns = missing_required_columns(workspace)
    assert not missing_columns, "missing required columns: " + json.dumps(
        missing_columns,
        indent=2,
        sort_keys=True,
    )

    actual = build_fingerprints(workspace)
    expected = json.loads(golden_file.read_text(encoding="utf-8"))
    assert_fingerprints_match(
        actual,
        expected,
        actual_path=workspace / "actual_fingerprints.json",
        expected_label=str(golden_file),
        failure_message="analysis fingerprints changed",
    )


def assert_fingerprints_match(
    actual: dict[str, object],
    expected: dict[str, object],
    *,
    actual_path: Path,
    expected_label: str,
    failure_message: str,
) -> None:
    if actual == expected:
        return

    write_fingerprints(actual_path, actual)
    expected_text = json.dumps(expected, indent=2, sort_keys=True).splitlines()
    actual_text = json.dumps(actual, indent=2, sort_keys=True).splitlines()
    diff = "\n".join(
        difflib.unified_diff(
            expected_text,
            actual_text,
            fromfile=expected_label,
            tofile=str(actual_path),
            lineterm="",
        )
    )
    pytest.fail(
        f"{failure_message}. Actual fingerprints were written to "
        f"{actual_path}.\n{diff}"
    )
