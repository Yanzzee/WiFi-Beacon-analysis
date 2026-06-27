from __future__ import annotations

import difflib
import json

import pytest

from pipeline_harness import (
    build_fingerprints,
    missing_expected_outputs,
    missing_expected_plots,
    missing_required_columns,
    run_analysis_pipeline,
    write_fingerprints,
)


pytestmark = pytest.mark.analysis_forward


def test_analysis_forward_regression(
    analysis_workspace,
    analysis_golden_file,
    analysis_timeout,
    analysis_deps,
):
    if not analysis_golden_file.is_file():
        pytest.skip(
            "golden fingerprint file is not available. "
            "Run tests/build_golden_outputs.py after placing fixtures."
        )

    run_analysis_pipeline(analysis_workspace, timeout=analysis_timeout)

    missing_outputs = missing_expected_outputs(analysis_workspace)
    assert not missing_outputs, "missing expected output files: " + ", ".join(
        missing_outputs
    )

    missing_plots = missing_expected_plots(analysis_workspace)
    assert not missing_plots, "missing expected plot files: " + ", ".join(
        missing_plots
    )

    missing_columns = missing_required_columns(analysis_workspace)
    assert not missing_columns, "missing required columns: " + json.dumps(
        missing_columns,
        indent=2,
        sort_keys=True,
    )

    actual = build_fingerprints(analysis_workspace)
    expected = json.loads(analysis_golden_file.read_text(encoding="utf-8"))

    if actual != expected:
        actual_path = analysis_workspace / "actual_fingerprints.json"
        write_fingerprints(actual_path, actual)
        expected_text = json.dumps(expected, indent=2, sort_keys=True).splitlines()
        actual_text = json.dumps(actual, indent=2, sort_keys=True).splitlines()
        diff = "\n".join(
            difflib.unified_diff(
                expected_text,
                actual_text,
                fromfile=str(analysis_golden_file),
                tofile=str(actual_path),
                lineterm="",
            )
        )
        pytest.fail(
            "analysis fingerprints changed. "
            f"Actual fingerprints were written to {actual_path}.\n{diff}"
        )
