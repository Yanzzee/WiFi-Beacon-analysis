from __future__ import annotations

import pytest

from golden_assertions import assert_analysis_golden
from pipeline_harness import run_analysis_pipeline


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
    assert_analysis_golden(analysis_workspace, analysis_golden_file)
