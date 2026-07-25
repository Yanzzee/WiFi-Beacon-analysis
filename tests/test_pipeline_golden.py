from __future__ import annotations

from pathlib import Path

import pytest

from golden_assertions import (
    assert_analysis_golden,
    assert_fingerprints_match,
)
from pipeline_harness import (
    FULL_CAPTURE_FILES,
    SOURCE_SAMPLE_FILES,
    build_analysis_scope_input_fingerprints,
    build_grouped_fingerprints,
    build_grouped_reference_fingerprints,
    build_sample_input_fingerprints,
    missing_grouped_reference_rows,
    run_analysis_pipeline,
    run_grouping_pipeline,
    run_pre_analysis_pipeline,
    run_sample_pipeline,
)


def _run_and_check_pre_analysis(
    workspace: Path,
    captures_dir: Path,
    analysis_fixture_dir: Path,
    pre_analysis_timeout: int,
    capture_workers: int,
) -> None:
    run_pre_analysis_pipeline(
        workspace,
        timeout=pre_analysis_timeout,
        capture_workers=capture_workers,
        captures_dir=captures_dir,
    )
    actual = build_analysis_scope_input_fingerprints(workspace)
    expected = build_analysis_scope_input_fingerprints(analysis_fixture_dir)
    assert_fingerprints_match(
        actual,
        expected,
        actual_path=workspace / "actual_input_fingerprints.json",
        expected_label=str(analysis_fixture_dir),
        failure_message=(
            "original captures and sample CSVs do not reproduce the "
            "class-session inputs used by the analysis golden test"
        ),
    )


def test_full_original_input_manifest_is_complete(
    golden_captures_dir,
    golden_sample_dir,
):
    missing_captures = [
        name
        for name in FULL_CAPTURE_FILES
        if not (golden_captures_dir / name).is_file()
    ]
    missing_samples = [
        name
        for name in SOURCE_SAMPLE_FILES
        if not (golden_sample_dir / name).is_file()
    ]
    assert not missing_captures, (
        "full golden capture input is incomplete: " + ", ".join(missing_captures)
    )
    assert not missing_samples, (
        "full golden sample CSV input is incomplete: " + ", ".join(missing_samples)
    )


def test_sample_csv_inputs_match_golden_fixture(
    sample_input_workspace,
    analysis_fixture_dir,
    pre_analysis_deps,
):
    run_sample_pipeline(sample_input_workspace)
    actual = build_sample_input_fingerprints(sample_input_workspace)
    expected = build_sample_input_fingerprints(analysis_fixture_dir)
    assert_fingerprints_match(
        actual,
        expected,
        actual_path=sample_input_workspace / "actual_sample_fingerprints.json",
        expected_label=str(analysis_fixture_dir / "Sample"),
        failure_message=(
            "original sample CSVs do not reproduce the sample parquet inputs "
            "used by the analysis golden test"
        ),
    )


@pytest.mark.pre_analysis_short
def test_short_processed_grouping_matches_known_good_rows(
    pytestconfig,
    short_grouping_workspace,
    short_processed_dir,
    short_grouped_dir,
    analysis_fixture_dir,
    pre_analysis_timeout,
    pre_analysis_deps,
):
    if not pytestconfig.getoption("--run-short-pre-analysis-golden"):
        pytest.skip(
            "use --run-short-pre-analysis-golden to run short grouping stages"
        )

    pd = pytest.importorskip("pandas")
    sample_aps = pd.read_parquet(
        analysis_fixture_dir / "Sample" / "sample_aps.parquet"
    )
    hostnames = sample_aps["Hostname"].dropna().astype(str).tolist()

    run_grouping_pipeline(
        short_grouping_workspace,
        timeout=pre_analysis_timeout,
    )
    actual = build_grouped_fingerprints(
        short_grouping_workspace / "Data" / "grouped_data",
        hostnames,
    )
    expected = build_grouped_reference_fingerprints(
        short_processed_dir,
        hostnames,
    )
    assert_fingerprints_match(
        actual,
        expected,
        actual_path=(
            short_grouping_workspace / "actual_short_grouped_fingerprints.json"
        ),
        expected_label=str(short_processed_dir),
        failure_message=(
            "short processed inputs were not grouped and cleaned without "
            "dropping or changing sampled-AP rows"
        ),
    )

    if not short_grouped_dir.is_dir():
        pytest.fail(
            f"short grouped reference directory is missing: {short_grouped_dir}"
        )
    missing_rows = missing_grouped_reference_rows(
        short_processed_dir,
        short_grouped_dir,
        hostnames,
    )
    assert not missing_rows, (
        "known-good grouped reference does not contain all short processed "
        f"rows: {missing_rows}"
    )


@pytest.mark.pre_analysis_golden
@pytest.mark.requires_external_tools
def test_pre_analysis_inputs_match_golden_fixture(
    pytestconfig,
    original_input_workspace,
    golden_captures_dir,
    analysis_fixture_dir,
    pre_analysis_timeout,
    capture_workers,
    pre_analysis_deps,
):
    if not pytestconfig.getoption("--run-pre-analysis-golden"):
        pytest.skip("use --run-pre-analysis-golden to run original-input stages")
    _run_and_check_pre_analysis(
        original_input_workspace,
        golden_captures_dir,
        analysis_fixture_dir,
        pre_analysis_timeout,
        capture_workers,
    )


@pytest.mark.full_pipeline_golden
@pytest.mark.requires_external_tools
def test_full_pipeline_regression_from_original_inputs(
    pytestconfig,
    original_input_workspace,
    golden_captures_dir,
    analysis_fixture_dir,
    analysis_golden_file,
    pre_analysis_timeout,
    analysis_timeout,
    capture_workers,
    analysis_deps,
    pre_analysis_deps,
):
    if not pytestconfig.getoption("--run-full-golden"):
        pytest.skip("use --run-full-golden to run the complete pipeline")
    if not analysis_golden_file.is_file():
        pytest.skip(
            "golden fingerprint file is not available. "
            "Run tests/build_golden_outputs.py after placing fixtures."
        )
    _run_and_check_pre_analysis(
        original_input_workspace,
        golden_captures_dir,
        analysis_fixture_dir,
        pre_analysis_timeout,
        capture_workers,
    )
    run_analysis_pipeline(original_input_workspace, timeout=analysis_timeout)
    assert_analysis_golden(original_input_workspace, analysis_golden_file)
