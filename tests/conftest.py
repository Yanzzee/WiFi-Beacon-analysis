from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline_harness import (
    fixture_dataset_is_present,
    missing_fixture_inputs,
    missing_original_inputs,
    prepare_analysis_workspace,
    prepare_grouping_workspace,
    prepare_original_input_workspace,
    prepare_sample_input_workspace,
)


def pytest_addoption(parser):
    group = parser.getgroup("beacon analysis regression")
    group.addoption(
        "--analysis-fixture-dir",
        action="store",
        default=None,
        help=(
            "Path to a fixture dataset containing Sample/*.parquet and "
            "Data/grouped_data/*.parquet. Defaults to "
            "BEACON_ANALYSIS_FIXTURE_DIR or tests/fixtures/dataset."
        ),
    )
    group.addoption(
        "--analysis-golden-file",
        action="store",
        default=None,
        help=(
            "Path to golden fingerprint JSON. Defaults to "
            "BEACON_ANALYSIS_GOLDEN_FILE or tests/golden/fingerprints.json."
        ),
    )
    group.addoption(
        "--analysis-timeout",
        action="store",
        type=int,
        default=600,
        help="Per-stage timeout in seconds for analysis-forward tests.",
    )
    group.addoption(
        "--run-pre-analysis-golden",
        action="store_true",
        default=False,
        help=(
            "Run capture conversion, Aruba grouping/cleanup, and sample CSV "
            "processing, then compare their outputs with the golden fixture."
        ),
    )
    group.addoption(
        "--run-full-golden",
        action="store_true",
        default=False,
        help="Run the full pipeline from original captures and sample CSVs.",
    )
    group.addoption(
        "--run-short-pre-analysis-golden",
        action="store_true",
        default=False,
        help=(
            "Run the short known-good processed-data grouping and cleanup "
            "checkpoint."
        ),
    )
    group.addoption(
        "--golden-captures-dir",
        action="store",
        default=None,
        help=(
            "Original capture directory. Defaults to "
            "BEACON_ANALYSIS_CAPTURES_DIR or Data/captures."
        ),
    )
    group.addoption(
        "--golden-sample-dir",
        action="store",
        default=None,
        help=(
            "Original sample CSV directory. Defaults to "
            "BEACON_ANALYSIS_SAMPLE_DIR or Sample."
        ),
    )
    group.addoption(
        "--short-captures-dir",
        action="store",
        default=None,
        help=(
            "Short raw capture fixture directory. Defaults to "
            "BEACON_ANALYSIS_SHORT_CAPTURES_DIR or Data/captures_test."
        ),
    )
    group.addoption(
        "--short-processed-dir",
        action="store",
        default=None,
        help=(
            "Known-good short processed parquet directory. Defaults to "
            "BEACON_ANALYSIS_SHORT_PROCESSED_DIR or "
            "Data/processed_data_test/original."
        ),
    )
    group.addoption(
        "--short-grouped-dir",
        action="store",
        default=None,
        help=(
            "Known-good grouped reference directory containing the short "
            "rows. Defaults to BEACON_ANALYSIS_SHORT_GROUPED_DIR or "
            "Data/grouped_data_test."
        ),
    )
    group.addoption(
        "--pre-analysis-timeout",
        action="store",
        type=int,
        default=3600,
        help="Per-stage timeout in seconds for original-input stages.",
    )
    group.addoption(
        "--capture-workers",
        action="store",
        type=int,
        default=4,
        help="Worker processes used by process_pcapng.py in golden tests.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "analysis_forward: regression tests for analyze_data and later stages",
    )
    config.addinivalue_line(
        "markers",
        "requires_external_tools: tests that need external tools such as tshark",
    )
    config.addinivalue_line(
        "markers",
        "pre_analysis_golden: capture/sample processing through analysis inputs",
    )
    config.addinivalue_line(
        "markers",
        "pre_analysis_short: short capture/processed/grouped regression stages",
    )
    config.addinivalue_line(
        "markers",
        "full_pipeline_golden: full regression from captures and sample CSVs",
    )


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def analysis_fixture_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--analysis-fixture-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_FIXTURE_DIR")
    return Path(configured or env_value or repo_root / "tests" / "fixtures" / "dataset")


@pytest.fixture(scope="session")
def analysis_golden_file(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--analysis-golden-file")
    env_value = os.environ.get("BEACON_ANALYSIS_GOLDEN_FILE")
    return Path(configured or env_value or repo_root / "tests" / "golden" / "fingerprints.json")


@pytest.fixture(scope="session")
def analysis_timeout(pytestconfig) -> int:
    return int(pytestconfig.getoption("--analysis-timeout"))


@pytest.fixture(scope="session")
def pre_analysis_timeout(pytestconfig) -> int:
    return int(pytestconfig.getoption("--pre-analysis-timeout"))


@pytest.fixture(scope="session")
def capture_workers(pytestconfig) -> int:
    return int(pytestconfig.getoption("--capture-workers"))


@pytest.fixture(scope="session")
def golden_captures_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--golden-captures-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_CAPTURES_DIR")
    return Path(configured or env_value or repo_root / "Data" / "captures")


@pytest.fixture(scope="session")
def golden_sample_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--golden-sample-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SAMPLE_DIR")
    return Path(configured or env_value or repo_root / "Sample")


@pytest.fixture(scope="session")
def short_captures_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--short-captures-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SHORT_CAPTURES_DIR")
    return Path(configured or env_value or repo_root / "Data" / "captures_test")


@pytest.fixture(scope="session")
def short_processed_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--short-processed-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SHORT_PROCESSED_DIR")
    return Path(
        configured
        or env_value
        or repo_root / "Data" / "processed_data_test" / "original"
    )


@pytest.fixture(scope="session")
def short_grouped_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--short-grouped-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SHORT_GROUPED_DIR")
    return Path(configured or env_value or repo_root / "Data" / "grouped_data_test")


@pytest.fixture(scope="session")
def analysis_deps():
    for module_name in (
        "pandas",
        "pyarrow",
        "matplotlib",
        "numpy",
        "scipy",
        "statsmodels",
        "dotenv",
        "tqdm",
    ):
        pytest.importorskip(module_name)


@pytest.fixture(scope="session")
def pre_analysis_deps():
    for module_name in (
        "pandas",
        "pyarrow",
        "dask",
        "dotenv",
        "tqdm",
    ):
        pytest.importorskip(module_name)


@pytest.fixture()
def analysis_workspace(tmp_path: Path, analysis_fixture_dir: Path, repo_root: Path) -> Path:
    if not fixture_dataset_is_present(analysis_fixture_dir):
        missing = ", ".join(missing_fixture_inputs(analysis_fixture_dir))
        pytest.skip(
            "analysis fixture dataset is not available. Missing: "
            f"{missing}. See tests/fixtures/README.md."
        )

    workspace = tmp_path / "analysis_workspace"
    prepare_analysis_workspace(analysis_fixture_dir, workspace, repo_root)
    return workspace


@pytest.fixture()
def original_input_workspace(
    pytestconfig,
    tmp_path: Path,
    golden_captures_dir: Path,
    golden_sample_dir: Path,
    repo_root: Path,
) -> Path:
    if not (
        pytestconfig.getoption("--run-pre-analysis-golden")
        or pytestconfig.getoption("--run-full-golden")
    ):
        pytest.skip(
            "use --run-pre-analysis-golden or --run-full-golden to stage "
            "original inputs"
        )

    configured_tshark = os.environ.get("TSHARK_PATH", "tshark")
    if shutil.which(configured_tshark) is None and not (
        os.path.isabs(configured_tshark) and Path(configured_tshark).is_file()
    ):
        pytest.skip(
            "the original-capture golden stages require tshark on PATH or "
            "TSHARK_PATH"
        )

    missing = missing_original_inputs(golden_captures_dir, golden_sample_dir)
    if missing:
        pytest.skip(
            "original pipeline inputs are not available. Missing: "
            + ", ".join(missing)
        )

    workspace = tmp_path / "original_input_workspace"
    prepare_original_input_workspace(
        golden_captures_dir,
        golden_sample_dir,
        workspace,
        repo_root,
    )
    return workspace


@pytest.fixture()
def sample_input_workspace(
    tmp_path: Path,
    golden_sample_dir: Path,
    repo_root: Path,
) -> Path:
    missing = [
        name
        for name in ("Sample_APs.csv", "Sample_classrooms.csv", "Sample_schedule.csv")
        if not (golden_sample_dir / name).is_file()
    ]
    if missing:
        pytest.skip(
            "original sample CSV inputs are not available. Missing: "
            + ", ".join(missing)
        )

    workspace = tmp_path / "sample_input_workspace"
    prepare_sample_input_workspace(golden_sample_dir, workspace, repo_root)
    return workspace


@pytest.fixture()
def short_grouping_workspace(
    tmp_path: Path,
    short_processed_dir: Path,
    analysis_fixture_dir: Path,
    repo_root: Path,
) -> Path:
    if not any(short_processed_dir.glob("*.parquet")):
        pytest.skip(
            f"short processed parquet inputs are not available in "
            f"{short_processed_dir}"
        )

    workspace = tmp_path / "short_grouping_workspace"
    prepare_grouping_workspace(
        short_processed_dir,
        analysis_fixture_dir,
        workspace,
        repo_root,
    )
    return workspace
