from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline_harness import (
    fixture_dataset_is_present,
    missing_fixture_inputs,
    prepare_analysis_workspace,
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
        help="Per-notebook timeout in seconds for analysis-forward tests.",
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
