from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_addoption(parser):
    group = parser.getgroup("beacon analysis capture fixtures")
    group.addoption(
        "--short-captures-dir",
        action="store",
        default=None,
        help=(
            "Short raw capture fixture directory. Defaults to "
            "BEACON_ANALYSIS_SHORT_CAPTURES_DIR or data/captures_test."
        ),
    )
    group.addoption(
        "--short-processed-dir",
        action="store",
        default=None,
        help=(
            "Known-good short processed Parquet directory. Defaults to "
            "BEACON_ANALYSIS_SHORT_PROCESSED_DIR or "
            "data/processed_data_test/original."
        ),
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_external_tools: tests that require TShark or another tool",
    )
    config.addinivalue_line(
        "markers",
        "pre_analysis_short: short real-capture conversion regression",
    )


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def short_captures_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--short-captures-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SHORT_CAPTURES_DIR")
    return Path(configured or env_value or repo_root / "data" / "captures_test")


@pytest.fixture(scope="session")
def short_processed_dir(pytestconfig, repo_root: Path) -> Path:
    configured = pytestconfig.getoption("--short-processed-dir")
    env_value = os.environ.get("BEACON_ANALYSIS_SHORT_PROCESSED_DIR")
    return Path(
        configured
        or env_value
        or repo_root / "data" / "processed_data_test" / "original"
    )
