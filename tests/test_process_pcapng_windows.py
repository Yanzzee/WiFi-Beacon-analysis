from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

import process_pcapng as pp

TEST_FILES = [
    "Beacon_Capture_20251110090000_00012.pcapng",
    "Beacon_Capture_20251110100000_00013.pcapng",
    "Beacon_Capture_20251110110000_00014.pcapng",
]


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _assert_dataframes_equivalent(left: pd.DataFrame, right: pd.DataFrame) -> None:
    # Compare schema and values; ignore row ordering by sorting on index and columns.
    left_sorted = left.sort_index(axis=0).sort_index(axis=1)
    right_sorted = right.sort_index(axis=0).sort_index(axis=1)

    pd.testing.assert_frame_equal(
        left_sorted,
        right_sorted,
        check_like=True,
        check_dtype=False,
        rtol=1e-6,
        atol=1e-8,
    )


@pytest.mark.requires_external_tools
@pytest.mark.skipif(shutil.which(os.getenv("TSHARK_PATH", "tshark")) is None, reason="tshark is not available on PATH")
def test_process_pcapng_windows_matches_known_good(tmp_path: Path):
    captures_dir = Path("Data") / "captures"
    processed_dir = Path("Data") / "processed_data"
    assert captures_dir.exists(), "Data/captures directory must exist"
    assert processed_dir.exists(), "Data/processed_data directory must exist"

    output_dir = tmp_path / "processed_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    for filename in TEST_FILES:
        input_path = captures_dir / filename
        expected_path = processed_dir / f"{filename}.parquet"
        assert input_path.exists(), f"Missing input file: {input_path}"
        assert expected_path.exists(), f"Missing expected output file: {expected_path}"

    result_files = []
    for filename in TEST_FILES:
        output_file = pp.process_pcapng(captures_dir / filename, output_dir, erm=True)
        result_files.append(output_file)

    for filename in TEST_FILES:
        expected_file = processed_dir / f"{filename}.parquet"
        actual_file = output_dir / f"{filename}.parquet"

        assert actual_file.exists(), f"Output parquet missing: {actual_file}"

        expected_df = _read_parquet(expected_file)
        actual_df = _read_parquet(actual_file)
        _assert_dataframes_equivalent(actual_df, expected_df)
