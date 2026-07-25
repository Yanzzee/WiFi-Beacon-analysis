from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

import process_pcapng as pp
from pipeline_harness import SHORT_CAPTURE_FILES

TEST_FILES = SHORT_CAPTURE_FILES
PCAP_MAGIC = b"\xd4\xc3\xb2\xa1"


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


def test_validate_pcapng_capture_warns_for_legacy_pcap_extension(tmp_path: Path):
    legacy_named_capture = tmp_path / "legacy_name.pcap"
    legacy_named_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)

    with pytest.warns(UserWarning, match="\\.pcap extension but pcapng magic bytes"):
        pp.validate_pcapng_capture(legacy_named_capture)


def test_validate_pcapng_capture_rejects_true_pcap_magic(tmp_path: Path):
    pcap_capture = tmp_path / "true_pcap.pcap"
    pcap_capture.write_bytes(PCAP_MAGIC + b"\x00" * 24)

    with pytest.raises(ValueError, match="is a pcap file based on its magic bytes"):
        pp.validate_pcapng_capture(pcap_capture)


def test_discover_pcapng_files_includes_legacy_pcap_named_pcapng(tmp_path: Path):
    pcapng_capture = tmp_path / "capture_a.pcapng"
    legacy_named_capture = tmp_path / "capture_b.pcap"
    ignored_file = tmp_path / "capture_c.txt"

    pcapng_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    legacy_named_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    ignored_file.write_text("not a capture")

    discovered = [Path(path).name for path in pp.discover_pcapng_files(tmp_path)]

    assert discovered == ["capture_a.pcapng", "capture_b.pcap"]


def test_discover_pcapng_files_rejects_true_pcap_magic(tmp_path: Path):
    pcap_capture = tmp_path / "true_pcap.pcap"
    pcap_capture.write_bytes(PCAP_MAGIC + b"\x00" * 24)

    with pytest.raises(ValueError, match="is a pcap file based on its magic bytes"):
        pp.discover_pcapng_files(tmp_path)


@pytest.mark.requires_external_tools
@pytest.mark.pre_analysis_short
@pytest.mark.skipif(shutil.which(os.getenv("TSHARK_PATH", "tshark")) is None, reason="tshark is not available on PATH")
def test_process_pcapng_windows_matches_known_good(
    tmp_path: Path,
    short_captures_dir: Path,
    short_processed_dir: Path,
):
    captures_dir = short_captures_dir
    processed_dir = short_processed_dir
    assert captures_dir.exists(), f"{captures_dir} directory must exist"
    assert processed_dir.exists(), f"{processed_dir} directory must exist"

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
