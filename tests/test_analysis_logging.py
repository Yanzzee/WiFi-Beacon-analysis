from __future__ import annotations

from datetime import datetime
from pathlib import Path

import analysis_logging


class _FixedDatetime:
    @classmethod
    def now(cls):
        return datetime(2025, 1, 2, 3, 4, 5)


def test_open_analysis_log_uses_exact_filename(monkeypatch, tmp_path):
    monkeypatch.setattr(analysis_logging, "datetime", _FixedDatetime)
    output_folder = str(tmp_path).replace("\\", "/")

    timestamp, log_filename, log_file = analysis_logging.open_analysis_log(
        output_folder,
        "analyze_data",
    )
    try:
        assert timestamp == "2025-01-02_03-04"
        assert log_filename == (
            f"{output_folder}/analyze_data_log_2025-01-02_03-04.txt"
        )
        assert log_file.mode == "a"
    finally:
        log_file.close()


def test_open_analysis_log_appends(monkeypatch, tmp_path):
    monkeypatch.setattr(analysis_logging, "datetime", _FixedDatetime)
    output_folder = str(tmp_path).replace("\\", "/")

    _, log_filename, first_log_file = analysis_logging.open_analysis_log(
        output_folder,
        "analyze_data",
    )
    first_log_file.write("first\n")
    first_log_file.close()

    _, second_log_filename, second_log_file = analysis_logging.open_analysis_log(
        output_folder,
        "analyze_data",
    )
    second_log_file.write("second\n")
    second_log_file.close()

    assert second_log_filename == log_filename
    assert Path(log_filename).read_text() == "first\nsecond\n"
