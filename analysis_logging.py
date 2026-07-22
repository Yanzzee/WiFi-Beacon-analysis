"""Shared helpers for analysis-stage log files."""

from datetime import datetime
from typing import TextIO


def open_analysis_log(
    output_folder: str,
    stage_name: str,
) -> tuple[str, str, TextIO]:
    """Open a timestamped stage log using the existing filename convention."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_filename = f"{output_folder}/{stage_name}_log_{timestamp}.txt"
    log_file = open(log_filename, "a")
    return timestamp, log_filename, log_file
