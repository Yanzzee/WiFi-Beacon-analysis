"""Human-readable CSV exports derived from compact corrected Parquet tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


COMPACT_CSV_EXPORTS: Mapping[str, tuple[str, str]] = {
    "project_identity_map": (
        "project_identity_map.parquet",
        "project_identity_map.csv",
    ),
    "radio_class_summary": (
        "radio_class_summary.parquet",
        "radio_class_summary.csv",
    ),
    "classroom_class_summary": (
        "classroom_class_summary.parquet",
        "classroom_class_summary.csv",
    ),
}


def _csv_value(value: Any) -> Any:
    """Render Parquet list values as deterministic JSON inside CSV cells."""

    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def write_compact_csv_exports(run_directory: str | Path) -> dict[str, int]:
    """Write the three small review CSVs from authoritative Parquet tables.

    The selected-observation dataset is intentionally excluded because it can
    contain millions of rows and is already efficiently partitioned as
    Parquet.
    """

    root = Path(run_directory)
    destination = root / "exports" / "csv"
    if destination.exists():
        raise FileExistsError(f"CSV export directory already exists: {destination}")
    destination.mkdir(parents=True)

    written: dict[str, int] = {}
    try:
        for artifact, (parquet_name, csv_name) in COMPACT_CSV_EXPORTS.items():
            source = root / parquet_name
            if not source.is_file():
                raise FileNotFoundError(
                    f"authoritative Parquet table is missing: {source}"
                )
            frame = pd.read_parquet(source)
            for column in frame.columns:
                if frame[column].dtype == "object":
                    frame[column] = frame[column].map(_csv_value)
            target = destination / csv_name
            frame.to_csv(
                target,
                index=False,
                encoding="utf-8",
                lineterminator="\n",
                na_rep="",
            )
            written[artifact] = len(frame)
    except BaseException:
        for path in destination.glob("*"):
            path.unlink(missing_ok=True)
        destination.rmdir()
        parent = destination.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
        raise
    return written
