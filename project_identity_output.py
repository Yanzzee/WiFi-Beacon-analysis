"""Durable corrected project identity-map artifact.

This module serializes the project-wide BSSID identity map produced by the
shared radio resolver. It intentionally does not choose a preferred CU BSSID:
that choice belongs to each radio/class session under ADR-0004.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import uuid
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


RESULT_SCHEMA_ID = "beacon.schema/analysis-results/v1"
CORRECTED_METRIC_ID = "beacon.metric/corrected-cu/v1"
PROJECT_IDENTITY_FILENAME = "project_identity_map.parquet"

_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_REQUIRED_COLUMNS = {
    "bssid",
    "ssid",
    "radio_group_id",
    "radio_number",
    "ap_id",
    "ap_name",
    "classroom",
    "band",
    "channels",
    "frequencies_mhz",
    "ap_identity_method",
    "radio_grouping_method",
    "result_grain",
    "mapping_status",
    "mapping_diagnostics",
}

PROJECT_IDENTITY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("schema_id", pa.string(), nullable=False),
        pa.field("metric_id", pa.string(), nullable=False),
        pa.field("bssid", pa.string(), nullable=False),
        pa.field("ssid", pa.string()),
        pa.field("radio_group_id", pa.string()),
        pa.field("radio_number", pa.int32()),
        pa.field("ap_id", pa.string()),
        pa.field("ap_name", pa.string()),
        pa.field("ap_model", pa.string()),
        pa.field("classroom", pa.string()),
        pa.field("band", pa.string()),
        pa.field("observed_channels", pa.list_(pa.int32())),
        pa.field("observed_frequencies_mhz", pa.list_(pa.int32())),
        pa.field("ap_identity_method", pa.string()),
        pa.field("radio_grouping_method", pa.string()),
        pa.field("result_grain", pa.string()),
        pa.field("mapping_status", pa.string()),
        pa.field("mapping_diagnostics", pa.list_(pa.string())),
    ]
)


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if hasattr(missing, "__len__"):
        return False
    return bool(missing)


def _integer_list(value: Any) -> list[int]:
    if _is_missing(value):
        return []
    if isinstance(value, str):
        values = re.split(r"[;,]", value)
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = [value]

    normalized: set[int] = set()
    for item in values:
        if _is_missing(item) or str(item).strip() == "":
            continue
        number = int(item)
        if number < 0:
            raise ValueError("channel/frequency evidence cannot be negative")
        normalized.add(number)
    return sorted(normalized)


def _diagnostic_list(value: Any) -> list[str]:
    if _is_missing(value):
        return []
    if isinstance(value, str):
        values = value.split(";")
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = [value]
    return sorted({str(item).strip() for item in values if str(item).strip()})


def _optional_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def build_project_identity_map(
    identity_map: pd.DataFrame,
    *,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> pd.DataFrame:
    """Return the stable logical rows written by PR-0039."""

    if not isinstance(identity_map, pd.DataFrame):
        raise TypeError("identity_map must be a pandas DataFrame")
    if not _SAFE_RUN_ID.fullmatch(str(run_id)):
        raise ValueError("run_id must be one safe path segment")
    missing = sorted(_REQUIRED_COLUMNS.difference(identity_map.columns))
    if missing:
        raise ValueError(
            "identity_map is missing durable identity columns: "
            + ", ".join(missing)
        )

    source = identity_map.copy(deep=True)
    if "ap_model" not in source.columns:
        source["ap_model"] = pd.Series(
            pd.NA, index=source.index, dtype="string"
        )
    source["bssid"] = source["bssid"].astype("string").str.strip()
    if source["bssid"].isna().any() or source["bssid"].eq("").any():
        raise ValueError("project identity rows require a non-empty BSSID")
    duplicates = sorted(
        source.loc[source["bssid"].duplicated(keep=False), "bssid"]
        .astype(str)
        .unique()
    )
    if duplicates:
        raise ValueError(
            "project identity map requires one row per BSSID: "
            + ", ".join(duplicates)
        )

    rows: list[dict[str, Any]] = []
    for row in source.sort_values("bssid", kind="stable").itertuples(
        index=False
    ):
        values = row._asdict()
        radio_number = values["radio_number"]
        rows.append(
            {
                "run_id": str(run_id),
                "schema_id": str(schema_id),
                "metric_id": str(metric_id),
                "bssid": str(values["bssid"]),
                "ssid": _optional_text(values["ssid"]),
                "radio_group_id": _optional_text(values["radio_group_id"]),
                "radio_number": (
                    None if _is_missing(radio_number) else int(radio_number)
                ),
                "ap_id": _optional_text(values["ap_id"]),
                "ap_name": _optional_text(values["ap_name"]),
                "ap_model": _optional_text(values["ap_model"]),
                "classroom": _optional_text(values["classroom"]),
                "band": _optional_text(values["band"]),
                "observed_channels": _integer_list(values["channels"]),
                "observed_frequencies_mhz": _integer_list(
                    values["frequencies_mhz"]
                ),
                "ap_identity_method": _optional_text(
                    values["ap_identity_method"]
                ),
                "radio_grouping_method": _optional_text(
                    values["radio_grouping_method"]
                ),
                "result_grain": _optional_text(values["result_grain"]),
                "mapping_status": _optional_text(values["mapping_status"]),
                "mapping_diagnostics": _diagnostic_list(
                    values["mapping_diagnostics"]
                ),
            }
        )
    return pd.DataFrame(rows, columns=PROJECT_IDENTITY_ARROW_SCHEMA.names)


def write_project_identity_map(
    identity_map: pd.DataFrame,
    *,
    output_root: str | Path | None = None,
    run_directory: str | Path | None = None,
    run_id: str,
    schema_id: str = RESULT_SCHEMA_ID,
    metric_id: str = CORRECTED_METRIC_ID,
) -> Path:
    """Write ``{output_root}/{run_id}/project_identity_map.parquet`` safely.

    ``output_root`` is the configured project root below ``Output/runs``.
    Existing artifacts are not overwritten; run lifecycle remains the future
    corrected-runner's responsibility.
    """

    logical = build_project_identity_map(
        identity_map,
        run_id=run_id,
        schema_id=schema_id,
        metric_id=metric_id,
    )
    if (output_root is None) == (run_directory is None):
        raise ValueError("provide exactly one of output_root or run_directory")
    destination = (
        Path(run_directory)
        if run_directory is not None
        else Path(output_root) / run_id
    )
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / PROJECT_IDENTITY_FILENAME
    if target.exists():
        raise FileExistsError(f"identity artifact already exists: {target}")

    table = pa.Table.from_pylist(
        logical.to_dict(orient="records"),
        schema=PROJECT_IDENTITY_ARROW_SCHEMA,
    )
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, temporary)
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target
