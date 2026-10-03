"""Shared configured-AP/classroom admission for vendor beacon evidence."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from beacon_normalization import normalized_text


@dataclass(frozen=True)
class ProjectScopeCounts:
    """Aggregate scope outcomes without retaining excluded beacon rows."""

    input_rows: int
    retained_rows: int
    missing_ap_name_rows: int
    outside_project_scope_rows: int
    other_classroom_rows: int


@dataclass(frozen=True)
class ProjectScopeResult:
    """In-scope records plus compact exclusion counts."""

    observations: pd.DataFrame
    counts: ProjectScopeCounts


def _configured_ap_mapping(configured_aps: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(configured_aps, pd.DataFrame):
        raise TypeError("configured_aps must be a pandas DataFrame")
    required = {"Hostname", "Building-Room"}
    missing = sorted(required.difference(configured_aps.columns))
    if missing:
        raise ValueError(
            "configured_aps is missing required columns: " + ", ".join(missing)
        )

    columns = ["Hostname", "Building-Room"]
    if "Model" in configured_aps.columns:
        columns.append("Model")
    mapping = configured_aps.loc[:, columns].copy()
    mapping["ap_name"] = normalized_text(mapping["Hostname"])
    mapping["classroom"] = normalized_text(mapping["Building-Room"])
    mapping["ap_model"] = (
        normalized_text(mapping["Model"])
        if "Model" in mapping.columns
        else pd.Series(pd.NA, index=mapping.index, dtype="string")
    )
    if mapping["ap_name"].isna().any():
        raise ValueError("configured AP hostnames must be non-empty")
    if mapping["classroom"].isna().any():
        raise ValueError("configured AP classrooms must be non-empty")

    mapping["_ap_name_key"] = mapping["ap_name"].str.casefold()
    duplicate_names = mapping.loc[
        mapping["_ap_name_key"].duplicated(keep=False),
        "ap_name",
    ].sort_values()
    if not duplicate_names.empty:
        duplicate_list = ", ".join(duplicate_names.astype(str).tolist())
        raise ValueError(
            "configured AP hostnames must be unique after normalization: "
            + duplicate_list
        )

    mapping["ap_id"] = mapping["ap_name"]
    return mapping.set_index("_ap_name_key")


def filter_configured_project_scope(
    records: pd.DataFrame,
    configured_aps: pd.DataFrame,
    *,
    observed_ap_names: pd.Series,
    classroom: str | None = None,
) -> ProjectScopeResult:
    """Retain configured AP names while counting discarded neighboring rows."""

    if not isinstance(records, pd.DataFrame):
        raise TypeError("records must be a pandas DataFrame")
    if not isinstance(observed_ap_names, pd.Series) or len(
        observed_ap_names
    ) != len(records):
        raise ValueError("observed_ap_names must contain one value per record")

    mapping = _configured_ap_mapping(configured_aps)
    observed_names = normalized_text(
        observed_ap_names.reset_index(drop=True)
    )
    observed_names.index = records.index
    observed_keys = observed_names.str.casefold()
    missing_name = observed_names.isna()
    configured = observed_keys.isin(mapping.index)

    mapped_classroom = observed_keys.map(mapping["classroom"])
    other_classroom = pd.Series(False, index=records.index)
    if classroom is not None:
        normalized_classroom = str(classroom).strip()
        if not normalized_classroom:
            raise ValueError("classroom must be non-empty when supplied")
        other_classroom = configured & mapped_classroom.ne(normalized_classroom)

    retained_mask = configured & ~other_classroom
    retained = records.loc[retained_mask].copy(deep=True)
    retained_keys = observed_keys.loc[retained_mask]
    retained["ap_name"] = retained_keys.map(mapping["ap_name"]).astype("string")
    retained["ap_id"] = retained_keys.map(mapping["ap_id"]).astype("string")
    retained["classroom"] = retained_keys.map(mapping["classroom"]).astype(
        "string"
    )
    retained["ap_model"] = retained_keys.map(mapping["ap_model"]).astype(
        "string"
    )
    retained["ap_identity_method"] = pd.Series(
        "vendor_ap_name", index=retained.index, dtype="string"
    )
    retained["mapping_status"] = pd.Series(
        "resolved", index=retained.index, dtype="string"
    )

    counts = ProjectScopeCounts(
        input_rows=int(len(records)),
        retained_rows=int(retained_mask.sum()),
        missing_ap_name_rows=int(missing_name.sum()),
        outside_project_scope_rows=int((~missing_name & ~configured).sum()),
        other_classroom_rows=int(other_classroom.sum()),
    )
    return ProjectScopeResult(observations=retained, counts=counts)
