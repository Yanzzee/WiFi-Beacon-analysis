"""Vendor-neutral project-wide BSSID-to-radio grouping."""

from __future__ import annotations

from dataclasses import dataclass
import re

import pandas as pd


_MAC_PATTERN = re.compile(r"^(?:[0-9a-f]{2}:){5}[0-9a-f]{2}$")

RADIO_IDENTITY_EVIDENCE_COLUMNS = (
    "bssid",
    "ap_id",
    "ap_name",
    "ap_model",
    "classroom",
    "ap_identity_method",
    "ssid",
    "band",
    "channel",
    "frequency_mhz",
    "band_channel_status",
)

RADIO_IDENTITY_ATTACHMENT_COLUMNS = (
    "bssid",
    "radio_group_id",
    "radio_number",
    "radio_grouping_method",
    "result_grain",
    "mapping_status",
    "mapping_diagnostics",
)


@dataclass(frozen=True)
class RadioResolutionCounts:
    """Compact result counts for project radio resolution."""

    input_rows: int
    input_bssids: int
    resolved_rows: int
    resolved_bssids: int
    fallback_rows: int
    fallback_bssids: int
    conflict_bssids: int


@dataclass(frozen=True)
class RadioResolutionResult:
    """Resolved observations, one identity row per BSSID, and counts."""

    observations: pd.DataFrame
    identity_map: pd.DataFrame
    counts: RadioResolutionCounts


def _joined_integers(values: set[int]) -> str:
    return ",".join(str(value) for value in sorted(values))


def _connected_channel_components(members: pd.DataFrame) -> list[list[str]]:
    """Group BSSIDs whose channel histories overlap within one AP/band."""

    bssids = sorted(members["bssid"].astype(str))
    channels = {
        str(row.bssid): set(row.channel_set)
        for row in members.itertuples(index=False)
    }
    parent = {bssid: bssid for bssid in bssids}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for position, left in enumerate(bssids):
        for right in bssids[position + 1 :]:
            if channels[left].intersection(channels[right]):
                union(left, right)

    components: dict[str, list[str]] = {}
    for bssid in bssids:
        components.setdefault(find(bssid), []).append(bssid)
    return sorted(components.values(), key=lambda component: component[0])


def attach_project_radio_identity(
    observations: pd.DataFrame,
    identity_map: pd.DataFrame,
) -> pd.DataFrame:
    """Attach one resolved project identity row to each observation BSSID."""

    if not isinstance(observations, pd.DataFrame):
        raise TypeError("observations must be a pandas DataFrame")
    if not isinstance(identity_map, pd.DataFrame):
        raise TypeError("identity_map must be a pandas DataFrame")
    missing_observations = sorted({"bssid"}.difference(observations.columns))
    missing_identity = sorted(
        set(RADIO_IDENTITY_ATTACHMENT_COLUMNS).difference(identity_map.columns)
    )
    if missing_observations:
        raise ValueError("observations are missing bssid")
    if missing_identity:
        raise ValueError(
            "identity map is missing attachment columns: "
            + ", ".join(missing_identity)
        )

    result = observations.copy(deep=True)
    result["bssid"] = result["bssid"].astype("string").str.strip().str.lower()
    attachment = identity_map.loc[
        :, list(RADIO_IDENTITY_ATTACHMENT_COLUMNS)
    ]
    result = result.drop(
        columns=[
            column
            for column in attachment.columns
            if column != "bssid" and column in result.columns
        ]
    ).merge(attachment, on="bssid", how="left", sort=False)
    result["radio_grouping_method"] = result[
        "radio_grouping_method"
    ].fillna("per_bssid_fallback")
    result["result_grain"] = result["result_grain"].fillna("bssid")
    result["mapping_status"] = result["mapping_status"].fillna("unresolved")
    result["mapping_diagnostics"] = result["mapping_diagnostics"].fillna(
        "bssid_missing"
    )
    return result


def resolve_project_radios(
    observations: pd.DataFrame,
) -> RadioResolutionResult:
    """Resolve stable radio groups from configured vendor observations.

    AP identity must already be attached. Band plus observed channel is the
    grouping evidence within that AP. MAC allocation patterns are deliberately
    not used when a usable AP name is available.
    """

    if not isinstance(observations, pd.DataFrame):
        raise TypeError("observations must be a pandas DataFrame")
    required = set(RADIO_IDENTITY_EVIDENCE_COLUMNS).difference(
        {"ap_identity_method", "ap_model"}
    )
    missing = sorted(required.difference(observations.columns))
    if missing:
        raise ValueError(
            "observations are missing radio-resolution columns: "
            + ", ".join(missing)
        )

    result = observations.copy(deep=True)
    result["bssid"] = result["bssid"].astype("string").str.strip().str.lower()
    if "ap_identity_method" not in result.columns:
        result["ap_identity_method"] = pd.Series(
            pd.NA, index=result.index, dtype="string"
        )
    if "ap_model" not in result.columns:
        result["ap_model"] = pd.Series(
            pd.NA, index=result.index, dtype="string"
        )
    evidence = result.loc[
        :, list(RADIO_IDENTITY_EVIDENCE_COLUMNS)
    ].drop_duplicates()

    all_bssids = sorted(
        value
        for value in evidence["bssid"].dropna().astype(str).unique()
        if value
    )
    summaries: list[dict[str, object]] = []
    for bssid in all_bssids:
        rows = evidence.loc[evidence["bssid"].eq(bssid)]
        ap_ids = set(rows["ap_id"].dropna().astype(str))
        ap_names = set(rows["ap_name"].dropna().astype(str))
        ap_models = set(rows["ap_model"].dropna().astype(str))
        classrooms = set(rows["classroom"].dropna().astype(str))
        ap_identity_methods = set(
            rows["ap_identity_method"].dropna().astype(str)
        )
        ssids = set(rows["ssid"].dropna().astype(str))
        bands = set(rows["band"].dropna().astype(str))
        channels = set(int(value) for value in rows["channel"].dropna())
        frequencies = set(
            int(value) for value in rows["frequency_mhz"].dropna()
        )
        statuses = set(rows["band_channel_status"].dropna().astype(str))

        diagnostics: list[str] = []
        if _MAC_PATTERN.fullmatch(bssid) is None:
            diagnostics.append("bssid_invalid")
        if len(ap_ids) != 1:
            diagnostics.append("project_bssid_ap_conflict")
        if len(ap_names) != 1:
            diagnostics.append("project_bssid_ap_name_conflict")
        if len(classrooms) != 1:
            diagnostics.append("project_bssid_classroom_conflict")
        if len(ap_identity_methods) > 1:
            diagnostics.append("project_bssid_ap_identity_method_conflict")
        if len(ssids) > 1:
            diagnostics.append("project_bssid_ssid_conflict")
        if "conflict" in statuses:
            diagnostics.append("band_channel_evidence_conflict")
        if len(bands) != 1:
            diagnostics.append(
                "band_missing" if not bands else "project_bssid_band_conflict"
            )
        if not channels:
            diagnostics.append("channel_missing")

        summaries.append(
            {
                "bssid": bssid,
                "ap_id": next(iter(ap_ids)) if len(ap_ids) == 1 else pd.NA,
                "ap_name": (
                    next(iter(ap_names)) if len(ap_names) == 1 else pd.NA
                ),
                "ap_model": (
                    next(iter(ap_models)) if len(ap_models) == 1 else pd.NA
                ),
                "classroom": (
                    next(iter(classrooms)) if len(classrooms) == 1 else pd.NA
                ),
                "ap_identity_method": (
                    next(iter(ap_identity_methods))
                    if len(ap_identity_methods) == 1
                    else pd.NA
                ),
                "ssid": next(iter(ssids)) if len(ssids) == 1 else pd.NA,
                "band": next(iter(bands)) if len(bands) == 1 else pd.NA,
                "channel_set": channels,
                "channels": _joined_integers(channels),
                "frequencies_mhz": _joined_integers(frequencies),
                "mapping_diagnostics": diagnostics,
            }
        )

    summary = pd.DataFrame(summaries)
    if summary.empty:
        empty_columns = {
            "bssid": "string",
            "ap_id": "string",
            "ap_name": "string",
            "ap_model": "string",
            "classroom": "string",
            "ap_identity_method": "string",
            "ssid": "string",
            "band": "string",
            "channels": "string",
            "frequencies_mhz": "string",
            "radio_group_id": "string",
            "radio_number": "Int64",
            "radio_grouping_method": "string",
            "result_grain": "string",
            "mapping_status": "string",
            "mapping_diagnostics": "string",
        }
        for column, dtype in empty_columns.items():
            summary[column] = pd.Series(dtype=dtype)
        for column in [
            "radio_group_id",
            "radio_number",
            "radio_grouping_method",
            "result_grain",
            "mapping_status",
            "mapping_diagnostics",
        ]:
            result[column] = pd.Series(
                index=result.index, dtype=empty_columns[column]
            )
        counts = RadioResolutionCounts(
            input_rows=len(result),
            input_bssids=0,
            resolved_rows=0,
            resolved_bssids=0,
            fallback_rows=len(result),
            fallback_bssids=0,
            conflict_bssids=0,
        )
        return RadioResolutionResult(result, summary, counts)

    eligible = summary["mapping_diagnostics"].map(len).eq(0)
    summary["radio_group_id"] = pd.Series(
        pd.NA, index=summary.index, dtype="string"
    )
    summary["radio_number"] = pd.Series(
        pd.NA, index=summary.index, dtype="Int64"
    )
    summary["radio_grouping_method"] = pd.Series(
        "per_bssid_fallback", index=summary.index, dtype="string"
    )
    summary["result_grain"] = pd.Series(
        "bssid", index=summary.index, dtype="string"
    )
    summary["mapping_status"] = pd.Series(
        "unresolved", index=summary.index, dtype="string"
    )
    initial_conflict = summary["mapping_diagnostics"].map(
        lambda values: any("conflict" in value for value in values)
    )
    summary.loc[initial_conflict, "mapping_status"] = "conflict"

    for ap_id in sorted(summary.loc[eligible, "ap_id"].astype(str).unique()):
        ap_rows = summary.loc[eligible & summary["ap_id"].eq(ap_id)]
        radio_components: list[list[str]] = []
        for band in sorted(ap_rows["band"].astype(str).unique()):
            band_rows = ap_rows.loc[ap_rows["band"].eq(band)]
            radio_components.extend(_connected_channel_components(band_rows))

        radio_components.sort(key=lambda component: component[0])
        for radio_number, component in enumerate(radio_components, start=1):
            indexes = summary.index[summary["bssid"].isin(component)]
            component_rows = summary.loc[indexes]
            duplicate_ssids = component_rows["ssid"].dropna().duplicated(
                keep=False
            )
            if duplicate_ssids.any():
                for index in indexes:
                    summary.at[index, "mapping_diagnostics"].append(
                        "radio_group_duplicate_ssid"
                    )
                summary.loc[indexes, "mapping_status"] = "conflict"
                continue

            if len(component) == 1:
                method = "band_channel_single_bssid"
            else:
                method = "band_channel"

            summary.loc[indexes, "radio_group_id"] = (
                f"{ap_id}/radio/{radio_number}"
            )
            summary.loc[indexes, "radio_number"] = radio_number
            summary.loc[indexes, "radio_grouping_method"] = method
            summary.loc[indexes, "result_grain"] = "ap_radio_group"
            summary.loc[indexes, "mapping_status"] = "resolved"

    summary["mapping_diagnostics"] = summary["mapping_diagnostics"].map(
        lambda values: ";".join(sorted(set(values)))
    ).astype("string")
    summary = summary.drop(columns=["channel_set"])
    for column in [
        "bssid",
        "ap_id",
        "ap_name",
        "ap_model",
        "classroom",
        "ap_identity_method",
        "ssid",
        "band",
        "channels",
        "frequencies_mhz",
    ]:
        summary[column] = summary[column].astype("string")

    result = attach_project_radio_identity(result, summary)

    resolved_bssids = int(summary["radio_group_id"].notna().sum())
    resolved_rows = int(result["radio_group_id"].notna().sum())
    conflict_bssids = int(summary["mapping_status"].eq("conflict").sum())
    counts = RadioResolutionCounts(
        input_rows=int(len(result)),
        input_bssids=int(len(summary)),
        resolved_rows=resolved_rows,
        resolved_bssids=resolved_bssids,
        fallback_rows=int(len(result) - resolved_rows),
        fallback_bssids=int(len(summary) - resolved_bssids),
        conflict_bssids=conflict_bssids,
    )
    return RadioResolutionResult(result, summary, counts)
