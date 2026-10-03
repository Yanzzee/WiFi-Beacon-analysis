"""Extreme Networks native-802.11 beacon adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from beacon_adapter import AdapterDescriptor, BeaconEvidenceAdapter
from beacon_normalization import (
    column_or_index,
    normalize_beacon_evidence,
    normalized_text,
)
from corrected_beacon_selection import (
    BeaconSelectionResult,
    DEFAULT_BEACON_INTERVAL_TU,
    select_bssid_session_observations,
)
from project_scope import (
    ProjectScopeCounts,
    ProjectScopeResult,
    filter_configured_project_scope,
)
from radio_resolver import (
    RadioResolutionCounts,
    resolve_project_radios,
)


@dataclass(frozen=True)
class ScopedExtremeSelectionResult:
    """Configured Extreme scope, radio identity, and corrected selection."""

    scope_counts: ProjectScopeCounts
    radio_resolution_counts: RadioResolutionCounts
    radio_identity_map: pd.DataFrame
    selection: BeaconSelectionResult


EXTREME_NATIVE_ADAPTER = AdapterDescriptor(
    identifier="beacon.adapter/extreme-native",
    version=3,
    vendor="extreme",
    capture_profiles=frozenset({"native_80211"}),
    capabilities=frozenset(
        {
            "frame_time_fallback",
            "tsf",
            "beacon_interval",
            "bssid",
            "ap_name_evidence",
            "ssid",
            "qbss",
            "frequency",
            "band_classification",
            "channel_classification",
            "radio_identity",
            "capture_lineage",
        }
    ),
    extraction_fields=("wlan.vs.extreme.ap_name",),
)


def filter_configured_extreme_scope(
    records: pd.DataFrame,
    configured_aps: pd.DataFrame,
    *,
    classroom: str | None = None,
) -> ProjectScopeResult:
    """Retain only configured Extreme AP hostnames before normalization."""

    return filter_configured_project_scope(
        records,
        configured_aps,
        observed_ap_names=column_or_index(
            records, "wlan.vs.extreme.ap_name", "ap_name"
        ),
        classroom=classroom,
    )


def adapt_extreme_beacons(
    records: pd.DataFrame,
    *,
    source_capture: str | None = None,
    source_rank: int | None = None,
) -> pd.DataFrame:
    """Normalize Extreme native beacons into shared selection columns."""

    return normalize_beacon_evidence(
        records,
        ap_name_fields=("wlan.vs.extreme.ap_name",),
        source_capture=source_capture,
        source_rank=source_rank,
    )


def _claim_extreme_rows(records: pd.DataFrame) -> pd.Series:
    return normalized_text(
        column_or_index(records, "wlan.vs.extreme.ap_name")
    ).notna()


EXTREME_BEACON_ADAPTER = BeaconEvidenceAdapter(
    descriptor=EXTREME_NATIVE_ADAPTER,
    claim_rows=_claim_extreme_rows,
    normalize_rows=adapt_extreme_beacons,
)


def select_extreme_class_observations(
    records: pd.DataFrame,
    *,
    class_session_id: str,
    class_start: Any,
    class_end: Any,
    bin_width_us: int,
    source_capture: str | None = None,
    source_rank: int | None = None,
    default_beacon_interval_tu: int = DEFAULT_BEACON_INTERVAL_TU,
) -> BeaconSelectionResult:
    """Normalize and select Extreme observations at BSSID grain."""

    observations = EXTREME_BEACON_ADAPTER.normalize_rows(
        records,
        source_capture=source_capture,
        source_rank=source_rank,
    )
    return select_bssid_session_observations(
        observations,
        class_session_id=class_session_id,
        class_start=class_start,
        class_end=class_end,
        bin_width_us=bin_width_us,
        default_beacon_interval_tu=default_beacon_interval_tu,
    )


def select_configured_extreme_class_observations(
    records: pd.DataFrame,
    configured_aps: pd.DataFrame,
    *,
    classroom: str,
    class_session_id: str,
    class_start: Any,
    class_end: Any,
    bin_width_us: int,
    source_capture: str | None = None,
    source_rank: int | None = None,
    default_beacon_interval_tu: int = DEFAULT_BEACON_INTERVAL_TU,
) -> ScopedExtremeSelectionResult:
    """Resolve configured Extreme radios, then select one classroom/class."""

    scope = filter_configured_extreme_scope(records, configured_aps)
    observations = EXTREME_BEACON_ADAPTER.normalize_rows(
        scope.observations,
        source_capture=source_capture,
        source_rank=source_rank,
    )
    radio_resolution = resolve_project_radios(observations)
    classroom_name = str(classroom).strip()
    if not classroom_name:
        raise ValueError("classroom must be non-empty when supplied")
    classroom_observations = radio_resolution.observations.loc[
        radio_resolution.observations["classroom"].eq(classroom_name)
    ].copy()
    selection = select_bssid_session_observations(
        classroom_observations,
        class_session_id=class_session_id,
        class_start=class_start,
        class_end=class_end,
        bin_width_us=bin_width_us,
        default_beacon_interval_tu=default_beacon_interval_tu,
    )
    scope_counts = ProjectScopeCounts(
        input_rows=scope.counts.input_rows,
        retained_rows=int(len(classroom_observations)),
        missing_ap_name_rows=scope.counts.missing_ap_name_rows,
        outside_project_scope_rows=scope.counts.outside_project_scope_rows,
        other_classroom_rows=int(
            len(scope.observations) - len(classroom_observations)
        ),
    )
    return ScopedExtremeSelectionResult(
        scope_counts=scope_counts,
        radio_resolution_counts=radio_resolution.counts,
        radio_identity_map=radio_resolution.identity_map,
        selection=selection,
    )
