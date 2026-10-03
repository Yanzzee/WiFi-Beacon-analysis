"""Minimal in-memory Aruba ERM adapter for corrected beacon selection.

The adapter translates the accepted Aruba/TShark fields into the small
vendor-neutral column set consumed by ``corrected_beacon_selection``. It does
not detect vendors, write an intermediate dataset, or change the legacy
analysis entry points. The configured path adds hostname-based AP/classroom
identity and resolves current-Aruba radio groups across the project.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from beacon_adapter import (
    AdapterDescriptor,
    BeaconEvidenceAdapter,
)
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
from aruba_radio_resolver import (
    ArubaRadioResolutionResult,
    RadioResolutionCounts,
    resolve_aruba_project_radios,
)
from project_scope import (
    ProjectScopeCounts,
    ProjectScopeResult,
    filter_configured_project_scope,
)


@dataclass(frozen=True)
class ScopedArubaSelectionResult:
    """Project/classroom scope outcome and corrected beacon selection."""

    scope_counts: ProjectScopeCounts
    radio_resolution_counts: RadioResolutionCounts
    radio_identity_map: pd.DataFrame
    selection: BeaconSelectionResult


ARUBA_ERM_ADAPTER = AdapterDescriptor(
    identifier="beacon.adapter/aruba-erm",
    version=3,
    vendor="aruba",
    capture_profiles=frozenset(
        {
            "aruba_erm_type0",
            "aruba_erm_type3",
            "aruba_erm_type6",
        }
    ),
    capabilities=frozenset(
        {
            "preferred_observation_time",
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
    extraction_fields=("wlan.vs.aruba.ap_name",),
)


def filter_configured_aruba_scope(
    records: pd.DataFrame,
    configured_aps: pd.DataFrame,
    *,
    classroom: str | None = None,
) -> ProjectScopeResult:
    """Keep configured Aruba APs before full normalization and selection.

    Missing hostnames and hostnames absent from the configured project mapping
    are counted and discarded. If ``classroom`` is supplied, configured APs in
    other classrooms are also counted and discarded for that class operation.
    """

    return filter_configured_project_scope(
        records,
        configured_aps,
        observed_ap_names=column_or_index(
            records, "wlan.vs.aruba.ap_name", "ap_name"
        ),
        classroom=classroom,
    )


def adapt_aruba_beacons(
    records: pd.DataFrame,
    *,
    source_capture: str | None = None,
    source_rank: int | None = None,
) -> pd.DataFrame:
    """Return Aruba records with canonical in-memory selection columns.

    ``source_capture`` and ``source_rank`` fill missing lineage for direct
    adapter use. Converted captures already carry both values. Lower source
    ranks have higher precedence when duplicate views otherwise tie.
    """

    return normalize_beacon_evidence(
        records,
        ap_name_fields=("wlan.vs.aruba.ap_name",),
        source_capture=source_capture,
        source_rank=source_rank,
    )


def _claim_aruba_rows(records: pd.DataFrame) -> pd.Series:
    """Recognize Aruba vendor evidence without treating ERM as the vendor."""

    return normalized_text(
        column_or_index(records, "wlan.vs.aruba.ap_name")
    ).notna()


ARUBA_BEACON_ADAPTER = BeaconEvidenceAdapter(
    descriptor=ARUBA_ERM_ADAPTER,
    claim_rows=_claim_aruba_rows,
    normalize_rows=adapt_aruba_beacons,
)


def select_aruba_class_observations(
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
    """Adapt Aruba records in memory and run corrected per-BSSID selection."""

    observations = ARUBA_BEACON_ADAPTER.normalize_rows(
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


def select_configured_aruba_class_observations(
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
) -> ScopedArubaSelectionResult:
    """Resolve the configured project, then select one classroom/class."""

    project_scope = filter_configured_aruba_scope(
        records,
        configured_aps,
    )
    observations = ARUBA_BEACON_ADAPTER.normalize_rows(
        project_scope.observations,
        source_capture=source_capture,
        source_rank=source_rank,
    )
    radio_resolution: ArubaRadioResolutionResult = (
        resolve_aruba_project_radios(observations)
    )
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
        input_rows=project_scope.counts.input_rows,
        retained_rows=int(len(classroom_observations)),
        missing_ap_name_rows=project_scope.counts.missing_ap_name_rows,
        outside_project_scope_rows=(
            project_scope.counts.outside_project_scope_rows
        ),
        other_classroom_rows=int(
            len(project_scope.observations) - len(classroom_observations)
        ),
    )
    return ScopedArubaSelectionResult(
        scope_counts=scope_counts,
        radio_resolution_counts=radio_resolution.counts,
        radio_identity_map=radio_resolution.identity_map,
        selection=selection,
    )
