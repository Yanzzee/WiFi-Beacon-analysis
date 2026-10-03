"""Shared vendor-evidence adapter contract and deterministic dispatcher.

Capture-profile detection is intentionally outside this module.  A capture
file has one selected decoding profile, while individual beacon records may
come from different network vendors.  Adapters therefore claim and normalize
records, not whole capture files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

import pandas as pd


@dataclass(frozen=True)
class AdapterDescriptor:
    """Versioned declaration of one vendor-evidence adapter.

    ``capture_profiles`` records profiles backed by representative fixtures;
    it is support evidence, not a dispatch whitelist. Vendor identity is
    claimed from each decoded beacon independently of its capture transport.
    """

    identifier: str
    version: int
    vendor: str
    capture_profiles: frozenset[str]
    capabilities: frozenset[str]
    extraction_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.identifier, str) or not self.identifier.strip():
            raise ValueError("adapter identifier must be non-empty")
        if (
            not isinstance(self.version, int)
            or isinstance(self.version, bool)
            or self.version < 1
        ):
            raise ValueError("adapter version must be positive")
        if not isinstance(self.vendor, str) or not self.vendor.strip():
            raise ValueError("adapter vendor must be non-empty")
        if not self.capture_profiles:
            raise ValueError("adapter must name at least one capture profile")
        if len(self.extraction_fields) != len(set(self.extraction_fields)):
            raise ValueError("adapter extraction fields must be unique")
        if any(
            not isinstance(field, str) or not field.strip()
            for field in self.extraction_fields
        ):
            raise ValueError("adapter extraction fields must be non-empty strings")


ClaimRows = Callable[[pd.DataFrame], pd.Series]
NormalizeRows = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class BeaconEvidenceAdapter:
    """Small internal adapter: recognize rows, then normalize their evidence."""

    descriptor: AdapterDescriptor
    claim_rows: ClaimRows
    normalize_rows: NormalizeRows


@dataclass(frozen=True)
class AdapterDispatchCounts:
    """Compact per-run dispatch outcomes; unknown rows are not retained."""

    input_rows: int
    dispatched_rows: int
    unknown_rows: int
    conflicting_rows: int
    rows_by_adapter: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class AdapterDispatchResult:
    """Normalized unambiguous rows and compact exclusion counts."""

    observations: pd.DataFrame
    counts: AdapterDispatchCounts


class AdapterRegistry:
    """Static ordered registry used by the in-process normalization stage.

    Ordering is retained only for deterministic output.  It is never used to
    resolve multiple claims: a record claimed by more than one adapter is
    excluded as conflicting rather than assigned to the first adapter.
    """

    def __init__(self, adapters: Iterable[BeaconEvidenceAdapter]) -> None:
        self._adapters = tuple(adapters)
        identifiers = [item.descriptor.identifier for item in self._adapters]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("adapter identifiers must be unique")

    @property
    def adapters(self) -> tuple[BeaconEvidenceAdapter, ...]:
        return self._adapters

    @property
    def descriptors(self) -> tuple[AdapterDescriptor, ...]:
        return tuple(item.descriptor for item in self._adapters)

    def dispatch(
        self,
        records: pd.DataFrame,
        *,
        capture_profile: str,
        source_capture: str | None = None,
        source_rank: int | None = None,
    ) -> AdapterDispatchResult:
        """Normalize records claimed by exactly one registered adapter."""

        if not isinstance(records, pd.DataFrame):
            raise TypeError("records must be a pandas DataFrame")
        if not self._adapters:
            raise ValueError("at least one adapter must be registered")
        if not isinstance(capture_profile, str) or not capture_profile.strip():
            raise ValueError("capture_profile must be non-empty")

        claim_columns: list[pd.Series] = []
        for adapter in self._adapters:
            claimed = adapter.claim_rows(records)
            if not isinstance(claimed, pd.Series) or len(claimed) != len(records):
                raise ValueError(
                    f"{adapter.descriptor.identifier} claim_rows must return "
                    "one pandas Series value per input row"
                )
            claim_columns.append(
                claimed.reset_index(drop=True).fillna(False).astype(bool)
            )

        claims = pd.concat(claim_columns, axis=1, ignore_index=True)
        claim_count = claims.sum(axis=1)
        normalized_parts: list[pd.DataFrame] = []
        rows_by_adapter: list[tuple[str, int]] = []

        for position, adapter in enumerate(self._adapters):
            selected_positions = claims.index[
                claims.iloc[:, position] & claim_count.eq(1)
            ].tolist()
            rows_by_adapter.append(
                (adapter.descriptor.identifier, len(selected_positions))
            )
            if not selected_positions:
                continue

            selected = records.iloc[selected_positions].copy(deep=True)
            selected["_adapter_input_position"] = pd.array(
                selected_positions, dtype="Int64"
            )
            normalized = adapter.normalize_rows(
                selected,
                source_capture=source_capture,
                source_rank=source_rank,
            )
            if not isinstance(normalized, pd.DataFrame) or len(normalized) != len(
                selected
            ):
                raise ValueError(
                    f"{adapter.descriptor.identifier} normalize_rows must "
                    "return one row per claimed input row"
                )
            normalized = normalized.copy(deep=False)
            normalized["vendor_id"] = adapter.descriptor.vendor
            normalized["adapter_id"] = adapter.descriptor.identifier
            normalized["adapter_version"] = adapter.descriptor.version
            normalized_parts.append(normalized)

        if normalized_parts:
            observations = pd.concat(normalized_parts, axis=0, sort=False)
            observations = observations.sort_values(
                "_adapter_input_position", kind="stable"
            ).drop(columns="_adapter_input_position")
        else:
            observations = records.iloc[0:0].copy(deep=True)

        counts = AdapterDispatchCounts(
            input_rows=len(records),
            dispatched_rows=int(claim_count.eq(1).sum()),
            unknown_rows=int(claim_count.eq(0).sum()),
            conflicting_rows=int(claim_count.gt(1).sum()),
            rows_by_adapter=tuple(rows_by_adapter),
        )
        return AdapterDispatchResult(observations=observations, counts=counts)
