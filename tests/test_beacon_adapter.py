from __future__ import annotations

import pandas as pd
import pytest

from beacon_adapter import (
    AdapterDescriptor,
    AdapterRegistry,
    BeaconEvidenceAdapter,
)


def _adapter(identifier: str, vendor: str, claim_column: str):
    descriptor = AdapterDescriptor(
        identifier=identifier,
        version=1,
        vendor=vendor,
        capture_profiles=frozenset({"native_80211"}),
        capabilities=frozenset({"ap_name_evidence"}),
    )

    def claim_rows(records: pd.DataFrame) -> pd.Series:
        return records[claim_column].fillna(False)

    def normalize_rows(
        records: pd.DataFrame,
        *,
        source_capture: str | None = None,
        source_rank: int | None = None,
    ) -> pd.DataFrame:
        result = records.copy(deep=True)
        result["source_capture"] = source_capture
        result["source_rank"] = source_rank
        return result

    return BeaconEvidenceAdapter(descriptor, claim_rows, normalize_rows)


def test_registry_dispatches_per_record_and_preserves_input_order():
    registry = AdapterRegistry(
        [
            _adapter("beacon.adapter/a", "vendor-a", "claim_a"),
            _adapter("beacon.adapter/b", "vendor-b", "claim_b"),
        ]
    )
    records = pd.DataFrame(
        {
            "record": ["a-1", "unknown", "b-1", "a-2"],
            "claim_a": [True, False, False, True],
            "claim_b": [False, False, True, False],
        },
        index=[5, 5, 2, 9],
    )

    result = registry.dispatch(
        records,
        capture_profile="native_80211",
        source_capture="mixed.pcapng",
        source_rank=3,
    )

    assert result.observations["record"].tolist() == ["a-1", "b-1", "a-2"]
    assert result.observations["vendor_id"].tolist() == [
        "vendor-a",
        "vendor-b",
        "vendor-a",
    ]
    assert result.observations["source_capture"].tolist() == [
        "mixed.pcapng"
    ] * 3
    assert result.counts.input_rows == 4
    assert result.counts.dispatched_rows == 3
    assert result.counts.unknown_rows == 1
    assert result.counts.conflicting_rows == 0
    assert result.counts.rows_by_adapter == (
        ("beacon.adapter/a", 2),
        ("beacon.adapter/b", 1),
    )


def test_registry_does_not_use_registration_order_to_resolve_conflicts():
    adapter_a = _adapter("beacon.adapter/a", "vendor-a", "claim_a")
    adapter_b = _adapter("beacon.adapter/b", "vendor-b", "claim_b")
    records = pd.DataFrame(
        {
            "record": ["only-a", "conflict", "only-b"],
            "claim_a": [True, True, False],
            "claim_b": [False, True, True],
        }
    )

    first = AdapterRegistry([adapter_a, adapter_b]).dispatch(
        records, capture_profile="native_80211"
    )
    reversed_result = AdapterRegistry([adapter_b, adapter_a]).dispatch(
        records, capture_profile="native_80211"
    )

    assert first.observations["record"].tolist() == ["only-a", "only-b"]
    assert reversed_result.observations["record"].tolist() == [
        "only-a",
        "only-b",
    ]
    assert first.counts.conflicting_rows == 1
    assert reversed_result.counts.conflicting_rows == 1


def test_registry_requires_unique_adapter_identifiers():
    adapter = _adapter("beacon.adapter/repeated", "vendor-a", "claim_a")

    with pytest.raises(ValueError, match="identifiers must be unique"):
        AdapterRegistry([adapter, adapter])


def test_capture_profile_evidence_does_not_suppress_a_vendor_claim():
    adapter = _adapter("beacon.adapter/a", "vendor-a", "claim_a")

    result = AdapterRegistry([adapter]).dispatch(
        pd.DataFrame({"claim_a": [True]}),
        capture_profile="aruba_erm_type3",
    )

    assert result.observations["vendor_id"].tolist() == ["vendor-a"]
    assert result.counts.dispatched_rows == 1


def test_adapter_descriptor_requires_versioned_identity_and_profile():
    with pytest.raises(ValueError, match="version must be positive"):
        AdapterDescriptor(
            identifier="beacon.adapter/example",
            version=0,
            vendor="example",
            capture_profiles=frozenset({"native_80211"}),
            capabilities=frozenset(),
        )

    with pytest.raises(ValueError, match="capture profile"):
        AdapterDescriptor(
            identifier="beacon.adapter/example",
            version=1,
            vendor="example",
            capture_profiles=frozenset(),
            capabilities=frozenset(),
        )
