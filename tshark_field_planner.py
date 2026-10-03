"""Deterministic one-pass TShark field planning.

Capture profiles own encapsulation, timestamp, and common 802.11 field
requirements. Vendor adapters independently contribute the evidence fields
they can recognize. The planner combines both sets once before the full
beacon extraction; it does not inspect packets or choose a primary vendor.
"""

from __future__ import annotations

from dataclasses import dataclass

from beacon_adapter import AdapterRegistry


BEACON_DISPLAY_FILTER = "wlan.fc.type_subtype == 8"


def _nonempty_tokens(values: tuple[str, ...], *, label: str) -> None:
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} values must be non-empty strings")


@dataclass(frozen=True)
class CaptureExtractionRequirements:
    """Fields and decode arguments supplied by one capture profile."""

    capture_profile: str
    fields: tuple[str, ...]
    decode_args: tuple[str, ...] = ()
    display_filter: str = BEACON_DISPLAY_FILTER

    def __post_init__(self) -> None:
        if (
            not isinstance(self.capture_profile, str)
            or not self.capture_profile.strip()
        ):
            raise ValueError("capture_profile must be non-empty")
        if not self.fields:
            raise ValueError("capture profile must require at least one field")
        _nonempty_tokens(self.fields, label="field")
        _nonempty_tokens(self.decode_args, label="decode argument")
        if not isinstance(self.display_filter, str) or not self.display_filter.strip():
            raise ValueError("display_filter must be non-empty")


@dataclass(frozen=True)
class TsharkExtractionPlan:
    """Complete deterministic plan for one full beacon extraction."""

    capture_profile: str
    fields: tuple[str, ...]
    decode_args: tuple[str, ...]
    display_filter: str
    adapter_versions: tuple[tuple[str, int], ...]


def _ordered_union(*groups: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for group in groups:
        for value in group:
            if value not in seen:
                seen.add(value)
                result.append(value)
    return tuple(result)


def _display_filter_string(value: str) -> str:
    """Quote one configured hostname for a Wireshark display filter."""

    if any(ord(character) < 32 for character in value):
        raise ValueError("configured AP hostnames cannot contain control characters")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def configured_ap_display_filter(
    registry: AdapterRegistry,
    configured_ap_names: tuple[str, ...],
) -> str:
    """Return a beacon filter limited to reviewed vendor AP-name fields.

    The optimization is based only on configured AP names. It does not infer
    BSSIDs or change adapter and selection semantics for emitted rows.
    """

    if not isinstance(registry, AdapterRegistry):
        raise TypeError("registry must be AdapterRegistry")
    normalized = sorted(
        {
            str(value).strip().lower()
            for value in configured_ap_names
            if str(value).strip()
        }
    )
    if not normalized:
        raise ValueError("configured_ap_names must contain at least one hostname")
    fields = _ordered_union(
        *tuple(
            descriptor.extraction_fields
            for descriptor in registry.descriptors
            if descriptor.extraction_fields
        )
    )
    if not fields:
        raise ValueError("no adapter supplies an AP-name extraction field")
    matches = [
        f"lower({field}) == {_display_filter_string(hostname)}"
        for field in fields
        for hostname in normalized
    ]
    return f"{BEACON_DISPLAY_FILTER} && ({' || '.join(matches)})"


def plan_tshark_extraction(
    requirements: CaptureExtractionRequirements,
    registry: AdapterRegistry,
) -> TsharkExtractionPlan:
    """Combine one capture profile with all reviewed adapter fields.

    Adapter extraction fields are deliberately not filtered by the adapter's
    currently tested normalization profiles. Requesting every reviewed vendor
    AP-name field in the same pass supports mixed captures and keeps capture
    transport separate from beacon vendor identity.
    """

    if not isinstance(requirements, CaptureExtractionRequirements):
        raise TypeError("requirements must be CaptureExtractionRequirements")
    if not isinstance(registry, AdapterRegistry):
        raise TypeError("registry must be AdapterRegistry")

    descriptors = registry.descriptors
    adapter_fields = tuple(
        field
        for descriptor in descriptors
        for field in descriptor.extraction_fields
    )
    fields = _ordered_union(requirements.fields, adapter_fields)
    return TsharkExtractionPlan(
        capture_profile=requirements.capture_profile,
        fields=fields,
        decode_args=requirements.decode_args,
        display_filter=requirements.display_filter,
        adapter_versions=tuple(
            (descriptor.identifier, descriptor.version)
            for descriptor in descriptors
            if descriptor.extraction_fields
        ),
    )
