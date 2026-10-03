"""Reviewed built-in vendor adapters for the shared beacon boundary."""

from aruba_beacon_adapter import ARUBA_BEACON_ADAPTER
from beacon_adapter import AdapterRegistry
from extreme_beacon_adapter import EXTREME_BEACON_ADAPTER


BEACON_ADAPTER_REGISTRY = AdapterRegistry(
    (
        ARUBA_BEACON_ADAPTER,
        EXTREME_BEACON_ADAPTER,
    )
)
