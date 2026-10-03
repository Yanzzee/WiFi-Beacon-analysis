"""Aruba wrapper around the shared project radio resolver."""

from __future__ import annotations

import pandas as pd

from radio_resolver import (
    RadioResolutionCounts,
    RadioResolutionResult,
    resolve_project_radios,
)


ArubaRadioResolutionResult = RadioResolutionResult


def resolve_aruba_project_radios(
    observations: pd.DataFrame,
) -> ArubaRadioResolutionResult:
    """Resolve named Aruba AP radios using shared band/channel rules."""

    return resolve_project_radios(observations)
