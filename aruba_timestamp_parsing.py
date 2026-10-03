"""Exact Aruba ERM timestamp parsing for the accepted capture contract.

This module is deliberately source-specific. It does not read configuration,
environment variables, machine timezone state, or wall-clock time. It never
uses general timestamp-string inference and never substitutes frame time for
the authoritative Aruba timestamp.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import re

import pandas as pd

from timestamp_normalization import (
    DiagnosticStatus,
    NormalizationResult,
    SourceParserContract,
    TimestampInput,
    normalize_timestamps,
)


ARUBA_TIMESTAMP_CONTRACT_ID = (
    "beacon.source-parser/aruba-erm-time/raw-seconds-microseconds"
)
ARUBA_TIMESTAMP_CONTRACT_VERSION = 1
ARUBA_TIMESTAMP_PARSER_METHOD_NAME = (
    "beacon.aruba-erm-time/raw-big-endian-seconds-microseconds"
)
ARUBA_TIMESTAMP_RAW_TOKEN_BYTE_COUNT = 8
ARUBA_TIMESTAMP_RAW_TOKEN_GRAMMAR = r"^(?:[0-9a-f]{2}:){7}[0-9a-f]{2}$"
ARUBA_TIMESTAMP_DISPLAY_GRAMMAR = (
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}[+-]\d{4}$"
)

_RAW_TOKEN_PATTERN = re.compile(ARUBA_TIMESTAMP_RAW_TOKEN_GRAMMAR)
_DISPLAY_TOKEN_PATTERN = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"\.(?P<fraction>\d{9})(?P<offset_sign>[+-])"
    r"(?P<offset_hour>\d{2})(?P<offset_minute>\d{2})$"
)
_FRAME_EPOCH_PATTERN = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
_MICROSECONDS_PER_SECOND = Decimal(1_000_000)
_NANOSECONDS_PER_MICROSECOND = Decimal(1_000)


class ArubaTimestampDiagnosticStatus(str, Enum):
    """Stable Aruba parser and optional-comparison diagnostic states."""

    VALID = "aruba_timestamp_valid"
    ABSENT_SOURCE_VALUE = "aruba_timestamp_absent"
    MALFORMED_RAW_TOKEN = "aruba_timestamp_malformed"
    OUT_OF_RANGE_SECONDS = "aruba_timestamp_seconds_out_of_range"
    OUT_OF_RANGE_MICROSECONDS = "aruba_timestamp_fraction_out_of_range"
    UNSUPPORTED_PARSER_CONTRACT = "unsupported_aruba_timestamp_contract"
    DISPLAY_TIMESTAMP_MATCH = "aruba_timestamp_render_match"
    DISPLAY_TIMESTAMP_MALFORMED = "aruba_timestamp_render_malformed"
    DISPLAY_TIMESTAMP_MISMATCH = "aruba_timestamp_render_conflict"
    FRAME_EPOCH_MATCH = "aruba_frame_epoch_match"
    FRAME_EPOCH_MALFORMED = "aruba_frame_epoch_unavailable"
    FRAME_EPOCH_DISAGREEMENT = "aruba_frame_epoch_delta_observed"
    INTERNAL_PARSER_ERROR = "aruba_timestamp_internal_error"


class ArubaTimestampComparisonSource(str, Enum):
    """Optional evidence sources compared with the authoritative raw value."""

    DISPLAY_TOKEN = "aruba_erm.time"
    FRAME_TIME_EPOCH = "frame.time_epoch"


@dataclass(frozen=True)
class ParsedArubaTimestampEvidence:
    """Immutable source and transient provenance retained by one parse."""

    raw_token: str | bytes | None
    decoded_seconds: int | None
    decoded_microseconds: int | None
    contract_id: str
    contract_version: int
    parser_method_name: str
    display_token: str | None
    frame_time_epoch: str | Decimal | float | None
    frame_number: int | None


@dataclass(frozen=True)
class ArubaTimestampComparisonDiagnostic:
    """One non-authoritative comparison against the canonical Aruba instant."""

    source: ArubaTimestampComparisonSource
    status: ArubaTimestampDiagnosticStatus
    signed_difference_microseconds: Decimal | None
    absolute_difference_microseconds: Decimal | None
    detail: str


@dataclass(frozen=True)
class ArubaTimestampParseResult:
    """One canonical Aruba result, separate from optional comparisons."""

    status: ArubaTimestampDiagnosticStatus
    canonical_instant: pd.Timestamp | None
    normalization_result: NormalizationResult | None
    evidence: ParsedArubaTimestampEvidence
    comparison_diagnostics: tuple[ArubaTimestampComparisonDiagnostic, ...]
    diagnostic_code: str
    diagnostic_detail: str

    @property
    def is_valid(self) -> bool:
        return self.status is ArubaTimestampDiagnosticStatus.VALID

    @property
    def canonical_epoch_nanoseconds(self) -> int | None:
        if self.canonical_instant is None:
            return None
        return int(self.canonical_instant.value)


@dataclass(frozen=True)
class ArubaTimestampParseRequest:
    """Immutable input for order-preserving batch parsing."""

    raw_token: str | bytes | None
    contract_id: str
    contract_version: int
    display_token: str | None = None
    frame_time_epoch: str | Decimal | float | None = None
    frame_number: int | None = None


def _raw_token_text(raw_token: str | bytes) -> str:
    if isinstance(raw_token, bytes):
        return raw_token.decode("ascii")
    if isinstance(raw_token, str):
        return raw_token
    raise TypeError("The raw Aruba timestamp token must be text or ASCII bytes.")


def _decode_raw_token(raw_token: str | bytes) -> tuple[int, int]:
    token_text = _raw_token_text(raw_token)
    if _RAW_TOKEN_PATTERN.fullmatch(token_text) is None:
        raise ValueError("The raw Aruba timestamp token does not match the contract grammar.")

    raw_bytes = bytes.fromhex(token_text.replace(":", ""))
    if len(raw_bytes) != ARUBA_TIMESTAMP_RAW_TOKEN_BYTE_COUNT:
        raise ValueError("The raw Aruba timestamp token does not contain eight bytes.")

    seconds = int.from_bytes(raw_bytes[:4], byteorder="big", signed=False)
    microseconds = int.from_bytes(raw_bytes[4:], byteorder="big", signed=False)
    if microseconds > 999_999:
        raise OverflowError("The raw Aruba microsecond word exceeds 999999.")
    return seconds, microseconds


def _timestamp_from_components(seconds: int, microseconds: int) -> pd.Timestamp:
    timestamp = pd.Timestamp(seconds, unit="s", tz="UTC")
    timestamp += pd.Timedelta(microseconds=microseconds)
    return timestamp.as_unit("ns")


def _parse_approved_raw_token(raw_token: str | bytes) -> pd.Timestamp:
    seconds, microseconds = _decode_raw_token(raw_token)
    return _timestamp_from_components(seconds, microseconds)


ARUBA_TIMESTAMP_SOURCE_CONTRACT = SourceParserContract(
    name=ARUBA_TIMESTAMP_CONTRACT_ID,
    version=str(ARUBA_TIMESTAMP_CONTRACT_VERSION),
    parser=_parse_approved_raw_token,
)


def parse_aruba_erm_timestamp(
    raw_token: str | bytes | None,
    *,
    contract_id: str,
    contract_version: int,
    display_token: str | None = None,
    frame_time_epoch: str | Decimal | float | None = None,
    frame_number: int | None = None,
) -> ArubaTimestampParseResult:
    """Parse one exact Aruba ERM raw timestamp and normalize it to UTC."""

    evidence = ParsedArubaTimestampEvidence(
        raw_token=raw_token,
        decoded_seconds=None,
        decoded_microseconds=None,
        contract_id=contract_id,
        contract_version=contract_version,
        parser_method_name=ARUBA_TIMESTAMP_PARSER_METHOD_NAME,
        display_token=display_token,
        frame_time_epoch=frame_time_epoch,
        frame_number=frame_number,
    )

    if (
        contract_id != ARUBA_TIMESTAMP_CONTRACT_ID
        or type(contract_version) is not int
        or contract_version != ARUBA_TIMESTAMP_CONTRACT_VERSION
    ):
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.UNSUPPORTED_PARSER_CONTRACT,
            evidence,
            "The requested Aruba timestamp parser contract is unsupported.",
        )

    if raw_token is None:
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.ABSENT_SOURCE_VALUE,
            evidence,
            "The authoritative raw Aruba timestamp value is absent.",
        )

    try:
        token_text = _raw_token_text(raw_token)
    except (TypeError, UnicodeDecodeError):
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.MALFORMED_RAW_TOKEN,
            evidence,
            "The raw Aruba timestamp token is not contract-compatible ASCII text.",
        )

    if _RAW_TOKEN_PATTERN.fullmatch(token_text) is None:
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.MALFORMED_RAW_TOKEN,
            evidence,
            "The raw Aruba timestamp token does not match the exact grammar.",
        )

    raw_bytes = bytes.fromhex(token_text.replace(":", ""))
    if len(raw_bytes) != ARUBA_TIMESTAMP_RAW_TOKEN_BYTE_COUNT:
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.MALFORMED_RAW_TOKEN,
            evidence,
            "The raw Aruba timestamp token does not contain exactly eight bytes.",
        )

    seconds = int.from_bytes(raw_bytes[:4], byteorder="big", signed=False)
    microseconds = int.from_bytes(raw_bytes[4:], byteorder="big", signed=False)
    evidence = ParsedArubaTimestampEvidence(
        raw_token=raw_token,
        decoded_seconds=seconds,
        decoded_microseconds=microseconds,
        contract_id=contract_id,
        contract_version=contract_version,
        parser_method_name=ARUBA_TIMESTAMP_PARSER_METHOD_NAME,
        display_token=display_token,
        frame_time_epoch=frame_time_epoch,
        frame_number=frame_number,
    )

    if microseconds > 999_999:
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.OUT_OF_RANGE_MICROSECONDS,
            evidence,
            "The raw Aruba microsecond word is outside 0..999999.",
        )

    timestamp_input = TimestampInput(
        value=raw_token,
        raw_source_token=raw_token,
        source_contract=ARUBA_TIMESTAMP_SOURCE_CONTRACT,
    )
    normalization = normalize_timestamps((timestamp_input,))[0]
    if not normalization.is_valid or normalization.canonical_instant is None:
        status = _normalization_failure_status(seconds, microseconds, normalization)
        return _rejected_result(
            status,
            evidence,
            "The accepted Aruba value could not be normalized to canonical UTC.",
            normalization_result=normalization,
        )

    canonical = normalization.canonical_instant
    if canonical.value % 1_000 != 0:
        return _rejected_result(
            ArubaTimestampDiagnosticStatus.INTERNAL_PARSER_ERROR,
            evidence,
            "The parser invented precision below the source microsecond.",
            normalization_result=normalization,
        )

    comparisons: list[ArubaTimestampComparisonDiagnostic] = []
    if display_token is not None:
        comparisons.append(_compare_display_timestamp(canonical, display_token))
    if frame_time_epoch is not None:
        comparisons.append(
            _compare_frame_epoch(seconds, microseconds, frame_time_epoch)
        )

    return ArubaTimestampParseResult(
        status=ArubaTimestampDiagnosticStatus.VALID,
        canonical_instant=canonical,
        normalization_result=normalization,
        evidence=evidence,
        comparison_diagnostics=tuple(comparisons),
        diagnostic_code=ArubaTimestampDiagnosticStatus.VALID.value,
        diagnostic_detail="The raw Aruba timestamp normalized to canonical UTC.",
    )


def parse_aruba_erm_timestamps(
    requests: Sequence[ArubaTimestampParseRequest],
) -> tuple[ArubaTimestampParseResult, ...]:
    """Parse a batch without sorting, filtering, deduplicating, or mutation."""

    return tuple(
        parse_aruba_erm_timestamp(
            request.raw_token,
            contract_id=request.contract_id,
            contract_version=request.contract_version,
            display_token=request.display_token,
            frame_time_epoch=request.frame_time_epoch,
            frame_number=request.frame_number,
        )
        for request in requests
    )


def _normalization_failure_status(
    seconds: int,
    microseconds: int,
    normalization: NormalizationResult,
) -> ArubaTimestampDiagnosticStatus:
    if normalization.status is DiagnosticStatus.INVALID_VALUE:
        try:
            _timestamp_from_components(seconds, microseconds)
        except (OverflowError, ValueError):
            return ArubaTimestampDiagnosticStatus.OUT_OF_RANGE_SECONDS
    return ArubaTimestampDiagnosticStatus.INTERNAL_PARSER_ERROR


def _compare_display_timestamp(
    canonical: pd.Timestamp,
    display_token: str,
) -> ArubaTimestampComparisonDiagnostic:
    match = _DISPLAY_TOKEN_PATTERN.fullmatch(display_token)
    if match is None:
        return ArubaTimestampComparisonDiagnostic(
            source=ArubaTimestampComparisonSource.DISPLAY_TOKEN,
            status=ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MALFORMED,
            signed_difference_microseconds=None,
            absolute_difference_microseconds=None,
            detail="The display token does not match the exact evidenced grammar.",
        )

    try:
        offset_minutes = int(match.group("offset_hour")) * 60 + int(
            match.group("offset_minute")
        )
        if match.group("offset_sign") == "-":
            offset_minutes *= -1
        display_timezone = timezone(timedelta(minutes=offset_minutes))
        fraction = match.group("fraction")
        display_datetime = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
            int(fraction[:6]),
            tzinfo=display_timezone,
        )
        displayed = pd.Timestamp(display_datetime).as_unit("ns")
        displayed += pd.Timedelta(nanoseconds=int(fraction[6:]))
        displayed = displayed.tz_convert("UTC").as_unit("ns")
    except (OverflowError, ValueError):
        return ArubaTimestampComparisonDiagnostic(
            source=ArubaTimestampComparisonSource.DISPLAY_TOKEN,
            status=ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MALFORMED,
            signed_difference_microseconds=None,
            absolute_difference_microseconds=None,
            detail="The exact display token contains invalid date or offset values.",
        )

    difference = Decimal(int(canonical.value) - int(displayed.value))
    difference /= _NANOSECONDS_PER_MICROSECOND
    status = (
        ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MATCH
        if difference == 0
        else ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MISMATCH
    )
    return ArubaTimestampComparisonDiagnostic(
        source=ArubaTimestampComparisonSource.DISPLAY_TOKEN,
        status=status,
        signed_difference_microseconds=difference,
        absolute_difference_microseconds=abs(difference),
        detail=(
            "The display token identifies the raw Aruba instant."
            if difference == 0
            else "The display token identifies a different instant."
        ),
    )


def _compare_frame_epoch(
    seconds: int,
    microseconds: int,
    frame_time_epoch: str | Decimal | float,
) -> ArubaTimestampComparisonDiagnostic:
    try:
        frame_epoch = _frame_epoch_decimal(frame_time_epoch)
    except (InvalidOperation, TypeError, ValueError):
        return ArubaTimestampComparisonDiagnostic(
            source=ArubaTimestampComparisonSource.FRAME_TIME_EPOCH,
            status=ArubaTimestampDiagnosticStatus.FRAME_EPOCH_MALFORMED,
            signed_difference_microseconds=None,
            absolute_difference_microseconds=None,
            detail="The frame epoch could not be parsed as explicit Unix seconds.",
        )

    aruba_microseconds = (
        Decimal(seconds) * _MICROSECONDS_PER_SECOND + Decimal(microseconds)
    )
    difference = aruba_microseconds - frame_epoch * _MICROSECONDS_PER_SECOND
    status = (
        ArubaTimestampDiagnosticStatus.FRAME_EPOCH_MATCH
        if difference == 0
        else ArubaTimestampDiagnosticStatus.FRAME_EPOCH_DISAGREEMENT
    )
    return ArubaTimestampComparisonDiagnostic(
        source=ArubaTimestampComparisonSource.FRAME_TIME_EPOCH,
        status=status,
        signed_difference_microseconds=difference,
        absolute_difference_microseconds=abs(difference),
        detail=(
            "The diagnostic frame epoch matches the Aruba instant."
            if difference == 0
            else "The diagnostic frame epoch differs from the Aruba instant."
        ),
    )


def _frame_epoch_decimal(value: str | Decimal | float) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("Boolean frame epochs are invalid.")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, float):
        parsed = Decimal(str(value))
    elif isinstance(value, str):
        if _FRAME_EPOCH_PATTERN.fullmatch(value) is None:
            raise ValueError("The frame epoch has an unsupported lexical form.")
        parsed = Decimal(value)
    else:
        raise TypeError("The frame epoch must be text, Decimal, or float.")
    if not parsed.is_finite():
        raise ValueError("The frame epoch must be finite.")
    return parsed


def _rejected_result(
    status: ArubaTimestampDiagnosticStatus,
    evidence: ParsedArubaTimestampEvidence,
    detail: str,
    *,
    normalization_result: NormalizationResult | None = None,
) -> ArubaTimestampParseResult:
    return ArubaTimestampParseResult(
        status=status,
        canonical_instant=None,
        normalization_result=normalization_result,
        evidence=evidence,
        comparison_diagnostics=(),
        diagnostic_code=status.value,
        diagnostic_detail=detail,
    )
