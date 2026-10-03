"""Pure canonical timestamp normalization with explicit timezone evidence.

This module converts already-parsed timestamps and structured local civil
values to timezone-aware UTC pandas ``Timestamp`` objects.  It does not read
configuration or environment state, write files, mutate inputs, or select any
analysis interval semantics.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd


NORMALIZATION_METHOD_NAME = "beacon.timestamp/canonical-utc"
NORMALIZATION_METHOD_VERSION = "1"


class EvidenceKind(str, Enum):
    """Trusted timezone evidence categories from accepted ADR-0001."""

    SOURCE_OR_RECORD = "source_or_record_metadata"
    ADAPTER = "adapter_provided_evidence"
    DATASET_TIMEZONE = "dataset_timezone"
    SCHEDULE_TIMEZONE = "schedule_timezone"
    EXPLICIT_FIXED_OFFSET = "explicit_fixed_offset"
    EXPLICIT_FOLD = "explicit_fold_resolution"


class EvidenceScope(str, Enum):
    """Scope to which one evidence item applies."""

    RECORD = "record"
    CAPTURE = "capture"
    DATASET = "dataset"
    LOCATION = "location"
    SCHEDULE = "schedule"


class DiagnosticStatus(str, Enum):
    """Mutually exclusive timestamp normalization outcomes."""

    VALID = "valid"
    INVALID_VALUE = "invalid_value"
    MISSING_TIMEZONE_EVIDENCE = "missing_timezone_evidence"
    CONFLICTING_TIMEZONE_EVIDENCE = "conflicting_timezone_evidence"
    NONEXISTENT_LOCAL_TIME = "nonexistent_local_time"
    AMBIGUOUS_REPEATED_LOCAL_TIME = "ambiguous_repeated_local_time"
    UNSUPPORTED_SOURCE_CONTRACT = "unsupported_source_contract"


@dataclass(frozen=True)
class TimestampEvidence:
    """One explicit, scoped timezone or repeated-time evidence item."""

    kind: EvidenceKind
    source_reference: str
    scope: EvidenceScope
    timezone_name: str | None = None
    utc_offset: timedelta | None = None
    fold: int | None = None
    adapter_or_normalizer_name: str | None = None
    method_version: str | None = None


@dataclass(frozen=True)
class LocalCivilTime:
    """A timestamp represented as explicit local date/time components."""

    year: int
    month: int
    day: int
    hour: int = 0
    minute: int = 0
    second: int = 0
    microsecond: int = 0
    nanosecond: int = 0


@dataclass(frozen=True)
class SourceParserContract:
    """An explicitly named/versioned caller-supplied lexical parser.

    A contract without a parser records a requested but unsupported source
    shape, such as the currently unapproved ``frame.time_epoch`` path.
    """

    name: str
    version: str
    parser: Callable[[Any], Any] | None = field(
        default=None,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True)
class TimestampInput:
    """One immutable batch request and its row-specific audit evidence."""

    value: Any
    evidence: tuple[TimestampEvidence, ...] = ()
    raw_source_token: Any = None
    source_contract: SourceParserContract | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))


@dataclass(frozen=True)
class NormalizationResult:
    """One immutable canonical instant or explicit rejection."""

    canonical_instant: pd.Timestamp | None
    status: DiagnosticStatus
    raw_source_token: Any
    original_parsed_value: Any
    original_source_offset: timedelta | None
    original_named_zone: str | None
    evidence_used: tuple[TimestampEvidence, ...]
    primary_evidence: TimestampEvidence | None
    normalization_method_name: str
    normalization_method_version: str
    source_contract_name: str | None
    source_contract_version: str | None
    diagnostic_code: str
    diagnostic_detail: str

    @property
    def is_valid(self) -> bool:
        return self.status is DiagnosticStatus.VALID

    @property
    def canonical_epoch_nanoseconds(self) -> int | None:
        if self.canonical_instant is None:
            return None
        return int(self.canonical_instant.value)


@dataclass(frozen=True)
class _CandidateResult:
    instants: dict[int, pd.Timestamp]
    status: DiagnosticStatus | None = None
    diagnostic_code: str | None = None
    diagnostic_detail: str | None = None


def normalize_timestamp(
    value: Any,
    *,
    evidence: Iterable[TimestampEvidence] = (),
    raw_source_token: Any = None,
    source_contract: SourceParserContract | None = None,
) -> NormalizationResult:
    """Normalize one value without consulting implicit state.

    Directly accepted values are pandas ``Timestamp`` objects, Python
    ``datetime`` objects, and ``LocalCivilTime`` values.  Other source shapes
    require a named/versioned caller-supplied parser contract.
    """

    evidence_items = tuple(evidence)
    raw_reference = _raw_reference(value, raw_source_token)

    if source_contract is not None and (
        not source_contract.name.strip() or not source_contract.version.strip()
    ):
        return _rejected_result(
            DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT,
            raw_source_token=raw_reference,
            original_parsed_value=None,
            evidence=evidence_items,
            source_contract=source_contract,
            detail="A source parser contract requires a non-empty name and version.",
        )

    parsed_value = value
    if not _is_directly_accepted(value):
        if source_contract is None or source_contract.parser is None:
            return _rejected_result(
                DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT,
                raw_source_token=raw_reference,
                original_parsed_value=None,
                evidence=evidence_items,
                source_contract=source_contract,
                detail=(
                    "The source value requires an explicitly named and "
                    "versioned parser contract."
                ),
            )
        try:
            parsed_value = source_contract.parser(value)
        except Exception as exc:  # the contract owns lexical failure details
            return _rejected_result(
                DiagnosticStatus.INVALID_VALUE,
                raw_source_token=raw_reference,
                original_parsed_value=None,
                evidence=evidence_items,
                source_contract=source_contract,
                detail=(
                    "The source parser contract rejected the value "
                    f"({type(exc).__name__})."
                ),
            )
        if not _is_directly_accepted(parsed_value):
            return _rejected_result(
                DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT,
                raw_source_token=raw_reference,
                original_parsed_value=parsed_value,
                evidence=evidence_items,
                source_contract=source_contract,
                detail=(
                    "The source parser contract did not return an accepted "
                    "parsed timestamp value."
                ),
            )

    evidence_error = _validate_evidence(evidence_items)
    if evidence_error is not None:
        status, code, detail = evidence_error
        return _rejected_result(
            status,
            raw_source_token=raw_reference,
            original_parsed_value=parsed_value,
            evidence=evidence_items,
            source_contract=source_contract,
            diagnostic_code=code,
            detail=detail,
        )

    try:
        local_or_aware = _to_timestamp(parsed_value)
    except (TypeError, ValueError, OverflowError) as exc:
        return _rejected_result(
            DiagnosticStatus.INVALID_VALUE,
            raw_source_token=raw_reference,
            original_parsed_value=parsed_value,
            evidence=evidence_items,
            source_contract=source_contract,
            detail=f"The parsed timestamp value is invalid ({type(exc).__name__}).",
        )

    if pd.isna(local_or_aware):
        return _rejected_result(
            DiagnosticStatus.INVALID_VALUE,
            raw_source_token=raw_reference,
            original_parsed_value=parsed_value,
            evidence=evidence_items,
            source_contract=source_contract,
            detail="The parsed timestamp value is missing or invalid.",
        )

    if local_or_aware.tzinfo is None:
        return _normalize_naive(
            local_or_aware,
            parsed_value=parsed_value,
            raw_source_token=raw_reference,
            evidence=evidence_items,
            source_contract=source_contract,
        )
    return _normalize_aware(
        local_or_aware,
        parsed_value=parsed_value,
        raw_source_token=raw_reference,
        evidence=evidence_items,
        source_contract=source_contract,
    )


def normalize_timestamps(
    values: Sequence[TimestampInput | Any],
    *,
    evidence: Iterable[TimestampEvidence] = (),
) -> tuple[NormalizationResult, ...]:
    """Normalize a sequence in place-preserving order without mutation.

    Common evidence is prepended to request-specific evidence.  Each input
    produces exactly one independent result; no sorting, filtering,
    deduplication, or dropping is performed.
    """

    common_evidence = tuple(evidence)
    results: list[NormalizationResult] = []
    for item in values:
        if isinstance(item, TimestampInput):
            results.append(
                normalize_timestamp(
                    item.value,
                    evidence=common_evidence + item.evidence,
                    raw_source_token=item.raw_source_token,
                    source_contract=item.source_contract,
                )
            )
        else:
            results.append(normalize_timestamp(item, evidence=common_evidence))
    return tuple(results)


def _is_directly_accepted(value: Any) -> bool:
    return isinstance(value, (pd.Timestamp, datetime, LocalCivilTime))


def _raw_reference(value: Any, raw_source_token: Any) -> Any:
    if raw_source_token is not None:
        return raw_source_token
    if not _is_directly_accepted(value) or pd.isna(value):
        return value
    return None


def _to_timestamp(value: pd.Timestamp | datetime | LocalCivilTime) -> pd.Timestamp:
    if isinstance(value, LocalCivilTime):
        if not 0 <= value.nanosecond <= 999:
            raise ValueError("nanosecond must be between 0 and 999")
        timestamp = pd.Timestamp(
            datetime(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond,
            )
        ) + pd.Timedelta(nanoseconds=value.nanosecond)
    else:
        timestamp = pd.Timestamp(value)
    return timestamp.as_unit("ns")


def _normalize_naive(
    local_value: pd.Timestamp,
    *,
    parsed_value: Any,
    raw_source_token: Any,
    evidence: tuple[TimestampEvidence, ...],
    source_contract: SourceParserContract | None,
) -> NormalizationResult:
    timezone_evidence = tuple(
        item
        for item in evidence
        if item.timezone_name is not None or item.utc_offset is not None
    )
    if not timezone_evidence:
        return _rejected_result(
            DiagnosticStatus.MISSING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail="A naive timestamp requires explicit trusted timezone evidence.",
        )

    fold_result = _selected_fold(evidence)
    if isinstance(fold_result, tuple):
        return _rejected_result(
            DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail=fold_result[1],
        )
    fold = fold_result

    candidate_sets: list[dict[int, pd.Timestamp]] = []
    for item in timezone_evidence:
        candidate = _candidate_instants(local_value, item, fold)
        if candidate.status is not None:
            return _rejected_result(
                candidate.status,
                raw_source_token=raw_source_token,
                original_parsed_value=parsed_value,
                evidence=evidence,
                source_contract=source_contract,
                diagnostic_code=candidate.diagnostic_code,
                detail=candidate.diagnostic_detail or "Timestamp rejected.",
            )
        candidate_sets.append(candidate.instants)

    common_nanoseconds = set(candidate_sets[0])
    for candidates in candidate_sets[1:]:
        common_nanoseconds.intersection_update(candidates)

    if not common_nanoseconds:
        return _rejected_result(
            DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail=(
                "Applicable timezone evidence resolves the local value to "
                "different instants."
            ),
        )
    if len(common_nanoseconds) > 1:
        return _rejected_result(
            DiagnosticStatus.AMBIGUOUS_REPEATED_LOCAL_TIME,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail=(
                "The repeated local time requires an explicit offset, fold, "
                "or equivalent trusted evidence."
            ),
        )

    epoch_nanoseconds = common_nanoseconds.pop()
    canonical = candidate_sets[0][epoch_nanoseconds]
    primary = _primary_timezone_evidence(timezone_evidence)
    named_evidence = tuple(
        item for item in timezone_evidence if item.timezone_name is not None
    )
    offset_evidence = tuple(
        item for item in timezone_evidence if item.utc_offset is not None
    )
    original_named_zone = (
        _primary_timezone_evidence(named_evidence).timezone_name
        if named_evidence
        else None
    )
    original_source_offset = (
        _primary_timezone_evidence(offset_evidence).utc_offset
        if offset_evidence
        else None
    )

    return _valid_result(
        canonical,
        raw_source_token=raw_source_token,
        original_parsed_value=parsed_value,
        original_source_offset=original_source_offset,
        original_named_zone=original_named_zone,
        evidence=evidence,
        primary_evidence=primary,
        source_contract=source_contract,
    )


def _normalize_aware(
    aware_value: pd.Timestamp,
    *,
    parsed_value: Any,
    raw_source_token: Any,
    evidence: tuple[TimestampEvidence, ...],
    source_contract: SourceParserContract | None,
) -> NormalizationResult:
    try:
        canonical = aware_value.tz_convert("UTC").as_unit("ns")
    except (TypeError, ValueError, OverflowError) as exc:
        return _rejected_result(
            DiagnosticStatus.INVALID_VALUE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail=(
                "The aware timestamp cannot be represented in ns "
                f"({type(exc).__name__})."
            ),
        )

    fold_result = _selected_fold(evidence)
    if isinstance(fold_result, tuple):
        return _rejected_result(
            DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            evidence=evidence,
            source_contract=source_contract,
            detail=fold_result[1],
        )
    fold = fold_result

    original_named_zone = _named_zone(aware_value)
    original_source_offset = aware_value.utcoffset()
    local_wall_value = aware_value.tz_localize(None)
    timezone_evidence = tuple(
        item
        for item in evidence
        if item.timezone_name is not None or item.utc_offset is not None
    )
    for item in timezone_evidence:
        candidate = _candidate_instants(local_wall_value, item, fold)
        if candidate.status is not None or canonical.value not in candidate.instants:
            return _rejected_result(
                DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
                raw_source_token=raw_source_token,
                original_parsed_value=parsed_value,
                original_source_offset=original_source_offset,
                original_named_zone=original_named_zone,
                evidence=evidence,
                source_contract=source_contract,
                detail=(
                    "Applicable timezone evidence conflicts with the instant "
                    "carried by the aware timestamp."
                ),
            )

    if fold is not None and original_named_zone is None:
        return _rejected_result(
            DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            original_source_offset=original_source_offset,
            original_named_zone=original_named_zone,
            evidence=evidence,
            source_contract=source_contract,
            detail="Fold evidence cannot apply to a fixed-offset-only timestamp.",
        )
    if fold is not None and aware_value.fold != fold:
        return _rejected_result(
            DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
            raw_source_token=raw_source_token,
            original_parsed_value=parsed_value,
            original_source_offset=original_source_offset,
            original_named_zone=original_named_zone,
            evidence=evidence,
            source_contract=source_contract,
            detail="Fold evidence conflicts with the aware timestamp.",
        )

    return _valid_result(
        canonical,
        raw_source_token=raw_source_token,
        original_parsed_value=parsed_value,
        original_source_offset=original_source_offset,
        original_named_zone=original_named_zone,
        evidence=evidence,
        primary_evidence=(
            _primary_timezone_evidence(timezone_evidence)
            if timezone_evidence
            else None
        ),
        source_contract=source_contract,
    )


def _candidate_instants(
    local_value: pd.Timestamp,
    evidence: TimestampEvidence,
    fold: int | None,
) -> _CandidateResult:
    if evidence.utc_offset is not None:
        try:
            fixed_timezone = timezone(evidence.utc_offset)
            aware = local_value.tz_localize(fixed_timezone).tz_convert("UTC")
            aware = aware.as_unit("ns")
        except (TypeError, ValueError, OverflowError) as exc:
            return _CandidateResult(
                {},
                DiagnosticStatus.INVALID_VALUE,
                "invalid_timezone_evidence",
                f"The fixed-offset evidence is invalid ({type(exc).__name__}).",
            )
        return _CandidateResult({int(aware.value): aware})

    try:
        named_timezone = ZoneInfo(evidence.timezone_name or "")
    except (ZoneInfoNotFoundError, ValueError) as exc:
        return _CandidateResult(
            {},
            DiagnosticStatus.INVALID_VALUE,
            "invalid_timezone_evidence",
            f"The named timezone evidence is invalid ({type(exc).__name__}).",
        )

    early = local_value.tz_localize(
        named_timezone,
        ambiguous=True,
        nonexistent="NaT",
    )
    late = local_value.tz_localize(
        named_timezone,
        ambiguous=False,
        nonexistent="NaT",
    )
    if pd.isna(early) and pd.isna(late):
        return _CandidateResult(
            {},
            DiagnosticStatus.NONEXISTENT_LOCAL_TIME,
            DiagnosticStatus.NONEXISTENT_LOCAL_TIME.value,
            "The local timestamp does not exist in the named timezone.",
        )

    candidates = [early] if early.value == late.value else [early, late]
    if fold is not None and len(candidates) == 2:
        candidates = [candidates[fold]]
    utc_candidates = [
        candidate.tz_convert("UTC").as_unit("ns") for candidate in candidates
    ]
    return _CandidateResult(
        {int(candidate.value): candidate for candidate in utc_candidates}
    )


def _selected_fold(
    evidence: tuple[TimestampEvidence, ...],
) -> int | None | tuple[None, str]:
    folds = {
        item.fold
        for item in evidence
        if item.kind is EvidenceKind.EXPLICIT_FOLD and item.fold is not None
    }
    if len(folds) > 1:
        return None, "Explicit fold evidence disagrees."
    return next(iter(folds)) if folds else None


def _primary_timezone_evidence(
    evidence: tuple[TimestampEvidence, ...],
) -> TimestampEvidence:
    ranks = {
        EvidenceKind.SOURCE_OR_RECORD: 0,
        EvidenceKind.EXPLICIT_FIXED_OFFSET: 0,
        EvidenceKind.EXPLICIT_FOLD: 0,
        EvidenceKind.ADAPTER: 1,
        EvidenceKind.DATASET_TIMEZONE: 2,
        EvidenceKind.SCHEDULE_TIMEZONE: 2,
    }
    return min(enumerate(evidence), key=lambda item: (ranks[item[1].kind], item[0]))[
        1
    ]


def _named_zone(value: pd.Timestamp) -> str | None:
    timezone_value = value.tzinfo
    for attribute in ("key", "zone"):
        name = getattr(timezone_value, attribute, None)
        if name:
            return str(name)
    return None


def _validate_evidence(
    evidence: tuple[TimestampEvidence, ...],
) -> tuple[DiagnosticStatus, str, str] | None:
    for item in evidence:
        if not item.source_reference.strip():
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Timezone evidence requires a non-empty source reference.",
            )
        if item.timezone_name is not None and item.utc_offset is not None:
            return (
                DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE,
                DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE.value,
                "One evidence item cannot claim both a named zone and a fixed offset.",
            )
        if item.kind is EvidenceKind.ADAPTER and (
            not item.adapter_or_normalizer_name or not item.method_version
        ):
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Adapter evidence requires an adapter name and version.",
            )
        if item.kind in {
            EvidenceKind.DATASET_TIMEZONE,
            EvidenceKind.SCHEDULE_TIMEZONE,
        } and (item.timezone_name is None or item.utc_offset is not None):
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Dataset and schedule evidence require a named timezone.",
            )
        if item.kind is EvidenceKind.EXPLICIT_FIXED_OFFSET and (
            item.utc_offset is None or item.timezone_name is not None
        ):
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Explicit fixed-offset evidence requires only a UTC offset.",
            )
        if item.kind is EvidenceKind.EXPLICIT_FOLD:
            if type(item.fold) is not int or item.fold not in {0, 1}:
                return (
                    DiagnosticStatus.INVALID_VALUE,
                    "invalid_evidence",
                    "Explicit fold evidence must be integer 0 or 1.",
                )
            if item.timezone_name is not None or item.utc_offset is not None:
                return (
                    DiagnosticStatus.INVALID_VALUE,
                    "invalid_evidence",
                    "Fold evidence cannot also define a timezone or offset.",
                )
        elif item.fold is not None:
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Fold values require explicit-fold evidence.",
            )
        elif item.timezone_name is None and item.utc_offset is None:
            return (
                DiagnosticStatus.INVALID_VALUE,
                "invalid_evidence",
                "Timezone evidence requires a named zone or fixed offset.",
            )
    return None


def _valid_result(
    canonical_instant: pd.Timestamp,
    *,
    raw_source_token: Any,
    original_parsed_value: Any,
    original_source_offset: timedelta | None,
    original_named_zone: str | None,
    evidence: tuple[TimestampEvidence, ...],
    primary_evidence: TimestampEvidence | None,
    source_contract: SourceParserContract | None,
) -> NormalizationResult:
    return NormalizationResult(
        canonical_instant=canonical_instant,
        status=DiagnosticStatus.VALID,
        raw_source_token=raw_source_token,
        original_parsed_value=original_parsed_value,
        original_source_offset=original_source_offset,
        original_named_zone=original_named_zone,
        evidence_used=evidence,
        primary_evidence=primary_evidence,
        normalization_method_name=NORMALIZATION_METHOD_NAME,
        normalization_method_version=NORMALIZATION_METHOD_VERSION,
        source_contract_name=source_contract.name if source_contract else None,
        source_contract_version=(source_contract.version if source_contract else None),
        diagnostic_code=DiagnosticStatus.VALID.value,
        diagnostic_detail="Timestamp normalized to a UTC-aware nanosecond instant.",
    )


def _rejected_result(
    status: DiagnosticStatus,
    *,
    raw_source_token: Any,
    original_parsed_value: Any,
    evidence: tuple[TimestampEvidence, ...],
    source_contract: SourceParserContract | None,
    detail: str,
    diagnostic_code: str | None = None,
    original_source_offset: timedelta | None = None,
    original_named_zone: str | None = None,
) -> NormalizationResult:
    timezone_evidence = tuple(
        item
        for item in evidence
        if item.timezone_name is not None or item.utc_offset is not None
    )
    return NormalizationResult(
        canonical_instant=None,
        status=status,
        raw_source_token=raw_source_token,
        original_parsed_value=original_parsed_value,
        original_source_offset=original_source_offset,
        original_named_zone=original_named_zone,
        evidence_used=evidence,
        primary_evidence=(
            _primary_timezone_evidence(timezone_evidence)
            if timezone_evidence
            else None
        ),
        normalization_method_name=NORMALIZATION_METHOD_NAME,
        normalization_method_version=NORMALIZATION_METHOD_VERSION,
        source_contract_name=source_contract.name if source_contract else None,
        source_contract_version=(source_contract.version if source_contract else None),
        diagnostic_code=diagnostic_code or status.value,
        diagnostic_detail=detail,
    )
