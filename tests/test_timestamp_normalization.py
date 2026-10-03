from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from timestamp_normalization import (
    NORMALIZATION_METHOD_NAME,
    NORMALIZATION_METHOD_VERSION,
    DiagnosticStatus,
    EvidenceKind,
    EvidenceScope,
    LocalCivilTime,
    NormalizationResult,
    SourceParserContract,
    TimestampEvidence,
    TimestampInput,
    normalize_timestamp,
    normalize_timestamps,
)


DENVER = "America/Denver"


def _source_zone(zone: str = DENVER) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.SOURCE_OR_RECORD,
        timezone_name=zone,
        source_reference="capture-1:record-7",
        scope=EvidenceScope.RECORD,
    )


def _adapter_zone(zone: str = DENVER) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.ADAPTER,
        timezone_name=zone,
        source_reference="synthetic.capture_time",
        scope=EvidenceScope.CAPTURE,
        adapter_or_normalizer_name="synthetic-adapter",
        method_version="1",
    )


def _schedule_zone(zone: str = DENVER) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.SCHEDULE_TIMEZONE,
        timezone_name=zone,
        source_reference="schedule-config:timezone",
        scope=EvidenceScope.SCHEDULE,
        adapter_or_normalizer_name="schedule-normalizer",
        method_version="1",
    )


def _dataset_zone(zone: str) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.DATASET_TIMEZONE,
        timezone_name=zone,
        source_reference="dataset-manifest:timezone",
        scope=EvidenceScope.DATASET,
    )


def _fixed_offset(hours: int) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.EXPLICIT_FIXED_OFFSET,
        utc_offset=timedelta(hours=hours),
        source_reference=f"source-token:offset={hours:+03d}:00",
        scope=EvidenceScope.RECORD,
    )


def _fold(value: int) -> TimestampEvidence:
    return TimestampEvidence(
        kind=EvidenceKind.EXPLICIT_FOLD,
        fold=value,
        source_reference=f"source-token:fold={value}",
        scope=EvidenceScope.RECORD,
    )


def _assert_valid_utc(result: NormalizationResult, expected: str) -> None:
    assert result.status is DiagnosticStatus.VALID
    assert result.diagnostic_code == "valid"
    assert result.canonical_instant == pd.Timestamp(expected)
    assert str(result.canonical_instant.tz) == "UTC"
    assert str(result.canonical_instant.asm8.dtype) == "datetime64[ns]"


def test_aware_utc_scalar_normalizes_idempotently_with_nanoseconds():
    original = pd.Timestamp("2025-01-15 15:00:00.000000123+00:00")

    first = normalize_timestamp(original)
    second = normalize_timestamp(first.canonical_instant)

    _assert_valid_utc(first, "2025-01-15 15:00:00.000000123+00:00")
    assert first.canonical_epoch_nanoseconds == original.value
    assert second.canonical_epoch_nanoseconds == original.value
    assert first.canonical_instant == second.canonical_instant


@pytest.mark.parametrize(
    ("local_value", "expected_utc"),
    [
        (datetime(2025, 1, 15, 8, tzinfo=ZoneInfo(DENVER)), "2025-01-15 15:00Z"),
        (datetime(2025, 7, 15, 8, tzinfo=ZoneInfo(DENVER)), "2025-07-15 14:00Z"),
    ],
    ids=("standard-time", "daylight-time"),
)
def test_aware_named_zone_preserves_instant_and_zone_evidence(
    local_value: datetime,
    expected_utc: str,
):
    original = pd.Timestamp(local_value)

    result = normalize_timestamp(original)

    _assert_valid_utc(result, expected_utc)
    assert result.canonical_epoch_nanoseconds == original.value
    assert result.original_named_zone == DENVER
    assert result.original_source_offset == original.utcoffset()


def test_aware_explicit_fixed_offset_remains_distinct_from_named_zone():
    original = datetime(
        2025,
        7,
        15,
        8,
        tzinfo=timezone(timedelta(hours=-7)),
    )

    result = normalize_timestamp(original)

    _assert_valid_utc(result, "2025-07-15 15:00Z")
    assert result.original_source_offset == timedelta(hours=-7)
    assert result.original_named_zone is None


@pytest.mark.parametrize(
    "evidence",
    [_source_zone(), _adapter_zone(), _schedule_zone()],
    ids=("source-record", "accepted-adapter", "schedule"),
)
def test_naive_values_normalize_with_each_accepted_evidence_level(
    evidence: TimestampEvidence,
):
    result = normalize_timestamp(
        pd.Timestamp("2025-01-15 08:00:00"),
        evidence=(evidence,),
    )

    _assert_valid_utc(result, "2025-01-15 15:00Z")
    assert result.evidence_used == (evidence,)
    assert result.primary_evidence == evidence
    assert result.original_source_offset is None


def test_structured_schedule_civil_value_preserves_nanoseconds():
    civil = LocalCivilTime(
        2025,
        1,
        15,
        hour=8,
        nanosecond=321,
    )

    result = normalize_timestamp(civil, evidence=(_schedule_zone(),))

    _assert_valid_utc(result, "2025-01-15 15:00:00.000000321Z")
    assert result.original_parsed_value is civil


def test_named_fixed_rule_zone_is_not_replaced_with_denver():
    evidence = _schedule_zone("Etc/GMT+7")

    result = normalize_timestamp(
        pd.Timestamp("2025-07-15 08:00:00"),
        evidence=(evidence,),
    )

    _assert_valid_utc(result, "2025-07-15 15:00Z")
    assert result.original_named_zone == "Etc/GMT+7"
    assert result.original_named_zone != DENVER


def test_naive_value_without_timezone_evidence_is_rejected():
    result = normalize_timestamp(pd.Timestamp("2025-01-15 08:00:00"))

    assert result.status is DiagnosticStatus.MISSING_TIMEZONE_EVIDENCE
    assert result.diagnostic_code == "missing_timezone_evidence"
    assert result.canonical_instant is None


def test_conflicting_source_and_configured_evidence_is_rejected():
    result = normalize_timestamp(
        pd.Timestamp("2025-01-15 08:00:00"),
        evidence=(_source_zone(), _dataset_zone("UTC")),
    )

    assert result.status is DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE
    assert result.diagnostic_code == "conflicting_timezone_evidence"
    assert result.primary_evidence == _source_zone()
    assert result.canonical_instant is None


def test_equivalent_evidence_can_agree_while_precedence_is_recorded():
    source = _source_zone("UTC")
    dataset = _dataset_zone("Etc/UTC")

    result = normalize_timestamp(
        pd.Timestamp("2025-01-15 08:00:00"),
        evidence=(dataset, source),
    )

    _assert_valid_utc(result, "2025-01-15 08:00Z")
    assert result.evidence_used == (dataset, source)
    assert result.primary_evidence == source


def test_machine_timezone_and_environment_are_never_consulted(monkeypatch):
    monkeypatch.setenv("TIMEZONE", DENVER)
    monkeypatch.setenv("PLOT_TIMEZONE", DENVER)
    monkeypatch.setenv("TZ", DENVER)

    result = normalize_timestamp(pd.Timestamp("2025-01-15 08:00:00"))

    assert result.status is DiagnosticStatus.MISSING_TIMEZONE_EVIDENCE


def test_denver_spring_gap_is_rejected_without_shifting():
    original = pd.Timestamp("2025-03-09 02:30:00")

    result = normalize_timestamp(original, evidence=(_source_zone(),))

    assert result.status is DiagnosticStatus.NONEXISTENT_LOCAL_TIME
    assert result.diagnostic_code == "nonexistent_local_time"
    assert result.canonical_instant is None
    assert result.original_parsed_value == original


def test_denver_repeated_time_without_resolution_is_rejected():
    result = normalize_timestamp(
        pd.Timestamp("2025-11-02 01:30:00"),
        evidence=(_source_zone(),),
    )

    assert result.status is DiagnosticStatus.AMBIGUOUS_REPEATED_LOCAL_TIME
    assert result.diagnostic_code == "ambiguous_repeated_local_time"
    assert result.canonical_instant is None


@pytest.mark.parametrize(
    ("offset_hours", "expected_utc"),
    [(-6, "2025-11-02 07:30Z"), (-7, "2025-11-02 08:30Z")],
)
def test_denver_repeated_occurrences_resolve_with_explicit_offsets(
    offset_hours: int,
    expected_utc: str,
):
    result = normalize_timestamp(
        pd.Timestamp("2025-11-02 01:30:00"),
        evidence=(_source_zone(), _fixed_offset(offset_hours)),
    )

    _assert_valid_utc(result, expected_utc)
    assert result.original_named_zone == DENVER
    assert result.original_source_offset == timedelta(hours=offset_hours)


@pytest.mark.parametrize(
    ("fold", "expected_utc"),
    [(0, "2025-11-02 07:30Z"), (1, "2025-11-02 08:30Z")],
)
def test_denver_repeated_occurrences_resolve_with_explicit_fold(
    fold: int,
    expected_utc: str,
):
    result = normalize_timestamp(
        pd.Timestamp("2025-11-02 01:30:00"),
        evidence=(_source_zone(), _fold(fold)),
    )

    _assert_valid_utc(result, expected_utc)


def test_conflicting_named_zone_and_fixed_offset_are_rejected():
    result = normalize_timestamp(
        pd.Timestamp("2025-07-15 08:00:00"),
        evidence=(_source_zone(), _fixed_offset(-7)),
    )

    assert result.status is DiagnosticStatus.CONFLICTING_TIMEZONE_EVIDENCE
    assert result.canonical_instant is None


def test_capture_and_schedule_values_in_different_zones_match_exactly():
    capture = normalize_timestamp(pd.Timestamp("2025-01-15 15:00:00Z"))
    schedule = normalize_timestamp(
        LocalCivilTime(2025, 1, 15, hour=8),
        evidence=(_schedule_zone(),),
    )

    assert capture.canonical_instant == schedule.canonical_instant
    assert (
        capture.canonical_epoch_nanoseconds
        == schedule.canonical_epoch_nanoseconds
    )


def test_invalid_parsed_value_has_a_stable_diagnostic_not_silent_nat():
    result = normalize_timestamp(pd.NaT, raw_source_token="invalid-token")

    assert result.status is DiagnosticStatus.INVALID_VALUE
    assert result.diagnostic_code == "invalid_value"
    assert result.canonical_instant is None
    assert result.raw_source_token == "invalid-token"
    assert result.original_parsed_value is pd.NaT


def test_arbitrary_source_string_is_rejected_without_parser_contract():
    token = "2025-01-15 08:00:00-0700"

    result = normalize_timestamp(token)

    assert result.status is DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT
    assert result.diagnostic_code == "unsupported_source_contract"
    assert result.raw_source_token == token
    assert result.original_parsed_value is None


def test_named_versioned_caller_parser_can_supply_a_parsed_value():
    contract = SourceParserContract(
        name="test.synthetic-iso",
        version="1",
        parser=lambda token: pd.Timestamp(token),
    )

    result = normalize_timestamp(
        "2025-01-15T15:00:00.000000456+00:00",
        source_contract=contract,
    )

    _assert_valid_utc(result, "2025-01-15 15:00:00.000000456Z")
    assert result.source_contract_name == contract.name
    assert result.source_contract_version == contract.version


def test_unnamed_or_unversioned_source_parser_contract_is_rejected():
    contract = SourceParserContract(
        name="",
        version="",
        parser=lambda token: pd.Timestamp(token),
    )

    result = normalize_timestamp("2025-01-15T15:00:00Z", source_contract=contract)

    assert result.status is DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT
    assert result.diagnostic_code == "unsupported_source_contract"


def test_native_epoch_contract_remains_explicitly_unsupported():
    contract = SourceParserContract(name="frame.time_epoch", version="unapproved")

    result = normalize_timestamp(1_736_953_200, source_contract=contract)

    assert result.status is DiagnosticStatus.UNSUPPORTED_SOURCE_CONTRACT
    assert result.diagnostic_code == "unsupported_source_contract"
    assert result.source_contract_name == "frame.time_epoch"
    assert result.raw_source_token == 1_736_953_200


def test_raw_token_evidence_and_normalization_metadata_are_preserved():
    evidence = _adapter_zone()
    parsed = pd.Timestamp("2025-01-15 08:00:00")

    result = normalize_timestamp(
        parsed,
        evidence=(evidence,),
        raw_source_token="raw-token-17",
    )

    _assert_valid_utc(result, "2025-01-15 15:00Z")
    assert result.raw_source_token == "raw-token-17"
    assert result.original_parsed_value is parsed
    assert result.evidence_used == (evidence,)
    assert result.normalization_method_name == NORMALIZATION_METHOD_NAME
    assert result.normalization_method_version == NORMALIZATION_METHOD_VERSION


def test_immutable_contract_objects_reject_assignment():
    evidence = _source_zone()
    request = TimestampInput(pd.Timestamp("2025-01-15 08:00"), (evidence,))
    result = normalize_timestamp(request.value, evidence=request.evidence)

    with pytest.raises(FrozenInstanceError):
        evidence.timezone_name = "UTC"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        request.value = pd.Timestamp("2025-01-16")  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.status = DiagnosticStatus.INVALID_VALUE  # type: ignore[misc]


def test_batch_preserves_order_duplicates_nonmutation_and_one_result_per_input():
    first = pd.Timestamp("2025-01-15 15:00:00Z")
    earlier = pd.Timestamp("2025-01-15 14:00:00Z")
    requests = [
        TimestampInput(first, raw_source_token="first"),
        TimestampInput(pd.Timestamp("2025-01-15 08:00"), raw_source_token="bad"),
        TimestampInput(first, raw_source_token="duplicate"),
        TimestampInput(earlier, raw_source_token="earlier"),
    ]
    before = list(requests)

    results = normalize_timestamps(requests)

    assert requests == before
    assert isinstance(results, tuple)
    assert len(results) == len(requests)
    assert [result.raw_source_token for result in results] == [
        "first",
        "bad",
        "duplicate",
        "earlier",
    ]
    assert [result.status for result in results] == [
        DiagnosticStatus.VALID,
        DiagnosticStatus.MISSING_TIMEZONE_EVIDENCE,
        DiagnosticStatus.VALID,
        DiagnosticStatus.VALID,
    ]
    assert [result.canonical_instant for result in results] == [
        first,
        None,
        first,
        earlier,
    ]
    assert results[0] is not results[2]


@pytest.mark.parametrize(
    "instant",
    [
        "2025-03-09 07:59:59.999999999Z",
        "2025-03-09 08:00:00Z",
        "2025-11-02 06:59:59.999999999Z",
        "2025-11-02 07:00:00Z",
        "2025-11-02 08:00:00Z",
        "2025-11-02 09:00:00Z",
    ],
)
def test_utc_idempotence_across_representative_denver_transition_range(
    instant: str,
):
    original = pd.Timestamp(instant)
    local = original.tz_convert(DENVER)

    first = normalize_timestamp(local)
    second = normalize_timestamp(first.canonical_instant)

    assert first.canonical_epoch_nanoseconds == original.value
    assert second.canonical_epoch_nanoseconds == original.value


def test_utc_parquet_round_trip_preserves_dtype_nanoseconds_and_nulls(tmp_path):
    values = [
        normalize_timestamp(pd.Timestamp("2025-01-15 15:00:00.000000123Z")),
        normalize_timestamp(pd.NaT),
        normalize_timestamp(pd.Timestamp("2025-07-15 14:00:00.000000987Z")),
    ]
    canonical = pd.Series(
        [result.canonical_instant for result in values],
        dtype="datetime64[ns, UTC]",
        name="timestamp",
    )
    expected_nanoseconds = canonical.array.asi8.tolist()
    expected_nulls = canonical.isna().tolist()
    path = tmp_path / "canonical-timestamps.parquet"

    pd.DataFrame({"timestamp": canonical}).to_parquet(path, index=False)

    arrow_field = pq.read_schema(path).field("timestamp")
    restored = pd.read_parquet(path)["timestamp"]
    assert canonical.dtype == pd.DatetimeTZDtype(unit="ns", tz="UTC")
    assert restored.dtype == pd.DatetimeTZDtype(unit="ns", tz="UTC")
    assert arrow_field.type == pa.timestamp("ns", tz="UTC")
    assert restored.array.asi8.tolist() == expected_nanoseconds
    assert restored.isna().tolist() == expected_nulls


def test_normalization_method_and_version_are_stable_and_pinned():
    valid = normalize_timestamp(pd.Timestamp("2025-01-15 15:00:00Z"))
    rejected = normalize_timestamp(pd.Timestamp("2025-01-15 08:00:00"))

    assert NORMALIZATION_METHOD_NAME == "beacon.timestamp/canonical-utc"
    assert NORMALIZATION_METHOD_VERSION == "1"
    assert valid.normalization_method_name == NORMALIZATION_METHOD_NAME
    assert valid.normalization_method_version == NORMALIZATION_METHOD_VERSION
    assert rejected.normalization_method_name == NORMALIZATION_METHOD_NAME
    assert rejected.normalization_method_version == NORMALIZATION_METHOD_VERSION
