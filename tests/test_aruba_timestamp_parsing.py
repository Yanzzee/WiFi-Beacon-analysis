from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pandas as pd
import pytest

import aruba_timestamp_parsing as atp
from timestamp_normalization import DiagnosticStatus


def _raw_token(seconds: int, microseconds: int) -> str:
    raw = seconds.to_bytes(4, byteorder="big") + microseconds.to_bytes(
        4,
        byteorder="big",
    )
    return raw.hex(":")


def _instant_token(instant: str) -> str:
    timestamp = pd.Timestamp(instant).as_unit("ns")
    seconds, nanoseconds = divmod(timestamp.value, 1_000_000_000)
    assert nanoseconds % 1_000 == 0
    return _raw_token(seconds, nanoseconds // 1_000)


def _parse(raw_token: str | bytes | None, **kwargs):
    return atp.parse_aruba_erm_timestamp(
        raw_token,
        contract_id=atp.ARUBA_TIMESTAMP_CONTRACT_ID,
        contract_version=atp.ARUBA_TIMESTAMP_CONTRACT_VERSION,
        **kwargs,
    )


def _comparison(result, source: atp.ArubaTimestampComparisonSource):
    return next(item for item in result.comparison_diagnostics if item.source is source)


def test_accepted_contract_constants_and_parser_metadata_are_stable():
    token = _instant_token("2025-11-10T16:00:00.212540Z")

    result = _parse(token)

    assert atp.ARUBA_TIMESTAMP_CONTRACT_ID == (
        "beacon.source-parser/aruba-erm-time/raw-seconds-microseconds"
    )
    assert atp.ARUBA_TIMESTAMP_CONTRACT_VERSION == 1
    assert atp.ARUBA_TIMESTAMP_RAW_TOKEN_BYTE_COUNT == 8
    assert result.is_valid
    assert result.evidence.contract_id == atp.ARUBA_TIMESTAMP_CONTRACT_ID
    assert result.evidence.contract_version == 1
    assert result.evidence.parser_method_name == atp.ARUBA_TIMESTAMP_PARSER_METHOD_NAME
    assert result.normalization_result is not None
    assert result.normalization_result.source_contract_name == (
        atp.ARUBA_TIMESTAMP_CONTRACT_ID
    )
    assert result.normalization_result.source_contract_version == "1"


@pytest.mark.parametrize(
    ("contract_id", "contract_version"),
    [
        ("unknown", atp.ARUBA_TIMESTAMP_CONTRACT_VERSION),
        (atp.ARUBA_TIMESTAMP_CONTRACT_ID, 2),
        (atp.ARUBA_TIMESTAMP_CONTRACT_ID, True),
    ],
)
def test_unknown_contract_id_or_version_is_rejected(contract_id, contract_version):
    result = atp.parse_aruba_erm_timestamp(
        _raw_token(1, 0),
        contract_id=contract_id,
        contract_version=contract_version,
    )

    assert result.status is atp.ArubaTimestampDiagnosticStatus.UNSUPPORTED_PARSER_CONTRACT
    assert result.canonical_instant is None


def test_exact_token_decodes_unsigned_big_endian_seconds_and_microseconds():
    result = _parse("01:02:03:04:00:01:e2:40")

    assert result.is_valid
    assert result.evidence.decoded_seconds == 0x01020304
    assert result.evidence.decoded_microseconds == 123_456
    assert result.canonical_instant == pd.Timestamp(
        0x01020304,
        unit="s",
        tz="UTC",
    ) + pd.Timedelta(microseconds=123_456)


@pytest.mark.parametrize("microseconds", [0, 999_999])
def test_zero_and_maximum_microseconds_are_valid(microseconds: int):
    result = _parse(_raw_token(1_762_793_200, microseconds))

    assert result.is_valid
    assert result.evidence.decoded_microseconds == microseconds
    assert result.canonical_epoch_nanoseconds is not None
    assert result.canonical_epoch_nanoseconds % 1_000 == 0
    assert result.canonical_epoch_nanoseconds % 1_000_000_000 == (
        microseconds * 1_000
    )


def test_microseconds_above_contract_range_are_rejected():
    result = _parse(_raw_token(1_762_793_200, 1_000_000))

    assert result.status is atp.ArubaTimestampDiagnosticStatus.OUT_OF_RANGE_MICROSECONDS
    assert result.canonical_instant is None
    assert result.evidence.decoded_microseconds == 1_000_000


def test_missing_token_is_rejected_as_absent():
    result = _parse(None)

    assert result.status is atp.ArubaTimestampDiagnosticStatus.ABSENT_SOURCE_VALUE
    assert result.canonical_instant is None


@pytest.mark.parametrize(
    "token",
    [
        "",
        "00:00:00:00:00:00:00",
        "00:00:00:00:00:00:00:00:00",
        "00:00:00:00:00:00:00:gg",
        "00:00:00:00:00:00:00:AF",
        " 00:00:00:00:00:00:00:00",
        "00:00:00:00:00:00:00:00 ",
        '"00:00:00:00:00:00:00:00"',
        "00-00-00-00-00-00-00-00",
    ],
)
def test_unapproved_raw_lexical_forms_are_rejected(token: str):
    result = _parse(token)

    assert result.status is atp.ArubaTimestampDiagnosticStatus.MALFORMED_RAW_TOKEN
    assert result.canonical_instant is None
    assert result.evidence.raw_token == token


def test_exact_ascii_bytes_are_accepted_and_retained_without_mutation():
    token = b"69:11:fd:a0:00:03:3e:3c"

    result = _parse(token)

    assert result.is_valid
    assert result.evidence.raw_token is token
    assert result.normalization_result is not None
    assert result.normalization_result.raw_source_token is token


def test_non_ascii_raw_bytes_are_rejected():
    result = _parse(b"\xff" * 8)

    assert result.status is atp.ArubaTimestampDiagnosticStatus.MALFORMED_RAW_TOKEN


def test_defensive_out_of_range_seconds_failure_is_structured(monkeypatch):
    def fail_timestamp(seconds: int, microseconds: int):
        raise OverflowError("synthetic pandas range")

    monkeypatch.setattr(atp, "_timestamp_from_components", fail_timestamp)

    result = _parse(_raw_token(1_762_793_200, 0))

    assert result.status is atp.ArubaTimestampDiagnosticStatus.OUT_OF_RANGE_SECONDS
    assert result.canonical_instant is None


def test_canonical_result_is_utc_aware_and_preserves_only_source_precision():
    token = _instant_token("2025-11-10T16:00:00.212540Z")

    result = _parse(token)

    assert result.status is atp.ArubaTimestampDiagnosticStatus.VALID
    assert result.normalization_result is not None
    assert result.normalization_result.status is DiagnosticStatus.VALID
    assert result.canonical_instant == pd.Timestamp("2025-11-10T16:00:00.212540Z")
    assert str(result.canonical_instant.tz) == "UTC"
    assert str(result.canonical_instant.asm8.dtype) == "datetime64[ns]"
    assert result.canonical_epoch_nanoseconds % 1_000 == 0


def test_frame_number_is_retained_as_transient_evidence():
    result = _parse(_raw_token(10, 20), frame_number=7)

    assert result.is_valid
    assert result.evidence.frame_number == 7


def test_duplicate_instants_with_distinct_frames_remain_distinct_results():
    token = _raw_token(10, 20)
    requests = (
        atp.ArubaTimestampParseRequest(
            token,
            atp.ARUBA_TIMESTAMP_CONTRACT_ID,
            atp.ARUBA_TIMESTAMP_CONTRACT_VERSION,
            frame_number=1,
        ),
        atp.ArubaTimestampParseRequest(
            token,
            atp.ARUBA_TIMESTAMP_CONTRACT_ID,
            atp.ARUBA_TIMESTAMP_CONTRACT_VERSION,
            frame_number=2,
        ),
    )

    results = atp.parse_aruba_erm_timestamps(requests)

    assert len(results) == 2
    assert results[0] is not results[1]
    assert results[0].canonical_instant == results[1].canonical_instant
    assert [item.evidence.frame_number for item in results] == [1, 2]


@pytest.mark.parametrize(
    "display_token",
    [
        "2025-11-10T16:00:00.212540000+0000",
        "2025-11-10T11:00:00.212540000-0500",
        "2025-11-10T09:00:00.212540000-0700",
        "2025-11-10T08:00:00.212540000-0800",
    ],
)
def test_exact_display_offset_forms_compare_as_the_same_instant(display_token: str):
    result = _parse(
        _instant_token("2025-11-10T16:00:00.212540Z"),
        display_token=display_token,
    )

    diagnostic = _comparison(
        result,
        atp.ArubaTimestampComparisonSource.DISPLAY_TOKEN,
    )
    assert result.is_valid
    assert result.evidence.display_token == display_token
    assert diagnostic.status is atp.ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MATCH
    assert diagnostic.signed_difference_microseconds == 0


def test_display_mismatch_is_diagnostic_and_never_replaces_raw_result():
    expected = pd.Timestamp("2025-11-10T16:00:00.212540Z")
    result = _parse(
        _instant_token(str(expected)),
        display_token="2025-11-10T09:00:01.212540000-0700",
    )

    diagnostic = _comparison(
        result,
        atp.ArubaTimestampComparisonSource.DISPLAY_TOKEN,
    )
    assert result.is_valid
    assert result.canonical_instant == expected
    assert diagnostic.status is (
        atp.ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MISMATCH
    )
    assert diagnostic.signed_difference_microseconds == Decimal("-1000000")


@pytest.mark.parametrize(
    "display_token",
    [
        "2025-11-10 09:00:00.212540000-0700",
        "2025-11-10T09:00:00.212540-0700",
        "2025-11-10T09:00:00.212540000-07:00",
        "2025-11-10T09:00:00.212540000MST",
        " 2025-11-10T09:00:00.212540000-0700",
    ],
)
def test_unapproved_display_grammar_is_rejected_diagnostically(display_token: str):
    result = _parse(_raw_token(10, 20), display_token=display_token)

    diagnostic = _comparison(
        result,
        atp.ArubaTimestampComparisonSource.DISPLAY_TOKEN,
    )
    assert result.is_valid
    assert diagnostic.status is (
        atp.ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MALFORMED
    )


def test_frame_epoch_comparison_uses_decimal_safe_signed_microseconds():
    seconds = 1_762_793_200
    result = _parse(
        _raw_token(seconds, 212_540),
        frame_time_epoch=f"{seconds - 1}.712540000",
    )

    diagnostic = _comparison(
        result,
        atp.ArubaTimestampComparisonSource.FRAME_TIME_EPOCH,
    )
    assert result.is_valid
    assert diagnostic.status is (
        atp.ArubaTimestampDiagnosticStatus.FRAME_EPOCH_DISAGREEMENT
    )
    assert diagnostic.signed_difference_microseconds == Decimal("500000.000000000")
    assert diagnostic.absolute_difference_microseconds == Decimal("500000.000000000")


def test_frame_epoch_disagreement_never_replaces_aruba_timestamp():
    token = _raw_token(1_762_793_200, 212_540)
    without_epoch = _parse(token)
    with_epoch = _parse(token, frame_time_epoch="0.000000000")

    assert with_epoch.is_valid
    assert with_epoch.canonical_instant == without_epoch.canonical_instant


@pytest.mark.parametrize("frame_epoch", ["not-epoch", " 1.0", Decimal("NaN")])
def test_malformed_frame_epoch_is_non_authoritative_diagnostic(frame_epoch):
    result = _parse(_raw_token(10, 20), frame_time_epoch=frame_epoch)

    diagnostic = _comparison(
        result,
        atp.ArubaTimestampComparisonSource.FRAME_TIME_EPOCH,
    )
    assert result.is_valid
    assert diagnostic.status is atp.ArubaTimestampDiagnosticStatus.FRAME_EPOCH_MALFORMED
    assert result.canonical_instant == pd.Timestamp(10, unit="s", tz="UTC") + pd.Timedelta(
        microseconds=20
    )


def test_parser_does_not_consult_machine_timezone_or_configuration(monkeypatch):
    token = _raw_token(1_762_793_200, 212_540)
    expected = _parse(token)
    monkeypatch.setenv("TZ", "PST8PDT")
    monkeypatch.setenv("TIMEZONE", "Etc/GMT+12")
    monkeypatch.setenv("PLOT_TIMEZONE", "Pacific/Kiritimati")

    actual = _parse(token)

    assert actual == expected


def test_parser_is_deterministic_and_does_not_mutate_batch_inputs():
    request = atp.ArubaTimestampParseRequest(
        _raw_token(20, 30),
        atp.ARUBA_TIMESTAMP_CONTRACT_ID,
        atp.ARUBA_TIMESTAMP_CONTRACT_VERSION,
        frame_number=2,
    )
    requests = [request, request]
    before = list(requests)

    first = atp.parse_aruba_erm_timestamps(requests)
    second = atp.parse_aruba_erm_timestamps(requests)

    assert requests == before
    assert first == second
    assert len(first) == len(requests)
    assert [item.evidence.frame_number for item in first] == [2, 2]

    with pytest.raises(FrozenInstanceError):
        request.frame_number = 3  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        first[0].status = (  # type: ignore[misc]
            atp.ArubaTimestampDiagnosticStatus.INTERNAL_PARSER_ERROR
        )
