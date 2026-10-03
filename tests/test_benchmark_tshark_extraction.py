from __future__ import annotations

import pytest

from benchmark_tshark_extraction import _probe_invocation_count, _record_count
import process_pcapng as pp


def _profile(*, erm: bool) -> pp.CaptureProfile:
    return pp.CaptureProfile(
        name="aruba_erm_type0" if erm else pp.CAPTURE_PROFILE_NATIVE,
        is_aruba_erm=erm,
        erm_type="aruba_erm_type0" if erm else None,
        detection_method="test",
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"", 0),
        (b'"header"\n', 0),
        (b'"header"\n"one"\n', 1),
        (b'"header"\r\n"one"\r\n"two"', 2),
    ],
)
def test_record_count_excludes_tshark_header(payload: bytes, expected: int):
    assert _record_count(payload) == expected


def test_probe_count_documents_current_bounded_detection_work():
    assert _probe_invocation_count(_profile(erm=False)) == 1
    assert _probe_invocation_count(_profile(erm=True)) == 4
