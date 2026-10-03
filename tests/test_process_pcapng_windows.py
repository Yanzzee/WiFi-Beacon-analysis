from __future__ import annotations

import csv
import io
import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import aruba_timestamp_parsing as atp
import process_pcapng as pp

TEST_FILES = (
    "Beacon_Capture_20251110090000_00012.pcapng",
    "Beacon_Capture_20251110100000_00013.pcapng",
    "Beacon_Capture_20251110110000_00014.pcapng",
)
PCAP_MAGIC = b"\xd4\xc3\xb2\xa1"


def _planned_fields(*, erm: bool) -> tuple[str, ...]:
    profile = pp.CaptureProfile(
        name="aruba_erm_type0" if erm else pp.CAPTURE_PROFILE_NATIVE,
        is_aruba_erm=erm,
        erm_type="aruba_erm_type0" if erm else None,
        detection_method="test",
    )
    return pp.build_tshark_extraction_plan(profile).fields


class _FakeTsharkProcess:
    def __init__(self, output: str):
        self.stdout = io.StringIO(output)
        self.stderr = io.StringIO("")
        self.returncode = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def kill(self):
        self.returncode = -9

    def wait(self):
        return self.returncode


def _raw_token(instant: str) -> str:
    timestamp = pd.Timestamp(instant).as_unit("ns")
    seconds, nanoseconds = divmod(timestamp.value, 1_000_000_000)
    raw = seconds.to_bytes(4, byteorder="big") + (nanoseconds // 1_000).to_bytes(
        4,
        byteorder="big",
    )
    return raw.hex(":")


def _export_row(
    display_token: str,
    raw_token: str,
    *,
    frame_epoch: str,
    frame_number: int,
    tsf_us: int | str = "",
    beacon_interval_tu: int | str = "",
    frequency_mhz: int | str = "",
    ds_channel: int | str = "",
    he_6ghz_channel: int | str = "",
    radio_channel: int | str = "",
) -> dict[str, object]:
    row = {field: "" for field in _planned_fields(erm=True)}
    row.update(
        {
            pp.ARUBA_DISPLAY_TIMESTAMP_FIELD: display_token,
            pp.ARUBA_RAW_TIMESTAMP_FIELD: raw_token,
            pp.ARUBA_FRAME_EPOCH_FIELD: frame_epoch,
            pp.ARUBA_FRAME_NUMBER_FIELD: frame_number,
            pp.ARUBA_TSF_FIELD: tsf_us,
            pp.ARUBA_BEACON_INTERVAL_FIELD: beacon_interval_tu,
            "wlan_radio.frequency": frequency_mhz,
            "wlan.ds.current_channel": ds_channel,
            "wlan.ext_tag.he_operation.6ghz.primary_channel": (
                he_6ghz_channel
            ),
            "wlan_radio.channel": radio_channel,
            "wlan.ta": f"00:00:00:00:00:{frame_number:02x}",
            "wlan.bssid": f"00:00:00:00:00:{frame_number:02x}",
            "wlan.vs.aruba.ap_name": "test-ap",
            "wlan.ssid": "test-ssid",
            "wlan.qbss.scount": frame_number,
            "wlan.qbss.cu": frame_number + 10,
            "wlan.qbss.adc": frame_number + 20,
        }
    )
    return row


def _mock_tshark_export(monkeypatch, rows: list[dict[str, object]]):
    output = pd.DataFrame(rows, columns=_planned_fields(erm=True)).to_csv(
        index=False,
        quoting=csv.QUOTE_ALL,
    )
    commands = []
    monkeypatch.setattr(pp, "resolve_tshark_executable", lambda: "tshark")
    monkeypatch.setattr(
        pp,
        "validate_supported_aruba_tshark_version",
        lambda executable: pp.SUPPORTED_ARUBA_TSHARK_VERSION,
    )

    def fake_popen(command, **kwargs):
        commands.append(command)
        return _FakeTsharkProcess(output)

    monkeypatch.setattr(pp.subprocess, "Popen", fake_popen)
    return commands


def _mock_native_tshark_export(monkeypatch, rows: list[dict[str, object]]):
    output = pd.DataFrame(rows, columns=_planned_fields(erm=False)).to_csv(
        index=False,
        quoting=csv.QUOTE_ALL,
    )
    commands = []
    monkeypatch.setattr(pp, "resolve_tshark_executable", lambda: "tshark")
    monkeypatch.setattr(pp, "read_tshark_version", lambda executable: "4.6.6")

    def fake_popen(command, **kwargs):
        commands.append(command)
        return _FakeTsharkProcess(output)

    monkeypatch.setattr(pp.subprocess, "Popen", fake_popen)
    return commands


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _assert_dataframes_equivalent(left: pd.DataFrame, right: pd.DataFrame) -> None:
    # Compare schema and values; ignore row ordering by sorting on index and columns.
    left_sorted = left.sort_index(axis=0).sort_index(axis=1)
    right_sorted = right.sort_index(axis=0).sort_index(axis=1)

    pd.testing.assert_frame_equal(
        left_sorted,
        right_sorted,
        check_like=True,
        check_dtype=False,
        rtol=1e-6,
        atol=1e-8,
    )


def test_validate_pcapng_capture_warns_for_legacy_pcap_extension(tmp_path: Path):
    legacy_named_capture = tmp_path / "legacy_name.pcap"
    legacy_named_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)

    with pytest.warns(UserWarning, match="\\.pcap extension but pcapng magic bytes"):
        pp.validate_pcapng_capture(legacy_named_capture)


def test_validate_capture_file_accepts_true_pcap_magic(tmp_path: Path):
    pcap_capture = tmp_path / "true_pcap.pcap"
    pcap_capture.write_bytes(PCAP_MAGIC + b"\x00" * 24)

    assert pp.validate_capture_file(pcap_capture) == "pcap"


def test_discover_pcapng_files_includes_legacy_pcap_named_pcapng(tmp_path: Path):
    pcapng_capture = tmp_path / "capture_a.pcapng"
    legacy_named_capture = tmp_path / "capture_b.pcap"
    ignored_file = tmp_path / "capture_c.txt"

    pcapng_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    legacy_named_capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    ignored_file.write_text("not a capture")

    discovered = [Path(path).name for path in pp.discover_pcapng_files(tmp_path)]

    assert discovered == ["capture_a.pcapng", "capture_b.pcap"]


def test_discover_pcapng_files_accepts_true_pcap_magic(tmp_path: Path):
    pcap_capture = tmp_path / "true_pcap.pcap"
    pcap_capture.write_bytes(PCAP_MAGIC + b"\x00" * 24)

    assert [Path(path).name for path in pp.discover_pcapng_files(tmp_path)] == [
        "true_pcap.pcap"
    ]


def test_capture_profile_detection_prefers_direct_native_beacons(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "native.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(
            returncode=0,
            stdout="00:11:22:33:44:55\n00:11:22:33:44:56\n",
            stderr="",
        )

    monkeypatch.setattr(pp.subprocess, "run", fake_run)

    profile = pp.detect_capture_profile(capture, "tshark")

    assert profile.name == pp.CAPTURE_PROFILE_NATIVE
    assert profile.is_aruba_erm is False
    assert profile.erm_type is None
    assert profile.plausible_beacon_count == 2
    assert len(calls) == 1
    assert "-d" not in calls[0]


@pytest.mark.parametrize("expected_type", pp.SUPPORTED_ARUBA_ERM_TYPES)
def test_capture_profile_detection_scores_all_supported_erm_decoders(
    tmp_path: Path,
    monkeypatch,
    expected_type: str,
):
    capture = tmp_path / "erm.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    counts = {
        "aruba_erm_type0": 0,
        "aruba_erm_type3": 1,
        "aruba_erm_type6": 2,
    }
    counts[expected_type] = 20

    def fake_run(command, **kwargs):
        decoder = next(
            (
                value.split(",", 1)[1]
                for value in command
                if value.startswith("aruba_erm.type,")
            ),
            None,
        )
        count = 0 if decoder is None else counts[decoder]
        return SimpleNamespace(
            returncode=0,
            stdout="00:11:22:33:44:55\n" * count,
            stderr="",
        )

    monkeypatch.setattr(pp.subprocess, "run", fake_run)

    profile = pp.detect_capture_profile(capture, "tshark")

    assert profile.name == expected_type
    assert profile.is_aruba_erm is True
    assert profile.erm_type == expected_type
    assert profile.plausible_beacon_count == 20


def test_capture_profile_detection_rejects_ambiguous_erm_result(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "ambiguous.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)

    def fake_run(command, **kwargs):
        has_decoder = any(
            value.startswith("aruba_erm.type,") for value in command
        )
        return SimpleNamespace(
            returncode=0,
            stdout="00:11:22:33:44:55\n" * (2 if has_decoder else 0),
            stderr="",
        )

    monkeypatch.setattr(pp.subprocess, "run", fake_run)

    with pytest.raises(pp.CaptureProfileDetectionError, match="ambiguous"):
        pp.detect_capture_profile(capture, "tshark")


@pytest.mark.requires_external_tools
@pytest.mark.parametrize(
    ("filename", "expected_profile"),
    [
        (
            "Aruba_ERM_Capture_635_type0_2026-09-14.pcapng",
            "aruba_erm_type0",
        ),
        (
            "Aruba_ERM_Capture_635_type3_2026-09-14.pcapng",
            "aruba_erm_type3",
        ),
        (
            "Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng",
            "aruba_erm_type6",
        ),
        (
            "Beacons_LVES_fr5220_00004_20260905195309.pcapng",
            pp.CAPTURE_PROFILE_NATIVE,
        ),
        (
            "Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng",
            pp.CAPTURE_PROFILE_NATIVE,
        ),
    ],
)
def test_owner_capture_profile_detection(
    filename: str,
    expected_profile: str,
):
    capture = Path("Data/captures_test") / filename
    if not capture.exists():
        pytest.skip(f"Owner capture is not available: {capture}")

    profile = pp.detect_capture_profile(capture)

    assert profile.name == expected_profile


@pytest.mark.requires_external_tools
@pytest.mark.parametrize(
    ("filename", "expected_profile", "aruba_time_expected"),
    [
        (
            "Aruba_ERM_Capture_635_type0_2026-09-14.pcapng",
            "aruba_erm_type0",
            True,
        ),
        (
            "Aruba_ERM_Capture_635_type3_2026-09-14.pcapng",
            "aruba_erm_type3",
            True,
        ),
        (
            "Aruba_ERM_Capture_635_radiotap_2026-09-14.pcapng",
            "aruba_erm_type6",
            False,
        ),
        (
            "Beacons_LVES_fr5220_00004_20260905195309.pcapng",
            pp.CAPTURE_PROFILE_NATIVE,
            False,
        ),
        (
            "Beacons_LVES-6GHz_fr6355_00001_20260917094712.pcapng",
            pp.CAPTURE_PROFILE_NATIVE,
            False,
        ),
    ],
)
def test_owner_capture_auto_conversion(
    tmp_path: Path,
    monkeypatch,
    filename: str,
    expected_profile: str,
    aruba_time_expected: bool,
):
    capture = Path("Data/captures_test") / filename
    if not capture.exists():
        pytest.skip(f"Owner capture is not available: {capture}")
    portable_tshark = Path(
        r"C:\temp\WiresharkPortable64\App\Wireshark\tshark.exe"
    )
    if portable_tshark.exists():
        monkeypatch.setattr(pp, "TSHARK_EXECUTABLE", str(portable_tshark))

    output_path = pp.process_pcapng(capture, tmp_path)
    output = pd.read_parquet(output_path)

    assert not output.empty
    assert output[pp.CAPTURE_PROFILE_FIELD].eq(expected_profile).all()
    assert output[pp.CAPTURE_PROFILE_DETECTION_FIELD].eq(
        "content_probe"
    ).all()
    assert output[pp.CANONICAL_FRAME_TIME_FIELD].notna().all()
    assert output[pp.CANONICAL_TSF_FIELD].notna().any()
    assert bool(
        output[pp.CANONICAL_ARUBA_TIME_FIELD].notna().any()
    ) is aruba_time_expected
    if filename == "Beacons_LVES_fr5220_00004_20260905195309.pcapng":
        extreme_names = output["wlan.vs.extreme.ap_name"].astype("string")
        extreme_rows = extreme_names.notna() & extreme_names.ne("<MISSING>")
        assert int(extreme_rows.sum()) == 33_977
        assert extreme_names.loc[extreme_rows].nunique() == 14
        assert output.loc[extreme_rows, "wlan.bssid"].nunique() == 39
        assert set(output.loc[extreme_rows, "frequency_mhz"].dropna()) == {
            5220
        }
        assert output.loc[extreme_rows, "wlan.qbss.cu"].notna().all()
    if expected_profile == "aruba_erm_type3":
        assert output["aruba_erm.channel"].notna().all()
    if expected_profile == "aruba_erm_type6":
        assert output["radiotap.channel.freq"].notna().all()


def test_capture_source_ranks_follow_normalized_path_order(tmp_path: Path):
    paths = [tmp_path / "z.pcapng", tmp_path / "A.pcapng"]

    ranked = pp.rank_capture_sources(reversed(paths))

    assert [(Path(path).name, rank) for path, rank in ranked] == [
        ("A.pcapng", 0),
        ("z.pcapng", 1),
    ]


def test_supported_aruba_tshark_version_is_required(monkeypatch):
    monkeypatch.setattr(
        pp.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="TShark (Wireshark) 4.6.6 (Git revision)\n",
        ),
    )

    assert pp.validate_supported_aruba_tshark_version("tshark") == "4.6.6"

    monkeypatch.setattr(
        pp.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="TShark (Wireshark) 4.8.0\n",
        ),
    )
    with pytest.raises(pp.ArubaTimestampConversionError) as error:
        pp.validate_supported_aruba_tshark_version("tshark")
    assert error.value.diagnostic_code == "unsupported_aruba_timestamp_contract"
    assert "Expected TShark 4.6.6; observed 4.8.0" in str(error.value)


def test_process_pcapng_uses_raw_aruba_contract_and_preserves_legacy_output(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "synthetic.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    display_tokens = [
        "2025-11-10T09:00:00.212540000-0700",
        "2025-11-10T09:00:01.654321000-0700",
    ]
    raw_tokens = [
        _raw_token("2025-11-10T16:00:00.212540Z"),
        _raw_token("2025-11-10T16:00:01.654321Z"),
    ]
    rows = [
        _export_row(
            display,
            raw,
            frame_epoch=epoch,
            frame_number=number,
            tsf_us=1_000_000 + ((number - 1) * 102_400),
            beacon_interval_tu=100,
            frequency_mhz=2462 if number == 1 else 6775,
            ds_channel=11 if number == 1 else "",
            he_6ghz_channel="" if number == 1 else 165,
            radio_channel=11 if number == 1 else 165,
        )
        for display, raw, epoch, number in zip(
            display_tokens,
            raw_tokens,
            ["1762790399.700000000", "1762790401.100000000"],
            [1, 2],
        )
    ]
    commands = _mock_tshark_export(monkeypatch, rows)
    parser_calls = []
    real_parser = pp.parse_aruba_erm_timestamp

    def observing_parser(raw_token, **kwargs):
        parser_calls.append((raw_token, kwargs.copy()))
        return real_parser(raw_token, **kwargs)

    monkeypatch.setattr(pp, "parse_aruba_erm_timestamp", observing_parser)

    output_file = pp.process_pcapng(capture, output_dir, erm=True)

    assert len(commands) == 1
    requested_fields = [
        commands[0][index + 1]
        for index, value in enumerate(commands[0][:-1])
        if value == "-e"
    ]
    assert requested_fields == list(_planned_fields(erm=True))
    assert commands[0][commands[0].index("-Y") + 1] == (
        "wlan.fc.type_subtype == 8"
    )
    assert pp.ARUBA_RAW_TIMESTAMP_FIELD in requested_fields
    assert pp.ARUBA_FRAME_EPOCH_FIELD in requested_fields
    assert pp.ARUBA_FRAME_NUMBER_FIELD in requested_fields
    assert pp.ARUBA_TSF_FIELD in requested_fields
    assert pp.ARUBA_BEACON_INTERVAL_FIELD in requested_fields
    assert [call[0] for call in parser_calls] == raw_tokens
    assert [call[1]["contract_id"] for call in parser_calls] == [
        atp.ARUBA_TIMESTAMP_CONTRACT_ID,
        atp.ARUBA_TIMESTAMP_CONTRACT_ID,
    ]
    assert [call[1]["contract_version"] for call in parser_calls] == [1, 1]

    output = pd.read_parquet(output_file)
    assert output.index.tolist() == display_tokens
    assert output.index.name == pp.ARUBA_DISPLAY_TIMESTAMP_FIELD
    assert not set(pp.ARUBA_TRANSIENT_TIMESTAMP_FIELDS).intersection(output.columns)
    assert output["wlan.qbss.cu"].tolist() == [11, 12]
    assert output[pp.CANONICAL_ARUBA_TIME_FIELD].tolist() == [
        pd.Timestamp("2025-11-10T16:00:00.212540Z"),
        pd.Timestamp("2025-11-10T16:00:01.654321Z"),
    ]
    assert output[pp.CANONICAL_FRAME_TIME_FIELD].tolist() == [
        pd.Timestamp("2025-11-10T15:59:59.700000Z"),
        pd.Timestamp("2025-11-10T16:00:01.100000Z"),
    ]
    assert output[pp.CANONICAL_TSF_FIELD].tolist() == [1_000_000, 1_102_400]
    assert output[pp.CANONICAL_BEACON_INTERVAL_FIELD].tolist() == [100, 100]
    assert output["frequency_mhz"].tolist() == [2462, 6775]
    assert output["wlan.ds.current_channel"].tolist() == [11, pd.NA]
    assert output[
        "wlan.ext_tag.he_operation.6ghz.primary_channel"
    ].tolist() == [pd.NA, 165]
    assert output["wlan_radio.channel"].tolist() == [11, 165]
    assert output[pp.SOURCE_CAPTURE_FIELD].tolist() == [
        "synthetic.pcapng",
        "synthetic.pcapng",
    ]
    assert output[pp.SOURCE_FRAME_FIELD].tolist() == [1, 2]
    assert output[pp.SOURCE_RANK_FIELD].tolist() == [0, 0]
    assert output[pp.CAPTURE_PROFILE_FIELD].tolist() == [
        "aruba_erm_type0",
        "aruba_erm_type0",
    ]
    assert output[pp.CAPTURE_PROFILE_DETECTION_FIELD].eq("explicit").all()
    assert output[pp.ERM_TYPE_FIELD].eq("aruba_erm_type0").all()


def test_process_native_capture_retains_canonical_timing_and_lineage(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "native.pcap"
    capture.write_bytes(PCAP_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    row = {field: "" for field in _planned_fields(erm=False)}
    row.update(
        {
            pp.ARUBA_FRAME_EPOCH_FIELD: "1762790400.212540000",
            pp.ARUBA_FRAME_NUMBER_FIELD: 7,
            pp.ARUBA_TSF_FIELD: "1000000",
            pp.ARUBA_BEACON_INTERVAL_FIELD: "100",
            "wlan.bssid": "00:11:22:33:44:55",
            "wlan.ta": "00:11:22:33:44:55",
            "wlan.vs.extreme.ap_name": "EXTREME-AP-01",
            "wlan.ssid": "test-ssid",
            "wlan.qbss.cu": "128",
            "radiotap.channel.freq": "5520",
            "wlan_radio.channel": "104",
        }
    )
    commands = _mock_native_tshark_export(monkeypatch, [row])

    output = pd.read_parquet(
        pp.process_pcapng(capture, output_dir, erm=False, source_rank=2)
    )

    assert len(commands) == 1
    assert not any(value.startswith("aruba_erm.type,") for value in commands[0])
    assert output[pp.CANONICAL_ARUBA_TIME_FIELD].isna().all()
    assert output[pp.CANONICAL_FRAME_TIME_FIELD].tolist() == [
        pd.Timestamp("2025-11-10T16:00:00.212540Z")
    ]
    assert output[pp.CANONICAL_TSF_FIELD].tolist() == [1_000_000]
    assert output[pp.CANONICAL_BEACON_INTERVAL_FIELD].tolist() == [100]
    assert output["wlan.vs.extreme.ap_name"].tolist() == ["EXTREME-AP-01"]
    assert output[pp.SOURCE_FRAME_FIELD].tolist() == [7]
    assert output[pp.SOURCE_RANK_FIELD].tolist() == [2]
    assert output[pp.CAPTURE_PROFILE_FIELD].tolist() == [
        pp.CAPTURE_PROFILE_NATIVE
    ]
    assert output[pp.CAPTURE_PROFILE_DETECTION_FIELD].eq("explicit").all()
    assert output[pp.ERM_TYPE_FIELD].isna().all()


def test_process_pcapng_rejects_malformed_raw_without_frame_epoch_fallback(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "malformed.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    display = "2025-11-10T09:00:00.212540000-0700"
    rows = [
        _export_row(
            display,
            "malformed",
            frame_epoch="1762790400.212540000",
            frame_number=17,
        )
    ]
    _mock_tshark_export(monkeypatch, rows)

    with pytest.raises(pp.ArubaTimestampConversionError) as error:
        pp.process_pcapng(capture, output_dir, erm=True)

    assert error.value.diagnostic_code == "aruba_timestamp_malformed"
    assert error.value.contract_id == atp.ARUBA_TIMESTAMP_CONTRACT_ID
    assert error.value.contract_version == 1
    assert error.value.row_position == 1
    assert error.value.frame_number == 17
    assert not (output_dir / "malformed.pcapng.parquet").exists()


def test_process_pcapng_uses_frame_epoch_when_aruba_time_is_absent(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "frame-fallback.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    rows = [
        _export_row(
            "",
            "",
            frame_epoch="1762790400.212540000",
            frame_number=1,
            frequency_mhz=6775,
            he_6ghz_channel=165,
            radio_channel=165,
        )
    ]
    _mock_tshark_export(monkeypatch, rows)

    output = pd.read_parquet(
        pp.process_pcapng(capture, output_dir, erm=True)
    )

    assert output[pp.CANONICAL_ARUBA_TIME_FIELD].isna().all()
    assert output[pp.CANONICAL_FRAME_TIME_FIELD].tolist() == [
        pd.Timestamp("2025-11-10T16:00:00.212540Z")
    ]


def test_process_pcapng_rejects_row_without_either_observation_time(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "missing-times.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    rows = [
        _export_row(
            "",
            "",
            frame_epoch="",
            frame_number=1,
        )
    ]
    _mock_tshark_export(monkeypatch, rows)

    with pytest.raises(pp.ArubaTimestampConversionError) as error:
        pp.process_pcapng(capture, output_dir, erm=True)

    assert error.value.diagnostic_code == "source_timestamp_unavailable"
    assert not (output_dir / "missing-times.pcapng.parquet").exists()


def test_process_pcapng_rejects_display_conflict_without_repair(
    tmp_path: Path,
    monkeypatch,
):
    capture = tmp_path / "display-conflict.pcapng"
    capture.write_bytes(pp.PCAPNG_MAGIC + b"\x00" * 24)
    output_dir = tmp_path / "processed"
    rows = [
        _export_row(
            "2025-11-10T09:00:01.212540000-0700",
            _raw_token("2025-11-10T16:00:00.212540Z"),
            frame_epoch="1762790400.212540000",
            frame_number=1,
        )
    ]
    _mock_tshark_export(monkeypatch, rows)

    with pytest.raises(pp.ArubaTimestampConversionError) as error:
        pp.process_pcapng(capture, output_dir, erm=True)

    assert error.value.diagnostic_code == "aruba_timestamp_render_conflict"
    assert not (output_dir / "display-conflict.pcapng.parquet").exists()


def test_timestamp_validation_counts_missing_epoch_and_reused_frame_number():
    display_tokens = [
        "2025-11-10T09:00:00.212540000-0700",
        "2025-11-10T09:00:01.654321000-0700",
    ]
    frame = pd.DataFrame(
        {
            pp.ARUBA_RAW_TIMESTAMP_FIELD: [
                _raw_token("2025-11-10T16:00:00.212540Z"),
                _raw_token("2025-11-10T16:00:01.654321Z"),
            ],
            pp.ARUBA_FRAME_EPOCH_FIELD: [None, "1762790401.654321000"],
            pp.ARUBA_FRAME_NUMBER_FIELD: [100_000, 1],
        },
        index=pd.Index(display_tokens, name=pp.ARUBA_DISPLAY_TIMESTAMP_FIELD),
    )

    summary = pp.validate_aruba_timestamp_rows(frame, source_path="synthetic.pcapng")

    assert summary.row_count == 2
    assert summary.valid_count == 2
    assert summary.display_match_count == 2
    assert summary.frame_epoch_match_count == 1
    assert summary.frame_epoch_unavailable_count == 1
    assert summary.reused_frame_number_count == 1


@pytest.mark.requires_external_tools
@pytest.mark.pre_analysis_short
@pytest.mark.skipif(
    shutil.which(os.getenv("TSHARK_PATH", "tshark")) is None,
    reason="tshark is not available on PATH",
)
def test_process_pcapng_windows_matches_known_good(
    tmp_path: Path,
    short_captures_dir: Path,
    short_processed_dir: Path,
):
    captures_dir = short_captures_dir
    processed_dir = short_processed_dir
    assert captures_dir.exists(), f"{captures_dir} directory must exist"
    assert processed_dir.exists(), f"{processed_dir} directory must exist"

    output_dir = tmp_path / "processed_data"
    output_dir.mkdir(parents=True, exist_ok=True)

    for filename in TEST_FILES:
        input_path = captures_dir / filename
        expected_path = processed_dir / f"{filename}.parquet"
        assert input_path.exists(), f"Missing input file: {input_path}"
        assert expected_path.exists(), f"Missing expected output file: {expected_path}"

    result_files = []
    for filename in TEST_FILES:
        output_file = pp.process_pcapng(captures_dir / filename, output_dir, erm=True)
        result_files.append(output_file)

    for filename in TEST_FILES:
        expected_file = processed_dir / f"{filename}.parquet"
        actual_file = output_dir / f"{filename}.parquet"

        assert actual_file.exists(), f"Output parquet missing: {actual_file}"

        expected_df = _read_parquet(expected_file)
        actual_df = _read_parquet(actual_file)
        _assert_dataframes_equivalent(actual_df[expected_df.columns], expected_df)
        assert {
            pp.CANONICAL_ARUBA_TIME_FIELD,
            pp.CANONICAL_FRAME_TIME_FIELD,
            pp.CANONICAL_TSF_FIELD,
            pp.CANONICAL_BEACON_INTERVAL_FIELD,
            pp.SOURCE_CAPTURE_FIELD,
            pp.SOURCE_FRAME_FIELD,
            pp.SOURCE_RANK_FIELD,
        }.issubset(actual_df.columns)


@pytest.mark.requires_external_tools
@pytest.mark.pre_analysis_short
@pytest.mark.skipif(
    shutil.which(os.getenv("TSHARK_PATH", "tshark")) is None,
    reason="tshark is not available on PATH",
)
def test_process_pcapng_windows_timestamp_source_contract(
    tmp_path: Path,
    short_captures_dir: Path,
    short_processed_dir: Path,
    monkeypatch,
):
    output_dir = tmp_path / "processed_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    real_parser = pp.parse_aruba_erm_timestamp

    for filename in TEST_FILES:
        input_path = short_captures_dir / filename
        expected_path = short_processed_dir / f"{filename}.parquet"
        assert input_path.exists(), f"Missing input file: {input_path}"
        assert expected_path.exists(), f"Missing expected output file: {expected_path}"

        stats = {
            "calls": 0,
            "invalid": 0,
            "display_matches": 0,
            "epoch_disagreements": 0,
            "frame_resets": 0,
            "previous_frame": None,
        }

        def observing_parser(raw_token, **kwargs):
            result = real_parser(raw_token, **kwargs)
            stats["calls"] += 1
            if not result.is_valid:
                stats["invalid"] += 1
            for diagnostic in result.comparison_diagnostics:
                if diagnostic.status is atp.ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MATCH:
                    stats["display_matches"] += 1
                if diagnostic.status is atp.ArubaTimestampDiagnosticStatus.FRAME_EPOCH_DISAGREEMENT:
                    stats["epoch_disagreements"] += 1
            frame_number = kwargs["frame_number"]
            previous_frame = stats["previous_frame"]
            if (
                frame_number is not None
                and previous_frame is not None
                and frame_number <= previous_frame
            ):
                stats["frame_resets"] += 1
            stats["previous_frame"] = frame_number
            return result

        monkeypatch.setattr(pp, "parse_aruba_erm_timestamp", observing_parser)
        actual_path = Path(pp.process_pcapng(input_path, output_dir, erm=True))
        expected = _read_parquet(expected_path)
        actual = _read_parquet(actual_path)

        assert stats["calls"] == len(expected)
        assert stats["invalid"] == 0
        assert stats["display_matches"] == len(expected)
        assert stats["epoch_disagreements"] == len(expected)
        assert stats["frame_resets"] > 0
        assert actual.index.equals(expected.index)
        pd.testing.assert_frame_equal(
            actual[expected.columns],
            expected,
            check_dtype=False,
            rtol=1e-6,
            atol=1e-8,
        )
        assert not set(pp.ARUBA_TRANSIENT_TIMESTAMP_FIELDS).intersection(actual.columns)
