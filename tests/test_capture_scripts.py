from __future__ import annotations

from pathlib import Path
import re

import pytest

from test_beacon_capture_direct import capture_shell


ROOT = Path(__file__).resolve().parents[1] / "capture_scripts"


@pytest.mark.parametrize("script,inputs", [
    ("beacon_capture_direct.sh", "LVES\n5\n5220\n.5\n"),
    ("capture_direct.sh", "LVES\n5\n5220\n.5\n"),
    ("beacon_capture.sh", "LVES\n24:1f:bd:53:f0:60\n.5\n\n"),
    ("beacon_capture_band.sh", "LVES\n5\n.025\n.5\n"),
])
@pytest.mark.parametrize("args,environment,expected", [
    (("-i", "wlan1"), "wlan2", "wlan1"),
    (("--interface", "wlan1"), "wlan2", "wlan1"),
    (("--interface=wlan1",), "wlan2", "wlan1"),
    ((), "wlan2", "wlan2"),
    ((), "", "wlan0"),
])
def test_interface_selection_applies_to_all_commands(
    capture_shell, script, inputs, args, environment, expected,
):
    result, calls, _ = capture_shell(
        inputs, script=ROOT / script, args=args, CAPTURE_INTERFACE=environment,
    )

    assert result.returncode == 0, result.stderr
    assert f"ip <link> <show> <dev> <{expected}>" in calls
    assert f"tshark <-i> <{expected}>" in calls
    for call in calls.splitlines():
        if call.startswith("iw "):
            assert call.startswith(f"iw <dev> <{expected}> ")
        elif call.startswith("ip <link> <set>"):
            assert call.startswith(f"ip <link> <set> <{expected}> ")


@pytest.mark.parametrize("args,error", [
    (("-i",), "requires an interface name"),
    (("--interface",), "requires an interface name"),
    (("--interface=",), "Invalid capture interface name"),
    (("--interface", "bad name"), "Invalid capture interface name"),
    (("--interface", "$(touch injected)"), "Invalid capture interface name"),
    (("--unknown",), "Unknown argument"),
])
def test_invalid_options_fail_before_interface_changes(capture_shell, args, error):
    result, calls, output = capture_shell(args=args)

    assert result.returncode == 1
    assert error in result.stderr
    assert calls == ""
    assert not list(output.iterdir())


@pytest.mark.parametrize("script", [
    "beacon_capture_direct.sh", "capture_direct.sh",
    "beacon_capture.sh", "beacon_capture_band.sh",
])
def test_help_exits_before_prompts_or_interface_changes(capture_shell, script):
    result, calls, output = capture_shell("", script=ROOT / script, args=("--help",))

    assert result.returncode == 0, result.stderr
    assert "--interface INTERFACE" in result.stdout
    assert "wlan0" in result.stdout
    if script == "beacon_capture.sh":
        assert "--rescan-time HH:MM" in result.stdout
        assert "default: 06:00" in result.stdout
    assert calls == ""
    assert not list(output.iterdir())


def test_full_traffic_direct_has_shared_validation_without_beacon_filter(capture_shell):
    result, calls, output = capture_shell(script=ROOT / "capture_direct.sh")

    assert result.returncode == 0, result.stderr
    assert "<5220>" in calls
    assert " <-f> " not in calls
    assert "all traffic" in result.stderr
    assert all(path.name.startswith("Capture_LVES_fr5220_") for path in output.iterdir())
    assert "chmod <a+r>" in calls


@pytest.mark.parametrize("scan_frequency,frequency", [
    ("5220", "5220"),
    ("6355", "6355"),
    ("2412.0", "2412"),
    ("5805.0", "5805"),
    ("6355.0", "6355"),
    ("5805.000", "5805"),
])
def test_scan_capture_uses_exact_bssid_and_frequency(capture_shell, scan_frequency, frequency):
    result, calls, _ = capture_shell(
        "LVES\n24:1F:BD:53:F0:60\n.5\n\n",
        script=ROOT / "beacon_capture.sh", FAKE_SCAN_FREQUENCY=scan_frequency,
    )

    assert result.returncode == 0, result.stderr
    assert f"<freq> <{frequency}>" in calls
    assert "<type mgt subtype beacon>" in calls
    assert "addr3" not in calls
    assert calls.count("<type> <managed>") == 2


def test_scan_capture_does_not_round_fractional_frequency(capture_shell):
    result, calls, output = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n.5\n\n",
        script=ROOT / "beacon_capture.sh", FAKE_SCAN_FREQUENCY="5805.5",
        CAPTURE_SCAN_ATTEMPTS="1",
    )

    assert result.returncode == 1
    assert "tshark <" not in calls
    assert not list(output.iterdir())


def test_bssid_only_scope_remains_explicit_and_warns_about_siblings(capture_shell):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n.5\nbssid\n", script=ROOT / "beacon_capture.sh"
    )

    assert result.returncode == 0, result.stderr
    assert "<type mgt subtype beacon and wlan addr3 24:1f:bd:53:f0:60>" in calls
    assert "omits sibling SSIDs" in result.stderr


def test_multi_day_scan_uses_default_local_rescan_time_without_overwriting_segments(capture_shell):
    result, calls, output = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n48\nchannel\n", script=ROOT / "beacon_capture.sh"
    )

    assert result.returncode == 0, result.stderr
    assert calls.count("iw <dev> <wlan0> <scan>") == 3
    assert "<duration:64800>" in calls
    assert "<duration:86400>" in calls
    assert "<duration:21600>" in calls
    assert "2026-11-10 06:00:00 UTC" in result.stdout
    assert "2026-11-11 06:00:00 UTC" in result.stdout
    assert len(list(output.glob("*.pcapng"))) == 6


@pytest.mark.parametrize("args", [
    ("--rescan-time", "05:30", "-i", "wlan1"),
    ("--interface=wlan1", "--rescan-time=05:30"),
])
def test_scan_capture_accepts_custom_local_rescan_time(capture_shell, args):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n24\nchannel\n",
        script=ROOT / "beacon_capture.sh", args=args,
    )

    assert result.returncode == 0, result.stderr
    assert "2026-11-10 05:30:00 UTC" in result.stdout
    assert calls.count("iw <dev> <wlan1> <scan>") == 2
    assert "<duration:63000>" in calls
    assert "<duration:23400>" in calls


@pytest.mark.parametrize("args", [
    ("--rescan-time",),
    ("--rescan-time=",),
    ("--rescan-time", "6:00"),
    ("--rescan-time", "24:00"),
    ("--rescan-time", "06:60"),
    ("--rescan-time", "-01:00"),
    ("--rescan-time", "$(touch injected)"),
])
def test_invalid_rescan_time_fails_before_interface_changes(capture_shell, args):
    result, calls, output = capture_shell("", script=ROOT / "beacon_capture.sh", args=args)

    assert result.returncode == 1
    assert "HH:MM" in result.stderr
    assert calls == ""
    assert not list(output.iterdir())


@pytest.mark.parametrize("now,hours,durations", [
    ("2026-11-09 05:59:50", ".01", [10, 26]),
    ("2026-11-09 06:00:00", "24", [86400]),
    ("2026-11-09 06:00:01", "24", [86399, 1]),
    ("2026-11-09 05:59:50", ".001", [4]),
])
def test_scan_capture_rescan_boundaries_and_final_partial_segment(capture_shell, now, hours, durations):
    result, calls, _ = capture_shell(
        f"LVES\n24:1f:bd:53:f0:60\n{hours}\nchannel\n",
        script=ROOT / "beacon_capture.sh", FAKE_NOW=now,
    )

    assert result.returncode == 0, result.stderr
    assert calls.count("iw <dev> <wlan0> <scan>") == len(durations)
    assert [int(value) for value in re.findall(r"<-a> <duration:(\d+)>", calls)] == durations


def test_scan_capture_accounts_for_setup_time_without_daily_drift(capture_shell):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n25\nchannel\n",
        script=ROOT / "beacon_capture.sh", FAKE_NOW="2026-11-09 05:59:50",
        FAKE_SETUP_SECONDS="2",
    )

    assert result.returncode == 0, result.stderr
    assert "<duration:8>" in calls
    assert "<duration:86398>" in calls
    assert "<duration:3594>" in calls
    assert calls.count("iw <dev> <wlan0> <scan>") == 3


@pytest.mark.parametrize("now,first_duration", [
    ("2026-03-07 06:00:00", 23 * 3600),
    ("2026-10-31 06:00:00", 25 * 3600),
])
def test_scan_capture_keeps_local_rescan_time_across_dst(capture_shell, now, first_duration):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n26\nchannel\n",
        script=ROOT / "beacon_capture.sh", FAKE_NOW=now,
        TZ="MST7MDT,M3.2.0,M11.1.0",
    )

    assert result.returncode == 0, result.stderr
    assert f"<duration:{first_duration}>" in calls
    assert f"<duration:{26 * 3600 - first_duration}>" in calls
    assert calls.count("iw <dev> <wlan0> <scan>") == 2


def test_scan_capture_skips_nonexistent_local_rescan_time(capture_shell):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n48\nchannel\n",
        script=ROOT / "beacon_capture.sh", args=("--rescan-time", "02:30"),
        FAKE_NOW="2026-03-07 03:00:00", TZ="MST7MDT,M3.2.0,M11.1.0",
    )

    assert result.returncode == 0, result.stderr
    assert "2026-03-09 02:30:00 MDT" in result.stdout
    assert "<duration:167400>" in calls
    assert "<duration:5400>" in calls
    assert calls.count("iw <dev> <wlan0> <scan>") == 2


def test_scan_capture_uses_repeated_local_rescan_time_only_once(capture_shell):
    result, calls, _ = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n27\nchannel\n",
        script=ROOT / "beacon_capture.sh", args=("--rescan-time", "01:30"),
        FAKE_NOW="2026-11-01 00:00:00", TZ="MST7MDT,M3.2.0,M11.1.0",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.count("Next BSSID rescan scheduled for: 2026-11-01 01:30:00") == 1
    assert result.stdout.count("Next BSSID rescan scheduled for: 2026-11-02 01:30:00") == 1
    assert calls.count("iw <dev> <wlan0> <scan>") == 3
    assert "<duration:1800>" in calls


@pytest.mark.parametrize("condition", ["FAKE_SCAN_MISSING", "FAKE_SCAN_FAIL"])
def test_scan_retries_are_bounded_and_capture_never_starts(capture_shell, condition):
    result, calls, output = capture_shell(
        "LVES\n24:1f:bd:53:f0:60\n.5\n\n", script=ROOT / "beacon_capture.sh",
        CAPTURE_SCAN_ATTEMPTS="3", **{condition: "true"},
    )

    assert result.returncode == 1
    assert "not found after 3 scan attempts" in result.stderr
    assert calls.count("iw <dev> <wlan0> <scan>") == 3
    assert "tshark <" not in calls
    assert not list(output.iterdir())


def test_scan_rejects_bad_bssid_and_invalid_scope_before_scanning(capture_shell):
    result, calls, _ = capture_shell(
        "LVES\nnot-a-mac\n24:1f:bd:53:f0:60\n.5\ninvalid\nchannel\n",
        script=ROOT / "beacon_capture.sh",
    )

    assert result.returncode == 0, result.stderr
    assert "Invalid BSSID" in result.stderr
    assert "Invalid scope" in result.stderr
    assert calls.count("iw <dev> <wlan0> <scan>") == 1


def test_hopping_accepts_fractional_minutes_and_dwell(capture_shell):
    result, calls, _ = capture_shell("LVES\n2.4 5 6 5\n.025\n.5\n",
                                    script=ROOT / "beacon_capture_band.sh")

    assert result.returncode == 0, result.stderr
    assert "<duration:2>" in calls
    assert "<type mgt subtype beacon>" in calls
    assert "<type> <managed>" in calls
    assert "Gaps on other channels" in result.stderr
    assert "chmod <a+r>" in calls


def test_hopping_rejects_invalid_inputs_before_interface_changes(capture_shell):
    result, calls, _ = capture_shell("LVES\n\n7\n5\n0\n.025\n0\n-1\nabc\n.5\n",
                                    script=ROOT / "beacon_capture_band.sh")

    assert result.returncode == 0, result.stderr
    assert result.stderr.count("Invalid bands") == 2
    assert "Invalid duration" in result.stderr
    assert result.stderr.count("Invalid dwell time") == 3
    assert calls.count("ip <link> <set> <wlan0> <down>") == 2


def test_hopping_all_unsupported_channels_fails_before_capture(capture_shell):
    result, calls, output = capture_shell("LVES\n2.4\n.025\n.5\n",
                                         script=ROOT / "beacon_capture_band.sh", FAKE_FAIL_TUNE="true")

    assert result.returncode == 1
    assert "No requested channel is supported" in result.stderr
    assert "tshark <" not in calls
    assert "<type> <managed>" in calls
    assert not list(output.iterdir())


def test_hopping_capture_failure_restores_interface_and_retains_files(capture_shell):
    result, calls, output = capture_shell("LVES\n5\n.025\n.5\n",
                                         script=ROOT / "beacon_capture_band.sh", FAKE_TSHARK_STATUS="7")

    assert result.returncode == 7
    assert "Capture complete" not in result.stdout
    assert "<type> <managed>" in calls
    assert len(list(output.glob("*.pcapng"))) == 2


@pytest.mark.parametrize("signal,exit_status", [("TERM", 143), ("INT", 130)])
def test_hopping_signal_stops_child_and_restores_managed_mode(capture_shell, signal, exit_status):
    result, calls, output = capture_shell("LVES\n5\n10\n2\n",
                                         script=ROOT / "beacon_capture_band.sh",
                                         interrupt=True, interrupt_signal=signal, FAKE_HOLD="true")

    assert result.returncode == exit_status
    assert "tshark-terminated" in calls
    assert "<type> <managed>" in calls
    assert "Capture complete" not in result.stdout
    assert len(list(output.glob("*.pcapng"))) == 2


@pytest.mark.parametrize("script", ["capture_common.sh", "capture_direct.sh", "beacon_capture.sh", "beacon_capture_band.sh"])
def test_capture_script_lf_endings(script):
    assert b"\r" not in (ROOT / script).read_bytes()
