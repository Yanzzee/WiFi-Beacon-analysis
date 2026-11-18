from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "capture_scripts" / "beacon_capture_direct.sh"


@pytest.fixture
def capture_shell(tmp_path):
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required for capture-script tests")
    tools = tmp_path / "tools"
    tools.mkdir()
    output = tmp_path / "capture files"
    output.mkdir()
    log = tmp_path / "commands.log"
    # Every network/capture/permission command is mocked: no interface changes,
    # real captures, chmod of user data, or two-second waits during these tests.
    mock = r'''#!/bin/bash
tool=${0##*/}
printf '%s' "$tool" >> "$FAKE_LOG"
printf ' <%s>' "$@" >> "$FAKE_LOG"
printf '\n' >> "$FAKE_LOG"
case "$tool" in
    ip) [[ "$FAKE_FAIL_IP" != true ]] ;;
    date)
        if [[ ! -f "$FAKE_CLOCK_FILE" ]]; then
            /bin/date -d "$FAKE_NOW" +%s > "$FAKE_CLOCK_FILE" || exit 1
        fi
        capture_now=$(< "$FAKE_CLOCK_FILE")
        if [[ "$1" == -d ]]; then
            if [[ "$2" =~ ^[+]([0-9]+)[[:space:]]seconds$ ]]; then
                set -- -d "@$((capture_now + BASH_REMATCH[1]))" "${@:3}"
            fi
            /bin/date "$@"
        else
            /bin/date -d "@$capture_now" "$@"
        fi
        ;;
    iw)
        [[ "$FAKE_FAIL_IW" != true ]] || exit 1
        if [[ "$*" == *'set freq'* && "$FAKE_FAIL_TUNE" == true ]]; then exit 1; fi
        if [[ "$*" == *' scan' ]]; then
            [[ "$FAKE_SCAN_FAIL" != true ]] || exit 1
            [[ "$FAKE_SCAN_MISSING" != true ]] || exit 0
            printf 'BSS aa:bb:cc:dd:ee:ff(on wlan0)\n\tfreq: 2412\n'
            printf 'BSS 24:1f:bd:53:f0:60(on wlan0)\n\tfreq: %s\n' "${FAKE_SCAN_FREQUENCY:-5220}"
        fi
        ;;
    sleep)
        if [[ "$1" == 0.1 ]]; then /bin/sleep 0.1; fi
        if [[ "$1" == 2 && -n "$FAKE_SETUP_SECONDS" ]]; then
            if [[ ! -f "$FAKE_CLOCK_FILE" ]]; then
                /bin/date -d "$FAKE_NOW" +%s > "$FAKE_CLOCK_FILE" || exit 1
            fi
            capture_now=$(< "$FAKE_CLOCK_FILE")
            printf '%s\n' "$((capture_now + FAKE_SETUP_SECONDS))" > "$FAKE_CLOCK_FILE"
        fi
        exit 0
        ;;
    chmod) [[ "$FAKE_FAIL_CHMOD" != true ]] ;;
    tshark)
        printf '%s\n' "$$" > "$FAKE_PID_FILE"
        trap 'printf "tshark-terminated\n" >> "$FAKE_LOG"; exit 143' TERM
        while (( $# )); do
            if [[ "$1" == -w ]]; then prefix=$2; shift; fi
            if [[ "$1" == -a && "$2" == duration:* ]]; then capture_duration=${2#duration:}; fi
            shift
        done
        if [[ "$FAKE_BAD_NAME" == true ]]; then
            printf '%s\n' "$FAKE_OUTPUT/unrelated.pcapng"
            exit 0
        fi
        for index in 00001 00002; do
            file="${prefix%.pcapng}_${index}_20261012120000.pcapng"
            printf 'fake capture\n' > "$file"
            if [[ "$FAKE_SILENT_FILE" != true ]]; then
                printf '%s\n' "$file"
                /bin/sleep 0.1
            fi
        done
        if [[ "$FAKE_HOLD" == true ]]; then
            printf 'tshark-ready\n' >> "$FAKE_LOG"
            while true; do /bin/sleep 0.1; done
        fi
        # A permission acknowledgement before this line proves chmod runs on
        # closed files during capture, rather than only when TShark exits.
        capture_now=$(< "$FAKE_CLOCK_FILE")
        printf '%s\n' "$((capture_now + capture_duration))" > "$FAKE_CLOCK_FILE"
        printf 'tshark-finished\n' >> "$FAKE_LOG"
        exit "${FAKE_TSHARK_STATUS:-0}"
        ;;
esac
'''
    for name in ("ip", "iw", "tshark", "sleep", "chmod", "date"):
        tool = tools / name
        tool.write_text(mock, encoding="utf-8", newline="\n")
        tool.chmod(0o755)

    def run(inputs="LVES\n5\n5220\n0.5\n", *, script=SCRIPT, args=(), interrupt=False,
            interrupt_signal="TERM", **overrides):
        env = os.environ.copy()
        env.update(
            {
                "PATH": str(tools) + os.pathsep + env.get("PATH", ""),
                "CAPTURE_OUTPUT_DIR": output.as_posix(),
                "CAPTURE_INTERFACE": "wlan0",
                "FAKE_LOG": log.as_posix(),
                "FAKE_OUTPUT": output.as_posix(),
                "FAKE_PID_FILE": (tmp_path / "capture.pid").as_posix(),
                "FAKE_SHELL_PID_FILE": (tmp_path / "shell.pid").as_posix(),
                "FAKE_CLOCK_FILE": (tmp_path / "clock.txt").as_posix(),
                "FAKE_NOW": "2026-11-09 12:00:00",
                "TZ": "UTC0",
            }
        )
        env.update(overrides)
        tool_path = tools.as_posix()
        if os.name == "nt":
            tool_path = "/" + tools.drive[0].lower() + tool_path[2:]
        # Git Bash prepends its coreutils to PATH on startup. Set the mock path
        # inside Bash so chmod/sleep cannot resolve to the real commands.
        command = [
                bash, "--noprofile", "--norc", "-c",
                'PATH="$1:$PATH"; export PATH; printf "%s\\n" "$BASHPID" > "$FAKE_SHELL_PID_FILE"; '
                'capture_test_script=$2; shift 2; source "$capture_test_script" "$@"',
                "capture-script-test", tool_path, str(script), *args,
            ]
        if interrupt:
            # Kill only this invocation's main shell, simulating TERM while its
            # own mocked TShark is active. The cleanup must reap that child.
            import time

            process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            process.stdin.write(inputs.encode("utf-8"))
            process.stdin.close()
            process.stdin = None
            try:
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    if log.exists() and "tshark-ready" in log.read_text():
                        break
                    time.sleep(0.05)
                else:
                    pytest.fail("Mock capture did not start")
                # Windows terminate() bypasses Bash traps; use its signal tool.
                pid = int((tmp_path / "shell.pid").read_text().strip())
                subprocess.run([bash, "--noprofile", "--norc", "-c", 'kill -s "$2" "$1"',
                                "signal-capture", str(pid), interrupt_signal], check=True, timeout=5)
                stdout, stderr = process.communicate(timeout=10)
                result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
            finally:
                if process.poll() is None:
                    for pid_file in (tmp_path / "capture.pid", tmp_path / "shell.pid"):
                        if pid_file.exists():
                            subprocess.run(
                                [bash, "--noprofile", "--norc", "-c", 'kill -TERM "$1"',
                                 "cleanup-mock", pid_file.read_text().strip()], timeout=5)
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)
        else:
            result = subprocess.run(command, input=inputs.encode("utf-8"),
                                    capture_output=True, env=env, timeout=15)
        result.stdout = result.stdout.decode("utf-8")
        result.stderr = result.stderr.decode("utf-8")
        calls = log.read_text() if log.exists() else ""
        return result, calls, output

    run.output_dir = output
    return run


@pytest.mark.parametrize(
    "band,tuning,frequency,channel",
    [
        ("5", "44", "5220", "44"),
        ("5", "5220", "5220", "44"),
        ("5", "044", "5220", "44"),
        ("2.4", "1", "2412", "1"),
        ("2.4", "2412", "2412", "1"),
        ("2.4", "14", "2484", "14"),
        ("2.4", "2484", "2484", "14"),
        ("6", "81", "6355", "81"),
        ("6", "6355", "6355", "81"),
        ("6", "7115", "7115", "233"),
    ],
)
def test_channel_and_frequency_inputs(capture_shell, band, tuning, frequency, channel):
    result, calls, output = capture_shell(f"LVES\n{band}\n{tuning}\n.5\n")

    assert result.returncode == 0, result.stderr
    assert f"<{frequency}>" in calls
    assert f"Channel {channel}, Band {band} GHz" in result.stdout
    assert "<duration:1800>" in calls
    assert "<type mgt subtype beacon>" in calls
    assert "<pcapng>" in calls
    assert len(list(output.glob("*.pcapng"))) == 2
    assert "chmod <a+r> <-->" in calls


def test_invalid_input_reprompts_before_interface_changes(capture_shell):
    result, calls, _ = capture_shell(
        "../bad\nLVES\nbad-band\n5\nabc\n5210\n6355\n44\nzero\n0\n-1\n0.00001\n0.5\n"
    )

    assert result.returncode == 0, result.stderr
    assert "Invalid band" in result.stderr
    assert result.stderr.count("Invalid channel/frequency") == 3
    assert result.stderr.count("Invalid duration") == 4
    assert "start with a letter or digit" in result.stderr
    assert calls.count("ip <link> <set> <wlan0> <down>") == 1
    assert "iw <dev> <wlan0> <set> <freq> <5220>" in calls


@pytest.mark.parametrize("invalid", ["", "999999999999999", "$(touch injected)", "0", "1655"])
def test_invalid_input_then_eof_does_not_touch_interface(capture_shell, invalid):
    result, calls, output = capture_shell(f"LVES\n5\n{invalid}\n")

    assert result.returncode == 1
    assert "capture cancelled" in result.stderr
    assert calls == ""
    assert not list(output.iterdir())


def test_permission_applied_during_capture_and_not_to_previous_files(capture_shell):
    previous = capture_shell.output_dir / "Beacons_LVES_fr5220_previous.pcapng"
    previous.write_text("existing capture")
    result, calls, output = capture_shell()
    assert result.returncode == 0, result.stderr
    assert calls.index("chmod <a+r>") < calls.index("tshark-finished")
    assert "<printname:stdout>" in calls
    assert previous.name not in calls
    assert previous.read_text() == "existing capture"
    assert all("_fr5220_" in path.name for path in output.iterdir())


def test_capture_failure_retains_and_permissions_unannounced_files(capture_shell):
    result, calls, output = capture_shell(FAKE_TSHARK_STATUS="7", FAKE_SILENT_FILE="true")

    assert result.returncode == 7
    assert "capture stopped with status 7" in result.stderr
    assert "Capture complete" not in result.stdout
    assert len(list(output.glob("*.pcapng"))) == 2
    assert calls.count("chmod <a+r>") == 2


@pytest.mark.parametrize("failure", ["FAKE_FAIL_IP", "FAKE_FAIL_IW"])
def test_interface_failures_do_not_start_capture(capture_shell, failure):
    result, calls, output = capture_shell(**{failure: "true"})

    assert result.returncode == 1
    assert "Error:" in result.stderr
    assert "tshark <" not in calls
    assert not list(output.iterdir())


def test_permission_failure_is_not_reported_as_success(capture_shell):
    result, _, _ = capture_shell(FAKE_FAIL_CHMOD="true")

    assert result.returncode != 0
    assert "could not grant read permission" in result.stderr
    assert "Capture complete" not in result.stdout


def test_unannounced_permission_failure_is_not_reported_as_success(capture_shell):
    result, _, _ = capture_shell(FAKE_FAIL_CHMOD="true", FAKE_SILENT_FILE="true")

    assert result.returncode == 1
    assert "could not grant read permission" in result.stderr
    assert "Capture complete" not in result.stdout


def test_unrelated_printed_file_is_not_permissioned(capture_shell):
    result, calls, _ = capture_shell(FAKE_BAD_NAME="true")

    assert result.returncode == 1
    assert "unexpected capture filename" in result.stderr
    assert "chmod <" not in calls


def test_linux_line_endings():
    assert b"\r" not in SCRIPT.read_bytes()
