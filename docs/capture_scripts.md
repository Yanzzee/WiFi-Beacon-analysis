# Linux capture scripts

These scripts acquire native over-the-air captures. They are separate from
`config.env` and the analysis CLI, and require a Linux monitor-mode adapter,
`ip`, `iw`, TShark, and appropriate interface/capture privileges. Acquisition
does not automatically start analysis or modify an existing analysis run.

Copy the entire `capture_scripts/` folder to the Linux host. Keep
`capture_scripts/capture_common.sh` beside **all four entry scripts**.
The shared helper requires Bash 4.1 or newer. This avoids
maintaining separate channel conversion, validation, and file-permission code.

All four entry scripts accept `-i INTERFACE` or `--interface INTERFACE`:

```bash
sudo bash capture_scripts/beacon_capture_direct.sh --interface wlan1
```

The flag overrides `CAPTURE_INTERFACE`. Without the flag, scripts use
`CAPTURE_INTERFACE` if nonempty, otherwise `wlan0`, preserving the existing
default. `--interface=wlan1` also works. Use `--help` for usage without starting
a capture.

## Recommended single-channel beacon capture

From the repository root, run `sudo bash capture_scripts/beacon_capture_direct.sh`.
Prompts accept:

1. Location tag (letters/digits, dots, underscores, hyphens; no path separators).
2. Band: `2.4`, `5`, or `6`.
3. Either a channel number or its primary-channel center frequency in integer
   MHz: for example, band `5` with `44` or `5220`, or band `6` with `81` or `6355`.
4. Positive hours, including fractional values such as `0.5` or `.5`.

The frequency is the center of the **primary beacon channel**, not the center
of an entire 40/80/160/320 MHz bonded channel. Invalid inputs are explained and
re-prompted before changing the interface; end-of-input cancels. Missing tools,
an unavailable interface, failed tuning, or failed capture produce a nonzero
exit and a clear message, not a misleading success message.

The existing supported channel set is retained: 2.4 GHz channels 1-14; 5 GHz
36-64 and 100-144 in steps of four, plus 149/153/157/161/165; and 6 GHz
1/5/9/.../233. Channel 14 is correctly mapped to 2484 MHz. Actual availability
is subject to the adapter, driver, and regulatory domain; the script does not
change country settings or bypass restrictions. The channel-14 special case
matches the [Linux frequency conversion](https://github.com/torvalds/linux/blob/master/net/wireless/util.c).

Defaults remain `wlan0`, `/var/tmp`, pcapng, beacon-only capture, and hourly
rotation. Override the interface/output directory with `CAPTURE_INTERFACE` and
`CAPTURE_OUTPUT_DIR`; the output directory must already exist and be writable.
The interface remains in monitor mode, as before.

Filenames retain the `Beacons_<location>_fr<frequency>` prefix and add a start
timestamp and process ID to separate invocations. Each completed rotated file
receives `chmod a+r`, without adding write/execute permissions or changing
ownership. Exit cleanup also permissions this invocation's retained partial
files if capture fails; it does not modify older captures. TShark's
[`-b printname:stdout`](https://www.wireshark.org/docs/man-pages/tshark.html)
provides closed-file notifications; its `-g` option alone grants group-read,
not read access for all users. Parent directories must also permit traversal
for other users to reach the files. World-readable captures can expose SSIDs,
MAC addresses, and other beacon metadata; this is intentional here.

All shell scripts are pinned to LF endings for Linux checkouts. Regression
tests mock interface/capture commands: Windows verification does not tune a
real adapter or prove Linux hardware compatibility.

## Full-traffic direct capture

`sudo bash capture_scripts/capture_direct.sh` has the same band/channel/frequency/hour prompts
and validation as the beacon-only direct script, but deliberately captures all
traffic on the selected channel. It prints that distinction, writes
`Capture_<location>_fr<frequency>...pcapng`, and uses the same readable-file
handling. It does not silently add a beacon filter to the all-traffic workflow.

## Scan-based beacon capture

`sudo bash capture_scripts/beacon_capture.sh` prompts for location, target BSSID, total hours,
and capture scope. It uses [managed-mode `iw` scanning](https://wireless.docs.kernel.org/en/latest/en/users/documentation/iw.html)
to locate the exact BSSID and its frequency, avoiding ambiguous band/channel
inference and the old `iwlist`/`iwconfig` dependency. Scanning is limited to six
attempts, five seconds apart; set `CAPTURE_SCAN_ATTEMPTS` to an integer 1-100
to change that bound. Missing BSSIDs and scan failures cannot retry forever.

The default `channel` scope captures **all beacons on the discovered channel**,
including sibling BSSIDs used for station totals. Explicit `bssid` scope keeps
a narrow one-BSSID capture and warns that sibling SSIDs are omitted. Analysis
still scopes channel-wide captures using configured AP names.

The script scans immediately at startup, then re-scans daily at **06:00 in the
capture host's local time zone**. Select another time with `--rescan-time HH:MM`
(24-hour clock); `--rescan-time=HH:MM` also works:

```bash
sudo bash capture_scripts/beacon_capture.sh -i wlan0 --rescan-time 05:30
```

Each capture segment ends at the next scheduled rescan, or when the requested
capture hours are exhausted, whichever comes first. For example, a run started
at 14:00 with the default schedule first rescans the next morning at 06:00.
Calendar dates keep the selected local time across daylight saving changes;
a selected time that does not exist during a clock change is skipped that day,
and a repeated local time is used once. The capture host's time zone, rather
than the computer used to SSH into it, determines the schedule.

Enter `168` hours for seven days of capture. Distinct segment prefixes prevent
overwrite; scan/setup overhead is in addition to the requested capture hours.
Hourly output rotation does not trigger a rescan. Channel changes within a
segment are not followed continuously; each scheduled rescan briefly interrupts
reception, and a failed rescan stops that run after the configured scan attempts.
The script restores managed mode on completion, failure, or interruption.

## Channel-hopping beacon capture

`sudo bash capture_scripts/beacon_capture_band.sh` prompts for location, a space-separated band
list, minutes, and dwell seconds. Fractional minutes and positive fractional
dwell values work; duplicates in the band list are removed. Minute totals are
rounded to whole capture seconds, with a minimum of one second. Dwell is at
most 3600 seconds and clipped to remaining capture time. The 2.4 GHz hopping
set remains channels 1-11; other bands retain the sets above.

Unsupported channels are warned about and skipped. An entirely unsupported
request fails before capture. TShark has its own total-duration limit, and
early capture failure is detected while hopping. Ctrl+C/TERM cleanup stops and
waits for this invocation's capture process, permissions retained files, and
restores managed mode; it does not signal unrelated analysis processes.

All scripts now default to `/var/tmp`, including the hopping script (formerly
the sudo user's home `captures` folder). Set `CAPTURE_OUTPUT_DIR` explicitly
to use another existing directory. There is no `eval`-based home expansion or
assumption that a sudo username is also its group, and ownership is unchanged.
Rotated captures have `.pcapng` from the start; no post-capture renaming is
needed. They receive the same world-readable handling as the direct scripts.

### Limits that cannot be fixed by input validation

A single hopping adapter receives only its currently tuned channel. Beacons on
other channels are **unobserved**, not evidence of a stopped AP or zero client
count/CU. Short visits may miss beacons, and channels in different bands are
not measured simultaneously. This is suitable for reconnaissance, not
continuous coverage or simultaneous classroom totals. Use fixed-channel
captures with enough independent adapters to monitor all required channels
concurrently when those metrics matter. Hardware, driver, and regulatory
availability also remain real constraints; none of these scripts override them.
