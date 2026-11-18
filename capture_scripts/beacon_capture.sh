#!/bin/bash
# Locate a BSSID using managed-mode scanning, then capture its channel.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
source "$SCRIPT_DIR/capture_common.sh" || exit 1
RESCAN_TIME=06:00
CAPTURE_ARGS=()
while (($#)); do
    case "$1" in
        --rescan-time)
            (($# >= 2)) || fail "Option '$1' requires a local time in HH:MM format."
            RESCAN_TIME=$2
            shift 2
            ;;
        --rescan-time=*)
            RESCAN_TIME=${1#*=}
            shift
            ;;
        -i|--interface)
            (($# >= 2)) || fail "Option '$1' requires an interface name."
            CAPTURE_ARGS+=("$1" "$2")
            shift 2
            ;;
        -h|--help)
            printf 'Usage: %s [-i INTERFACE | --interface INTERFACE] [--rescan-time HH:MM]\n' "${0##*/}"
            printf 'Select the capture interface (default: CAPTURE_INTERFACE or wlan0).\n'
            printf 'Re-scan daily at HH:MM in the capture host local time zone (24-hour clock; default: 06:00).\n'
            exit 0
            ;;
        *) CAPTURE_ARGS+=("$1"); shift ;;
    esac
done
[[ "$RESCAN_TIME" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || fail '--rescan-time must be a local time in HH:MM format, from 00:00 to 23:59.'
initialize_capture "${CAPTURE_ARGS[@]}"

schedule_capture_segment() {
    local now today offset day next_rescan seconds_until_rescan label
    now=$(date +%s) || fail 'Could not read the local clock.'
    today=$(date -d "@$now" +%F) || fail 'Could not determine the local date.'
    # Use calendar dates rather than adding 86400 seconds across DST changes.
    # A local time skipped by a clock change has no rescan that day.
    for offset in 0 1 2; do
        day=$(date -d "$today 12:00 $offset days" +%F) || fail 'Could not calculate the next local date.'
        next_rescan=$(date -d "$day $RESCAN_TIME" +%s 2>/dev/null) || continue
        ((next_rescan > now)) || continue
        seconds_until_rescan=$((next_rescan - now))
        DURATION=$REMAINING_CAPTURE_SECONDS
        if ((DURATION > seconds_until_rescan)); then
            DURATION=$seconds_until_rescan
            label=$(date -d "@$next_rescan" '+%Y-%m-%d %H:%M:%S %Z') || fail 'Could not format the next rescan time.'
            printf 'Next BSSID rescan scheduled for: %s\n' "$label"
        fi
        return 0
    done
    fail "Could not find the next local rescan time for $RESCAN_TIME."
}

prompt_location
while true; do
    prompt BSSID 'Enter BSSID to locate (e.g., 24:1f:bd:53:f0:60): '
    [[ "$BSSID" =~ ^([[:xdigit:]]{2}:){5}[[:xdigit:]]{2}$ ]] && break
    printf 'Invalid BSSID: use six hexadecimal pairs separated by colons.\n' >&2
done
BSSID=${BSSID,,}
prompt_duration 3600 hours
while true; do
    prompt SCOPE 'Capture scope: channel (all beacons) or bssid (one BSSID) [channel]: '
    SCOPE=${SCOPE:-channel}
    case "$SCOPE" in channel|bssid) break ;; esac
    printf 'Invalid scope: choose channel or bssid.\n' >&2
done
SCAN_ATTEMPTS=${CAPTURE_SCAN_ATTEMPTS:-6}
[[ "$SCAN_ATTEMPTS" =~ ^[0-9]{1,3}$ ]] || fail 'CAPTURE_SCAN_ATTEMPTS must be an integer from 1 to 100.'
SCAN_ATTEMPTS=$((10#$SCAN_ATTEMPTS))
((SCAN_ATTEMPTS >= 1 && SCAN_ATTEMPTS <= 100)) || fail 'CAPTURE_SCAN_ATTEMPTS must be an integer from 1 to 100.'
RESTORE_MANAGED=true
REMAINING_CAPTURE_SECONDS=$DURATION
SEGMENT=0
# Scan immediately, then end each segment at the next local rescan time.
while ((REMAINING_CAPTURE_SECONDS > 0)); do
    SEGMENT=$((SEGMENT + 1))
    set_interface_type managed
    FOUND_FREQUENCY=''
    for ((attempt=1; attempt<=SCAN_ATTEMPTS; attempt++)); do
        printf 'Searching for %s (attempt %s/%s)...\n' "$BSSID" "$attempt" "$SCAN_ATTEMPTS"
        if SCAN=$(iw dev "$INTERFACE" scan); then
            FOUND_FREQUENCY=$(printf '%s\n' "$SCAN" | awk -v target="$BSSID" '
                $1 == "BSS" {
                    address = $2
                    sub(/\(.*/, "", address)
                    matched = (tolower(address) == target)
                }
                # iw may report whole MHz as either 5805 or 5805.0.
                matched && $1 == "freq:" && $2 ~ /^[0-9]+([.]0+)?$/ {
                    frequency = $2
                    sub(/[.]0+$/, "", frequency)
                    print frequency
                    exit
                }
            ')
            [[ -z "$FOUND_FREQUENCY" ]] || break
        else
            printf 'Warning: scan failed; check driver support and scan permissions.\n' >&2
        fi
        if ((attempt < SCAN_ATTEMPTS)); then sleep 5 || fail 'Scan retry wait failed.'; fi
    done
    [[ -n "$FOUND_FREQUENCY" ]] || fail "BSSID $BSSID was not found after $SCAN_ATTEMPTS scan attempts."
    case "$FOUND_FREQUENCY" in
        24??) BAND=2.4 ;;
        5[0-8]??) BAND=5 ;;
        59??|6???|7???) BAND=6 ;;
        *) fail "Unsupported frequency from scan: $FOUND_FREQUENCY MHz." ;;
    esac
    resolve_tuning "$FOUND_FREQUENCY" || fail "Unsupported primary-channel frequency from scan: $FOUND_FREQUENCY MHz."
    FILTER='type mgt subtype beacon'
    if [[ "$SCOPE" == bssid ]]; then
        FILTER="$FILTER and wlan addr3 $BSSID"
        printf 'Warning: BSSID-only capture omits sibling SSIDs and cannot supply complete radio station totals.\n' >&2
    fi
    printf 'Found %s on %s MHz, %s GHz channel %s. Channel changes are followed at scheduled daily rescans.\n' "$BSSID" "$FREQ" "$BAND" "$CHANNEL"
    setup_monitor
    schedule_capture_segment
    prepare_output "Beacons_${LOCATION}_fr${FREQ}_segment${SEGMENT}"
    start_capture "$FILTER"
    await_capture
    printf 'Segment %s complete. Files saved to: %s\n' "$SEGMENT" "$OUTPUT_DIR"
    REMAINING_CAPTURE_SECONDS=$((REMAINING_CAPTURE_SECONDS - DURATION))
done
printf 'Capture complete. Files saved to: %s\n' "$OUTPUT_DIR"
