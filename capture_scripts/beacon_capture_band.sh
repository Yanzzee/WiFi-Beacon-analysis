#!/bin/bash
# Channel-hopping beacon capture: sampling gaps are expected, not continuous coverage.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
source "$SCRIPT_DIR/capture_common.sh" || exit 1
initialize_capture "$@"
prompt_location
while true; do
    prompt BAND_INPUT 'Enter bands to scan (e.g., 2.4 5 6): '
    read -r -a REQUESTED_BANDS <<< "$BAND_INPUT"
    VALID_BANDS=true
    ((${#REQUESTED_BANDS[@]} > 0)) || VALID_BANDS=false
    for band in "${REQUESTED_BANDS[@]}"; do
        case "$band" in 2.4|5|6) ;; *) VALID_BANDS=false ;; esac
    done
    [[ "$VALID_BANDS" == true ]] && break
    printf 'Invalid bands: enter one or more of 2.4, 5, and 6, separated by spaces.\n' >&2
done
prompt_duration 60 minutes
while true; do
    prompt DWELL_TIME 'Enter dwell time per channel in seconds (e.g., 0.5, 2): '
    if [[ ${#DWELL_TIME} -le 20 && "$DWELL_TIME" =~ ^[0-9]*[.]?[0-9]+$ ]] &&
        LC_ALL=C awk -v dwell="$DWELL_TIME" 'BEGIN { exit !(dwell > 0 && dwell <= 3600) }'; then
        break
    fi
    printf 'Invalid dwell time: enter positive seconds, at most 3600.\n' >&2
done

declare -A SEEN_BANDS=()
BAND_CHANNELS=()
for band in "${REQUESTED_BANDS[@]}"; do
    [[ -z "${SEEN_BANDS[$band]}" ]] || continue
    SEEN_BANDS[$band]=true
    case "$band" in
        2.4) for channel in {1..11}; do BAND_CHANNELS+=("$band:$channel"); done ;;
        5) for channel in 36 40 44 48 52 56 60 64 100 104 108 112 116 120 124 128 132 136 140 144 149 153 157 161 165; do BAND_CHANNELS+=("$band:$channel"); done ;;
        6) for ((channel=1; channel<=233; channel+=4)); do BAND_CHANNELS+=("$band:$channel"); done ;;
    esac
done
printf 'Warning: hopping captures only the currently tuned channel. Gaps on other channels are expected.\n' >&2
RESTORE_MANAGED=true
prepare_output "Beacons_${LOCATION}_multi-band"
# Find a usable channel before starting TShark: do not capture on an old channel
# when every requested channel is unsupported.
set_interface_type monitor
FIRST_USABLE=false
for entry in "${BAND_CHANNELS[@]}"; do
    BAND=${entry%%:*}
    CHANNEL=${entry##*:}
    FREQ=$(channel_frequency "$BAND" "$CHANNEL") || fail 'Invalid generated channel.'
    if iw dev "$INTERFACE" set freq "$FREQ"; then FIRST_USABLE=true; break; fi
    printf 'Warning: skipping unavailable channel %s GHz/%s (%s MHz).\n' "$BAND" "$CHANNEL" "$FREQ" >&2
done
[[ "$FIRST_USABLE" == true ]] || fail 'No requested channel is supported by this adapter/regulatory domain.'
END_TICK=$((SECONDS + DURATION))
start_capture 'type mgt subtype beacon'
CAPTURE_FINISHED=false
while ((SECONDS < END_TICK)); do
    PASS_USABLE=false
    for entry in "${BAND_CHANNELS[@]}"; do
        if ! kill -0 "$CAPTURE_PID" 2>/dev/null; then
            await_capture
            CAPTURE_FINISHED=true
            break 2
        fi
        REMAINING=$((END_TICK - SECONDS))
        ((REMAINING > 0)) || break 2
        BAND=${entry%%:*}
        CHANNEL=${entry##*:}
        FREQ=$(channel_frequency "$BAND" "$CHANNEL") || fail 'Invalid generated channel.'
        if ! iw dev "$INTERFACE" set freq "$FREQ"; then
            printf 'Warning: skipping unavailable channel %s GHz/%s (%s MHz).\n' "$BAND" "$CHANNEL" "$FREQ" >&2
            continue
        fi
        PASS_USABLE=true
        printf 'Switching to %s GHz channel %s (%s MHz).\n' "$BAND" "$CHANNEL" "$FREQ"
        BOUNDED_DWELL=$(LC_ALL=C awk -v dwell="$DWELL_TIME" -v remaining="$REMAINING" 'BEGIN { print (dwell < remaining ? dwell : remaining) }')
        sleep "$BOUNDED_DWELL" || fail 'Channel dwell wait failed.'
    done
    [[ "$PASS_USABLE" == true ]] || fail 'All requested channels became unavailable during capture.'
done
# TShark's own duration limit closes the final file; normal completion never
# treats an intentional termination signal as a successful capture.
[[ "$CAPTURE_FINISHED" == true ]] || await_capture
printf 'Capture complete. Files saved to: %s\n' "$OUTPUT_DIR"
