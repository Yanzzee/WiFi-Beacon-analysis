#!/bin/bash
# Shared Linux capture helpers. Source from the four capture entry scripts.
# Overrides: CAPTURE_INTERFACE (wlan0), CAPTURE_OUTPUT_DIR (/var/tmp).

set -o pipefail

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

prompt() {
    IFS= read -r -p "$2" "$1" || fail 'Input ended; capture cancelled.'
}

# Supported primary-channel centers; iw checks hardware/regulatory support.
channel_frequency() {
    local band=$1 channel=$2
    case "$band" in
        2.4)
            (( channel >= 1 && channel <= 14 )) || return 1
            if (( channel == 14 )); then
                printf '2484\n'
            else
                printf '%s\n' "$((2407 + channel * 5))"
            fi
            ;;
        5)
            case "$channel" in
                36|40|44|48|52|56|60|64|100|104|108|112|116|120|124|128|132|136|140|144|149|153|157|161|165)
                    printf '%s\n' "$((5000 + channel * 5))" ;;
                *) return 1 ;;
            esac
            ;;
        6)
            (( channel >= 1 && channel <= 233 && channel % 4 == 1 )) || return 1
            printf '%s\n' "$((5950 + channel * 5))"
            ;;
        *) return 1 ;;
    esac
}

resolve_tuning() {
    local number candidate
    [[ "$1" =~ ^[0-9]{1,5}$ ]] || return 1
    number=$((10#$1))
    if (( number < 1000 )); then
        CHANNEL=$number
        FREQ=$(channel_frequency "$BAND" "$CHANNEL") || return 1
    else
        case "$BAND" in
            2.4)
                if (( number == 2484 )); then
                    CHANNEL=14
                else
                    CHANNEL=$(((number - 2407) / 5))
                fi
                ;;
            5) CHANNEL=$(((number - 5000) / 5)) ;;
            6) CHANNEL=$(((number - 5950) / 5)) ;;
        esac
        candidate=$(channel_frequency "$BAND" "$CHANNEL") || return 1
        [[ "$candidate" == "$number" ]] || return 1
        FREQ=$number
    fi
}

grant_read_permission() {
    local file=$1
    case "$file" in
        "$OUTPATH"|"${CAPTURE_STEM}"_*.pcapng) ;;
        *) printf 'Error: unexpected capture filename: %s\n' "$file" >&2; return 1 ;;
    esac
    [[ -f "$file" && ! -L "$file" ]] || return 1
    chmod a+r -- "$file" || {
        printf 'Error: could not grant read permission on %s\n' "$file" >&2
        return 1
    }
}

grant_retained_permissions() {
    local status=0 file
    shopt -s nullglob
    for file in "$OUTPATH" "${CAPTURE_STEM}"_*.pcapng; do
        [[ -f "$file" && ! -L "$file" ]] || continue
        grant_read_permission "$file" || status=1
    done
    return "$status"
}


require_tools() {
    local dependency
    for dependency in ip iw tshark awk date chmod sleep "$@"; do
        command -v "$dependency" >/dev/null 2>&1 || fail "Required command '$dependency' is not installed."
    done
}

initialize_capture() {
    ((BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 1))) || fail 'Bash 4.1 or newer is required.'
    INTERFACE=${CAPTURE_INTERFACE:-wlan0}
    while (($#)); do
        case "$1" in
            -i|--interface)
                (($# >= 2)) || fail "Option '$1' requires an interface name."
                INTERFACE=$2
                shift 2
                ;;
            --interface=*)
                INTERFACE=${1#*=}
                shift
                ;;
            -h|--help)
                printf 'Usage: %s [-i INTERFACE | --interface INTERFACE]\n' "${0##*/}"
                printf 'Select the capture interface (default: CAPTURE_INTERFACE or wlan0).\n'
                exit 0
                ;;
            *) fail "Unknown argument: $1. Use --help for usage." ;;
        esac
    done
    [[ "$INTERFACE" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,14}$ ]] || fail 'Invalid capture interface name.'
    require_tools
    OUTPUT_DIR=${CAPTURE_OUTPUT_DIR:-/var/tmp}
    [[ -d "$OUTPUT_DIR" && -w "$OUTPUT_DIR" ]] || fail "Output directory does not exist or is not writable: $OUTPUT_DIR"
    OUTPUT_DIR=$(cd -- "$OUTPUT_DIR" && pwd) || fail 'Cannot resolve output directory.'
    CAPTURE_STARTED=false
    CAPTURE_PID=''
    PERMISSION_PID=''
    RESTORE_MANAGED=false
    INTERFACE_CHANGED=false
    trap capture_cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
}

prompt_location() {
    while true; do
        prompt LOCATION 'Enter location tag (e.g., LVES-B105R22S30): '
        [[ "$LOCATION" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$ ]] && return
        printf 'Use 1-100 letters, digits, dots, underscores or hyphens; start with a letter or digit.\n' >&2
    done
}

prompt_tuning() {
    while true; do
        prompt BAND 'Enter band (2.4 / 5 / 6): '
        case "$BAND" in 2.4|5|6) break ;; esac
        printf 'Invalid band: choose 2.4, 5, or 6.\n' >&2
    done
    while true; do
        prompt TUNING 'Enter Wi-Fi channel or primary-channel center frequency in MHz (e.g., 44 or 5220): '
        resolve_tuning "$TUNING" && return
        printf 'Invalid channel/frequency for %s GHz. Enter a supported integer channel or its center frequency in MHz.\n' "$BAND" >&2
        printf 'Use the primary beacon channel, not a 40/80/160/320 MHz wide-channel center.\n' >&2
    done
}

positive_seconds() {
    [[ ${#1} -le 20 && "$1" =~ ^[0-9]*[.]?[0-9]+$ ]] || return 1
    LC_ALL=C awk -v value="$1" -v scale="$2" 'BEGIN {
        seconds = int(value * scale + 0.5)
        if (seconds < 1 || seconds > 2147483647) exit 1
        printf "%.0f", seconds
    }'
}

prompt_duration() {
    local scale=$1 label=$2
    while true; do
        prompt AMOUNT "Enter capture duration in $label (e.g., 0.5, 2): "
        DURATION=$(positive_seconds "$AMOUNT" "$scale") && return
        printf 'Invalid duration: enter positive %s that round to at least one second.\n' "$label" >&2
    done
}

prepare_output() {
    local prefix=$1
    RUN_TAG=$(date '+%Y%m%d_%H%M%S')_$$ || fail 'Could not generate capture filename.'
    CAPTURE_STEM="$OUTPUT_DIR/${prefix}_${RUN_TAG}"
    OUTPATH="$CAPTURE_STEM.pcapng"
    END_TIME=$(date -d "+$DURATION seconds" '+%Y-%m-%d %H:%M:%S %Z') || fail 'Could not calculate capture end time.'
}

set_interface_type() {
    ip link show dev "$INTERFACE" >/dev/null 2>&1 || fail "Interface '$INTERFACE' is unavailable; check --interface, CAPTURE_INTERFACE and privileges."
    INTERFACE_CHANGED=true
    ip link set "$INTERFACE" down || fail "Could not bring '$INTERFACE' down; check privileges."
    iw dev "$INTERFACE" set type "$1" || fail "Could not enable $1 mode on '$INTERFACE'."
    ip link set "$INTERFACE" up || fail "Could not bring '$INTERFACE' up."
}

setup_monitor() {
    printf 'Setting %s to monitor mode on %s MHz (Channel %s, Band %s GHz)...\n' "$INTERFACE" "$FREQ" "$CHANNEL" "$BAND"
    set_interface_type monitor
    iw dev "$INTERFACE" set freq "$FREQ" || fail "Could not tune to $FREQ MHz; check adapter and regulatory support."
    sleep 2 || fail 'Interface settling wait failed.'
}

stop_capture() {
    local attempt
    if [[ -n "$CAPTURE_PID" ]] && kill -0 "$CAPTURE_PID" 2>/dev/null; then
        kill -TERM "$CAPTURE_PID" 2>/dev/null || true
        for ((attempt=0; attempt<50; attempt++)); do
            kill -0 "$CAPTURE_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$CAPTURE_PID" 2>/dev/null; then
            printf 'Warning: capture did not stop gracefully; forcing the owned process to exit.\n' >&2
            kill -KILL "$CAPTURE_PID" 2>/dev/null || true
        fi
    fi
    if [[ -n "$CAPTURE_PID" ]]; then wait "$CAPTURE_PID" 2>/dev/null || true; fi
    CAPTURE_PID=''
}

capture_cleanup() {
    local status=$?
    trap - EXIT INT TERM
    stop_capture
    if [[ -n "$PERMISSION_PID" ]]; then wait "$PERMISSION_PID" || status=1; fi
    if [[ "$CAPTURE_STARTED" == true ]]; then grant_retained_permissions || status=1; fi
    if [[ "$RESTORE_MANAGED" == true && "$INTERFACE_CHANGED" == true ]]; then
        ip link set "$INTERFACE" down && iw dev "$INTERFACE" set type managed && ip link set "$INTERFACE" up || {
            printf 'Error: could not restore managed mode on %s.\n' "$INTERFACE" >&2
            status=1
        }
    fi
    exit "$status"
}

start_capture() {
    local filter=$1 file
    local -a arguments=(-i "$INTERFACE" -F pcapng -g -a "duration:$DURATION" -b duration:3600 -b files:9999 -b printname:stdout -w "$OUTPATH")
    [[ -z "$filter" ]] || arguments+=(-f "$filter")
    printf 'Capture will end at approximately: %s\nOutput prefix: %s\n' "$END_TIME" "$OUTPATH"
    CAPTURE_STARTED=true
    # Keep the TShark PID separate from the closed-file permission consumer.
    exec {NOTIFY_FD}> >(
        while IFS= read -r file; do grant_read_permission "$file" || exit 1; done
    )
    PERMISSION_PID=$!
    tshark "${arguments[@]}" >&"$NOTIFY_FD" &
    CAPTURE_PID=$!
    exec {NOTIFY_FD}>&-
}

await_capture() {
    local status=0
    wait "$CAPTURE_PID" || status=$?
    CAPTURE_PID=''
    wait "$PERMISSION_PID" || status=1
    PERMISSION_PID=''
    if ((status != 0)); then
        printf 'Error: capture stopped with status %s. Existing capture files are retained.\n' "$status" >&2
        exit "$status"
    fi
    grant_retained_permissions || fail 'Could not grant read access to all retained capture files.'
    CAPTURE_STARTED=false
}

run_direct_capture() {
    local mode=$1 filter prefix
    shift
    initialize_capture "$@"
    prompt_location
    prompt_tuning
    prompt_duration 3600 hours
    if [[ "$mode" == all ]]; then
        filter=''
        prefix="Capture_${LOCATION}_fr${FREQ}"
        printf 'Warning: this captures all traffic on the selected channel, not just beacons.\n' >&2
    else
        filter='type mgt subtype beacon'
        prefix="Beacons_${LOCATION}_fr${FREQ}"
    fi
    prepare_output "$prefix"
    setup_monitor
    start_capture "$filter"
    await_capture
    printf 'Capture complete. Files saved to: %s\n' "$OUTPUT_DIR"
}
