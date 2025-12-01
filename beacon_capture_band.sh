#!/bin/bash

# ------------ USER INPUTS ------------

read -p "Enter location tag (e.g., ITB-1203): " LOCATION
read -p "Enter bands to scan (e.g., 2.4 5 6): " BAND_INPUT
read -p "Enter total capture duration in minutes: " MINUTES
if ! [[ "$MINUTES" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Error: duration must be a number (e.g., 1.5)"
    exit 1
fi
read -p "Enter dwell time per channel (seconds): " DWELL_TIME

# ------------ SETUP ------------

INTERFACE="wlan0"
SECONDS_TOTAL=$((MINUTES * 60))
TIMESTAMP=$(date "+%Y%m%d_%H%M%S")

declare -a CHANNELS

# Populate channels based on selected bands
# Store band:channel as tuples
BAND_CHANNELS=()

for BAND in $BAND_INPUT; do
    if [[ "$BAND" == "2.4" ]]; then
        for ch in {1..11}; do
            BAND_CHANNELS+=("2.4:$ch")
        done
    elif [[ "$BAND" == "5" ]]; then
        for ch in 36 40 44 48 52 56 60 64 100 104 108 112 116 120 124 128 132 136 140 144 149 153 157 161 165; do
            BAND_CHANNELS+=("5:$ch")
        done
    elif [[ "$BAND" == "6" ]]; then
        for (( ch=1; ch<=233; ch+=4 )); do
            BAND_CHANNELS+=("6:$ch")
        done
    else
        echo "Unknown band: $BAND"
        exit 1
    fi
done


# Remove duplicates and sort channels
CHANNELS=($(printf "%s\n" "${CHANNELS[@]}" | sort -n | uniq))

# Function to calculate frequency
get_freq() {
    local ch=$1
    if (( ch >= 1 && ch <= 14 )); then
        echo $((2407 + ch * 5))         # 2.4 GHz
    elif (( ch >= 36 && ch <= 165 )); then
        echo $((5000 + ch * 5))         # 5 GHz
    elif (( ch >= 1 && ch <= 233 && ch % 4 == 1 )); then
        echo $((5955 + (ch - 1) * 5))   # 6 GHz
    else
        echo "0"
    fi
}

# ------------ INTERFACE SETUP ------------

echo "Setting $INTERFACE to monitor mode..."
ip link set $INTERFACE down
iw dev $INTERFACE set type monitor
ip link set $INTERFACE up
sleep 1

# ------------ START TSHARK ------------

USER_HOME=$(eval echo "~$SUDO_USER")


CAPTURE_DIR="$USER_HOME/captures"
# Create directory as original user (not root)
if [ ! -d "$CAPTURE_DIR" ]; then
    mkdir -p "$CAPTURE_DIR"
    chmod 775 "$CAPTURE_DIR" 
fi
BASE_NAME="$USER_HOME/captures/${LOCATION}_multi-band_${TIMESTAMP}"




echo "Starting capture to $BASE_NAME"

tshark -i "$INTERFACE" \
  -F pcap \
  -w "$BASE_NAME" \
  -f "type mgt subtype beacon" \
  -b duration:3600 \
  -b files:9999 \
  -g &

TSHARK_PID=$!
sleep 2

# ------------ CHANNEL HOPPING LOOP ------------

START_TIME=$(date +%s)
END_TIME=$((START_TIME + SECONDS_TOTAL))

echo "Starting channel hopping for $MINUTES minute(s) (ends around $(date -d "@$END_TIME"))"

while [ "$(date +%s)" -lt "$END_TIME" ]; do
    for entry in "${BAND_CHANNELS[@]}"; do
        CUR_TIME=$(date +%s)
        if [ "$CUR_TIME" -ge "$END_TIME" ]; then
            break 2  # Exit both loops
        fi

        BAND="${entry%%:*}"
        CH="${entry##*:}"

        # Calculate frequency based on band
        if [[ "$BAND" == "2.4" ]]; then
            FREQ=$((2407 + CH * 5))
        elif [[ "$BAND" == "5" ]]; then
            FREQ=$((5000 + CH * 5))
        elif [[ "$BAND" == "6" ]]; then
            FREQ=$((5955 + (CH - 1) * 5))
        fi

        echo "[$(date "+%H:%M:%S")] Switching to $BAND GHz Channel $CH ($FREQ MHz)"

        if ! iw $INTERFACE set freq $FREQ 2>/dev/null; then
            echo "[WARN] Unsupported channel: $BAND GHz CH $CH ($FREQ MHz)"
            continue
        fi

        sleep $DWELL_TIME
    done
done

# ------------ CLEANUP ------------

# Change ownership of output files to the user who ran the script

echo "Stopping capture..."
kill $TSHARK_PID
wait $TSHARK_PID 2>/dev/null

echo "Resetting $INTERFACE to managed mode..."
ip link set $INTERFACE down
iw dev $INTERFACE set type managed
ip link set $INTERFACE up


# Add .pcap to any rolled files if missing
shopt -s nullglob
for f in "${BASE_NAME}"_*; do
  [[ "$f" == *.pcap ]] && continue
  mv -- "$f" "$f.pcap"
done
shopt -u nullglob

chown "$SUDO_USER":"$SUDO_USER" "${BASE_NAME}"*

echo "Done. Capture saved to: $BASE_NAME*.pcap"
