#!/bin/bash

# Prompt user for inputs
read -p "Enter location tag (e.g., ITB-1203): " LOCATION
read -p "Enter band (2.4 / 5 / 6): " BAND
read -p "Enter Wi-Fi channel: " CHANNEL
read -p "Enter capture duration in hours (e.g., 0.5, 2): " HOURS

# Convert hours to seconds using bc for floating point support
if ! command -v bc &> /dev/null; then
    echo "Error: 'bc' is required but not installed."
    exit 1
fi
DURATION=$(printf "%.0f" "$(echo "$HOURS * 3600" | bc -l)")

# Calculate expected end time (current + duration in seconds)
END_TIME=$(date -d "+$DURATION seconds" "+%Y-%m-%d %H:%M:%S")
echo "Capture will end at approximately: $END_TIME"


# Validate and calculate frequency
FREQ=""

if [[ "$BAND" == "2.4" ]]; then
    if (( CHANNEL >= 1 && CHANNEL <= 14 )); then
        FREQ=$((2407 + CHANNEL * 5))
    else
        echo "Invalid 2.4 GHz channel: $CHANNEL"
        exit 1
    fi

elif [[ "$BAND" == "5" ]]; then
    case $CHANNEL in
        36)  FREQ=5180 ;;
        40)  FREQ=5200 ;;
        44)  FREQ=5220 ;;
        48)  FREQ=5240 ;;
        52)  FREQ=5260 ;;
        56)  FREQ=5280 ;;
        60)  FREQ=5300 ;;
        64)  FREQ=5320 ;;
        100) FREQ=5500 ;;
        104) FREQ=5520 ;;
        108) FREQ=5540 ;;
        112) FREQ=5560 ;;
        116) FREQ=5580 ;;
        120) FREQ=5600 ;;
        124) FREQ=5620 ;;
        128) FREQ=5640 ;;
        132) FREQ=5660 ;;
        136) FREQ=5680 ;;
        140) FREQ=5700 ;;
        144) FREQ=5720 ;;
        149) FREQ=5745 ;;
        153) FREQ=5765 ;;
        157) FREQ=5785 ;;
        161) FREQ=5805 ;;
        165) FREQ=5825 ;;
        *) echo "Unsupported 5 GHz channel: $CHANNEL"; exit 1 ;;
    esac

elif [[ "$BAND" == "6" ]]; then
    # Only include beaconable 20 MHz channels: 1, 5, 9, ..., 233 (steps of 4)
    if (( CHANNEL >= 1 && CHANNEL <= 233 && CHANNEL % 4 == 1 )); then
        FREQ=$((5955 + (CHANNEL - 1) * 5))
    else
        echo "Invalid 6 GHz beaconable channel: $CHANNEL"
        echo "Valid beaconable 6 GHz channels: 1, 5, 9, ..., 233"
        exit 1
    fi
else
    echo "Invalid band selection. Must be 2.4, 5, or 6."
    exit 1
fi

echo "Setting wlan0 to monitor mode on $FREQ MHz (Channel $CHANNEL, Band $BAND GHz)..."

# Prepare interface
ip link set wlan0 down
iw dev wlan0 set type monitor
ip link set wlan0 up
iw wlan0 set freq $FREQ

sleep 2

# Set output path (tshark will append timestamps automatically)
OUTPATH="/var/tmp/Beacons_${LOCATION}_fr${FREQ}.pcap"

echo "Starting capture for $HOURS hour(s)... Output prefix: $OUTPATH"

tshark -i wlan0 \
  -g \
  -a duration:$DURATION \
  -b duration:3600 \
  -b files:9999 \
  -w "$OUTPATH" \
  -f "type mgt subtype beacon"

echo "Capture complete. Files saved to: /var/tmp/"
