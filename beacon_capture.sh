#!/bin/bash

# Replace with your BSSID
BSSID="48:00:20:D2:F6:E3"
LOCATION="ITB-1203"



#loop number of days
for i in {1..7}
do

        #put this in a try/catch if possible
        # set radio to standard for scanning
        ip link set wlan0 down
        iw dev wlan0 set type managed
        ip link set wlan0 up

        echo "Changing wlan0 to managed mode to scan for channel."

        sleep 5

        echo "Searching for BSSID $BSSID"

        # Scan for the BSSID and extract the channel
        CHANNEL=$(iwlist wlan0 scan | grep -A 5 "$BSSID" | grep 'Channel:' | awk -F: '{print $2}')

        #if [ -z "$CHANNEL" ]; then
        #    echo "BSSID not found."
        #    exit 1
        #fi
        #need to change this to a loop, wait, and try again
        while [ -z "$CHANNEL" ]
        do
                echo "BSSID not found. Trying again in 5 seconds.";
                sleep 5;
                CHANNEL=$(iwlist wlan0 scan | grep -A 5 "$BSSID" | grep 'Channel:' | awk -F: '{print $2}')
        done


        echo "Found BSSID on Channel: $CHANNEL"



        echo "Changing wlan0 to monitor mode."

        # set the interface to monitor
        ip link set wlan0 down
        iw dev wlan0 set type monitor
        ip link set wlan0 up

        sleep 1

        # Set the Wi-Fi interface to the found channel
        iwconfig wlan0 channel $CHANNEL

        echo "Wi-Fi interface set to Channel: $CHANNEL"


        sleep 2

        iw dev wlan0 info


        echo "Starting new captures."

        tshark -i wlan0 -w /var/tmp/"$LOCATION".pcap -g -a duration:86400 -b interval:3600 -b nametimenum:2 -f "type mgt subtype beacon and ether src $BSSID"

        echo $i
done

#add logging
#capture AP hostname
