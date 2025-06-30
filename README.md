# Beacon-analysis
802.11 beacon analysis with python

Processing 802.11 wireless captures from .pcapng to .parquet for analysis of 802.11e channel utilization, station count, frequency of beacons, and other information observable from captured beacons

Some development was done in python notebooks. This should be converted to .py eventually.

Data requirements
capture files in pcap format that include beacons from classroom APs
    APs must include 802.11e information elements of channel utilization, station count, and admission capacity, which are optional
    beacons probably also need advertise AP Name enabled
Sample_APs.csv - list of AP hostnames and locations, see example
Sample_classrooms.csv - list of classroom location, see example
Sample_schedule.csv - course schedule with location, Days of the week, start/end times, and enrolled count


### Run Files
Files are listed in the general order that they are run.

beacon_capture.sh (optional) - bash script to capture using tshark on a raspberry pi or other linux device. This is an alternative to capturing from infrastructure. For capturing using aruba infrastructure, see https://www.youtube.com/watch?v=o3CL5KLBWK0
Wireshark-profile-Beacon-QBSS-load.zip - Wireshark profile to color code beacons by channel utilization. 

process_data.py - process raw .pcapng capture files to .parquet files
    requires tshark on linux

group_data.py - group data by AP name instead of time, one file per hostname for entire date range

group_data_cleanup.py (optional) - adds an index and moves aruba_erm.time to a regular column, if needed

samples.py - process sample files from CSV to dataframe/parquet listed below. Uses config.env for date range, sample_aps.csv, sample_classrooms.csv, and 
sample_schedule.csv input files.
    sample_aps.parquet - saved dataframe file with information about all sampled APs
    sample_classrooms.parquet - saved dataframe file with information about sampled classroom locations
    sample_classes.parquet - saved dataframe file with information about sampled class periods

analyze_data.ipynb - further processes and begins to analyze data within scope in the sample files
    saves plots for each class session in Output/plots/
    all_class_beacons.parquet -  all beacon data within the scope of class sessions
    radio_summary.parquet - summary information for each radio/session
    analyze_data_log - detailed information for each radio/session

analyze_all_class_beacons.ipynb - performs aggregate analysis on the all_class_beacons as a single dataset, required for analyze_class_sessions
    all_class_beacons_processed.parquet - includes location and room station count
    analyze_all_class_beacons_log - output and analysis results
    Output/all_class_beacons/ - includes plot files saved as png

analyze_radio_summary.ipynb - analyze the class_summary data that includes one row per radio per class session
    radio_summary/ - includes plots
    analyze_radio_summary_log - text output

analyze_class_sessions.ipynb - analyze each class session as a single row, combining multiple radios in the location


### Folders
Data - input and working files
    /captures - put pcapng files here
    /processed_data - pcapng files are converted to parquet files and saved here
    /grouped_data - parquet files are saved per AP in this location
    /sampled - AP beacon files are saved here after processing to remove duplicates and only representing the sampled scope
Output - the output of processing including graphs, logs etc. these folders will be created dynamically if they don't already exist
    /plots - plotted graphs for radio/class sessions
    /all_class_beacons - output from analyze_all_class_beacons
    /radio_summary - output from analyze_radio_summary    
Sample - files defining the sampled courses, locations, and APs CSV files are converted to parquet files with Samples.py


## License
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
