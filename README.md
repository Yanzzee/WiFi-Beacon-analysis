# Beacon-analysis
802.11 beacon analysis with python
Doug Hales

processing 802.11 wireless captures from .pcapng to .parquet for analysis of 802.11e channel utilization, station count, frequency of beacons, and other information observable from captured beacons


Files are listed in general order that they are run.
beacon_capture.sh - bash script to capture using tshark on a raspberry pi or other linux device. This is an alternative to capturing from infrastructure. For capturing using aruba infrastructure, see https://www.youtube.com/watch?v=o3CL5KLBWK0
Wireshark-profile-Beacon-QBSS-load.zip - Wireshark profile to color code beacons by channel utilization. 
process_data.py - process raw pcap capture files to .parquet files
group_data.py - group data by AP name, one file per hostname for entire date range
samples.py - used to process sample files from CSV to dataframe/pickle below. Uses sample aps, classrooms, and schedule inputs.
    sample_aps.pkl - saved dataframe file with information about all sampled APs
    sample_classrooms.pkl - saved dataframe file with information about sampled classroom locations
    sample_classes.pkl - saved dataframe file with information about sampled class periods
analyze_data.ipynb - further processes and analyzes data within scope in the sample files
    saves output files for each class session
    creates and saves all_class_beacons_df which has data from all beacons within the scope and not others
    creates class_summary_df and saves a local pkl file, which has summary information for each radio/session (csv for convenience only)
analyze_all_class_beacons.ipynb - performs aggregate analysis on the all_class_beacons as a single dataset
analyze_radio_summary.ipynb - analyze the class_summary data that includes one row per radio per class session
analyze_class_sessions.ipynb - analyze each class session as a single row, combining multiple radios in the location


Folders
Data - the beacon data
    /captures - put pcapng files here
    /processed_data - pcapng files are converted to parquet files here
    /grouped_data - files are saved per AP in this location
    /sampled - files are saved here after processing to remove duplicates and only representing the sampled scope
Output - the output of processing including graphs, logs etc
    /plots - plotted graphs etc.
Sample - files representing the sampled courses, locations, and APs CSV files are converted to parquet files with Samples.py



