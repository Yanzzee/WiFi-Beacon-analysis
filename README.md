# Beacon-analysis
802.11 beacon analysis with python
Working repository - not published publicly

processing 802.11 wireless captures from .pcapng to .parquet for analysis of 802.11e channel utilization, station count, frequency of beacons, and other information observable from captured beacons

Data requirements
capture files in .pcapng format that include beacons from classroom APs. Use .pcapng for all new captures. Legacy files with a .pcap extension are accepted only when their magic bytes show they are actually pcapng files; true pcap files are not currently supported.
    APs must include 802.11e information elements of channel utilization, station count, and admission capacity, which are optional
    beacons probably also need advertise AP Name enabled
    BEACON_RATE in config.env is the effective metadata interval used by the analysis, not the raw 802.11 beacon interval. IEEE 802.11 commonly defaults to 0.1024 seconds, while this application defaults to 1.024 seconds because vendor metadata is commonly updated every 10 intervals; Aruba ERM forwards these metadata-bearing beacon updates at the longer interval.
Sample_APs.csv - list of AP hostnames and locations, see example
Sample_classrooms.csv - list of classroom location, see example
Sample_schedule.csv - course schedule with location, Days of the week, start/end times, and enrolled count


### Run Files
Files are listed in the general order that they are run.

beacon_capture.sh (optional) - bash script to capture .pcapng files using tshark on a raspberry pi or other linux device. The capture scripts use Linux wireless monitor-mode tools and are not expected to run unchanged on Windows or macOS. This is an alternative to capturing from infrastructure. For capturing using aruba infrastructure, see https://www.youtube.com/watch?v=o3CL5KLBWK0
Wireshark-profile-Beacon-QBSS-load.zip - Wireshark profile to color code beacons by channel utilization. 

process_pcapng.py - process raw .pcapng capture files to .parquet files
    requires tshark and should run on Windows, macOS, and Linux when Python dependencies and tshark are installed
    --no-erm converts native/non-Aruba captures to parquet, but those outputs do not currently feed the analysis notebooks without a timestamp and AP-name mapping/adaptation layer

group_data_bssid.py - group generic capture data by BSSID instead of time, one file per AP/BSSID for the entire date range. This is intended as the generic grouping path.

group_data_aruba.py - group Aruba-specific captures by Aruba AP name instead of time, one file per hostname for the entire date range. Use this when working with Aruba-specific captures that include wlan.vs.aruba.ap_name.

group_data_cleanup.py (optional) - clean grouped parquet files when index cleanup is needed, especially when aruba_erm.time is stored as the index and needs to be a regular column. Current timestamp handling is format-dependent and should not be treated as the final timezone contract.

The grouping and cleanup scripts are currently separate. Eventually they should be combined into a single script or process that can detect vendor-specific fields and choose the right grouping and cleanup behavior.

samples.py - process sample files from CSV to dataframe/parquet listed below. Uses config.env for date range, sample_aps.csv, sample_classrooms.csv, and 
sample_schedule.csv input files. These should be UTF-8 encoded, sections in the same class should be combined, Day of week uses M T W R F.
    sample_aps.parquet - saved dataframe file with information about all sampled APs
    sample_classrooms.parquet - saved dataframe file with information about sampled classroom locations
    sample_classes.parquet - saved dataframe file with information about sampled class periods. Sections in the same location must be combined.

analyze_data.ipynb - further processes and begins to analyze data within scope in the sample files
    grouped input must include aruba_erm.time as a timezone-aware datetime dtype; varied beacon timestamp strings, naive datetimes, and broader schedule time parsing need a dedicated normalization pass after notebook conversion
    saves plots for each class session in Output/plots/
    separate parquet files for each AP are saved as class_beacons files, these contain only data within the sample scope
    all_class_beacons.parquet - combined all beacon data within the scope of class sessions
    radio_summary.parquet - summary information for each radio/session
    analyze_data_log - detailed information for each radio/session

analyze_all_class_beacons.ipynb - performs aggregate analysis on the all_class_beacons as a single dataset, required for analyze_class_sessions
    all_class_beacons_processed.parquet - includes location and room station count
    analyze_all_class_beacons_log - output and analysis results
    Output/all_class_beacons/ - includes plot files saved as png

analyze_radio_summary.ipynb - analyze the class_summary data that includes one row per radio per class session
    radio_summary/ - includes plots
    analyze_radio_summary_log - text output

analyze_class_sessions.ipynb - analyze each class session as a single row. Current high-CU duration and streak metrics combine all beacons in the location and mark a bin high when any beacon exceeds the threshold; they are not calculated independently per radio.

### Documentation

- `docs/current_pipeline.md` documents the behavior and assumptions of the current pipeline.
- `docs/post_refactor_design.md` tracks analysis and architecture changes intentionally deferred until after notebook refactoring.


### Folders
Data - input and working files
    /captures - put .pcapng files here
    /processed_data - .pcapng files are converted to parquet files and saved here
    /grouped_data - parquet files are saved per AP in this location
    /sampled - AP beacon files are saved here after processing to remove duplicates and only representing the sampled scope
Output - the output of processing including graphs, logs etc. these folders will be created dynamically if they don't already exist
    /plots - plotted graphs for radio/class sessions
    /all_class_beacons - output from analyze_all_class_beacons
    /radio_summary - output from analyze_radio_summary    
Sample - files defining the sampled courses, locations, and APs CSV files are converted to parquet files with samples.py
old - archived obsolete/reference files that are not part of the normal pipeline. See old/README.md.



