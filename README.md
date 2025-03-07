# Beacon-analysis
802.11 beacon analysis with python
Doug Hales

processing 802.11 wireless captures from .pcapng to .parquet for analysis of channel utilization, client count and other information elements in the 802.11e protocol


Files are listed in general order that they are run.

process_data.py - process raw pcap capture files to .parquet files
group_data.py - group data by AP name, one file per hostname
explore_data.ipynb - further processes and analyzes data within scope in the sample files
    saves output files for each class session
    creates and saves all_class_beacons_df which has data from all beacons within the scope and not others
    creates class_summary_df and saves a local pkl file, which has summary information for each radio/session (csv for convenience only)
analyze_data.ipynb - performs aggregate analysis on the all_class_beacons 

samples.py - used to process sample files from CSV to dataframe/pickle below
    sample_aps.pkl - saved dataframe file with information about all sampled APs
    sample_classrooms.pkl - saved dataframe file with information about sampled classroom locations
    sample_classes.pkl - saved dataframe file with information about sampled class periods



