# Beacon-analysis
802.11 beacon analysis with python
Doug Hales

processing 802.11 wireless captures from .pcapng to .parquet for analysis of channel utilization, client count and other information elements in the 802.11e protocol


Files
explore_data.ipynb - main development file
process_data.py - process raw capture files to .parquet files
group_data.py - group data by AP name, one file per hostname
sample_aps.pkl - saved dataframe file with information about all sampled APs
sample_classrooms.pkl - saved dataframe file with information about sampled classroom locations
sample_classes.pkl - saved dataframe file with information about sampled class periods
samples.py - used to process sample files from CSV to dataframe/pickle


