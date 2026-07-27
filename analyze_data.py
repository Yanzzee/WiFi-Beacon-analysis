# =============================================================================
# Cell 1 - Read sample information, processed beacon captures
# =============================================================================
# Read sample information, processed beacon captures
# Create plot for each radio class session
# Create summary parquet files

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import glob
import os
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
from tqdm import tqdm
from analysis_config import ensure_directory, get_beacon_rate
from analysis_io import (
    append_to_parquet as _append_to_parquet,
    combine_all_class_beacons as _combine_all_class_beacons,
)
from analysis_logging import open_analysis_log
from aruba_radio_inference import (
    add_aruba_transmitter_integers,
    assign_aruba_radio_numbers,
)
from beacon_filtering import remove_duplicate_beacons
from radio_analysis import (
    analyze_radio_group,
    calculate_total_scount,
)
from radio_session_plotting import plot_channel_stations
from analysis_validation import (
    REQUIRED_GROUPED_AP_COLUMNS,
    _grouped_timestamp_series,
    validate_grouped_files_for_sample_aps as _validate_grouped_files_for_sample_aps,
    validate_grouped_ap_schema,
    validate_grouped_ap_timestamps,
    validate_no_existing_analysis_outputs as _validate_no_existing_analysis_outputs,
    validate_radio_inference_platform as _validate_radio_inference_platform,
)

load_dotenv("config.env")


DATA_FOLDER = os.getenv("DATA_FOLDER")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
PLOTS_FOLDER = os.getenv("PLOTS_FOLDER")
PLOT_TIMEZONE = os.getenv("PLOT_TIMEZONE")
TIMEZONE = os.getenv("TIMEZONE")
SAMPLED_FOLDER = os.getenv("SAMPLED_FOLDER")
SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")
GROUPED_DATA_FOLDER = os.getenv("GROUPED_DATA_FOLDER")
AP_PLATFORM = os.getenv("AP_PLATFORM")
BEACON_RATE = get_beacon_rate()



if DATA_FOLDER is None or OUTPUT_FOLDER is None or SAMPLE_FOLDER is None:
    raise ValueError("Please set the environment variables DATA_FOLDER, OUTPUT_FOLDER, and SAMPLE_FOLDER.")
ensure_directory(OUTPUT_FOLDER)
ensure_directory(PLOTS_FOLDER)
ensure_directory(SAMPLED_FOLDER)
ensure_directory(SAMPLE_FOLDER)
ensure_directory(GROUPED_DATA_FOLDER)
# Check if the required folders exist, and create them if they don't


# Initialize the global DataFrame for aggregate analysis
all_class_beacons_df = pd.DataFrame()

#log to a file and print to terminal
log_file = f"{OUTPUT_FOLDER}/log.txt" #placeholder
def log_and_print(message):
    #print(message)  # Print to terminal is too noisy
    log_file.write(message + "\n")  # Write to file

def validate_radio_inference_platform():
    return _validate_radio_inference_platform(AP_PLATFORM)

# aggregate all beacons to a single dataframe for analysis of those during class sessions
# this is periodically written, and is faster than writing to a parquet file often
def add_to_all_class_beacons(filtered_df):
    global all_class_beacons_df
    all_class_beacons_df = pd.concat([all_class_beacons_df, filtered_df], ignore_index=True)
    #log_and_print("ran add_to_all_class_beacons")


def validate_no_missing_cu(df, ap_name, start_time, end_time):
    missing_mask = df["wlan.qbss.cu"].isna()
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return

    total_count = len(df)
    missing_percent = (missing_count / total_count) * 100 if total_count else 0
    example_columns = [
        column
        for column in ("aruba_erm.time", "wlan.ta", "wlan.bssid", "wlan.ssid")
        if column in df.columns
    ]
    examples = df.loc[missing_mask, example_columns].head(5).to_dict("records")
    raise ValueError(
        f"Grouped AP data for {ap_name} from {start_time} to {end_time} contains "
        f"{missing_count} of {total_count} deduplicated in-scope rows "
        f"({missing_percent:.4f}%) with missing wlan.qbss.cu. Current analysis "
        "does not define missing-CU semantics and would otherwise convert these "
        f"values to measured zero. Example source rows: {examples}"
    )


# Receives the information for each AP and class time information
# Reads the file and processes the information for that time/AP
def graph(ap_name, start_time, end_time, location, course, enrolled, capacity, ap_count, ap_df, radio_summary_df):

    #log_and_print("")
    log_and_print("\n-----------------------------------------------------------------\n")
    log_and_print(f"AP name: {ap_name}")

    # copy the AP df for manipulation
    filtered_df = ap_df.copy() # this is needed, otherwise the original df is modified

    # time column
    filtered_df.reset_index(inplace=True) #reset index so that the timestamp is in a column instead

    # Fix timestamp consistency
    # Localize only if the timestamps are naive
    if filtered_df["aruba_erm.time"].dt.tz is None:
        filtered_df["aruba_erm.time"] = filtered_df["aruba_erm.time"].dt.tz_localize("UTC")

    # Convert to UTC-7
    filtered_df["aruba_erm.time"] = filtered_df["aruba_erm.time"].dt.tz_convert(TIMEZONE) # update to use env file

    # Filter the DataFrame by the date/time range
    filtered_df = filtered_df[(filtered_df['aruba_erm.time'] >= start_time) & (filtered_df['aruba_erm.time'] <= end_time)]

    # return if there's no data
    if filtered_df.empty:
        log_and_print(f"No data available for {ap_name} from {start_time} to {end_time}.")
        return

    filtered_df = remove_duplicate_beacons(filtered_df, BEACON_RATE)

    # Preserve the distinction between missing CU and measured zero.
    # Current CU analysis semantics require observed values for all retained rows.
    validate_no_missing_cu(filtered_df, ap_name, start_time, end_time)

    # Ensure the DataFrame is sorted by timestamp
    filtered_df = filtered_df.sort_values(by="aruba_erm.time").reset_index(drop=True)


    # Convert wlan.qbss.scount to integer, handling NaN values
    filtered_df["wlan.qbss.scount"] = filtered_df["wlan.qbss.scount"].fillna(0).astype(int)
    filtered_df["wlan.qbss.cu"] = filtered_df["wlan.qbss.cu"].fillna(0).astype(int)
    filtered_df["wlan.qbss.adc"] = filtered_df["wlan.qbss.adc"].fillna(0).astype(int)

    # Convert Aruba transmitter MACs to integers for radio allocation.
    filtered_df = add_aruba_transmitter_integers(filtered_df)
    
    

    # Check Radio and SSID count
    # Verify all values in wlan.vs.aruba.ap_name are the same
    if filtered_df["wlan.vs.aruba.ap_name"].nunique() != 1:
        raise ValueError("Values in wlan.vs.aruba.ap_name are not all the same.")
    #else:
        #wlan_ap_name_value = filtered_df["wlan.vs.aruba.ap_name"].iloc[0]
        #print(f"Data from {wlan_ap_name_value}")
        


    # Count unique transmit addresses and corresponding SSIDs
    # ensures that data is clean and shows multi-radio APs
    # Group by wlan.ta and collect the corresponding wlan.ssid values
    #bssid_info = (
    #    filtered_df.groupby("wlan.ta")["wlan.ssid"]
    #    .unique()  # Get unique SSIDs for each wlan.ta
    #    .reset_index()
        #.rename(columns={"wlan.ta": "transmit_address", "wlan.ssid": "SSIDs"})
    #)

    # Count the number of unique BSSIDs, SSIDs
    #unique_bssid_count = bssid_info["wlan.ta"].nunique()
    bssid_count = filtered_df["wlan.ta"].nunique() #bssid count for all radios until divided below
    ssid_count = filtered_df["wlan.ssid"].nunique()



    # Assign radio numbers using the current Aruba adjacent-MAC rule.
    filtered_df = assign_aruba_radio_numbers(filtered_df)

    # Apply the function to calculate total_scount
    filtered_df = calculate_total_scount(filtered_df)

    # send the df to aggregate function
    add_to_all_class_beacons(filtered_df)

    #print number of radios
    radio_numbers = sorted(filtered_df["radio_number"].dropna().unique())
    radio_count = len(radio_numbers)
    #print(f"Number or 5GHz radios: {radio_count}")
    log_and_print(f"Number of radios: {radio_count}")
    #bssid_count = bssid_count / radio_count if radio_count != 0 else 0 # assumes that all radios have the same number of BSSIDs

    # Output the BSSID results
    log_and_print(f"Number of BSSIDs: {bssid_count}")
    log_and_print(f"Number of SSIDs: {ssid_count}")
    #print("List of addresses and their corresponding SSIDs:")
    #print(bssid_info)

    for current_radio_number in radio_numbers:
        radio_number_value = int(current_radio_number)
        filtered_radio_df = filtered_df[filtered_df["radio_number"] == current_radio_number]
        if filtered_radio_df.empty:
            continue

        total_beacons, high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu, high_scount, median_scount, low_scount, longest_duration_high_cu = analyze_radio_group(
            filtered_radio_df,
            radio_number_value,
            BEACON_RATE,
            log_and_print,
        )

        # SSID-specific station-count plots are currently disabled for performance.
        eduroam_radio_df = None
        byu_wifi_radio_df = None
        plot_file = plot_channel_stations(
            filtered_radio_df,
            eduroam_radio_df,
            byu_wifi_radio_df,
            ap_name,
            start_time,
            end_time,
            PLOTS_FOLDER,
            PLOT_TIMEZONE,
        )

        radio_summary_df.loc[len(radio_summary_df)] = [
        course, location, ap_name, radio_number_value, start_time, end_time,
        capacity, enrolled, ap_count, radio_count, bssid_count, ssid_count, total_beacons,
        high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu,
        high_scount, median_scount, low_scount, longest_duration_high_cu, plot_file
        ]

    if radio_count == 1:
        log_and_print(f"{ap_name} has only one radio")

    #print()
    
    # return df not needed since it is directly modified
    #return radio_summary_df


# Write to parquet files for each AP that can be combined later
# all_class_beacons is a global variable that is updated by the add_to_all_class_beacons function called in the graph function
def append_to_parquet(AP_name, all_class_beacons_df):
    return _append_to_parquet(
        AP_name,
        all_class_beacons_df,
        SAMPLED_FOLDER,
        log_and_print,
    )


def validate_no_existing_analysis_outputs():
    return _validate_no_existing_analysis_outputs(SAMPLED_FOLDER, DATA_FOLDER)


def validate_grouped_files_for_sample_aps(sample_aps_df, sample_classes_df=None):
    return _validate_grouped_files_for_sample_aps(
        sample_aps_df,
        GROUPED_DATA_FOLDER,
        sample_classes_df=sample_classes_df,
    )


# combine all files in Data/sampled into one file
def combine_all_class_beacons():
    return _combine_all_class_beacons(SAMPLED_FOLDER, DATA_FOLDER)


# main function to process all data in sample files

def main():
    
    validate_radio_inference_platform()

    validate_no_existing_analysis_outputs()

    #read in sample files
    # this can be done after parquet files have been created from samples.py
    sample_classes_df = pd.read_parquet(f'{SAMPLE_FOLDER}/sample_classes.parquet')
    sample_aps_df = pd.read_parquet(f'{SAMPLE_FOLDER}/sample_aps.parquet')
    sample_classrooms_df = pd.read_parquet(f'{SAMPLE_FOLDER}/sample_classrooms.parquet')

    validate_grouped_files_for_sample_aps(sample_aps_df, sample_classes_df)

    process(sample_aps_df, sample_classes_df, sample_classrooms_df)

    log_and_print("\n-----------------------------------------------------------------\n")
    log_and_print(f"Analysis completed on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    combine_all_class_beacons()
    log_and_print(f"all_class_beacons combined on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_file.close()



def process(sample_aps_df, sample_classes_df, sample_classrooms_df):
    # will loop for each class row, iterate through each AP at that location
    # this will go through ALL data and take a while

    global all_class_beacons_df

    #create a separate dataframe for aggregate radio information
    # Define column names and data types
    columns = {
        "subject": str,
        "location": str,
        "AP_hostname": str,
        "radio_number": int,
        "start_time": "datetime64[ns]",
        "end_time": "datetime64[ns]",
        "capacity": int,
        "enrolled": int,
        "AP_count": int,
        "radio_count": int,
        "bssid_count": int,
        "ssid_count": int,
        "total_beacons": int,
        "high_cu_beacons": int,
        "percent_high_cu": float,
        "highest_cu": int,
        "median_cu": int,
        "lowest_cu": int,
        "high_scount": int,
        "median_scount": int,
        "low_scount": int,
        "longest_duration_high_cu": float,
        "graph_filename": str
    }

    # Create an empty DataFrame with specified columns and data types
    radio_summary_df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in columns.items()})

    # Display the empty DataFrame
    #print(radio_summary_df.head)
    

    # Initialize the global DataFrame for aggregate analysis
    #all_class_beacons_df = pd.DataFrame() # moved to top

    # Define the base directory for the files
    base_dir = GROUPED_DATA_FOLDER

    # Initialize variables to ensure it's defined
    ap_df = pd.DataFrame()
    AP_name = ""


    # Loop through each row in sample_classes_df
    for index, class_row in tqdm(sample_classes_df.iterrows(), total=len(sample_classes_df)):
        location = class_row['location']
        start_time = class_row['start_time']
        end_time = class_row['end_time']
        course = class_row['course']
        enrolled = class_row['enrolled']
        capacity = class_row['capacity']
        ap_count = sample_classrooms_df.loc[sample_classrooms_df['Location'] == location, 'ap_count'].iloc[0]


        log_and_print("\n-----------------------------------------------------------------")
        log_and_print("-----------------------------------------------------------------\n")
        log_and_print(f"Subject: {course}")
        log_and_print(f"Location: {location}")
        log_and_print(f"Start Time: {start_time}")
        log_and_print(f"End Time: {end_time}")
        log_and_print(f"Capacity: {capacity}")
        log_and_print(f"Enrolled: {enrolled}")
        log_and_print(f"Number of APs: {ap_count}")
        log_and_print("")
        
        # Sub loop for each matching AP in sample_aps_df
        for ap_index, ap_row in sample_aps_df.iterrows():
            building_room = ap_row['Building-Room']
            
            # Check if the building-room matches the location
            if building_room == location:
                    
                if AP_name != ap_row['Hostname']:
                    AP_name = ap_row['Hostname']
                    file_name = AP_name + ".parquet" #temp using cleaned up files
                    # Load the data for the AP file into a dataframe
                    ap_df = pd.read_parquet(
                        f"{base_dir}/{file_name}",
                        engine="pyarrow",
                    )
                    validate_grouped_ap_schema(ap_df, f"{base_dir}/{file_name}", AP_name)
                
                # Call the graph function with the variables (do not need to set to a variable in order to update radio_summary_df)
                # Graph function will remove duplicates, sort, clean the data, add to all_class_beacons_df, calculate station counts, create plots, and do some analysis
                graph(AP_name, start_time, end_time, location, course, enrolled, capacity, ap_count, ap_df, radio_summary_df)
            #write to parquet file
            append_to_parquet(AP_name, all_class_beacons_df) #writes the file for the AP
            all_class_beacons_df = all_class_beacons_df.iloc[0:0]  # Clear contents of the df, otherwise it would build to become all

    # write summary dataframe to a file so it can be analyzed further later
    radio_summary_df.to_parquet(f'{DATA_FOLDER}/radio_summary.parquet')




# =============================================================================
# Cell 2 - Run analysis
# =============================================================================
#run
if __name__ == "__main__":
    timestamp, log_filename, log_file = open_analysis_log(
        OUTPUT_FOLDER,
        "analyze_data",
    )
    log_and_print(f"Analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    main()

# Cells 3-9 preserve non-operational scratch/debug content from the source notebook.
# The converted pipeline and regression harness execute only Cells 1-2.

# =============================================================================
# Cell 3 - Testing a single location
# =============================================================================
# # Testing a single location
# #need to re-do this to use existing code
# '''# run analysis for only one location
# # Specify the target location
# target_location = "TMCB-155A"
#
# # Initialize the global DataFrame for aggregate analysis
# #all_class_beacons_df = pd.DataFrame()
#
# # Open logging file
# #log_file = open("Output/output.txt", "a")  # Open file in append mode
#
# log_and_print(f"Analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#
# # Define the base directory for the files
# base_dir = "Data"
#
# # Initialize variables to ensure it's defined
# ap_df = pd.DataFrame()
# AP_name = ""
#
# # Loop through each row in sample_classes_df
# for index, class_row in sample_classes_df.iterrows():
#     location = class_row['location']
#     
#     # Skip rows that don't match the target location
#     if location != target_location:
#         continue
#     
#     start_time = class_row['start_time']
#     end_time = class_row['end_time']
#     course = class_row['course']
#     enrolled = class_row['enrolled']
#     capacity = class_row['capacity']
#     ap_count = sample_classrooms_df.loc[sample_classrooms_df['Location'] == location, 'ap_count'].iloc[0]
#
#
#     log_and_print("\n-----------------------------------------------------------------\n")
#     log_and_print(f"Subject: {course}")
#     log_and_print(f"Location: {location}")
#     log_and_print(f"Start Time: {start_time}")
#     log_and_print(f"End Time: {end_time}")
#     log_and_print(f"Capacity: {capacity}")
#     log_and_print(f"Enrolled: {enrolled}")
#     log_and_print(f"Number of APs: {ap_count}")
#     log_and_print("")
#     
#     # Sub loop for each matching AP in sample_aps_df
#     for ap_index, ap_row in sample_aps_df.iterrows():
#         building_room = ap_row['Building-Room']
#         
#         # Check if the building-room matches the location
#         if building_room == location:
#             
#             if AP_name != ap_row['Hostname']:
#                 AP_name = ap_row['Hostname']
#                 file_name = AP_name + ".parquet"
#                 # Load the data for the AP file into a dataframe
#                 ap_df = pd.read_parquet(
#                     f"{base_dir}/{file_name}",
#                     engine="pyarrow",
#                 )
#
#             
#             # Call the graph function with the variables - setting to variable not needed
#             #radio_summary_df = graph(AP_name, start_time, end_time, location, course, enrolled, capacity, ap_count, ap_df, radio_summary_df)
#             graph(AP_name, start_time, end_time, location, course, enrolled, capacity, ap_count, ap_df, radio_summary_df)
#
#
#
# log_and_print("\n-----------------------------------------------------------------\n")
# log_and_print(f"Analysis ended on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# log_file.close()
# '''

# =============================================================================
# Cell 4 - Debug - Check samples
# =============================================================================
# #debug
# #check samples
#
# sample_classes_df = pd.read_parquet('Sample/sample_classes.parquet')
# sample_aps_df = pd.read_parquet('Sample/sample_aps.parquet')
# sample_classrooms_df = pd.read_parquet('Sample/sample_classrooms.parquet')
#
# print("sample classes")
# print(sample_classes_df.head(10))
# sample_classes_df.to_csv('Output/sample_classes.csv', index=False)
# print("sample aps")
# print(sample_aps_df.head())
# print("sample classrooms")
# print(sample_classrooms_df)
#
# pq.read_table('Sample/sample_classes.parquet').schema

# =============================================================================
# Cell 5 - Debug - Inspect grouped AP data
# =============================================================================
# #debug
# temp_df = pd.read_parquet('Data/grouped_data/P-HBLL-1000-AP-01-1301.parquet')
# print(temp_df.head(10))
# print(temp_df.columns)
# print(temp_df.dtypes)

# =============================================================================
# Cell 6 - Moved grouped-data cleanup scratch code
# =============================================================================
# #MOVED TO group_data_cleanup.py 
# #Test and delete this section
# # This code is for cleaning up the grouped_data parquet files that do not have a separate index, and have the aruba_erm.time as the index
#
#
# import os
# import pandas as pd
#
# # Set the folder containing your parquet files
# folder_path = "Data/grouped_data"  # Adjust if needed
#
# # Process each .parquet file in the folder
# for filename in os.listdir(folder_path):
#     if filename.endswith(".parquet"):
#         full_path = os.path.join(folder_path, filename)
#         print(f"Processing {filename}...")
#
#         try:
#             df = pd.read_parquet(full_path)
#
#             # Reset index if it's aruba_erm.time
#             if df.index.name == "aruba_erm.time":
#                 df.reset_index(inplace=True)
#
#             # Ensure aruba_erm.time is a datetime column
#             if "aruba_erm.time" in df.columns:
#                 df["aruba_erm.time"] = (
#                     df["aruba_erm.time"]
#                     .astype(str)
#                     .str.replace("MDT", "", regex=False)
#                     .str.strip()
#                 )
#                 df["aruba_erm.time"] = pd.to_datetime(
#                     df["aruba_erm.time"], errors="coerce"
#                 )
#                 df["aruba_erm.time"] = df["aruba_erm.time"].dt.tz_localize(
#                     PLOT_TIMEZONE, ambiguous="NaT", nonexistent="NaT"
#                 )
#
#
#
#             # Save it back
#             df.to_parquet(full_path, index=False)
#             print(f"âœ”ï¸ Fixed and saved: {filename}")
#
#         except Exception as e:
#             print(f"âŒ Error processing {filename}: {e}")
#
#

# =============================================================================
# Cell 7 - Do not use - Duplicate-beacon cleanup scratch code
# =============================================================================
# #DO NOT USE
# # #Send all sampled files through the function to remove duplicate beacons
# #this could probably be done before the main processing loop above, or incorporated into it
# #Done. this is no longer needed
# # Folder containing .parquet files
# folder_path = 'Data/grouped_data'
#
# # Loop through all files in the folder
# for filename in os.listdir(folder_path):
#     if filename.endswith('.parquet'):
#         # Construct full file path
#         file_path = os.path.join(folder_path, filename)
#         
#         # Read the .parquet file into a DataFrame
#         df = pd.read_parquet(file_path)
#         
#         # Filter the DataFrame for "eduroam" and "BYU-WiFi" SSIDs
#         # this should be removed here, and only done when needed
#         #df = df[df["wlan.ssid"].isin(["eduroam", "BYU-WiFi"])]
#
#         # Pass the dataframe to the remove_duplicate_beacons function
#         df_cleaned = remove_duplicate_beacons(df, BEACON_RATE)
#         
#         # Save the cleaned DataFrame back to the same file
#         #df_cleaned.to_parquet(file_path)
#         df_cleaned.to_parquet('Data/' + filename)
#         
#
#         print(f"Processed and saved: {filename}")

# =============================================================================
# Cell 8 - Debug - Inspect global DataFrames
# =============================================================================
# #debug
# # # Check all variables in the global scope and filter DataFrames
# dataframes = {name: obj for name, obj in globals().items() if isinstance(obj, pd.DataFrame)}
#
# # Print the names of the DataFrames
# print("DataFrames in memory:", list(dataframes.keys()))
#
# # Print the head of each DataFrame
# for name, df in dataframes.items():
#     print(f"DataFrame: {name}")
#     print(df.head(), "\n")  # Print the first 5 rows
#

# =============================================================================
# Cell 9 - Debug - Verify Python environment and architecture
# =============================================================================
# # debug
# # Verifying Python environment and architecture
# # 
# # import sys, struct, platform
#
# print(sys.executable)
# print(platform.python_version())
# print(struct.calcsize("P") * 8)
