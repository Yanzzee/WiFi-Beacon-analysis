# =============================================================================
# Cell 1 - Read sample information, processed beacon captures
# =============================================================================
# Read sample information, processed beacon captures
# Create plot for each radio class session
# Create summary parquet files

import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytz
import glob
import matplotlib.dates as mdates
import os
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
from tqdm import tqdm
from analysis_config import get_beacon_rate

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
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)
if not os.path.exists(PLOTS_FOLDER):
    os.makedirs(PLOTS_FOLDER)
if not os.path.exists(SAMPLED_FOLDER):
    os.makedirs(SAMPLED_FOLDER)
if not os.path.exists(SAMPLE_FOLDER):
    os.makedirs(SAMPLE_FOLDER)
if not os.path.exists(GROUPED_DATA_FOLDER):
    os.makedirs(GROUPED_DATA_FOLDER)
# Check if the required folders exist, and create them if they don't


# Initialize the global DataFrame for aggregate analysis
all_class_beacons_df = pd.DataFrame()

#log to a file and print to terminal
log_file = f"{OUTPUT_FOLDER}/log.txt" #placeholder
def log_and_print(message):
    #print(message)  # Print to terminal is too noisy
    log_file.write(message + "\n")  # Write to file

def validate_radio_inference_platform():
    configured_platform = (AP_PLATFORM or "").strip().lower()
    if configured_platform != "aruba":
        configured_display = AP_PLATFORM if AP_PLATFORM else "<unset>"
        raise ValueError(
            "analyze_data.py radio inference is supported only when "
            "AP_PLATFORM=aruba. The current algorithm groups adjacent transmitter "
            "MAC addresses using Aruba-specific allocation assumptions; channel- "
            "or vendor-neutral radio inference is deferred until a dedicated "
            f"analysis change. Configured AP_PLATFORM: {configured_display}."
        )

# # populate total station count for all SSIDs per radio
def calculate_total_scount(df):

    # Get unique wlan.ta values for each observed radio_number.
    radio_transmitters_by_number = {
        radio_number: group["wlan.ta"].drop_duplicates().tolist()
        for radio_number, group in df.groupby("radio_number", dropna=False, observed=True)
    }

    # Initialize the total_scount column
    if "total_scount" not in df.columns:
        df["total_scount"] = 0


    # Iterate over the rows to calculate total_scount
    for i in range(len(df)):
        current_ta = df.at[i, "wlan.ta"]
        current_scount = df.at[i, "wlan.qbss.scount"]
        current_radio = df.at[i, "radio_number"]

        if pd.isna(current_radio):
            raise ValueError("Missing radio_number while calculating total_scount.")

        radio_transmitters = radio_transmitters_by_number.get(current_radio)
        if radio_transmitters is None:
            raise ValueError(f"No transmitter list found for radio_number {current_radio}.")
        radio_transmitters = radio_transmitters.copy()

        # remove the current line radio from the list
        if current_ta not in radio_transmitters:
            raise ValueError(f"Transmitter {current_ta} not found for radio_number {current_radio}.")
        radio_transmitters.remove(current_ta)
        
        #create loop for every entry in radio_x_transmitters
        for transmitter in radio_transmitters:
             #find the next value for this transmitter and add it to current_scount

            # Look for a next record where wlan.ta matches the one in the list
            j = i + 1  # Start with the next row
            while j < (len(df)):
                next_ta = df.at[j, "wlan.ta"]
                if next_ta == transmitter:
                    # Found a record matching the ta
                    next_scount = df.at[j, "wlan.qbss.scount"]
                    current_scount += next_scount
                    break
                j += 1  # Go to the next row

            # If no valid next record was found, look backwards
            if j >= (len(df)):
                k = i - 1  # Start with the previous row
                while k > 0:
                    prev_ta = df.at[k, "wlan.ta"]
                    if prev_ta == transmitter:
                        # Found the next record with the same wlan.ta_int
                        prev_scount = df.at[k, "wlan.qbss.scount"]
                        current_scount += prev_scount
                        break
                    k -= 1
            
            # If no next record exists, keep total_scount as current_scount
        #update the value after all is added
        df.at[i, "total_scount"] = current_scount
    
    # Ensure total_scount is of type integer
    df["total_scount"] = df["total_scount"].astype(int)

    return df


#  Define the function to generate the plot and save it as a file
#inputs are dataframes filtered to the radio, and further filtered for each SSID
def plot_channel_stations(radio_df, eduroam_df, byu_wifi_df, AP_name, start_time, end_time):
    #print("plot_channel_stations")
    plt.figure(figsize=(14, 7))  # Example size: 12 inches wide, 6 inches tall
    # Add a horizontal line at value 75 with 20% opacity
    #plt.axhline(y=75, color='red', linestyle='-', alpha=0.2, zorder=-1)

    # Access the first record and get the value of 'radio_number'
    plot_radio_number = radio_df.iloc[0]["radio_number"]
    
    # Plot wlan.qbss.cu normalized as percentages
    plt.plot(radio_df["aruba_erm.time"], (radio_df["wlan.qbss.cu"] / 255) * 100, label="Channel Utilization (%)")

    # Fill the area below the line
    plt.fill_between(radio_df["aruba_erm.time"], (radio_df["wlan.qbss.cu"] / 255) * 100)


    # Plot total_scount
    plt.plot(radio_df["aruba_erm.time"], radio_df["total_scount"], label="Total Station Count")

    # not graphing these, but it may be interesting later
    # Plot total_scount for "eduroam"
    #plt.plot(eduroam_df["aruba_erm.time"], eduroam_df["wlan.qbss.scount"], label="eduroam Station Count")

    # Plot total_scount for "BYU-WiFi"
    #plt.plot(byu_wifi_df["aruba_erm.time"], byu_wifi_df["wlan.qbss.scount"], label="BYU-WiFi Station Count")

    # Plot total_adc
    #plt.plot(radio_df["aruba_erm.time"], radio_df["wlan.qbss.adc"] / 32767 * 100, label="ADC (%)")

    #plot adc% + cu
    #adc updates less frequently than cu, but seems to be well correlated
    #plt.plot(radio_df["aruba_erm.time"], 100 - ((radio_df["wlan.qbss.adc"] / 32767 * 100) + (radio_df["wlan.qbss.cu"] / 255 * 100)), label="ADC + CU")

    formatted_date = start_time.strftime("%Y-%m-%d")
    formatted_start = start_time.strftime("%H:%M")  # Keep date and time (no seconds or timezone)
    formatted_end = end_time.strftime("%H:%M")  # Keep only the time (no date, seconds, or timezone)

    plt.xlabel(f"Time ({formatted_date})", fontsize=14)

    # Add labels and title
    #plt.xlabel(f"{start_time}        to        {end_time}", fontsize=14)
    plt.ylabel("Values", fontsize=14)
    plt.title(f"{AP_name} Radio {plot_radio_number} {formatted_start} to {formatted_end}", fontsize=16)

    # Set the y-axis limits from 0 to 100
    plt.ylim(0, 100)

    # Format x-axis to show only time (hour:minute)
    # Define Time zone
    time_zone = pytz.timezone(PLOT_TIMEZONE) # update to use env file
    # Set the date formatter with time zone
    time_formatter = mdates.DateFormatter("%H:%M", tz=time_zone)
    plt.gca().xaxis.set_major_formatter(time_formatter)

    # Add a legend
    plt.legend()

    # Save the plot as a PNG file
    plot_file = PLOTS_FOLDER + "/" + AP_name + "_" + start_time.strftime("%Y-%m-%d_%H-%M") + "_R" + str(plot_radio_number) + ".png" #change to use env file
    #print(plot_file)

    #save to a file
    plt.savefig(plot_file)
    
    plt.close()
    #print(plot_file)
    # Display the plot
    #plt.show()
    return plot_file


# Function to perform the analysis for a given radio DataFrame
def analyze_radio_group(radio_df, radio_number):
    #print("analyze_radio_group")
    #need to add > 127 count etc.
    # Ensure the DataFrame is sorted by timestamp
    radio_df = radio_df.sort_values(by="aruba_erm.time").reset_index(drop=True)

    # Count the total number of data points
    total_data_points = len(radio_df)

    # Calculate the greatest value of wlan.qbss.cu
    greatest_cutil = ((radio_df["wlan.qbss.cu"].max() / 255) * 100)

    # Calculate the least value of wlan.qbss.cu
    least_cutil = ((radio_df["wlan.qbss.cu"].min() / 255) * 100)

    # Count the number of data points where wlan.qbss.cu is greater than 191 (75%)
    greater_than_191_count = (radio_df["wlan.qbss.cu"] > 191).sum()

    # Calculate the percentage of data points where wlan.qbss.cu is greater than 191
    percentage_greater_than_191 = (greater_than_191_count / total_data_points) * 100

    # Median wlan.qbss.cu
    median_cutil = ((radio_df["wlan.qbss.cu"].median() / 255) * 100)

    # Calculate the greatest value of wlan.qbss.scount
    greatest_scount = radio_df["total_scount"].max()

    # Calculate the median value of wlan.qbss.scount
    median_scount = radio_df["total_scount"].median()

    # Calculate the least value of wlan.qbss.scount
    least_scount = radio_df["total_scount"].min()

    # Print the results
    log_and_print("")
    log_and_print(f"--- Radio Number {radio_number} ---")
    log_and_print(f"Total beacons: {total_data_points}")
    log_and_print(f"Quantity with very high channel utilization(>75%): {greater_than_191_count}")
    log_and_print(f"Percent with very high channel utilization: {percentage_greater_than_191:.2f}%")
    log_and_print(f"Highest channel utilization: {greatest_cutil:.2f}%")
    log_and_print(f"Median channel utilization: {median_cutil:.2f}%")
    log_and_print(f"Lowest channel utilization: {least_cutil:.2f}%")
    log_and_print(f"Highest station count: {greatest_scount:.0f}")
    log_and_print(f"Median station count: {median_scount:.0f}")
    log_and_print(f"Lowest station count: {least_scount:.0f}")

    # Create a boolean mask for wlan.qbss.cu > 191
    condition = radio_df["wlan.qbss.cu"] > 191

    # Assign group numbers for consecutive periods where condition is True
    radio_df["group"] = (condition != condition.shift()).cumsum() * condition

    # Filter groups where wlan.qbss.cu > 191
    valid_groups = radio_df[radio_df["group"] > 0]

    # Check if there are valid groups (i.e., groups where wlan.qbss.cu > 191)
    if valid_groups.empty:
        log_and_print(f"No records found where wlan.qbss.cu > 191 for radio_number {radio_number}.")
        longest_duration = 0
    else:
       # Find the start and end of each group
        group_details = valid_groups.groupby("group").agg(
            first_index=("aruba_erm.time", "idxmin"),
            last_index=("aruba_erm.time", "idxmax")
        )

        # Extract the first and last timestamps directly
        group_details["first_in_sequence_time"] = valid_groups.groupby("group")["aruba_erm.time"].min()
        group_details["end_time"] = valid_groups.groupby("group")["aruba_erm.time"].max()

        # Calculate duration across observed high-CU beacons and add one BEACON_RATE window.
        # BEACON_RATE is the effective metadata interval used by this analysis; IEEE's common 0.1024 second beacon interval is shorter.
        group_details["duration"] = (group_details["end_time"] - group_details["first_in_sequence_time"]).dt.total_seconds() + BEACON_RATE

        # Find the group with the longest duration
        longest_group = group_details["duration"].idxmax()
        longest_row = group_details.loc[longest_group]

        # Extract details of the longest duration
        longest_duration = longest_row["duration"]
        longest_start_time = longest_row["first_in_sequence_time"]
        longest_end_time = longest_row["end_time"]

        # Print results for the specific radio_number
        log_and_print(f"Longest duration very high channel utilization: {longest_duration:.2f} seconds")
        log_and_print(f"First beacon of sequence: {longest_start_time}")
        log_and_print(f"Last beacon of sequence: {longest_end_time}")

    return total_data_points, greater_than_191_count, percentage_greater_than_191, greatest_cutil, median_cutil, least_cutil, greatest_scount, median_scount, least_scount, longest_duration


# aggregate all beacons to a single dataframe for analysis of those during class sessions
# this is periodically written, and is faster than writing to a parquet file often
def add_to_all_class_beacons(filtered_df):
    global all_class_beacons_df
    all_class_beacons_df = pd.concat([all_class_beacons_df, filtered_df], ignore_index=True)
    #log_and_print("ran add_to_all_class_beacons")


# clean up the parquet files further
# keep one beacon per configured effective metadata interval
# Function to filter rows for each wlan.ta
def filter_by_time(df):
    df = df.sort_values("aruba_erm.time")  # Ensure sorted order
    keep_rows = [df.iloc[0]]  # Always keep the first row
    
    last_time = df.iloc[0]["aruba_erm.time"]
    for _, row in df.iloc[1:].iterrows():
        if (row["aruba_erm.time"] - last_time).total_seconds() >= (0.95 * BEACON_RATE): # keep rows at least 95% of the effective interval apart
            keep_rows.append(row)
            last_time = row["aruba_erm.time"]
    
    return pd.DataFrame(keep_rows)

#receives the dataframe for an AP, sorts it, and sends it to be filtered by time
def remove_duplicate_beacons(ap_df):

    # Group by 'wlan.ta' without dropping it
    # has deprecation warning, can probably just sort by time then wlan.ta without grouping since the times will be different between different tas
    filtered_ap_df = (
        ap_df.groupby("wlan.ta", group_keys=False, observed=True)
        .apply(lambda group: filter_by_time(group.reset_index())) #was apply(filter_by_time)
        .reset_index(drop=True)  # Reset index to avoid multi-index issues
    )

    # Convert specific columns back to string
    string_columns = ["wlan.ta", "wlan.vs.aruba.ap_name", "wlan.ssid"]
    filtered_ap_df[string_columns] = filtered_ap_df[string_columns].astype("string")


    return(filtered_ap_df)


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

    filtered_df = remove_duplicate_beacons(filtered_df)

    # Preserve the distinction between missing CU and measured zero.
    # Current CU analysis semantics require observed values for all retained rows.
    validate_no_missing_cu(filtered_df, ap_name, start_time, end_time)

    # Ensure the DataFrame is sorted by timestamp
    filtered_df = filtered_df.sort_values(by="aruba_erm.time").reset_index(drop=True)


    # Convert wlan.qbss.scount to integer, handling NaN values
    filtered_df["wlan.qbss.scount"] = filtered_df["wlan.qbss.scount"].fillna(0).astype(int)
    filtered_df["wlan.qbss.cu"] = filtered_df["wlan.qbss.cu"].fillna(0).astype(int)
    filtered_df["wlan.qbss.adc"] = filtered_df["wlan.qbss.adc"].fillna(0).astype(int)

    # create wlan.ta_int column
    # Function to convert hex MAC address string to integer
    def mac_to_int(mac):
        return int(mac.replace(":", ""), 16)

    # Apply the conversion to the wlan.ta column
    filtered_df["wlan.ta_int"] = filtered_df["wlan.ta"].apply(mac_to_int)
    
    

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



    # add a column for unique radio number
    # this would be better with channel numbers than BSSIDs
    # Create the 'radio_number' column
    filtered_df = filtered_df.sort_values("wlan.ta_int").reset_index(drop=True)

    # Initialize the 'radio_number' column
    radio_number = 1
    radio_numbers = [radio_number]

    # Assign radio numbers based on 'wlan.ta_int'
    # would probably be better to use channel number, but this works for Aruba
    for i in range(1, len(filtered_df)):
        # Check if the difference between consecutive wlan.ta_int values is greater than 1
        if abs(filtered_df.loc[i, "wlan.ta_int"] - filtered_df.loc[i - 1, "wlan.ta_int"]) > 1:
            radio_number += 1  # Increment radio number if the condition is met
        radio_numbers.append(radio_number)

    # Add the radio_number column to the DataFrame
    filtered_df["radio_number"] = radio_numbers

    # Ensure the DataFrame is sorted by timestamp
    filtered_df = filtered_df.sort_values(by="aruba_erm.time").reset_index(drop=True)

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

        total_beacons, high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu, high_scount, median_scount, low_scount, longest_duration_high_cu = analyze_radio_group(filtered_radio_df, radio_number_value)

        # SSID-specific station-count plots are currently disabled for performance.
        eduroam_radio_df = None
        byu_wifi_radio_df = None
        plot_file = plot_channel_stations(filtered_radio_df, eduroam_radio_df, byu_wifi_radio_df, ap_name, start_time, end_time)

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
    # Ensure all_class_beacons_df is not empty before appending
    if not all_class_beacons_df.empty:
        ap_beacons_file = os.path.join(SAMPLED_FOLDER, f"{AP_name}_class_beacons.parquet")
        
        # Define schema based on the DataFrame
        schema = pa.Schema.from_pandas(all_class_beacons_df)

        # Check if the file exists
        if os.path.exists(ap_beacons_file):
            # If file exists, open it and append using ParquetWriter
            existing_table = pq.read_table(ap_beacons_file)
            with pq.ParquetWriter(ap_beacons_file, schema, compression="snappy") as writer:
                # Append the existing data first
                writer.write_table(existing_table)
                # Append the new data
                table = pa.Table.from_pandas(all_class_beacons_df, schema=schema)
                writer.write_table(table)
            log_and_print(f"Adding to {ap_beacons_file}")
        else:
            # If the file doesn't exist, create it and write data
            all_class_beacons_df.to_parquet(ap_beacons_file, engine="pyarrow", compression="snappy", index=False)
            log_and_print(f"Creating {ap_beacons_file}")
        
    #else:
        #log_and_print("DataFrame is empty, nothing to append.")


def validate_no_existing_analysis_outputs():
    existing_outputs = []

    sampled_files = glob.glob(os.path.join(SAMPLED_FOLDER, "*_class_beacons.parquet"))
    existing_outputs.extend(sampled_files)

    for output_file in [
        os.path.join(DATA_FOLDER, "all_class_beacons.parquet"),
        os.path.join(DATA_FOLDER, "radio_summary.parquet"),
    ]:
        if os.path.exists(output_file):
            existing_outputs.append(output_file)

    if existing_outputs:
        output_list = "\n".join(f"- {path}" for path in sorted(existing_outputs))
        raise FileExistsError(
            "Existing analyze_data outputs were found. "
            "Use a fresh workspace or remove these generated files before rerunning:\n"
            f"{output_list}"
        )


def validate_grouped_files_for_sample_aps(sample_aps_df, sample_classes_df=None):
    if "Hostname" not in sample_aps_df.columns:
        raise ValueError(
            "Sample AP metadata must include a Hostname column for grouped AP lookup."
        )

    scoped_sample_aps_df = sample_aps_df
    if (
        sample_classes_df is not None
        and "Building-Room" in sample_aps_df.columns
        and "location" in sample_classes_df.columns
    ):
        sampled_locations = set(
            sample_classes_df["location"].dropna().astype("string").str.strip()
        )
        scoped_sample_aps_df = sample_aps_df[
            sample_aps_df["Building-Room"].astype("string").str.strip().isin(sampled_locations)
        ]

    hostnames = scoped_sample_aps_df["Hostname"].dropna().astype("string").str.strip()
    expected_paths = []
    for hostname in sorted(hostname for hostname in hostnames.unique().tolist() if hostname):
        expected_path = os.path.join(GROUPED_DATA_FOLDER, f"{hostname}.parquet")
        if not os.path.exists(expected_path):
            expected_paths.append(expected_path)

    if expected_paths:
        expected_list = "\n".join(f"- {path}" for path in expected_paths)
        raise FileNotFoundError(
            "Missing grouped AP parquet files for sample AP Hostname values. "
            "analyze_data.py currently loads grouped data by Sample/sample_aps.parquet "
            "Hostname. BSSID fallback files cannot be reliably associated with all "
            "SSIDs on the same radio or all radios on the same AP without vendor AP-name "
            "metadata. Aruba AP-name beacons are supported now; broader vendor mapping "
            "is deferred. Expected files:\n"
            f"{expected_list}"
        )


REQUIRED_GROUPED_AP_COLUMNS = (
    "wlan.ta",
    "wlan.vs.aruba.ap_name",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
)


def _grouped_timestamp_series(ap_df):
    if "aruba_erm.time" in ap_df.columns:
        return pd.Series(ap_df["aruba_erm.time"])
    if ap_df.index.name == "aruba_erm.time":
        return pd.Series(ap_df.index)
    return None


def validate_grouped_ap_timestamps(ap_df, file_path, ap_name):
    timestamps = _grouped_timestamp_series(ap_df)
    if timestamps is None:
        return

    if int(timestamps.notna().sum()) == 0:
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} has no non-null "
            "aruba_erm.time values."
        )

    if not pd.api.types.is_datetime64_any_dtype(timestamps):
        parsed = pd.to_datetime(timestamps, errors="coerce", utc=True)
        invalid_count = int((timestamps.notna() & parsed.isna()).sum())
        invalid_detail = (
            f" Found {invalid_count} non-null timestamp values that pandas could not parse."
            if invalid_count
            else ""
        )
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} must store "
            "aruba_erm.time as a timezone-aware datetime dtype before "
            "script analysis. Mixed string/input timestamp normalization remains "
            "deferred to a dedicated normalization change."
            f"{invalid_detail}"
        )

    if getattr(timestamps.dt, "tz", None) is None:
        raise ValueError(
            f"Grouped AP data for {ap_name} at {file_path} has timezone-naive "
            "aruba_erm.time values. Run or repair grouped-data cleanup so "
            "timestamps are timezone-aware before script analysis."
        )


def validate_grouped_ap_schema(ap_df, file_path, ap_name):
    missing_columns = [
        column for column in REQUIRED_GROUPED_AP_COLUMNS
        if column not in ap_df.columns
    ]
    if "aruba_erm.time" not in ap_df.columns and ap_df.index.name != "aruba_erm.time":
        missing_columns.insert(0, "aruba_erm.time")

    if not missing_columns:
        validate_grouped_ap_timestamps(ap_df, file_path, ap_name)
        return

    missing_text = ", ".join(missing_columns)
    message = (
        f"Grouped AP data for {ap_name} at {file_path} is missing required "
        f"analysis columns: {missing_text}. The current analysis pipeline "
        "require Aruba-shaped grouped data with aruba_erm.time and "
        "wlan.vs.aruba.ap_name."
    )
    if "frame.time_epoch" in ap_df.columns or ap_df.index.name == "frame.time_epoch":
        message += (
            " Detected native --no-erm conversion output with frame.time_epoch. "
            "Native/non-Aruba captures are conversion-only for now and need "
            "a timestamp plus AP-name mapping/adaptation before script analysis."
        )
    raise ValueError(message)


# combine all files in Data/sampled into one file
def combine_all_class_beacons():
    all_class_beacons_df = pd.DataFrame()

    # Directory containing AP .parquet files
    directory = SAMPLED_FOLDER

    # Get a list of all .parquet files
    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            "No sampled class beacon parquet files found in "
            f"SAMPLED_FOLDER ({directory}). Check grouped AP/sample matches "
            "before combining all class beacons."
        )

    all_class_beacons_df = all_class_beacons_df.iloc[0:0] #clear contents of the df
    # Read and concatenate all Parquet files (faster than .pkl)
    all_class_beacons_df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)

    all_class_beacons_df.to_parquet(f'{DATA_FOLDER}/all_class_beacons.parquet')


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
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_filename = f"{OUTPUT_FOLDER}/analyze_data_log_{timestamp}.txt" #logging - need to change to logging function
    log_file = open(log_filename, "a")  # Open file in append mode
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
#         df_cleaned = remove_duplicate_beacons(df)
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
