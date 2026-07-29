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

#debug tkinter error
#import sys
#print("Python executable:", sys.executable)
#import os
#print("TCL_LIBRARY:", os.environ.get("TCL_LIBRARY"))
#!!!
#!!!
#!!!
#CURRENTLY HAS A FATAL BUG IN THE .py VERSION NOT EVIDENT IN THE ipynb VERSION
# Need to convert ipynb file to py, this file is outdated
#!!!
#!!!
#!!!

# Initialize the global DataFrame for aggregate analysis
all_class_beacons_df = pd.DataFrame()

#log to a file and print to terminal
log_file = 'Output/log.txt' #placeholder
def log_and_print(message):
    #print(message)  # Print to terminal is too noisy
    log_file.write(message + "\n")  # Write to file

# # populate total station count for all SSIDs per radio
def calculate_total_scount(df):

    # Get unique wlan.ta and radio_number pairs
    radio_1_transmitters = df[df["radio_number"] == 1]["wlan.ta"].drop_duplicates().tolist()
    radio_2_transmitters = df[df["radio_number"] == 2]["wlan.ta"].drop_duplicates().tolist()

    # Initialize the total_scount column
    if "total_scount" not in df.columns:
        df["total_scount"] = 0


    # Iterate over the rows to calculate total_scount
    for i in range(len(df)):
        current_ta = df.at[i, "wlan.ta"]
        current_scount = df.at[i, "wlan.qbss.scount"]
        current_radio = df.at[i, "radio_number"]

        # set transmitter list based on radio
        if current_radio == 1:
             radio_transmitters = radio_1_transmitters.copy()
        elif current_radio == 2:
             radio_transmitters = radio_2_transmitters.copy()
        else:
            log_and_print("error - no radio")

        # remove the current line radio from the list
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

    formatted_start = start_time.strftime("%Y-%m-%d %H:%M")  # Keep date and time (no seconds or timezone)
    formatted_end = end_time.strftime("%H:%M")  # Keep only the time (no date, seconds, or timezone)

    plt.xlabel(f"{formatted_start} to {formatted_end}", fontsize=14)

    # Add labels and title
    #plt.xlabel(f"{start_time}        to        {end_time}", fontsize=14)
    plt.ylabel("Values", fontsize=14)
    plt.title(f"{AP_name} Radio {plot_radio_number}", fontsize=16)

    # Set the y-axis limits from 0 to 100
    plt.ylim(0, 100)

    # Format x-axis to show only time (hour:minute)
    # Define Mountain Time zone
    mountain_tz = pytz.timezone("America/Denver") # update to use env file
    # Set the date formatter with Mountain Time
    time_formatter = mdates.DateFormatter("%H:%M", tz=mountain_tz)
    plt.gca().xaxis.set_major_formatter(time_formatter)

    # Add a legend
    plt.legend()

    # Save the plot as a PNG file
    plot_file = "Output/plots/" + AP_name + "_" + start_time.strftime("%Y-%m-%d_%H-%M") + "_R" + str(plot_radio_number) + ".png"
    #print(plot_file)

    #save to a file
    plt.savefig(plot_file)

    #print(plot_file)
    # Display the plot
    #plt.show()
    plt.close()
    return plot_file


# Function to perform the analysis for a given radio DataFrame
def analyze_radio_group(radio_df, radio_number):
    #print("analyze_radio_group")
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
    log_and_print(f"Quantity with very high channel utilization: {greater_than_191_count}")
    log_and_print(f"Percentage with very high channel utilization: {percentage_greater_than_191:.2f}%")
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

        # Calculate the duration between the start and end times, and add 1 second 
        # Aruba updates beacons once per 10, which is also the rate that we have captured. 1.024 is the time the first beacon represents
        group_details["duration"] = (group_details["end_time"] - group_details["first_in_sequence_time"]).dt.total_seconds() + 1.024

        # Find the group with the longest duration
        longest_group = group_details["duration"].idxmax()
        longest_row = group_details.loc[longest_group]

        # Extract details of the longest duration
        longest_duration = longest_row["duration"]
        longest_start_time = longest_row["first_in_sequence_time"]
        longest_end_time = longest_row["end_time"]

        # Print results for the specific radio_number
        log_and_print(f"Longest duration of very high channel use: {longest_duration:.2f} seconds")
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
# keep only ~1 beacon per second
# Function to filter rows for each wlan.ta
def filter_by_time(df):
    df = df.sort_values("aruba_erm.time")  # Ensure sorted order
    keep_rows = [df.iloc[0]]  # Always keep the first row
    
    last_time = df.iloc[0]["aruba_erm.time"]
    for _, row in df.iloc[1:].iterrows():
        if (row["aruba_erm.time"] - last_time).total_seconds() >= 0.9728: # keep ones that are more than 95 TU apart
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

    # Convert to UTC-7 (this can always be done after localization)
    filtered_df["aruba_erm.time"] = filtered_df["aruba_erm.time"].dt.tz_convert("Etc/GMT+7") # update to use env file

    # Filter the DataFrame by the date/time range
    filtered_df = filtered_df[(filtered_df['aruba_erm.time'] >= start_time) & (filtered_df['aruba_erm.time'] <= end_time)]

    # return if there's no data
    if filtered_df.empty:
        log_and_print(f"No data available for {ap_name} from {start_time} to {end_time}.")
        return

    filtered_df = remove_duplicate_beacons(filtered_df)

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
    radio_count = filtered_df['radio_number'].max()
    #print(f"Number or 5GHz radios: {filtered_df['radio_number'].max()}")
    log_and_print(f"Number or 5GHz radios: {radio_count}")
    #bssid_count = bssid_count / radio_count if radio_count != 0 else 0 # assumes that all radios have the same number of BSSIDs

    # Output the BSSID results
    log_and_print(f"Number of BSSIDs: {bssid_count}")
    log_and_print(f"Number of SSIDs: {ssid_count}")
    #print("List of addresses and their corresponding SSIDs:")
    #print(bssid_info)

    # Filter the DataFrame for records where radio_number is 1
    filtered_radio_1_df = filtered_df[filtered_df["radio_number"] == 1]
    # Filter the data for "eduroam" and "BYU-WiFi" SSIDs
    #eduroam_radio_1_df = filtered_df[(filtered_df["wlan.ssid"] == "eduroam") & (filtered_df["radio_number"] == 1)]
    #byu_wifi_radio_1_df = filtered_df[(filtered_df["wlan.ssid"] == "BYU-WiFi") & (filtered_df["radio_number"] == 1)]
    eduroam_radio_1_df = None   # temp because this is not used
    byu_wifi_radio_1_df = None  # temp because this is not used

    #  Filter the DataFrame for records where radio_number is 2
    filtered_radio_2_df = filtered_df[filtered_df["radio_number"] == 2]

    # Run the analysis for radio_number 1 (using filtered_radio_1_df)
    if not filtered_radio_1_df.empty:
        total_beacons, high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu, high_scount, median_scount, low_scount, longest_duration_high_cu = analyze_radio_group(filtered_radio_1_df, 1)
        # Plot for the first radio
        #temp removed for performance
        #plot_file = ""
        plot_file = plot_channel_stations(filtered_radio_1_df, eduroam_radio_1_df, byu_wifi_radio_1_df, ap_name, start_time, end_time)
        #print(plot_file)
        #update totals summary
        radio_summary_df.loc[len(radio_summary_df)] = [
        course, location, ap_name, 1, start_time, end_time,
        capacity, enrolled, ap_count, radio_count, bssid_count, ssid_count, total_beacons,
        high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu,
        high_scount, median_scount, low_scount, longest_duration_high_cu, plot_file
        ]

    # Run the analysis for radio_number 2 (using filtered_radio_2_df) only if it's not empty
    if not filtered_radio_2_df.empty:
        total_beacons, high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu, high_scount, median_scount, low_scount, longest_duration_high_cu = analyze_radio_group(filtered_radio_2_df, 2)


        # Filter the data for "eduroam" and "BYU-WiFi" SSIDs
        #eduroam_radio_2_df = filtered_df[(filtered_df["wlan.ssid"] == "eduroam") & (filtered_df["radio_number"] == 2)] #update to use env file
        #byu_wifi_radio_2_df = filtered_df[(filtered_df["wlan.ssid"] == "BYU-WiFi") & (filtered_df["radio_number"] == 2)] #update to use env file
        eduroam_radio_2_df = None # temp for performance, this is not used
        byu_wifi_radio_2_df = None # temp for performance, this is not used
        # Plot for the second radio
        #temp remove for performance
        #plot_file = ""
        plot_file = plot_channel_stations(filtered_radio_2_df, eduroam_radio_2_df, byu_wifi_radio_2_df, ap_name, start_time, end_time)
        #print(plot_file)
        #update totals summary
        radio_summary_df.loc[len(radio_summary_df)] = [
        course, location, ap_name, 2, start_time, end_time,
        capacity, enrolled, ap_count, radio_count, bssid_count, ssid_count, total_beacons,
        high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu,
        high_scount, median_scount, low_scount, longest_duration_high_cu, plot_file
        ]
    else:
        log_and_print(f"{ap_name} has only one 5GHz radio")

    #print()
    
    # return df not needed since it is directly modified
    #return radio_summary_df


# Write to parquet files for each AP that can be combined later
# all_class_beacons is a global variable that is updated by the add_to_all_class_beacons function called in the graph function
def append_to_parquet(AP_name, all_class_beacons_df):
    # Ensure all_class_beacons_df is not empty before appending
    if not all_class_beacons_df.empty:
        ap_beacons_file = f"Data/sampled/{AP_name}_class_beacons.parquet"
        
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


# combine all files in Data/sampled into one file
def combine_all_class_beacons():
    all_class_beacons_df = pd.DataFrame()

    # Directory containing AP .parquet files
    directory = "Data/sampled/"

    # Get a list of all .parquet files
    parquet_files = glob.glob(directory + "*.parquet")

    all_class_beacons_df = all_class_beacons_df.iloc[0:0] #clear contents of the df
    # Read and concatenate all Parquet files (faster than .pkl)
    all_class_beacons_df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)

    all_class_beacons_df.to_parquet('Output/all_class_beacons.parquet')


# main function to process all data in sample files

def main():
    
    #read in sample files
    # this can be done after pickle files have been created from samples.py
    sample_classes_df = pd.read_parquet('Sample/sample_classes.parquet')
    sample_aps_df = pd.read_parquet('Sample/sample_aps.parquet')
    sample_classrooms_df = pd.read_parquet('Sample/sample_classrooms.parquet')

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
    base_dir = "Data/grouped_data"

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
                
                # Call the graph function with the variables (do not need to set to a variable in order to update radio_summary_df)
                # Graph function will remove duplicates, sort, clean the data, add to all_class_beacons_df, calculate station counts, create plots, and do some analysis
                graph(AP_name, start_time, end_time, location, course, enrolled, capacity, ap_count, ap_df, radio_summary_df)
            #write to parquet file
            append_to_parquet(AP_name, all_class_beacons_df) #writes the file for the AP
            all_class_beacons_df = all_class_beacons_df.iloc[0:0]  # Clear contents of the df, otherwise it would build to become all

    # write summary dataframe to a file so it can be analyzed further later
    radio_summary_df.to_parquet('Output/radio_summary.parquet')


#run
# Open logging file
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
log_filename = f"Output/analyze_data_log_{timestamp}.txt"
log_file = open(log_filename, "a")# Open file in append mode
log_and_print(f"Analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
main()