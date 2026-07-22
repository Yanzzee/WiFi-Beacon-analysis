def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and input data
    # =============================================================================
    import pandas as pd
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
    import numpy as np
    from tqdm import tqdm
    from pathlib import Path
    from datetime import datetime
    from scipy.stats import linregress
    from tqdm import tqdm
    import os
    from dotenv import load_dotenv
    from analysis_config import ensure_directory, env_or_default as _env_or_default
    from analysis_logging import open_analysis_log
    from analysis_statistics import compute_pdf_cdf
    
    load_dotenv("config.env")
    
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
    SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")
    ALL_CLASS_BEACONS_FOLDER = os.getenv("ALL_CLASS_BEACONS_FOLDER")
    
    SSID1 = _env_or_default("SSID1", "eduroam")
    SSID2 = _env_or_default("SSID2", "BYU-WiFi")
    CONFIGURED_SSIDS = [SSID1, SSID2]
    
    ensure_directory(ALL_CLASS_BEACONS_FOLDER)
    
    #log to a file and print to terminal
    # Open logging file
    timestamp, log_filename, log_file = open_analysis_log(
        OUTPUT_FOLDER,
        "analyze_all_class_beacons",
    )
    def log_and_print(message):
        print(message)  # Print to terminal is too noisy
        log_file.write(message + "\n")  # Write to file
    
    log_and_print(f"Analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    
    # read in relevant data
    all_class_beacons_df = pd.read_parquet(f'{DATA_FOLDER}/all_class_beacons.parquet')
    sample_aps_df = pd.read_parquet(f'{SAMPLE_FOLDER}/sample_aps.parquet')
    
    #print(sample_aps_df.head())
    
    
    
    
    # =============================================================================
    # Cell 2 - Calculate station count for each location
    # =============================================================================
    # create a column for total room station count and populate it
    # This needs to be done before additional SSIDs are removed
    # iterate through each location, find all APs in that location, and find the unique wlan.ta MAC addresses of all the APs
    # then add the wlan.qbss.scount values for all the wlan.ta values for each beacon
    # the result is the total number of stations in the room
    
    # Step 1: Set up the mappings
    # Map AP names in sample_aps_df to locations
    hostname_to_location = sample_aps_df.set_index("Hostname")["Building-Room"].to_dict()
    location_to_hostnames = sample_aps_df.groupby("Building-Room")["Hostname"].apply(list).to_dict()
    print("Calculating station count for each location")
    print("Step 1 done")
    # Step 2: Prepare main DataFrame
    df = all_class_beacons_df.sort_values("aruba_erm.time").reset_index(drop=True)
    df["room_scount"] = 0
    df["location"] = df["wlan.vs.aruba.ap_name"].map(hostname_to_location) #add location information for quicker analysis
    
    print("Step 2 done")
    # Step 3: Preprocess: create lookup per AP (wlan.vs.aruba.ap_name)
    # Directory where your parquet files live
    parquet_dir = Path("Data/sampled")
    
    host_dfs = {}
    host_time_arrays = {}
    host_ta_arrays = {}
    host_scount_arrays = {}
    
    for hostname in tqdm(df["wlan.vs.aruba.ap_name"].unique(), desc="Preprocessing APs for faster lookup"):
      
        host_df = df[df["wlan.vs.aruba.ap_name"] == hostname].copy() # more reliable than using existing .parquet files
        host_df = host_df.sort_values("aruba_erm.time").reset_index(drop=True)
    
        times = host_df["aruba_erm.time"].astype("int64").values
        tas = host_df["wlan.ta"].values
        scounts = host_df["wlan.qbss.scount"].values
    
        assert len(times) == len(tas) == len(scounts), f"Array mismatch for AP {hostname}"
    
        host_time_arrays[hostname] = times
        host_ta_arrays[hostname] = tas
        host_scount_arrays[hostname] = scounts
    
    
    print("Step 3 done")
    # Step 4: Process each row
    results = np.zeros(len(df), dtype=int)
    five_seconds_ns = np.timedelta64(5, 's').astype('timedelta64[ns]').astype('int64')
    
    for i in tqdm(range(len(df)), desc="Computing room_scount for each beacon"):
        row = df.iloc[i]
        current_time = row["aruba_erm.time"]
        current_hostname = row["wlan.vs.aruba.ap_name"]
        current_ta = row["wlan.ta"]
        location = hostname_to_location.get(current_hostname)
    
        if not location:
            continue
    
        room_hostnames = location_to_hostnames[location]  # include current AP
    
        seen_ta = {current_ta}
        room_scount = row["wlan.qbss.scount"]
        current_time_ns = current_time.value
    
        ta_to_best_idx = {}
    
        for other_hostname in room_hostnames:
            if other_hostname not in host_time_arrays:
                continue
    
            times = host_time_arrays[other_hostname]         # int64 nanoseconds
            scounts = host_scount_arrays[other_hostname]
            tas = host_ta_arrays[other_hostname]
    
            #assert len(times) == len(host_ta_arrays[hostname]) == len(host_scount_arrays[hostname]), \
            #f"Array length mismatch for AP {hostname}"
    
    
            # Get ALL rows from this AP within ±5 seconds of current time
            left_bound = current_time_ns - five_seconds_ns
            right_bound = current_time_ns + five_seconds_ns
    
            left_idx = np.searchsorted(times, left_bound, side='left')
            right_idx = np.searchsorted(times, right_bound, side='right')
    
            matching_indices = range(left_idx, right_idx)
    
    
            
    
            for j in matching_indices:
                ta = tas[j]
                time_diff = np.abs(times[j] - current_time_ns)
        
                if ta not in ta_to_best_idx or time_diff < np.abs(
                    host_time_arrays[ta_to_best_idx[ta][0]][ta_to_best_idx[ta][1]] - current_time_ns):
                    ta_to_best_idx[ta] = (other_hostname, j)
    
        for ta, (host, j) in ta_to_best_idx.items():
            if ta not in seen_ta:
                seen_ta.add(ta)
                room_scount += host_scount_arrays[host][j]
    
        results[i] = int(room_scount)
    
    
    # Step 5: Assign results back to the DataFrame
    df["room_scount"] = results
    all_class_beacons_df = df  # Optional: assign back to original
    
    #this is used in the class sessions file:
    all_class_beacons_df.to_parquet(f"{DATA_FOLDER}/all_class_beacons_processed.parquet", engine="pyarrow")
    
    
    
    # =============================================================================
    # Cell 3 - Filter to configured SSIDs
    # =============================================================================
    #reduce overrepresentation of areas that have additional SSIDs
    print(all_class_beacons_df["wlan.ssid"].unique())
    all_class_beacons_df = all_class_beacons_df[all_class_beacons_df["wlan.ssid"].isin(CONFIGURED_SSIDS)]
    print(all_class_beacons_df["wlan.ssid"].unique())
    
    
    # =============================================================================
    # Cell 4 - Calculate aggregate class-beacon metrics
    # =============================================================================
    # metrics for all beacons during class sessions in a single dataframe
    
    total_data_points = len(all_class_beacons_df)
    
    # Count the number of data points where wlan.qbss.cu is greater than 191 (75%)
    greater_than_191_count = (all_class_beacons_df["wlan.qbss.cu"] > 191).sum()
    
    # Calculate the percentage of data points where wlan.qbss.cu is greater than 191
    percentage_greater_than_191 = (greater_than_191_count / total_data_points) * 100
    
    log_and_print(f"percent of beacons with very high utilization (> 75% cu): {percentage_greater_than_191:.4f} %")
    
    # Count the number of data points where wlan.qbss.cu is greater than 127 (50%)
    greater_than_127_count = (all_class_beacons_df["wlan.qbss.cu"] > 127).sum()
    
    # Calculate the percentage of data points where wlan.qbss.cu is greater than 127
    percentage_greater_than_127 = (greater_than_127_count / total_data_points) * 100
    
    log_and_print(f"percent of beacons with high utilization (> 50% cu): {percentage_greater_than_127:.4f} %")
    
    log_and_print(f"median number of associated devices per radio: {all_class_beacons_df['total_scount'].median()}")
    
    log_and_print(f"average number of associated devices per radio: {all_class_beacons_df['total_scount'].mean():.2f}")
    
    log_and_print(f"median channel utilization qbss: {all_class_beacons_df['wlan.qbss.cu'].median()}")
    
    log_and_print(f"average channel utilization qbss: {all_class_beacons_df['wlan.qbss.cu'].mean():.2f}")
    
    # =============================================================================
    # Cell 5 - Plot channel-utilization histogram
    # =============================================================================
    
    # # Create histogram for the 'wlan.qbss.cu' column
    
    
    # Convert the wlan.qbss.cu values to percentage (0-100 scale)
    cu_percent = all_class_beacons_df["wlan.qbss.cu"] * (100 / 255)
    
    # Create bins that match the percentage scale (still 256 bins)
    bins = np.linspace(0, 100, 256)
    
    # Make the figure larger
    plt.figure(figsize=(10, 6))
    
    # Create histogram
    plt.hist(cu_percent, bins=bins, edgecolor='black', align='mid')
    
    # Add labels and title with spacing adjustment
    plt.xlabel('Channel Utilization (%)')
    plt.ylabel('Frequency', labelpad=15)  # Add padding to prevent label overlap
    plt.title('Histogram of Channel Utilization Values')
    
    # Adjust layout to prevent label/title overlap
    plt.tight_layout()
    
    # Save to a file
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Channel_use_histogram.png"
    plt.savefig(plot_file)
    
    # Show the plot
    plt.show()
    plt.close()
    
    
    
    # =============================================================================
    # Cell 6 - Plot channel-utilization PDF and CDF
    # =============================================================================
    # PDF/CDF plots (as percentage)
    # Drop NaNs and convert to percentage scale
    cu_data = all_class_beacons_df["wlan.qbss.cu"].dropna() * (100 / 255)
    
    # Compute PDF/CDF
    bin_centers, pdf, cdf = compute_pdf_cdf(cu_data, bins=256)
    
    # --- PDF Plot ---
    plt.figure(figsize=(10, 6))
    plt.plot(bin_centers, pdf, color='purple', label='PDF - wlan.qbss.cu (%)')
    plt.xlabel('Channel Utilization (%)')
    plt.ylabel('Probability Density')
    plt.title('PDF of Channel Utilization')
    plt.legend()
    plt.grid()
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Channel_use_pdf.png"
    plt.savefig(plot_file)
    plt.show()
    
    # --- CDF Plot ---
    plt.figure(figsize=(10, 6))
    plt.plot(bin_centers, cdf, color='orange', label='CDF - wlan.qbss.cu (%)')
    plt.xlabel('Channel Utilization (%)')
    plt.ylabel('Cumulative Probability')
    plt.title('CDF of Channel Utilization')
    plt.legend()
    plt.grid()
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Channel_use_cdf.png"
    plt.savefig(plot_file)
    plt.show()
    
    # Report CDF at ~50% and ~75% thresholds
    cdf_at_50 = (cu_data <= 50).mean()
    log_and_print(f"CDF at wlan.qbss.cu = 50%: {cdf_at_50:.4f}")
    log_and_print(f"{(cdf_at_50 * 100):.2f} % of beacons during class show channel utilization under 50%")
    
    cdf_at_75 = (cu_data <= 75).mean()
    log_and_print(f"CDF at wlan.qbss.cu = 75%: {cdf_at_75:.4f}")
    log_and_print(f"{(cdf_at_75 * 100):.2f} % of beacons during class show channel utilization under 75%")
    
    
    # =============================================================================
    # Cell 7 - Plot station-count histogram
    # =============================================================================
    # Histogram of radio scount
    # Set the number of bins to be the highest value of total_scount + 1
    num_bins = max(all_class_beacons_df["total_scount"]) + 1
    
    # Make the figure larger
    plt.figure(figsize=(10, 6))
    
    # Create histogram
    plt.hist(all_class_beacons_df["total_scount"], bins=num_bins, edgecolor='black')
    
    # Add labels and title
    plt.xlabel('Total Station Count')
    plt.ylabel('Frequency', labelpad=15)  # Add padding to prevent label overlap
    plt.title('Histogram of total station count values for all class beacons')
    
    # Adjust layout to prevent label/title overlap
    plt.tight_layout()
    
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Scount_histogram.png"
    #save to a file
    plt.savefig(plot_file)
    
    # Show the plot
    plt.show()
    plt.close()
    
    
    # =============================================================================
    # Cell 8 - Calculate correlations
    # =============================================================================
    # Calculate the pearson correlation between wlan.qbss.cu and total_scount
    correlation = all_class_beacons_df["total_scount"].corr(all_class_beacons_df["wlan.qbss.cu"])
    
    # Display the result
    log_and_print(f"The correlation between total_scount and wlan.qbss.cu is: {correlation:.4f}")
    
    # Calculate the pearson correlation between wlan.qbss.adc and total_scount
    correlation_adc = all_class_beacons_df["wlan.qbss.adc"].corr(all_class_beacons_df["wlan.qbss.cu"])
    
    log_and_print(f"The correlation between wlan.qbss.adc and wlan.qbss.cu is: {correlation_adc:.4f}")
    
    # Calculate the pearson correlation between wlan.qbss.cu and total_scount
    correlation_adc_sc = all_class_beacons_df["total_scount"].corr(all_class_beacons_df["wlan.qbss.adc"])
    
    log_and_print(f"The correlation between total_scount and wlan.qbss.adc is: {correlation_adc_sc:.4f}")
    
    
    # =============================================================================
    # Cell 9 - Plot station count versus channel utilization
    # =============================================================================
    
    # # create a hexbin plot to visualize the correlation between station count and channel utilization
    # Create a hexbin plot with channel utilization shown as percentage (0–100%)
    plt.hexbin(
        all_class_beacons_df["total_scount"],                      # X-axis: station count
        all_class_beacons_df["wlan.qbss.cu"] * (100 / 255),        # Y-axis: channel utilization in %
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm()  # Log scaling for better visibility
    )
    
    plt.xlabel("Total Station Count")  # X-axis label
    plt.ylabel("Channel Utilization (%)")  # Updated Y-axis label
    plt.title("Hexbin Plot: Channel Utilization (%) vs. Station Count")
    
    plt.colorbar(label="Log Count")
    
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Scount_channel_use_hexbin.png"
    plt.savefig(plot_file)
    
    plt.show()
    plt.close()
    
    
    # =============================================================================
    # Cell 10 - Plot station count versus admission capacity
    # =============================================================================
    
    # create a hexbin plot to visualize the correlation between station count and admission capacity
    
    plt.hexbin(
        all_class_beacons_df["total_scount"],  # Now on x-axis
        all_class_beacons_df["wlan.qbss.adc"],  # Now on y-axis
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm()  # Log scaling for better visibility
    )
    
    plt.xlabel("Total Station Count")  # X-axis: station count
    plt.ylabel("Admission Capacity")  # Y-axis: admission capacity
    plt.title("Hexbin Plot: Admission Capacity vs. Station Count")
    
    plt.colorbar(label="Log Count")
    
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/Scount_adc_hexbin.png"
    #save to a file
    plt.savefig(plot_file)
    
    plt.show()
    plt.close()
    
    # =============================================================================
    # Cell 11 - Calculate linear regression
    # =============================================================================
    # Linear regression
    
    x = all_class_beacons_df["total_scount"]
    y = all_class_beacons_df["wlan.qbss.cu"]
    
    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    log_and_print("Linear regression values of total_scount to wlan.qbss.cu:")
    log_and_print(f"Slope: {slope:.3f}")
    log_and_print(f"Intercept: {intercept:.3f}")
    log_and_print(f"R-squared: {r_value**2:.3f}")
    log_and_print(f"P-value: {p_value:.3g}")
    
    log_file.close()
    
    #255 = .442x + 21.618
    #x = 528.01
    # this model suggests that 528 clients would use about 100% of airtime
    #127.5 = .442x + 21.618
    #x = 239.4
    #also suggests that 239 clients would use 50% of airtime
    
    # =============================================================================
    # Cell 12 - Plot channel utilization versus admission capacity
    # =============================================================================
    # # Channel utilization vs admission capacity
    plt.figure()
    plt.hexbin(
        all_class_beacons_df['wlan.qbss.cu'] * (100 / 255),  # CU as percentage
        all_class_beacons_df['wlan.qbss.adc'],               # ADC remains unchanged
        gridsize=50,
        cmap="Blues",
        norm=mcolors.LogNorm()  # Log scaling for better visibility
    )
    plt.colorbar(label='Log Count')
    plt.xlabel('Channel Utilization (%)')  # Updated label
    plt.ylabel('Admission Capacity')
    plt.title('Hexbin of Channel Utilization (%) vs. wlan.qbss.adc')
    
    plot_file = f"{ALL_CLASS_BEACONS_FOLDER}/adc_channel_use_hexbin.png"
    plt.savefig(plot_file)
    
    plt.show()
    plt.close()
    
    
    # =============================================================================
    # Cell 13 - Write processed class-beacon data
    # =============================================================================
    #write out if needed
    #!saves the df afer removing additional SSIDs
    all_class_beacons_df.to_parquet(f"{DATA_FOLDER}/all_class_beacons_processed.parquet", engine="pyarrow")


if __name__ == "__main__":
    main()


# Cell 14 preserves debug-only content from the source notebook.
# The converted pipeline and regression harness execute only Cells 1-13.

# =============================================================================
# Cell 14 - Debug summary statistics
# =============================================================================
# #debug
# print(all_class_beacons_df.head)
# median_cu = all_class_beacons_df['wlan.qbss.cu'].median()
# mean_cu = all_class_beacons_df['wlan.qbss.cu'].mean()
#
# print(f"Median wlan.qbss.cu: {median_cu}")
# print(f"Mean wlan.qbss.cu: {mean_cu}")
#
