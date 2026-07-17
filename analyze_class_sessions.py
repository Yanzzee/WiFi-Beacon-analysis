def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and class-session inputs
    # =============================================================================
    import pandas as pd
    from tqdm import tqdm
    import matplotlib.pyplot as plt
    import numpy as np
    from datetime import datetime
    import os
    from dotenv import load_dotenv
    
    load_dotenv("config.env")
    
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
    SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")
    CLASS_SESSIONS_FOLDER = os.getenv("CLASS_SESSIONS_FOLDER")
    
    def _env_or_default(name, default):
        value = os.getenv(name)
        return value.strip() if value and value.strip() else default
    
    RADIO_COUNT_SSID = _env_or_default("SSID1", "eduroam")
    try:
        BEACON_RATE = float(os.getenv("BEACON_RATE"))
    except ValueError:
        print("Invalid float value in BEACON_RATE env variable.")
        BEACON_RATE = 0.1024
    if not os.path.exists(CLASS_SESSIONS_FOLDER):   
        os.makedirs(CLASS_SESSIONS_FOLDER)
    
    #log to a file and print to terminal
    # Open logging file
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_filename = f"{OUTPUT_FOLDER}/analyze_class_sessions_log_{timestamp}.txt"
    log_file = open(log_filename, "a")# Open file in append mode
    def log_and_print(message):
        print(message)  # Print to terminal is too noisy
        log_file.write(message + "\n")  # Write to file
    
    
    log_and_print(f"Class session analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # read in the sample for class sessions as a basis for all information for class sessions
    class_sessions_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_classes.parquet")
    sample_aps_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_aps.parquet")
    sample_classrooms_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_classrooms.parquet")
    
    #Adding additional columns to the DataFrame
    # Ensure string columns use Pandas' dedicated string dtype
    class_sessions_df['group'] = pd.Series(dtype="string")
    
    # Set numerical columns with nullable integer dtype (Int64)
    class_sessions_df['number_aps'] = pd.Series(dtype="Int64")
    class_sessions_df['number_radios'] = pd.Series(dtype="Int64")
    class_sessions_df['number_beacons'] = pd.Series(dtype="Int64")
    class_sessions_df['max_stations'] = pd.Series(dtype="Int64")
    class_sessions_df['max_cu'] = pd.Series(dtype="Int64")
    class_sessions_df['average_cu'] = pd.Series(dtype="Float64")
    class_sessions_df['median_cu'] = pd.Series(dtype="Float64")
    class_sessions_df['stations_enrolled'] = pd.Series(dtype="Float64")
    
    # Compute class_duration in SECONDS (float since it involves time differences)
    class_sessions_df['class_duration'] = (class_sessions_df['end_time'] - class_sessions_df['start_time']).dt.total_seconds().astype("Float64")
    
    # Ensure duration_75 and duration_50 are also float types but nullable
    class_sessions_df['duration_75'] = pd.Series(dtype="Float64")
    class_sessions_df['duration_50'] = pd.Series(dtype="Float64")
    class_sessions_df['max_streak_75'] = pd.Series(dtype="Float64")
    class_sessions_df['max_streak_50'] = pd.Series(dtype="Float64")
    class_sessions_df['sum_scount_75'] = pd.Series(dtype="Float64")
    class_sessions_df['sum_scount_50'] = pd.Series(dtype="Float64")
    
    # Count occurrences of each Building-Room in sample_aps_df
    aps_count = sample_aps_df['Building-Room'].value_counts()
    
    # Map these counts to the number_aps column in class_sessions_df
    class_sessions_df['number_aps'] = class_sessions_df['location'].map(aps_count).astype("Int64")
    
    #Group A, B, and C are from the sample selection based on classroom size
    # Create a mapping from sample_classrooms_df: Location -> Group
    location_to_group = sample_classrooms_df.set_index('Location')['Group']
    
    # Map the Group values to class_sessions_df based on location
    class_sessions_df['group'] = class_sessions_df['location'].map(location_to_group).astype("string")
    
    # =============================================================================
    # Cell 2 - Calculate metrics for each class session
    # =============================================================================
    # calculate metrics for each class session
    all_class_beacons_processed_df = pd.read_parquet(f"{DATA_FOLDER}/all_class_beacons_processed.parquet")
    
    # Use the configured effective metadata interval as the high-CU duration/streak bin width.
    BEACON_WINDOW_SEC = BEACON_RATE 
    CU_THRESHOLDS = {'75': 191, '50': 127}
    
    print("Calculating metrics for each class session...")
    for idx, session in tqdm(class_sessions_df.iterrows(), total=len(class_sessions_df)):
        start = session['start_time']
        end = session['end_time']
        location = session['location']
    
        mask = (
            (all_class_beacons_processed_df['aruba_erm.time'] >= start) &
            (all_class_beacons_processed_df['aruba_erm.time'] <= end) &
            (all_class_beacons_processed_df['location'] == location)
        )
        filtered = all_class_beacons_processed_df[mask].copy()
        filtered.sort_values('aruba_erm.time', inplace=True)
    
        number_beacons = len(filtered)
    
        if number_beacons > 0:
            max_scount = filtered['room_scount'].max()
            cu_values = filtered['wlan.qbss.cu'].dropna()
            max_cu = cu_values.max()
            avg_cu = cu_values.mean()
            median_cu = cu_values.median()
    
            # Time binning for duration + streaks
            time_range = pd.date_range(start, end, freq=f"{BEACON_WINDOW_SEC}s")
            duration = {'75': 0, '50': 0}
            current_streak = {'75': 0, '50': 0}
            max_streak = {'75': 0, '50': 0}
    
            for i in range(len(time_range) - 1):
                t_start = time_range[i]
                t_end = time_range[i + 1]
    
                bin_slice = filtered[
                    (filtered['aruba_erm.time'] >= t_start) & (filtered['aruba_erm.time'] < t_end)
                ]
    
                if not bin_slice.empty:
                    for level, threshold in CU_THRESHOLDS.items():
                        if (bin_slice['wlan.qbss.cu'] > threshold).any():
                            duration[level] += BEACON_WINDOW_SEC
                            current_streak[level] += BEACON_WINDOW_SEC
                            max_streak[level] = max(max_streak[level], current_streak[level])
                        else:
                            current_streak[level] = 0
    
            # Scount sums for each threshold based on unique wlan.ta
            sum_scount = {}
            for level, threshold in CU_THRESHOLDS.items():
                cu_filtered = filtered[filtered['wlan.qbss.cu'] > threshold]
                # Drop duplicate wlan.ta, keep first (could use .max() if preferred)
                unique_scount = cu_filtered.drop_duplicates('wlan.ta')
                sum_scount[level] = unique_scount['wlan.qbss.scount'].sum()
            ssid_radios = filtered[filtered['wlan.ssid'] == RADIO_COUNT_SSID]['wlan.ta'].nunique()
     
        else:
            max_scount = max_cu = avg_cu = median_cu = 0
            duration = {'75': 0, '50': 0}
            max_streak = {'75': 0, '50': 0}
            sum_scount = {'75': 0, '50': 0}
            ssid_radios = 0
    
        # Save values back to DataFrame
        class_sessions_df.at[idx, 'max_stations'] = max_scount
        class_sessions_df.at[idx, 'stations_enrolled'] = max_scount / session['enrolled'] if session['enrolled'] else 0
        class_sessions_df.at[idx, 'max_cu'] = max_cu
        class_sessions_df.at[idx, 'average_cu'] = avg_cu
        class_sessions_df.at[idx, 'median_cu'] = median_cu
        class_sessions_df.at[idx, 'number_beacons'] = number_beacons
    
        class_sessions_df.at[idx, 'duration_75'] = duration['75']
        class_sessions_df.at[idx, 'duration_50'] = duration['50']
        class_sessions_df.at[idx, 'max_streak_75'] = max_streak['75']
        class_sessions_df.at[idx, 'max_streak_50'] = max_streak['50']
        class_sessions_df.at[idx, 'sum_scount_75'] = sum_scount['75']
        class_sessions_df.at[idx, 'sum_scount_50'] = sum_scount['50']
        class_sessions_df.at[idx, 'number_radios'] = ssid_radios
    
    
    # =============================================================================
    # Cell 3 - Calculate aggregate class-session metrics
    # =============================================================================
    # Total number of class sessions
    total_sessions = len(class_sessions_df)
    
    # Count of sessions with CU > 50% (i.e., duration_50 > 0)
    sessions_over_50 = (class_sessions_df['duration_50'] > 0).sum()
    
    # Count of sessions with CU > 75% (i.e., duration_75 > 0)
    sessions_over_75 = (class_sessions_df['duration_75'] > 0).sum()
    
    # Percentages
    percent_over_50 = (sessions_over_50 / total_sessions) * 100
    percent_over_75 = (sessions_over_75 / total_sessions) * 100
    
    log_and_print(f"Percentage of class sessions with at least 1s CU > 50%: {percent_over_50:.2f}%")
    log_and_print(f"Percentage of class sessions with at least 1s CU > 75%: {percent_over_75:.2f}%")
    
    
    # Total durations
    total_duration_50 = class_sessions_df['duration_50'].sum()
    total_duration_75 = class_sessions_df['duration_75'].sum()
    total_duration_all = class_sessions_df['class_duration'].sum()
    
    # Total number of stations affected (from sum_scount columns)
    total_scount_50 = class_sessions_df['sum_scount_50'].sum()
    total_scount_75 = class_sessions_df['sum_scount_75'].sum()
    total_scount_all = class_sessions_df['max_stations'].sum()
    
    log_and_print(f"Total duration all classes: {total_duration_all:.2f} seconds")
    log_and_print(f"Total class duration with CU > 50%: {total_duration_50:.2f} seconds ({total_duration_50 / total_duration_all * 100:.2f}%)")
    log_and_print(f"Total class duration with CU > 75%: {total_duration_75:.2f} seconds ({total_duration_75 / total_duration_all * 100:.2f}%)")
    
    #this is probably not uesful because the same students/stations are in multiple classes
    log_and_print(f"Total stations in all classes: {int(total_scount_all)}")
    log_and_print(f"Total stations affected (CU > 50%): {int(total_scount_50)} ({total_scount_50 / total_scount_all * 100:.2f}%)")
    log_and_print(f"Total stations affected (CU > 75%): {int(total_scount_75)} ({total_scount_75 / total_scount_all * 100:.2f}%)")
    
    # For max_streak_50
    mean_streak_50 = class_sessions_df['max_streak_50'].mean()
    median_streak_50 = class_sessions_df['max_streak_50'].median()
    
    # For max_streak_75
    mean_streak_75 = class_sessions_df['max_streak_75'].mean()
    median_streak_75 = class_sessions_df['max_streak_75'].median()
    
    log_and_print("Duration of longest continuous channel utilization")
    log_and_print(f"CU > 50% - Mean Streak: {mean_streak_50:.2f} sec, Median Streak: {median_streak_50:.2f} sec")
    log_and_print(f"CU > 75% - Mean Streak: {mean_streak_75:.2f} sec, Median Streak: {median_streak_75:.2f} sec")
    
    
    
    
    # =============================================================================
    # Cell 4 - Plot high-CU streak histograms
    # =============================================================================
    # histogram of max streak durations for each class session
    
    # Get the maximum value across both columns
    max_val = int(max(class_sessions_df['max_streak_50'].max(),
                      class_sessions_df['max_streak_75'].max()))
    
    # Create bins from 0 to max_val with step 1
    bins = range(0, max_val + 2)  # +2 to include the last value in the bin range
    
    plt.figure(figsize=(10, 5))
    plt.hist(class_sessions_df['max_streak_50'], bins=bins, alpha=0.6, label='CU > 50%')
    plt.hist(class_sessions_df['max_streak_75'], bins=bins, alpha=0.6, label='CU > 75%')
    
    #plt.xlim(0, 120)  # Adjust based on where most of your data falls
    plt.xlabel('Max Continuous High CU Duration (seconds)')
    plt.ylabel('Number of Class Sessions')
    plt.title('Max High CU Streaks (showing outliers)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plot_file = f"{CLASS_SESSIONS_FOLDER}/High_CU_streaks.png"
    #save to a file
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")
    
    #plot again zoomed in
    plt.figure(figsize=(10, 5))
    plt.hist(class_sessions_df['max_streak_50'], bins=bins, alpha=0.6, label='CU > 50%')
    plt.hist(class_sessions_df['max_streak_75'], bins=bins, alpha=0.6, label='CU > 75%')
    plt.xlim(0, 60)  # Adjust based on where most of your data falls
    plt.xlabel('Max Continuous High CU Duration (seconds)')
    plt.ylabel('Number of Class Sessions')
    plt.title('Max High CU Streaks (zoomed in to 60 seconds)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    plot_file = f"{CLASS_SESSIONS_FOLDER}/High_CU_streaks_zoomed.png"
    #save to a file
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")
    
    
    
    # =============================================================================
    # Cell 5 - Calculate average devices per enrolled student
    # =============================================================================
    # Calculate the total of max_stations and enrolled
    total_max_stations = class_sessions_df['max_stations'].sum()
    total_enrolled = class_sessions_df['enrolled'].sum()
    
    # Prevent division by zero
    if total_enrolled == 0:
        average_devices_per_enrolled = 0
    else:
        average_devices_per_enrolled = total_max_stations / total_enrolled
    
    log_and_print(f"Average devices per enrolled: {average_devices_per_enrolled:.2f}")
    
    # =============================================================================
    # Cell 6 - Plot stations-per-enrolled distribution
    # =============================================================================
    # Distribution of Stations per enrolled
    class_sessions_df['stations_enrolled'].hist(bins=60)
    #plt.figure(figsize=(10, 5))
    plt.title("Distribution of Stations per Enrolled")
    plt.xlabel("Stations per Enrolled")
    plt.ylabel("Number of Class Sessions")
    plot_file = f"{CLASS_SESSIONS_FOLDER}/stations_enrolled_histogram.png"
    #save to a file
    plt.savefig(plot_file)
    plt.show()
    plt.close("all")
    
    # =============================================================================
    # Cell 7 - Plot enrollment versus station count and close log
    # =============================================================================
    
    # Create scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(class_sessions_df['enrolled'], class_sessions_df['max_stations'], alpha=0.5, edgecolors='k')
    
    # Labels and title
    plt.xlabel("Enrolled")
    plt.ylabel("Associated stations")
    plt.title("Comparison of class enrollment to station count")
    plt.grid(True)
    
    plot_file = f"{CLASS_SESSIONS_FOLDER}/stations_enrolled_plot.png"
    #save to a file
    plt.savefig(plot_file)
    
    # Show the plot
    plt.show()
    plt.close("all")
    
    log_file.close()


if __name__ == "__main__":
    main()
