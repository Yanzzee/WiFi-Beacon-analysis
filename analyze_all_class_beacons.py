def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and input data
    # =============================================================================
    import pandas as pd
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
    import numpy as np
    from datetime import datetime
    from scipy.stats import linregress
    import os
    from dotenv import load_dotenv
    from analysis_config import ensure_directory, env_or_default as _env_or_default
    from analysis_logging import open_analysis_log
    from analysis_statistics import compute_pdf_cdf
    from class_beacon_plotting import (
        plot_channel_utilization_admission_capacity_hexbin,
        plot_channel_utilization_histogram,
        plot_channel_utilization_pdf_cdf,
        plot_station_count_admission_capacity_hexbin,
        plot_station_count_channel_utilization_hexbin,
        plot_station_count_histogram,
    )
    from room_station_count import enrich_room_station_counts
    
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
    
    all_class_beacons_df = enrich_room_station_counts(
        all_class_beacons_df,
        sample_aps_df,
    )

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
    
    plot_channel_utilization_histogram(
        cu_percent,
        bins,
        ALL_CLASS_BEACONS_FOLDER,
    )
    
    
    
    # =============================================================================
    # Cell 6 - Plot channel-utilization PDF and CDF
    # =============================================================================
    # PDF/CDF plots (as percentage)
    # Drop NaNs and convert to percentage scale
    cu_data = all_class_beacons_df["wlan.qbss.cu"].dropna() * (100 / 255)
    
    # Compute PDF/CDF
    bin_centers, pdf, cdf = compute_pdf_cdf(cu_data, bins=256)
    
    plot_channel_utilization_pdf_cdf(
        bin_centers,
        pdf,
        cdf,
        ALL_CLASS_BEACONS_FOLDER,
    )
    
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
    
    plot_station_count_histogram(
        all_class_beacons_df["total_scount"],
        num_bins,
        ALL_CLASS_BEACONS_FOLDER,
    )
    
    
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
    plot_station_count_channel_utilization_hexbin(
        all_class_beacons_df["total_scount"],
        all_class_beacons_df["wlan.qbss.cu"],
        ALL_CLASS_BEACONS_FOLDER,
    )
    
    
    # =============================================================================
    # Cell 10 - Plot station count versus admission capacity
    # =============================================================================
    
    # create a hexbin plot to visualize the correlation between station count and admission capacity
    
    plot_station_count_admission_capacity_hexbin(
        all_class_beacons_df["total_scount"],
        all_class_beacons_df["wlan.qbss.adc"],
        ALL_CLASS_BEACONS_FOLDER,
    )
    
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
    plot_channel_utilization_admission_capacity_hexbin(
        all_class_beacons_df["wlan.qbss.cu"],
        all_class_beacons_df["wlan.qbss.adc"],
        ALL_CLASS_BEACONS_FOLDER,
    )
    
    
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
