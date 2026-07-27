def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and radio-summary input
    # =============================================================================
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import statsmodels.api as sm
    from datetime import datetime
    import os
    from dotenv import load_dotenv
    from analysis_config import ensure_directory
    from analysis_logging import open_analysis_log
    from analysis_statistics import compute_pdf_cdf
    from radio_summary_processing import (
        prepare_radio_summary_frames,
        sort_radio_summary,
    )
    from radio_summary_plotting import (
        plot_beacon_rate_histogram,
        plot_beacon_rate_channel_utilization_scatter,
        plot_radio_count_split_scatter,
    )
    
    load_dotenv("config.env")
    
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
    RADIO_SUMMARY_FOLDER = os.getenv("RADIO_SUMMARY_FOLDER")
    
    ensure_directory(RADIO_SUMMARY_FOLDER)
    
    
    #log to a file and print to terminal
    # Open logging file
    timestamp, log_filename, log_file = open_analysis_log(
        OUTPUT_FOLDER,
        "analyze_radio_summary",
    )
    def log_and_print(message):
        print(message)  # Print to terminal is too noisy
        log_file.write(message + "\n")  # Write to file
    
    
    log_and_print(f"Radio analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    
    radio_summary_df = pd.read_parquet(f"{DATA_FOLDER}/radio_summary.parquet")
    #radio_summary_df.head()
    
    radio_summary_df = sort_radio_summary(radio_summary_df)
    #radio_summary_df.head(10)
    
    #analyzing radio_summary - dataframe containing 1 row per radio per class session
    log_and_print("rows in the radio_summary_df represent a single 5GHz radio per class session")
    log_and_print(f"total number of rows: {len(radio_summary_df)}")
    log_and_print(f"median number of beacons: {radio_summary_df['total_beacons'].median()}")
    #expecting 5859 beacons per 50 minutes if there's 2 SSIDs
    log_and_print(f"median row of high channel utilization duration: {radio_summary_df['longest_duration_high_cu'].median()}")
    log_and_print(f"(channel utilization is in percentage: {(radio_summary_df['highest_cu'].max())})")
    log_and_print(f"number of rows with total beacons less than 200: {((radio_summary_df['total_beacons'] < 200).sum())}")
    log_and_print(f"number of rows with some very high channel use (>75%): {((radio_summary_df['highest_cu'] > 75).sum())}")
    log_and_print(f"number of rows with some high channel use (>50%): {((radio_summary_df['highest_cu'] > 50).sum())}")
    log_and_print(f"median number of devices per radio (peak per class session): {((radio_summary_df['high_scount']).median())}")
    log_and_print(f"average number of devices per radio (peak per class session): {((radio_summary_df['high_scount']).mean()):.2f}")
    
    # Calculate the correlation between high_scount and median_cu
    correlation = radio_summary_df["high_scount"].corr(radio_summary_df["median_cu"])
    
    # Display the result
    log_and_print(f"The correlation between high_scount and median channel utilization is: {correlation}")
    
    radio_summary_df.to_csv(f"{OUTPUT_FOLDER}/radio_summary.csv", index=False)
    
    (
        filtered_radio_summary_df,
        df_radio_1,
        df_radio_2,
        combined_df,
    ) = prepare_radio_summary_frames(radio_summary_df, log_and_print)
    
    # =============================================================================
    # Cell 2 - Filter radio summaries and plot beacon rate versus channel utilization
    # =============================================================================
    
    plot_beacon_rate_channel_utilization_scatter(
        filtered_radio_summary_df,
        OUTPUT_FOLDER,
    )
    
    
    
    
    # =============================================================================
    # Cell 3 - Split data by radio count
    # =============================================================================
    plot_radio_count_split_scatter(
        filtered_radio_summary_df,
        df_radio_1,
        df_radio_2,
        RADIO_SUMMARY_FOLDER,
    )
    
    # =============================================================================
    # Cell 4 - Plot linear regressions by radio count
    # =============================================================================
    # Function to perform linear regression and return model & predictions
    def linear_regression(x, y):
        X = sm.add_constant(x)  # Add intercept term
        model = sm.OLS(y, X).fit()  # Fit model
        return model, model.predict(X)
    
    def plot_radio_regression(ax, radio_df, radio_count, color):
        title = f"Linear Regression for Radio Count {radio_count} ({color.title()})"
        ax.set_title(title)
        ax.set_xlabel("Highest Channel Utilization")
        ax.set_xlim(0, 100)
        ax.grid(True)
    
        if radio_df.empty:
            log_and_print(f"Skipping linear regression for radio_count {radio_count}: no rows after filtering.")
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            return None
    
        y = radio_df['beacons_per_second'] / radio_df['ssid_count']
        model, predictions = linear_regression(radio_df['highest_cu'], y)
        ax.scatter(radio_df['highest_cu'], y, alpha=0.5, edgecolors='k', c=color, label="Data Points")
        ax.plot(radio_df['highest_cu'], predictions, color='black', linewidth=2, label="Regression Line")
        ax.legend()
        return model
    
    # Create a figure with 2 subplots
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    
    model_1 = plot_radio_regression(axes[0], df_radio_1, 1, 'blue')
    axes[0].set_ylabel("Beacons per Second of Class")
    model_2 = plot_radio_regression(axes[1], df_radio_2, 2, 'red')
    
    # Adjust layout and show the plots
    plt.tight_layout()
    
    plot_file = f"{RADIO_SUMMARY_FOLDER}/Beacons_per_second_class_linear_regression.png"
    #save to a file
    plt.savefig(plot_file)
    
    plt.show()
    
    # Print regression summaries
    if model_1 is not None:
        log_and_print("Linear Regression for Radio Count 1:")
        log_and_print(model_1.summary().as_text())
    if model_2 is not None:
        log_and_print("\nLinear Regression for Radio Count 2:")
        log_and_print(model_2.summary().as_text())
    
    
    # =============================================================================
    # Cell 5 - Plot beacon-rate histogram
    # =============================================================================
    plot_beacon_rate_histogram(
        filtered_radio_summary_df,
        RADIO_SUMMARY_FOLDER,
    )
    
    # =============================================================================
    # Cell 6 - Plot stacked beacon-rate histogram
    # =============================================================================
    # adjusting beacon rate based on number of SSIDs
    # # Safe element-wise division (with aligned indices)
    df_radio_1 = (df_radio_1['beacons_per_second'] / df_radio_1['ssid_count']).dropna()
    df_radio_2 = (df_radio_2['beacons_per_second'] / df_radio_2['ssid_count']).dropna()
    data_combined = (combined_df['beacons_per_second'] / combined_df['ssid_count']).dropna()
    
    
    # Create stacked histogram
    plt.figure(figsize=(10, 6))
    plt.hist(
        [df_radio_1, df_radio_2], 
        bins=50, 
        edgecolor='black', 
        alpha=0.7, 
        stacked=True, 
        color=['blue', 'red'], 
        label=['Radio Count 1', 'Radio Count 2']
    )
    
    # Labels and title
    plt.xlabel("Beacon Rate (per second)")
    plt.ylabel("Frequency")
    plt.title("Stacked Histogram of Beacons per Second of Class by Radio Count")
    plt.legend()
    plt.grid(axis='y', alpha=0.75)
    
    plot_file = f"{RADIO_SUMMARY_FOLDER}/Beacons_per_second_stacked.png"
    #save to a file
    plt.savefig(plot_file)
    
    # Show the plot
    plt.show()
    
    
    # =============================================================================
    # Cell 7 - Plot beacon-rate PDF and CDF
    # =============================================================================
    # Compute all distributions
    bin_centers1, pdf1, cdf1 = compute_pdf_cdf(df_radio_1, bins=50)
    bin_centers2, pdf2, cdf2 = compute_pdf_cdf(df_radio_2, bins=50)
    bin_centers_comb, pdf_comb, cdf_comb = compute_pdf_cdf(
        data_combined,
        bins=50,
    )
    
    # --- PDF Plot ---
    plt.figure(figsize=(10, 6))
    plt.plot(bin_centers1, pdf1, label='PDF - Radio Count 1', color='blue')
    plt.plot(bin_centers2, pdf2, label='PDF - Radio Count 2', color='red')
    plt.plot(bin_centers_comb, pdf_comb, label='PDF - Combined', color='green')
    plt.xlabel('Beacon Rate (per second)')
    plt.ylabel('Probability Density')
    plt.title('Probability Distribution Function (PDF)')
    plt.legend()
    plt.grid()
    
    plot_file = f"{RADIO_SUMMARY_FOLDER}/Beacons_per_second_class_PDF.png"
    #save to a file
    plt.savefig(plot_file)
    
    plt.show()
    
    # --- CDF Plot ---
    plt.figure(figsize=(10, 6))
    plt.plot(bin_centers1, cdf1, label='CDF - Radio Count 1', color='blue')
    plt.plot(bin_centers2, cdf2, label='CDF - Radio Count 2', color='red')
    plt.plot(bin_centers_comb, cdf_comb, label='CDF - Combined', color='green')
    plt.xlabel('Beacon Rate (per second)')
    plt.ylabel('Cumulative Probability')
    plt.title('Cumulative Distribution Function (CDF)')
    plt.legend()
    plt.grid()
    
    plot_file = f"{RADIO_SUMMARY_FOLDER}/Beacons_per_second_class_CDF.png"
    #save to a file
    plt.savefig(plot_file)
    
    plt.show()
    
    
    # =============================================================================
    # Cell 8 - Report null counts and write filtered summary
    # =============================================================================
    # check for null values
    print("Radio 1 - Count:", df_radio_1.count(), "NaNs:", df_radio_1.isna().sum())
    print("Radio 2 - Count:", df_radio_2.count(), "NaNs:", df_radio_2.isna().sum())
    
    log_file.close()
    
    filtered_radio_summary_df.to_csv(f"{OUTPUT_FOLDER}/filtered_radio_summary.csv", index=False)


if __name__ == "__main__":
    main()
