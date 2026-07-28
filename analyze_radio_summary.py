def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and radio-summary input
    # =============================================================================
    import pandas as pd
    from datetime import datetime
    import os
    from dotenv import load_dotenv
    from analysis_config import ensure_directory
    from analysis_logging import open_analysis_log
    from radio_summary_processing import (
        compute_beacon_rate_distributions,
        normalize_beacon_rate_series,
        prepare_radio_summary_frames,
        print_beacon_rate_null_counts,
        report_radio_summary_statistics,
        sort_radio_summary,
    )
    from radio_summary_plotting import (
        plot_beacon_rate_histogram,
        plot_beacon_rate_channel_utilization_scatter,
        plot_beacon_rate_pdf_cdf,
        plot_radio_count_split_scatter,
        plot_radio_regressions,
        plot_stacked_beacon_rate_histogram,
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
    
    correlation = report_radio_summary_statistics(
        radio_summary_df,
        log_and_print,
    )
    
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
    model_1, model_2 = plot_radio_regressions(
        df_radio_1,
        df_radio_2,
        RADIO_SUMMARY_FOLDER,
        log_and_print,
    )
    
    
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
    df_radio_1, df_radio_2, data_combined = normalize_beacon_rate_series(
        df_radio_1,
        df_radio_2,
        combined_df,
    )
    
    plot_stacked_beacon_rate_histogram(
        df_radio_1,
        df_radio_2,
        RADIO_SUMMARY_FOLDER,
    )
    
    
    # =============================================================================
    # Cell 7 - Plot beacon-rate PDF and CDF
    # =============================================================================
    (
        (bin_centers1, pdf1, cdf1),
        (bin_centers2, pdf2, cdf2),
        (bin_centers_comb, pdf_comb, cdf_comb),
    ) = compute_beacon_rate_distributions(
        df_radio_1,
        df_radio_2,
        data_combined,
    )
    
    plot_beacon_rate_pdf_cdf(
        bin_centers1,
        pdf1,
        cdf1,
        bin_centers2,
        pdf2,
        cdf2,
        bin_centers_comb,
        pdf_comb,
        cdf_comb,
        RADIO_SUMMARY_FOLDER,
    )
    
    
    # =============================================================================
    # Cell 8 - Report null counts and write filtered summary
    # =============================================================================
    print_beacon_rate_null_counts(df_radio_1, df_radio_2)
    
    log_file.close()
    
    filtered_radio_summary_df.to_csv(f"{OUTPUT_FOLDER}/filtered_radio_summary.csv", index=False)


if __name__ == "__main__":
    main()
