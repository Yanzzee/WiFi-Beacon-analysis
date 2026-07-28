def main():
    # =============================================================================
    # Cell 1 - Imports, configuration, logging, and class-session inputs
    # =============================================================================
    import pandas as pd
    from datetime import datetime
    import os
    from dotenv import load_dotenv
    from analysis_config import (
        ensure_directory,
        env_or_default as _env_or_default,
        get_beacon_rate,
    )
    from analysis_logging import open_analysis_log
    from class_session_analysis import (
        calculate_all_class_session_metrics,
        initialize_class_sessions,
        report_average_devices_per_enrolled,
        report_class_session_aggregates,
    )
    from class_session_plotting import (
        plot_enrollment_station_scatter,
        plot_high_cu_streak_histograms,
        plot_stations_per_enrolled_histogram,
    )
    
    load_dotenv("config.env")
    
    DATA_FOLDER = os.getenv("DATA_FOLDER")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")
    SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER")
    CLASS_SESSIONS_FOLDER = os.getenv("CLASS_SESSIONS_FOLDER")
    
    RADIO_COUNT_SSID = _env_or_default("SSID1", "eduroam")
    BEACON_RATE = get_beacon_rate()
    ensure_directory(CLASS_SESSIONS_FOLDER)
    
    #log to a file and print to terminal
    # Open logging file
    timestamp, log_filename, log_file = open_analysis_log(
        OUTPUT_FOLDER,
        "analyze_class_sessions",
    )
    def log_and_print(message):
        print(message)  # Print to terminal is too noisy
        log_file.write(message + "\n")  # Write to file
    
    
    log_and_print(f"Class session analysis started on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # read in the sample for class sessions as a basis for all information for class sessions
    class_sessions_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_classes.parquet")
    sample_aps_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_aps.parquet")
    sample_classrooms_df = pd.read_parquet(f"{SAMPLE_FOLDER}/sample_classrooms.parquet")
    
    class_sessions_df = initialize_class_sessions(
        class_sessions_df,
        sample_aps_df,
        sample_classrooms_df,
    )
    
    # =============================================================================
    # Cell 2 - Calculate metrics for each class session
    # =============================================================================
    # calculate metrics for each class session
    all_class_beacons_processed_df = pd.read_parquet(f"{DATA_FOLDER}/all_class_beacons_processed.parquet")
    
    # Use the configured effective metadata interval as the high-CU duration/streak bin width.
    BEACON_WINDOW_SEC = BEACON_RATE 
    
    class_sessions_df = calculate_all_class_session_metrics(
        class_sessions_df,
        all_class_beacons_processed_df,
        BEACON_WINDOW_SEC,
        RADIO_COUNT_SSID,
    )
    
    
    # =============================================================================
    # Cell 3 - Calculate aggregate class-session metrics
    # =============================================================================
    report_class_session_aggregates(
        class_sessions_df,
        log_and_print,
    )
    
    
    
    
    # =============================================================================
    # Cell 4 - Plot high-CU streak histograms
    # =============================================================================
    plot_high_cu_streak_histograms(
        class_sessions_df,
        CLASS_SESSIONS_FOLDER,
    )
    
    
    
    # =============================================================================
    # Cell 5 - Calculate average devices per enrolled student
    # =============================================================================
    average_devices_per_enrolled = report_average_devices_per_enrolled(
        class_sessions_df,
        log_and_print,
    )
    
    # =============================================================================
    # Cell 6 - Plot stations-per-enrolled distribution
    # =============================================================================
    plot_stations_per_enrolled_histogram(
        class_sessions_df,
        CLASS_SESSIONS_FOLDER,
    )
    
    # =============================================================================
    # Cell 7 - Plot enrollment versus station count and close log
    # =============================================================================
    
    plot_enrollment_station_scatter(
        class_sessions_df,
        CLASS_SESSIONS_FOLDER,
    )
    
    log_file.close()


if __name__ == "__main__":
    main()
