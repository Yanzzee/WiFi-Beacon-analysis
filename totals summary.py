        #update totals summary
        class_summary_df.loc[len(class_summary_df)] = [
        course, location, AP_name, 1, start_time, end_time,
        capacity, enrolled, ap_count, radio_count, total_beacons,
        high_cu_beacons, percent_high_cu, highest_cu, median_cu, lowest_cu,
        high_scount, median_scount, low_scount, longest_duration_high_cu


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