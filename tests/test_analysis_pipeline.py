from __future__ import annotations

import pandas as pd

from analysis_pipeline import (
    RADIO_SUMMARY_COLUMNS,
    create_radio_summary_dataframe,
    process_class_samples,
)


def test_create_radio_summary_dataframe_preserves_schema_and_order():
    radio_summary_df = create_radio_summary_dataframe()

    assert list(radio_summary_df.columns) == list(
        RADIO_SUMMARY_COLUMNS
    )
    assert radio_summary_df.empty
    expected_dtypes = {
        "subject": "object",
        "location": "object",
        "AP_hostname": "object",
        "radio_number": "int64",
        "start_time": "datetime64[ns]",
        "end_time": "datetime64[ns]",
        "capacity": "int64",
        "enrolled": "int64",
        "AP_count": "int64",
        "radio_count": "int64",
        "bssid_count": "int64",
        "ssid_count": "int64",
        "total_beacons": "int64",
        "high_cu_beacons": "int64",
        "percent_high_cu": "float64",
        "highest_cu": "int64",
        "median_cu": "int64",
        "lowest_cu": "int64",
        "high_scount": "int64",
        "median_scount": "int64",
        "low_scount": "int64",
        "longest_duration_high_cu": "float64",
        "graph_filename": "object",
    }
    assert {
        column: str(dtype)
        for column, dtype in radio_summary_df.dtypes.items()
    } == expected_dtypes


def test_process_class_samples_preserves_iteration_append_and_clear_order(
    tmp_path,
):
    base = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    sample_classes_df = pd.DataFrame(
        {
            "location": ["BLDG-101"],
            "start_time": [base],
            "end_time": [base + pd.Timedelta(minutes=1)],
            "course": ["TEST 101"],
            "enrolled": [10],
            "capacity": [20],
        }
    )
    sample_aps_df = pd.DataFrame(
        {
            "Hostname": ["AP-01", "AP-OUT", "AP-02"],
            "Building-Room": [
                "BLDG-101",
                "BLDG-999",
                "BLDG-101",
            ],
        }
    )
    sample_classrooms_df = pd.DataFrame(
        {"Location": ["BLDG-101"], "ap_count": [2]}
    )
    grouped_data_folder = tmp_path / "grouped"
    grouped_data_folder.mkdir()
    for hostname in ("AP-01", "AP-02"):
        pd.DataFrame(
            {"source_ap": [hostname], "value": [1]}
        ).to_parquet(grouped_data_folder / f"{hostname}.parquet")

    aggregate = {"frame": pd.DataFrame()}
    events: list[tuple] = []
    logs: list[str] = []

    def graph(
        ap_name,
        start_time,
        end_time,
        location,
        course,
        enrolled,
        capacity,
        ap_count,
        ap_df,
        radio_summary_df,
    ):
        events.append(
            (
                "graph",
                ap_name,
                ap_df["source_ap"].iloc[0],
                location,
                course,
                enrolled,
                capacity,
                ap_count,
            )
        )
        aggregate["frame"] = pd.DataFrame(
            {"source_ap": [ap_name]}
        )
        radio_summary_df.loc[len(radio_summary_df)] = [
            course,
            location,
            ap_name,
            1,
            start_time,
            end_time,
            capacity,
            enrolled,
            ap_count,
            1,
            1,
            1,
            1,
            0,
            0.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            "plot.png",
        ]

    def append_to_parquet(ap_name, frame):
        events.append(
            (
                "append",
                ap_name,
                frame["source_ap"].tolist()
                if "source_ap" in frame
                else [],
            )
        )

    def clear_all_class_beacons():
        events.append(("clear",))
        aggregate["frame"] = aggregate["frame"].iloc[0:0]

    schema_calls: list[tuple] = []

    radio_summary_df = process_class_samples(
        sample_aps_df,
        sample_classes_df,
        sample_classrooms_df,
        grouped_data_folder=str(grouped_data_folder),
        log_and_print=logs.append,
        graph=graph,
        append_to_parquet=append_to_parquet,
        get_all_class_beacons=lambda: aggregate["frame"],
        clear_all_class_beacons=clear_all_class_beacons,
        validate_grouped_ap_schema=(
            lambda frame, path, ap_name: schema_calls.append(
                (frame["source_ap"].iloc[0], path, ap_name)
            )
        ),
    )

    assert events == [
        (
            "graph",
            "AP-01",
            "AP-01",
            "BLDG-101",
            "TEST 101",
            10,
            20,
            2,
        ),
        ("append", "AP-01", ["AP-01"]),
        ("clear",),
        ("append", "AP-01", []),
        ("clear",),
        (
            "graph",
            "AP-02",
            "AP-02",
            "BLDG-101",
            "TEST 101",
            10,
            20,
            2,
        ),
        ("append", "AP-02", ["AP-02"]),
        ("clear",),
    ]
    assert schema_calls == [
        (
            "AP-01",
            f"{grouped_data_folder}/AP-01.parquet",
            "AP-01",
        ),
        (
            "AP-02",
            f"{grouped_data_folder}/AP-02.parquet",
            "AP-02",
        ),
    ]
    assert logs == [
        "\n-----------------------------------------------------------------",
        "-----------------------------------------------------------------\n",
        "Subject: TEST 101",
        "Location: BLDG-101",
        f"Start Time: {base}",
        f"End Time: {base + pd.Timedelta(minutes=1)}",
        "Capacity: 20",
        "Enrolled: 10",
        "Number of APs: 2",
        "",
    ]
    assert radio_summary_df["AP_hostname"].tolist() == [
        "AP-01",
        "AP-02",
    ]
