from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa

from corrected_class_session_reports import (
    CLASS_SESSION_REPORT_FILENAME,
    ENROLLMENT_SCATTER_FILENAME,
    METHODOLOGY_FILENAME,
    RATIO_HISTOGRAM_FILENAME,
    REPORT_DIRECTORY,
    build_class_session_report,
    write_class_session_report_package,
)
from corrected_station_reports import STATION_CLASS_REPORT_COLUMNS
from corrected_summary_output import (
    CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
    RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
)


def _default(field: pa.Field):
    if pa.types.is_string(field.type):
        return "value"
    if pa.types.is_timestamp(field.type):
        return pd.Timestamp("2026-07-13T15:00:00Z")
    if pa.types.is_integer(field.type):
        return 0
    if pa.types.is_floating(field.type):
        return 0.0
    if pa.types.is_list(field.type) or pa.types.is_fixed_size_list(field.type):
        return [] if pa.types.is_list(field.type) else [0] * field.type.list_size
    if pa.types.is_boolean(field.type):
        return False
    return None


def _row(schema: pa.Schema, **overrides) -> pd.DataFrame:
    values = {field.name: _default(field) for field in schema}
    values.update(overrides)
    return pd.DataFrame([values])


def _inputs():
    start = pd.Timestamp("2026-07-13T15:00:00Z")
    end = pd.Timestamp("2026-07-13T15:50:00Z")
    sessions = pd.DataFrame(
        {
            "class_session_id": ["C1"],
            "classroom": ["BLDG-101"],
            "class_start": [start],
            "class_end": [end],
            "course": ["COURSE-A + COURSE-B"],
            "course_labels": ["COURSE-A | COURSE-B"],
            "source_section_count": [2],
            "enrolled": [25],
            "capacity": [None],
            "capacity_status": ["conflict"],
        }
    )
    classroom = _row(
        CLASSROOM_CLASS_SUMMARY_ARROW_SCHEMA,
        run_id="run-1",
        schema_id="beacon.schema/analysis-results/v1",
        metric_id="beacon.metric/corrected-cu/v1",
        class_session_id="C1",
        classroom="BLDG-101",
        class_start=start,
        class_end=end,
        class_duration_us=3_000_000_000,
        radio_count_5_ghz=1,
        radio_count_total=1,
        high_actual_observed_max_us=5_000_000,
        high_actual_observed_responsible_radio_ids=["AP-01/radio/5"],
    )
    radio = _row(
        RADIO_CLASS_SUMMARY_ARROW_SCHEMA,
        run_id="run-1",
        schema_id="beacon.schema/analysis-results/v1",
        metric_id="beacon.metric/corrected-cu/v1",
        class_session_id="C1",
        classroom="BLDG-101",
        class_start=start,
        class_end=end,
        radio_group_id="AP-01/radio/5",
        cu_coverage=0.9,
    )
    station_values = {column: None for column in STATION_CLASS_REPORT_COLUMNS}
    station_values.update(
        {
            "run_id": "run-1",
            "schema_id": "beacon.schema/analysis-results/v1",
            "metric_id": "beacon.metric/corrected-station-count/v1",
            "class_session_id": "C1",
            "classroom": "BLDG-101",
            "rollup_type": "classroom",
            "rollup_id": "BLDG-101",
            "available_bin_count": 100,
            "unavailable_bin_count": 2,
            "stale_assisted_bin_count": 3,
            "peak_client_count": 20,
            "peak_bin_number": 10,
            "peak_bin_start_utc": "2026-07-13T15:00:10Z",
            "peak_stale_assisted": True,
            "combined_enrollment": 25,
            "peak_client_count_per_enrolled": 0.8,
        }
    )
    return sessions, classroom, radio, pd.DataFrame([station_values])


def test_class_session_report_preserves_context_without_capacity_calculation():
    sessions, classroom, radio, station = _inputs()

    report = build_class_session_report(
        sessions,
        classroom,
        radio,
        station,
        display_timezone="America/Denver",
    )

    row = report.iloc[0]
    assert row["local_date"] == "2026-07-13"
    assert row["local_start"] == "09:00"
    assert row["course_labels"] == "COURSE-A | COURSE-B"
    assert row["source_section_count"] == 2
    assert row["combined_enrollment"] == 25
    assert pd.isna(row["room_capacity"])
    assert row["capacity_status"] == "conflict"
    assert row["radio_cu_coverage_min"] == 0.9
    assert row["peak_client_count_per_enrolled"] == 0.8


def test_class_session_package_writes_table_plots_and_methodology(tmp_path: Path):
    sessions, classroom, radio, station = _inputs()

    written = write_class_session_report_package(
        tmp_path,
        class_sessions=sessions,
        classroom_summary=classroom,
        radio_summary=radio,
        station_class_report=station,
        display_timezone="America/Denver",
    )

    root = tmp_path / REPORT_DIRECTORY
    assert written.rows == 1
    assert pd.read_csv(root / CLASS_SESSION_REPORT_FILENAME).shape[0] == 1
    assert (root / RATIO_HISTOGRAM_FILENAME).is_file()
    assert (root / ENROLLMENT_SCATTER_FILENAME).is_file()
    methodology = (root / METHODOLOGY_FILENAME).read_text(encoding="utf-8")
    assert "descriptive schedule context only" in methodology
    assert "No room-capacity utilization" in methodology
