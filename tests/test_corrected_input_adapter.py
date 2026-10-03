from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from corrected_input_adapter import (
    CLASS_SESSION_ID_METHOD,
    CURRENT_CLASS_PROFILE,
    NORMALIZED_CLASS_PROFILE,
    PROVIDED_CLASS_SESSION_ID_METHOD,
    RAW_SAMPLE_SCHEDULE_PROFILE,
    adapt_ap_mapping,
    adapt_class_sessions,
    load_corrected_project_inputs,
    load_raw_sample_project_inputs,
)


def test_current_class_rows_use_room_and_utc_interval_as_identity():
    frame = pd.DataFrame(
        {
            "location": ["BLDG-101", "BLDG-202"],
            "course": ["COURSE-A", "COURSE-B"],
            "start_time": [
                pd.Timestamp("2026-09-26T09:00:00-06:00"),
                pd.Timestamp("2026-09-26T09:00:00-06:00"),
            ],
            "end_time": [
                pd.Timestamp("2026-09-26T09:50:00-06:00"),
                pd.Timestamp("2026-09-26T09:50:00-06:00"),
            ],
        }
    )

    sessions, profile, id_method = adapt_class_sessions(
        frame,
        default_bin_width_us=1_024_000,
    )

    assert profile == CURRENT_CLASS_PROFILE
    assert id_method == CLASS_SESSION_ID_METHOD
    assert sessions["class_session_id"].tolist() == [
        "BLDG-101|2026-09-26T15:00:00Z|2026-09-26T15:50:00Z",
        "BLDG-202|2026-09-26T15:00:00Z|2026-09-26T15:50:00Z",
    ]
    assert sessions["class_session_id"].is_unique
    assert str(sessions["class_start"].dtype) == "datetime64[ns, UTC]"


def test_current_class_rows_reject_duplicate_room_and_interval():
    frame = pd.DataFrame(
        {
            "location": ["BLDG-101", "BLDG-101"],
            "start_time": [
                "2026-09-26T09:00:00-06:00",
                "2026-09-26T09:00:00-06:00",
            ],
            "end_time": [
                "2026-09-26T09:50:00-06:00",
                "2026-09-26T09:50:00-06:00",
            ],
        }
    )

    with pytest.raises(ValueError, match="combine shared-room sections"):
        adapt_class_sessions(frame, default_bin_width_us=1_024_000)


def test_normalized_rows_preserve_an_explicit_session_id():
    frame = pd.DataFrame(
        {
            "class_session_id": ["C-OWNER"],
            "classroom": ["BLDG-101"],
            "class_start": ["2026-09-26T15:00:00Z"],
            "class_end": ["2026-09-26T15:50:00Z"],
        }
    )

    sessions, profile, id_method = adapt_class_sessions(
        frame,
        default_bin_width_us=1_024_000,
    )

    assert profile == NORMALIZED_CLASS_PROFILE
    assert id_method == PROVIDED_CLASS_SESSION_ID_METHOD
    assert sessions["class_session_id"].tolist() == ["C-OWNER"]


def test_loader_accepts_current_sample_parquet_shapes(tmp_path: Path):
    mapping_path = tmp_path / "sample_aps.parquet"
    classes_path = tmp_path / "sample_classes.parquet"
    pd.DataFrame(
        {
            "Hostname": ["AP-01"],
            "Building-Room": ["BLDG-101"],
        }
    ).to_parquet(mapping_path)
    pd.DataFrame(
        {
            "location": ["BLDG-101"],
            "course": ["COURSE-A"],
            "start_time": [pd.Timestamp("2026-09-26T09:00:00-06:00")],
            "end_time": [pd.Timestamp("2026-09-26T09:50:00-06:00")],
            "enrolled": [20],
            "capacity": [30],
        }
    ).to_parquet(classes_path)

    inputs = load_corrected_project_inputs(
        ap_mapping_path=mapping_path,
        class_schedule_path=classes_path,
        default_bin_width_us=1_024_000,
    )

    assert inputs.configured_aps["Hostname"].tolist() == ["AP-01"]
    assert inputs.class_schedule_profile == CURRENT_CLASS_PROFILE
    assert inputs.class_session_id_method == CLASS_SESSION_ID_METHOD
    assert inputs.class_sessions["classroom"].tolist() == ["BLDG-101"]


def test_ap_mapping_accepts_hostname_index():
    frame = pd.DataFrame(
        {"Building-Room": ["BLDG-101"]},
        index=pd.Index(["AP-01"], name="Hostname"),
    )

    mapping, _profile = adapt_ap_mapping(frame)

    assert mapping.loc[0, "Hostname"] == "AP-01"


def _write_raw_sample_csvs(tmp_path: Path, *, class_period: str):
    aps = tmp_path / "Sample_APs.csv"
    classrooms = tmp_path / "Sample_classrooms.csv"
    schedule = tmp_path / "Sample_schedule.csv"
    pd.DataFrame(
        {
            "Hostname": ["AP-01"],
            "Building-Room": ["BLDG-101"],
        }
    ).to_csv(aps, index=False)
    pd.DataFrame(
        {
            "Location": ["BLDG-101"],
            "Capacity": [30],
        }
    ).to_csv(classrooms, index=False)
    pd.DataFrame(
        {
            "Location": ["BLDG-101"],
            "Course": ["COURSE-A"],
            "Sec": ["1"],
            "LQ": ["A"],
            "H": ["B"],
            "S": ["C"],
            "Cr Hr": ["1"],
            "Sec Type": ["Regular"],
            "Class Period": [class_period],
            "Days": ["M"],
            "Instructor": ["Instructor"],
            "Status": ["Active"],
            "Assign To": ["BLDG-101"],
            "Begin Date": ["2026-09-01"],
            "End Date": ["2026-12-01"],
            "sample": ["1"],
            "Size": [30],
            "Enrl": [20],
        }
    ).to_csv(schedule, index=False)
    return aps, classrooms, schedule


def test_raw_sample_csvs_expand_in_memory_without_intermediate_parquet(
    tmp_path: Path,
):
    aps, classrooms, schedule = _write_raw_sample_csvs(
        tmp_path,
        class_period="9:00a - 9:50a",
    )

    inputs = load_raw_sample_project_inputs(
        ap_mapping_path=aps,
        classroom_path=classrooms,
        class_schedule_path=schedule,
        timezone="America/Denver",
        start_date="2026-09-28",
        end_date="2026-09-28",
        default_bin_width_us=1_024_000,
    )

    assert inputs.class_schedule_profile == RAW_SAMPLE_SCHEDULE_PROFILE
    assert inputs.class_session_id_method == CLASS_SESSION_ID_METHOD
    assert inputs.class_sessions["class_session_id"].tolist() == [
        "BLDG-101|2026-09-28T15:00:00Z|2026-09-28T15:50:00Z"
    ]
    assert not list(tmp_path.glob("*.parquet"))


def test_raw_sample_csvs_collapse_shared_room_sections_and_sum_enrollment(
    tmp_path: Path,
):
    aps, classrooms, schedule = _write_raw_sample_csvs(
        tmp_path,
        class_period="9:00a - 9:50a",
    )
    raw = pd.read_csv(schedule)
    second = raw.iloc[0].copy()
    second["Course"] = "COURSE-B"
    second["Sec"] = "2"
    second["Enrl"] = 7
    pd.concat([raw, second.to_frame().T], ignore_index=True).to_csv(
        schedule,
        index=False,
    )

    inputs = load_raw_sample_project_inputs(
        ap_mapping_path=aps,
        classroom_path=classrooms,
        class_schedule_path=schedule,
        timezone="America/Denver",
        start_date="2026-09-28",
        end_date="2026-09-28",
        default_bin_width_us=1_024_000,
    )

    assert len(inputs.class_sessions) == 1
    assert len(inputs.shared_room_session_merges) == 1
    merge = inputs.shared_room_session_merges[0]
    assert merge.classroom == "BLDG-101"
    assert merge.courses == ("COURSE-A", "COURSE-B")
    assert merge.source_row_count == 2
    assert merge.enrolled_total == 27
    assert merge.class_start == "2026-09-28T15:00:00Z"
    assert merge.class_end == "2026-09-28T15:50:00Z"


def test_raw_sample_csvs_reject_malformed_period_instead_of_skipping(
    tmp_path: Path,
):
    aps, classrooms, schedule = _write_raw_sample_csvs(
        tmp_path,
        class_period="wrong",
    )

    with pytest.raises(ValueError, match="Malformed Class Period"):
        load_raw_sample_project_inputs(
            ap_mapping_path=aps,
            classroom_path=classrooms,
            class_schedule_path=schedule,
            timezone="America/Denver",
            start_date="2026-09-28",
            end_date="2026-09-28",
            default_bin_width_us=1_024_000,
        )
