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


def _write_raw_sample_csvs(tmp_path: Path, *, class_period: str, days: str = "M"):
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
            "Days": [days],
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


@pytest.mark.parametrize(
    ("days", "expected_dates"),
    [
        ("S", ["2026-09-05"]),
        ("Sa", ["2026-09-05"]),
        ("U", ["2026-09-06"]),
        ("Su", ["2026-09-06"]),
        ("SaSu", ["2026-09-05", "2026-09-06"]),
        ("SSaUSu", ["2026-09-05", "2026-09-06"]),
    ],
)
def test_raw_schedule_weekend_codes_expand_without_wrong_day_or_duplicates(
    tmp_path: Path,
    days: str,
    expected_dates: list[str],
):
    aps, classrooms, schedule = _write_raw_sample_csvs(
        tmp_path,
        class_period="9:00a - 9:50a",
        days=days,
    )
    inputs = load_raw_sample_project_inputs(
        ap_mapping_path=aps,
        classroom_path=classrooms,
        class_schedule_path=schedule,
        timezone="America/Denver",
        start_date="2026-09-05",
        end_date="2026-09-06",
        default_bin_width_us=1_024_000,
    )

    assert inputs.class_sessions["class_start"].dt.strftime("%Y-%m-%d").tolist() == (
        expected_dates
    )


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


@pytest.mark.parametrize(
    ("date", "expected_start"),
    [
        ("2026-01-12", "2026-01-12T16:00:00Z"),
        ("2026-07-13", "2026-07-13T15:00:00Z"),
    ],
    ids=("denver-standard-time", "denver-daylight-time"),
)
def test_raw_schedule_named_zone_applies_date_specific_dst_offset(
    tmp_path: Path,
    date: str,
    expected_start: str,
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
        start_date=date,
        end_date=date,
        default_bin_width_us=1_024_000,
    )

    assert inputs.class_sessions.loc[0, "class_start"].isoformat().replace(
        "+00:00", "Z"
    ) == expected_start


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


def test_raw_shared_room_capacity_conflict_is_null_and_flagged():
    from corrected_input_adapter import _collapse_shared_room_sessions

    normalized = pd.DataFrame(
        {
            "location": ["BLDG-101", "BLDG-101"],
            "start_time": [
                pd.Timestamp("2026-09-28T15:00:00Z"),
                pd.Timestamp("2026-09-28T15:00:00Z"),
            ],
            "end_time": [
                pd.Timestamp("2026-09-28T15:50:00Z"),
                pd.Timestamp("2026-09-28T15:50:00Z"),
            ],
            "course": ["COURSE-A", "COURSE-B"],
            "enrolled": [20, 7],
            "capacity": [30, 35],
        }
    )
    collapsed, _merges = _collapse_shared_room_sessions(normalized)

    assert pd.isna(collapsed.loc[0, "capacity"])
    assert collapsed.loc[0, "capacity_status"] == "conflict"
    assert collapsed.loc[0, "course_labels"] == "COURSE-A | COURSE-B"
    assert collapsed.loc[0, "source_section_count"] == 2


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


def test_classroom_scope_filters_broad_schedule_and_ap_mapping_before_validation(tmp_path):
    aps, classrooms, schedule = _write_raw_sample_csvs(tmp_path, class_period="9:00a - 9:50a")
    mapping = pd.read_csv(aps)
    mapping.loc[0, "Building-Room"] = " BLDG-101\u00a0"
    other_ap = mapping.iloc[0].copy()
    other_ap["Hostname"], other_ap["Building-Room"], other_ap["6GHz_Enabled"] = "OTHER-AP", "OTHER", "invalid"
    pd.concat([mapping, other_ap.to_frame().T], ignore_index=True).to_csv(aps, index=False)
    rooms = pd.read_csv(classrooms)
    rooms.loc[0, "Location"] = "\u00a0BLDG-101 "
    rooms.to_csv(classrooms, index=False)
    rows = pd.read_csv(schedule)
    rows.loc[0, "Location"] = " BLDG-101 "
    other_class = rows.iloc[0].copy()
    other_class["Location"], other_class["Class Period"] = "OTHER", "malformed"
    other_class["Size"], other_class["Enrl"] = "invalid", "invalid"
    pd.concat([rows, other_class.to_frame().T], ignore_index=True).to_csv(schedule, index=False)
    before = {path: path.read_bytes() for path in (aps, classrooms, schedule)}

    inputs = load_raw_sample_project_inputs(
        ap_mapping_path=aps, classroom_path=classrooms, class_schedule_path=schedule,
        timezone="America/Denver", start_date="2026-09-28", end_date="2026-09-28",
        default_bin_width_us=1_024_000,
    )
    assert inputs.location_scope == ("BLDG-101",)
    assert inputs.configured_aps.Hostname.tolist() == ["AP-01"]
    assert inputs.configured_aps["Building-Room"].tolist() == ["BLDG-101"]
    assert inputs.class_sessions.classroom.tolist() == ["BLDG-101"]
    assert inputs.class_sessions.enrolled.tolist() == [20]
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize("damage,message", [
    ("empty_scope", "Sample_classrooms.csv must contain at least one location"),
    ("blank_location", "no blank Location values"),
    ("missing_mapping", "no AP mapping for scoped locations: BLDG-101"),
    ("no_schedule", "no rows for locations listed in Sample_classrooms.csv"),
])
def test_invalid_location_scope_fails_before_capture_processing(tmp_path, damage, message):
    aps, classrooms, schedule = _write_raw_sample_csvs(tmp_path, class_period="9:00a - 9:50a")
    if damage == "empty_scope":
        pd.read_csv(classrooms).iloc[:0].to_csv(classrooms, index=False)
    elif damage == "blank_location":
        rooms = pd.read_csv(classrooms)
        rooms.loc[0, "Location"] = " "
        rooms.to_csv(classrooms, index=False)
    elif damage == "missing_mapping":
        mapping = pd.read_csv(aps)
        mapping.loc[0, "Building-Room"] = "OTHER"
        mapping.to_csv(aps, index=False)
    else:
        rows = pd.read_csv(schedule)
        rows.loc[0, "Location"] = "OTHER"
        rows.to_csv(schedule, index=False)
    with pytest.raises(ValueError, match=message):
        load_raw_sample_project_inputs(
            ap_mapping_path=aps, classroom_path=classrooms, class_schedule_path=schedule,
            timezone="America/Denver", start_date="2026-09-28", end_date="2026-09-28",
            default_bin_width_us=1_024_000,
        )
