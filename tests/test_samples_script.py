import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from sample_processing import (
    clean_cell,
    expand_schedule_rows,
    parse_class_period,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_clean_cell_preserves_existing_cleanup_behavior():
    assert clean_cell("  Room\u00a0\u00a0101\t\r\n") == "Room 101"
    assert clean_cell("A\x00B\x7fC\x80D") == "ABCD"
    assert clean_cell(12) == "12"
    assert clean_cell(None) == "None"


def test_parse_class_period_preserves_accepted_time_strings():
    start_time, end_time = parse_class_period("8:00a - 9:50a")
    assert start_time.strftime("%H:%M") == "08:00"
    assert end_time.strftime("%H:%M") == "09:50"

    start_time, end_time = parse_class_period("1:05p - 2:20p")
    assert start_time.strftime("%H:%M") == "13:05"
    assert end_time.strftime("%H:%M") == "14:20"


@pytest.mark.parametrize(
    "class_period",
    [
        "not-a-time",
        "8:00 AM - 9:00 AM",
    ],
)
def test_parse_class_period_preserves_rejected_time_strings(class_period):
    with pytest.raises(ValueError):
        parse_class_period(class_period)


def test_expand_schedule_rows_preserves_row_and_weekday_order():
    sample_schedule_df = pd.DataFrame(
        {
            "Location": ["Room1", "Room2"],
            "Course": ["First", "Second"],
            "Enrl": [10, 20],
            "Class Period": ["8:00a - 9:00a", "1:00p - 2:00p"],
            "Days": ["WM", "F"],
        }
    )
    sample_classes_df = pd.DataFrame(
        columns=[
            "location",
            "course",
            "start_time",
            "end_time",
            "enrolled",
            "capacity",
        ]
    )
    schedule_days = pd.date_range(
        "2025-01-06",
        "2025-01-10",
        freq="D",
    ).to_pydatetime().tolist()

    result_df = expand_schedule_rows(
        sample_schedule_df,
        sample_classes_df,
        schedule_days,
    )

    assert result_df["location"].tolist() == ["Room1", "Room1", "Room2"]
    assert result_df["course"].tolist() == ["First", "First", "Second"]
    assert pd.to_datetime(result_df["start_time"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    ).tolist() == [
        "2025-01-06 08:00",
        "2025-01-08 08:00",
        "2025-01-10 13:00",
    ]
    assert result_df["enrolled"].tolist() == [10, 10, 20]
    assert result_df["capacity"].isna().all()


def test_expand_schedule_rows_skips_malformed_periods(capsys):
    sample_schedule_df = pd.DataFrame(
        {
            "Location": ["Room1", "Room2"],
            "Course": ["Broken", "Valid"],
            "Enrl": [10, 20],
            "Class Period": ["not-a-time", "1:00p - 2:00p"],
            "Days": ["M", "M"],
        }
    )
    sample_classes_df = pd.DataFrame(
        columns=[
            "location",
            "course",
            "start_time",
            "end_time",
            "enrolled",
            "capacity",
        ]
    )

    result_df = expand_schedule_rows(
        sample_schedule_df,
        sample_classes_df,
        [pd.Timestamp("2025-01-06").to_pydatetime()],
    )

    assert result_df["course"].tolist() == ["Valid"]
    assert capsys.readouterr().out == (
        "Malformed Class Period: not-a-time for Course: Broken "
        "in Location: Room1\n"
        "Check the format of the Class Period.\n"
    )


def test_samples_script_handles_short_schedule(tmp_path):
    sample_dir = tmp_path

    (sample_dir / "Sample_APs.csv").write_text(
        "Building-Room\nRoom1\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_classrooms.csv").write_text(
        "Location,Capacity\nRoom1,50\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_schedule.csv").write_text(
        "Location,Course,Sec,LQ,H,S,Cr Hr,Sec Type,Class Period,Days,Instructor,Status,Assign To,Begin Date,End Date,sample,Size,Enrl\n"
        "Room1,Test Course,1,A,B,C,1,Regular,not-a-time,M,Prof,Active,Room1,2025-01-01,2025-01-31,1,10,10\n",
        encoding="utf-8",
    )
    (sample_dir / "config.env").write_text(
        "TIMEZONE=Etc/GMT+7\n"
        "SAMPLE_FOLDER=.\n"
        "START_DATE=2025-01-01\n"
        "END_DATE=2025-01-01\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    for key in ("TIMEZONE", "SAMPLE_FOLDER", "START_DATE", "END_DATE"):
        env.pop(key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "samples.py")],
        cwd=sample_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (sample_dir / "sample_classes.parquet").exists()


def test_samples_script_handles_dirty_numeric_cells(tmp_path):
    sample_dir = tmp_path

    (sample_dir / "Sample_APs.csv").write_text(
        "Building-Room\nRoom1\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_classrooms.csv").write_text(
        "Location,Capacity\nRoom1,50\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_schedule.csv").write_text(
        "Location,Course,Sec,LQ,H,S,Cr Hr,Sec Type,Class Period,Days,Instructor,Status,Assign To,Begin Date,End Date,sample,Size,Enrl\n"
        "Room1,Test Course,1,A,B,C,1,Regular,8:00 AM - 9:00 AM,M,Prof,Active,Room1,2025-01-01,2025-01-31,1,1\u00a0,2\u00a0\n",
        encoding="utf-8",
    )
    (sample_dir / "config.env").write_text(
        "TIMEZONE=Etc/GMT+7\n"
        "SAMPLE_FOLDER=.\n"
        "START_DATE=2025-01-01\n"
        "END_DATE=2025-01-01\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    for key in ("TIMEZONE", "SAMPLE_FOLDER", "START_DATE", "END_DATE"):
        env.pop(key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "samples.py")],
        cwd=sample_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (sample_dir / "sample_classes.parquet").exists()


def test_samples_script_expands_inclusive_weekdays_and_localizes_times(
    tmp_path,
):
    sample_dir = tmp_path

    (sample_dir / "Sample_APs.csv").write_text(
        "Building-Room\nRoom1\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_classrooms.csv").write_text(
        "Location,Capacity\nRoom1,50\n",
        encoding="utf-8",
    )
    (sample_dir / "Sample_schedule.csv").write_text(
        "Location,Course,Sec,LQ,H,S,Cr Hr,Sec Type,Class Period,Days,Instructor,Status,Assign To,Begin Date,End Date,sample,Size,Enrl\n"
        "Room1,Test Course,1,A,B,C,1,Regular,8:00a - 9:00a,MW,Prof,Active,Room1,2025-01-01,2025-01-31,1,10,12\n",
        encoding="utf-8",
    )
    (sample_dir / "config.env").write_text(
        "TIMEZONE=Etc/GMT+7\n"
        "SAMPLE_FOLDER=.\n"
        "START_DATE=2025-01-06\n"
        "END_DATE=2025-01-08\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    for key in ("TIMEZONE", "SAMPLE_FOLDER", "START_DATE", "END_DATE"):
        env.pop(key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get(
        "PYTHONPATH",
        "",
    )

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "samples.py")],
        cwd=sample_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    sample_classes_df = pd.read_parquet(
        sample_dir / "sample_classes.parquet"
    )
    assert sample_classes_df["location"].tolist() == ["Room1", "Room1"]
    assert sample_classes_df["course"].tolist() == [
        "Test Course",
        "Test Course",
    ]
    assert sample_classes_df["start_time"].dt.strftime(
        "%Y-%m-%d %H:%M"
    ).tolist() == [
        "2025-01-06 08:00",
        "2025-01-08 08:00",
    ]
    assert sample_classes_df["end_time"].dt.strftime(
        "%Y-%m-%d %H:%M"
    ).tolist() == [
        "2025-01-06 09:00",
        "2025-01-08 09:00",
    ]
    assert str(sample_classes_df["start_time"].dt.tz) == "Etc/GMT+7"
    assert str(sample_classes_df["end_time"].dt.tz) == "Etc/GMT+7"
    assert sample_classes_df["enrolled"].tolist() == [12, 12]
    assert sample_classes_df["capacity"].tolist() == [50, 50]
