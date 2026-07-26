from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from script_sections import script_cell_source


CONFIG_ENV_KEYS = (
    "DATA_FOLDER",
    "OUTPUT_FOLDER",
    "SAMPLE_FOLDER",
    "CLASS_SESSIONS_FOLDER",
    "BEACON_RATE",
    "SSID1",
    "SSID2",
)


def _env_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _write_config_env(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items())
        + "\n",
        encoding="utf-8",
    )


def _run_class_session_metrics(
    monkeypatch,
    tmp_path,
    class_sessions_df,
    beacons_df,
    primary_ssid="CampusSecure",
):
    for key in CONFIG_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)

    data_dir = tmp_path / "Data"
    output_dir = tmp_path / "Output"
    sample_dir = tmp_path / "Sample"
    class_sessions_dir = output_dir / "class_sessions"
    data_dir.mkdir()
    output_dir.mkdir()
    sample_dir.mkdir()

    _write_config_env(
        tmp_path / "config.env",
        {
            "DATA_FOLDER": _env_path(data_dir),
            "OUTPUT_FOLDER": _env_path(output_dir),
            "SAMPLE_FOLDER": _env_path(sample_dir),
            "CLASS_SESSIONS_FOLDER": _env_path(class_sessions_dir),
            "BEACON_RATE": "1",
            "SSID1": primary_ssid,
            "SSID2": "CampusGuest",
        },
    )

    class_sessions_df.to_parquet(
        sample_dir / "sample_classes.parquet"
    )
    locations = class_sessions_df["location"].drop_duplicates().tolist()
    pd.DataFrame({"Building-Room": locations}).to_parquet(
        sample_dir / "sample_aps.parquet"
    )
    pd.DataFrame(
        {
            "Location": locations,
            "Group": ["A"] * len(locations),
        }
    ).to_parquet(sample_dir / "sample_classrooms.parquet")
    beacons_df.to_parquet(
        data_dir / "all_class_beacons_processed.parquet"
    )

    cell1, script_path = script_cell_source(
        "analyze_class_sessions.py",
        1,
    )
    cell2, _ = script_cell_source("analyze_class_sessions.py", 2)
    namespace = {"__name__": "not_main"}
    try:
        exec(
            compile(cell1, f"{script_path}::cell_1", "exec"),
            namespace,
        )
        exec(
            compile(cell2, f"{script_path}::cell_2", "exec"),
            namespace,
        )
    finally:
        log_file = namespace.get("log_file")
        if log_file is not None:
            log_file.close()

    return namespace["class_sessions_df"].set_index("location")


def _sessions(locations, start, end, enrolled=10):
    enrolled_values = (
        enrolled
        if isinstance(enrolled, list)
        else [enrolled] * len(locations)
    )
    return pd.DataFrame(
        {
            "location": locations,
            "start_time": [start] * len(locations),
            "end_time": [end] * len(locations),
            "enrolled": enrolled_values,
            "capacity": [20] * len(locations),
        }
    )


def _beacons(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "aruba_erm.time",
            "location",
            "room_scount",
            "wlan.qbss.cu",
            "wlan.qbss.scount",
            "wlan.ssid",
            "wlan.ta",
        ],
    )


def test_class_session_empty_and_exact_boundaries(monkeypatch, tmp_path):
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    end = start + pd.Timedelta(seconds=3)
    sessions_df = _sessions(["EMPTY", "BOUNDARY"], start, end)
    beacons_df = _beacons(
        [
            (
                start - pd.Timedelta(milliseconds=1),
                "BOUNDARY",
                99,
                250,
                9,
                "CampusSecure",
                "outside-before",
            ),
            (
                start,
                "BOUNDARY",
                3,
                100,
                2,
                "CampusSecure",
                "at-start",
            ),
            (
                end,
                "BOUNDARY",
                7,
                200,
                4,
                "CampusSecure",
                "at-end",
            ),
            (
                end + pd.Timedelta(milliseconds=1),
                "BOUNDARY",
                100,
                250,
                10,
                "CampusSecure",
                "outside-after",
            ),
        ]
    )

    result_df = _run_class_session_metrics(
        monkeypatch,
        tmp_path,
        sessions_df,
        beacons_df,
    )

    empty = result_df.loc["EMPTY"]
    for column in [
        "number_beacons",
        "max_stations",
        "max_cu",
        "average_cu",
        "median_cu",
        "stations_enrolled",
        "duration_75",
        "duration_50",
        "max_streak_75",
        "max_streak_50",
        "sum_scount_75",
        "sum_scount_50",
        "number_radios",
    ]:
        assert empty[column] == 0

    boundary = result_df.loc["BOUNDARY"]
    assert boundary["number_beacons"] == 2
    assert boundary["max_stations"] == 7
    assert boundary["max_cu"] == 200
    assert boundary["average_cu"] == pytest.approx(150.0)
    assert boundary["median_cu"] == pytest.approx(150.0)
    assert boundary["duration_75"] == 0
    assert boundary["duration_50"] == 0
    assert boundary["max_streak_75"] == 0
    assert boundary["max_streak_50"] == 0
    assert boundary["sum_scount_75"] == 4
    assert boundary["sum_scount_50"] == 4
    assert boundary["number_radios"] == 2


def test_class_session_empty_bins_do_not_reset_streaks(monkeypatch, tmp_path):
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    end = start + pd.Timedelta(seconds=3)
    sessions_df = _sessions(["EMPTY-GAP", "NON-HIGH"], start, end)
    beacons_df = _beacons(
        [
            (
                start + pd.Timedelta(milliseconds=100),
                "EMPTY-GAP",
                5,
                200,
                5,
                "CampusSecure",
                "gap-radio",
            ),
            (
                start + pd.Timedelta(seconds=2, milliseconds=100),
                "EMPTY-GAP",
                9,
                200,
                9,
                "CampusSecure",
                "gap-radio",
            ),
            (
                start + pd.Timedelta(milliseconds=100),
                "NON-HIGH",
                5,
                200,
                5,
                "CampusSecure",
                "reset-radio",
            ),
            (
                start + pd.Timedelta(seconds=1, milliseconds=100),
                "NON-HIGH",
                6,
                100,
                6,
                "CampusSecure",
                "reset-radio",
            ),
            (
                start + pd.Timedelta(seconds=2, milliseconds=100),
                "NON-HIGH",
                7,
                200,
                7,
                "CampusSecure",
                "reset-radio",
            ),
        ]
    )

    result_df = _run_class_session_metrics(
        monkeypatch,
        tmp_path,
        sessions_df,
        beacons_df,
    )

    empty_gap = result_df.loc["EMPTY-GAP"]
    assert empty_gap["duration_75"] == 2
    assert empty_gap["duration_50"] == 2
    assert empty_gap["max_streak_75"] == 2
    assert empty_gap["max_streak_50"] == 2
    assert empty_gap["sum_scount_75"] == 5
    assert empty_gap["sum_scount_50"] == 5

    non_high = result_df.loc["NON-HIGH"]
    assert non_high["duration_75"] == 2
    assert non_high["duration_50"] == 2
    assert non_high["max_streak_75"] == 1
    assert non_high["max_streak_50"] == 1


def test_class_session_uses_location_wide_any_across_radios(
    monkeypatch,
    tmp_path,
):
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    end = start + pd.Timedelta(seconds=3)
    sessions_df = _sessions(["MULTI-RADIO"], start, end)
    beacons_df = _beacons(
        [
            (
                start + pd.Timedelta(milliseconds=100),
                "MULTI-RADIO",
                10,
                200,
                4,
                "CampusSecure",
                "radio-a",
            ),
            (
                start + pd.Timedelta(milliseconds=200),
                "MULTI-RADIO",
                11,
                100,
                6,
                "CampusSecure",
                "radio-b",
            ),
            (
                start + pd.Timedelta(seconds=1, milliseconds=100),
                "MULTI-RADIO",
                12,
                100,
                5,
                "CampusSecure",
                "radio-a",
            ),
            (
                start + pd.Timedelta(seconds=1, milliseconds=200),
                "MULTI-RADIO",
                13,
                200,
                6,
                "CampusSecure",
                "radio-b",
            ),
            (
                start + pd.Timedelta(milliseconds=300),
                "MULTI-RADIO",
                9,
                100,
                8,
                "OtherSSID",
                "radio-c",
            ),
        ]
    )

    result_df = _run_class_session_metrics(
        monkeypatch,
        tmp_path,
        sessions_df,
        beacons_df,
    )
    session = result_df.loc["MULTI-RADIO"]

    # Current behavior uses location-wide any(), so alternating high radios
    # form one two-bin streak. The per-radio redesign is intentionally deferred.
    assert session["duration_75"] == 2
    assert session["duration_50"] == 2
    assert session["max_streak_75"] == 2
    assert session["max_streak_50"] == 2
    assert session["sum_scount_75"] == 10
    assert session["sum_scount_50"] == 10
    assert session["number_radios"] == 2


def test_class_session_missing_cu_and_zero_enrollment(monkeypatch, tmp_path):
    start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    end = start + pd.Timedelta(seconds=3)
    sessions_df = _sessions(
        ["MISSING-CU", "ZERO-ENROLLED"],
        start,
        end,
        enrolled=[10, 0],
    )
    beacons_df = _beacons(
        [
            (
                start + pd.Timedelta(milliseconds=100),
                "MISSING-CU",
                5,
                None,
                7,
                "CampusSecure",
                "missing-radio",
            ),
            (
                start + pd.Timedelta(milliseconds=100),
                "ZERO-ENROLLED",
                6,
                200,
                6,
                "CampusSecure",
                "zero-radio",
            ),
        ]
    )

    result_df = _run_class_session_metrics(
        monkeypatch,
        tmp_path,
        sessions_df,
        beacons_df,
    )

    missing_cu = result_df.loc["MISSING-CU"]
    assert missing_cu["number_beacons"] == 1
    assert missing_cu["max_stations"] == 5
    assert pd.isna(missing_cu["max_cu"])
    assert pd.isna(missing_cu["average_cu"])
    assert pd.isna(missing_cu["median_cu"])
    assert missing_cu["duration_75"] == 0
    assert missing_cu["duration_50"] == 0
    assert missing_cu["max_streak_75"] == 0
    assert missing_cu["max_streak_50"] == 0
    assert missing_cu["sum_scount_75"] == 0
    assert missing_cu["sum_scount_50"] == 0
    assert missing_cu["number_radios"] == 1

    zero_enrolled = result_df.loc["ZERO-ENROLLED"]
    assert zero_enrolled["max_stations"] == 6
    assert zero_enrolled["stations_enrolled"] == 0
    assert zero_enrolled["duration_75"] == 1
    assert zero_enrolled["duration_50"] == 1
