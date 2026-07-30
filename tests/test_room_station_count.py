from __future__ import annotations

from pathlib import Path

import pandas as pd

from script_sections import script_cell_source


CONFIG_ENV_KEYS = (
    "DATA_FOLDER",
    "OUTPUT_FOLDER",
    "SAMPLE_FOLDER",
    "ALL_CLASS_BEACONS_FOLDER",
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


def _run_station_count_cell(
    monkeypatch,
    tmp_path,
    beacons_df,
    sample_aps_df,
):
    for key in CONFIG_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)

    data_dir = tmp_path / "Data"
    output_dir = tmp_path / "Output"
    sample_dir = tmp_path / "Sample"
    all_class_beacons_dir = output_dir / "all_class_beacons"
    data_dir.mkdir()
    output_dir.mkdir()
    sample_dir.mkdir()

    _write_config_env(
        tmp_path / "config.env",
        {
            "DATA_FOLDER": _env_path(data_dir),
            "OUTPUT_FOLDER": _env_path(output_dir),
            "SAMPLE_FOLDER": _env_path(sample_dir),
            "ALL_CLASS_BEACONS_FOLDER": _env_path(
                all_class_beacons_dir
            ),
            "SSID1": "CampusSecure",
            "SSID2": "CampusGuest",
        },
    )
    beacons_df.to_parquet(data_dir / "all_class_beacons.parquet")
    sample_aps_df.to_parquet(sample_dir / "sample_aps.parquet")

    cell1, script_path = script_cell_source(
        "analyze_all_class_beacons.py",
        1,
    )
    cell2, _ = script_cell_source("analyze_all_class_beacons.py", 2)
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

    written_df = pd.read_parquet(
        data_dir / "all_class_beacons_processed.parquet"
    )
    return namespace["all_class_beacons_df"], written_df


def test_room_station_count_current_multi_ap_behavior(
    monkeypatch,
    tmp_path,
):
    base = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")
    rows = [
        (12, "AP-B", "tie", 9.9, "tie-later"),
        (10, "UNKNOWN", "unknown", 99.9, "unknown-ap"),
        (10, "AP-A", "current", 10.9, "target"),
        (8, "AP-B", "tie", 7.6, "tie-earlier"),
        (15, "AP-B", "edge-plus", 5.5, "plus-five"),
        (5, "AP-B", "edge-minus", 4.4, "minus-five"),
        (16, "AP-B", "outside-plus", 100.1, "plus-six"),
        (4, "AP-B", "outside-minus", 200.1, "minus-six"),
        (6, "AP-B", "nearest", 3.3, "nearest-far"),
        (9, "AP-B", "nearest", 6.8, "nearest-close"),
        (11, "AP-B", "current", 50.9, "current-duplicate"),
        (10, "NO-LOCATION", "no-location", 88.8, "no-location"),
    ]
    beacons_df = pd.DataFrame(
        {
            "aruba_erm.time": [
                base + pd.Timedelta(seconds=offset)
                for offset, _, _, _, _ in rows
            ],
            "wlan.vs.aruba.ap_name": [
                hostname for _, hostname, _, _, _ in rows
            ],
            "wlan.ta": [
                transmitter for _, _, transmitter, _, _ in rows
            ],
            "wlan.qbss.scount": [
                station_count for _, _, _, station_count, _ in rows
            ],
            "source_row": [
                source_row for _, _, _, _, source_row in rows
            ],
        }
    )
    original_df = beacons_df.copy(deep=True)
    sample_aps_df = pd.DataFrame(
        {
            "Hostname": ["AP-A", "AP-B", "AP-NO-DATA", "NO-LOCATION"],
            "Building-Room": ["BLDG-101", "BLDG-101", "BLDG-101", None],
        }
    )

    result_df, written_df = _run_station_count_cell(
        monkeypatch,
        tmp_path,
        beacons_df,
        sample_aps_df,
    )

    # The entry row includes exact +/-5-second observations, selects the
    # nearest duplicate transmitter, keeps the first equal-distance tie,
    # excludes observations outside the window, and does not add its own TA.
    target = result_df[result_df["source_row"] == "target"].iloc[0]
    assert target["room_scount"] == 35
    assert target["location"] == "BLDG-101"

    # Unknown APs and APs without a location keep the current zero default.
    unknown = result_df[result_df["source_row"] == "unknown-ap"].iloc[0]
    no_location = result_df[
        result_df["source_row"] == "no-location"
    ].iloc[0]
    assert unknown["room_scount"] == 0
    assert pd.isna(unknown["location"])
    assert no_location["room_scount"] == 0
    assert pd.isna(no_location["location"])

    assert result_df["room_scount"].dtype == "int64"
    assert result_df.index.equals(pd.RangeIndex(len(result_df)))
    assert result_df["aruba_erm.time"].is_monotonic_increasing
    assert result_df.columns.tolist() == [
        *original_df.columns,
        "room_scount",
        "location",
    ]
    pd.testing.assert_frame_equal(
        result_df.convert_dtypes(),
        written_df.convert_dtypes(),
    )
    pd.testing.assert_frame_equal(beacons_df, original_df)
